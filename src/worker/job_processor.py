import asyncio
import json
import logging
import os
import signal
import sys
import time
import traceback
from dataclasses import dataclass
from enum import Enum
from typing import Any, BinaryIO

from pydantic import ValidationError

from src.common.metrics import metrics
from src.config import JOB_TIMEOUT_SECONDS, MAX_CONCURRENT_TASKS, PROJECT_ROOT
from src.core import job_queue
from src.core import state_manager as state
from src.models.job import (
    WORKER_RESULT_ADAPTER,
    JobError,
    JobErrorCode,
    JobStatus,
    QueuedJob,
    WorkerCompleted,
    WorkerResult,
)

logger = logging.getLogger(__name__)

JOB_RUNNER_PATH = os.path.join(PROJECT_ROOT, "src", "worker", "job_runner.py")
RESULT_FILENAME = "result.json"
STDOUT_LOG_FILENAME = "stdout.log"
STDERR_LOG_FILENAME = "stderr.log"
LOG_READ_CHUNK_BYTES = 64 * 1024
LOG_TAIL_BYTES = 64 * 1024
LOG_DRAIN_TIMEOUT_SECONDS = 5.0

_workers: list[asyncio.Task[None]] = []
_running_job_tasks: dict[str, asyncio.Task[None]] = {}


class ResultFileState(str, Enum):
    LOADED = "LOADED"
    MISSING = "MISSING"
    INVALID = "INVALID"


@dataclass(frozen=True)
class ResultFileRead:
    state: ResultFileState
    result: WorkerResult | None = None
    message: str = ""


@dataclass(frozen=True)
class JobExecutionOutcome:
    status: JobStatus
    result: Any


@dataclass(frozen=True)
class WorkerProcessContext:
    process: asyncio.subprocess.Process
    stdout_task: asyncio.Task[str]
    stderr_task: asyncio.Task[str]


@dataclass(frozen=True)
class WorkerProcessOutput:
    return_code: int | None
    timed_out: bool
    stdout_tail: str
    stderr_tail: str


async def _run_file_operation(function, *args):
    operation = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError as cancellation:
        try:
            await operation
        except Exception:
            logger.exception("File operation failed while job cancellation was pending.")
        raise cancellation


async def _open_file(*args, **kwargs):
    operation = asyncio.create_task(asyncio.to_thread(open, *args, **kwargs))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError as cancellation:
        try:
            opened_file = await operation
        except Exception:
            logger.exception("File open failed while job cancellation was pending.")
        else:
            try:
                await asyncio.to_thread(opened_file.close)
            except Exception:
                logger.exception("Failed to close file opened during job cancellation.")
        raise cancellation


def _load_result_file(result_path: str) -> object:
    with open(result_path, encoding="utf-8") as result_file:
        return json.load(result_file)


async def _read_result_file(job_path: str) -> ResultFileRead:
    result_path = os.path.join(job_path, RESULT_FILENAME)
    try:
        payload = await _run_file_operation(_load_result_file, result_path)
        worker_result = WORKER_RESULT_ADAPTER.validate_python(payload)
    except FileNotFoundError:
        return ResultFileRead(ResultFileState.MISSING)
    except (OSError, json.JSONDecodeError, ValidationError) as exception:
        logger.error(
            "Failed to read a valid %s from %s: %s",
            RESULT_FILENAME,
            job_path,
            exception,
        )
        return ResultFileRead(ResultFileState.INVALID, message=str(exception))

    return ResultFileRead(ResultFileState.LOADED, result=worker_result)


def _write_log_chunk(log_file: BinaryIO, chunk: bytes) -> None:
    log_file.write(chunk)
    log_file.flush()


async def _stream_output_to_log(stream: asyncio.StreamReader, path: str) -> str:
    tail = bytearray()
    log_file = await _open_file(path, "wb")
    try:
        while chunk := await stream.read(LOG_READ_CHUNK_BYTES):
            await _run_file_operation(_write_log_chunk, log_file, chunk)
            tail.extend(chunk)
            if len(tail) > LOG_TAIL_BYTES:
                del tail[:-LOG_TAIL_BYTES]
    except BaseException:
        try:
            await _run_file_operation(log_file.close)
        except Exception:
            logger.exception("Failed to close a job log while another error was pending.")
        raise
    else:
        await _run_file_operation(log_file.close)

    return tail.decode("utf-8", errors="replace")


async def _terminate_process(
    process: asyncio.subprocess.Process,
    job_id: str,
    force: bool = False,
) -> None:
    signal_to_send = signal.SIGKILL if force else signal.SIGTERM
    logger.warning("Terminating subprocess group for job %s with %s.", job_id, signal_to_send.name)

    try:
        if os.name == "posix":
            os.killpg(process.pid, signal_to_send)
        elif process.returncode is None:
            process.kill() if force else process.terminate()
    except ProcessLookupError:
        return

    if process.returncode is not None:
        return

    try:
        await asyncio.wait_for(process.wait(), timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning("Job %s did not exit after SIGTERM; killing its subprocess group.", job_id)
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        except ProcessLookupError:
            return
        await process.wait()


async def _drain_output_tasks(
    stdout_task: asyncio.Task[str],
    stderr_task: asyncio.Task[str],
    process: asyncio.subprocess.Process,
    job_id: str,
) -> tuple[str, str]:
    output_tasks = asyncio.gather(stdout_task, stderr_task)
    try:
        output = await asyncio.wait_for(
            asyncio.shield(output_tasks),
            timeout=LOG_DRAIN_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        logger.warning("Output pipes for job %s did not close; killing its process group.", job_id)
        await _terminate_process(process, job_id, force=True)
        output = await asyncio.wait_for(output_tasks, timeout=LOG_DRAIN_TIMEOUT_SECONDS)
    return tuple(output)


def _timeout_error(
    result_file: ResultFileRead,
    stdout_tail: str,
    stderr_tail: str,
) -> JobError:
    loaded_result = result_file.result if result_file.state == ResultFileState.LOADED else None
    return JobError(
        code=JobErrorCode.WORKER_TIMED_OUT,
        message="Worker exceeded its execution timeout",
        stdout=stdout_tail,
        stderr=stderr_tail,
        timeout_seconds=JOB_TIMEOUT_SECONDS,
        worker_result=(
            loaded_result.result if isinstance(loaded_result, WorkerCompleted) else None
        ),
        worker_error=(
            loaded_result.error
            if loaded_result is not None and not isinstance(loaded_result, WorkerCompleted)
            else None
        ),
    )


async def _start_worker_process(
    script_path: str,
    job_id: str,
) -> WorkerProcessContext:
    job_path = os.path.dirname(script_path)
    worker_command = [
        sys.executable,
        "-u",
        JOB_RUNNER_PATH,
        job_id,
        script_path,
        job_path,
    ]
    subprocess_options: dict[str, Any] = {
        "cwd": job_path,
        "stdout": asyncio.subprocess.PIPE,
        "stderr": asyncio.subprocess.PIPE,
    }
    if os.name == "posix":
        subprocess_options["start_new_session"] = True

    process = await asyncio.create_subprocess_exec(
        *worker_command,
        **subprocess_options,
    )
    if process.stdout is None or process.stderr is None:
        await _terminate_process(process, job_id, force=True)
        raise RuntimeError("Worker subprocess pipes were not created")

    return WorkerProcessContext(
        process=process,
        stdout_task=asyncio.create_task(
            _stream_output_to_log(
                process.stdout,
                os.path.join(job_path, STDOUT_LOG_FILENAME),
            )
        ),
        stderr_task=asyncio.create_task(
            _stream_output_to_log(
                process.stderr,
                os.path.join(job_path, STDERR_LOG_FILENAME),
            )
        ),
    )


async def _wait_for_worker_process(
    process_context: WorkerProcessContext,
    job_id: str,
) -> WorkerProcessOutput:
    process = process_context.process
    timed_out = False
    try:
        try:
            await asyncio.wait_for(process.wait(), timeout=JOB_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            timed_out = True
            await _terminate_process(process, job_id)
        except asyncio.CancelledError:
            await _terminate_process(process, job_id)
            raise

        stdout_tail, stderr_tail = await _drain_output_tasks(
            process_context.stdout_task,
            process_context.stderr_task,
            process,
            job_id,
        )
    finally:
        # A descendant can outlive the direct runner while still holding the process group.
        await _terminate_process(process, job_id, force=True)

    return WorkerProcessOutput(
        return_code=process.returncode,
        timed_out=timed_out,
        stdout_tail=stdout_tail,
        stderr_tail=stderr_tail,
    )


def _failed_job_outcome(job_error: JobError) -> JobExecutionOutcome:
    return JobExecutionOutcome(status=JobStatus.FAILED, result=job_error)


async def _resolve_worker_outcome(
    job_path: str,
    return_code: int | None,
    timed_out: bool,
    stdout_tail: str,
    stderr_tail: str,
) -> JobExecutionOutcome:
    if timed_out:
        result_file = await _read_result_file(job_path)
        return _failed_job_outcome(_timeout_error(result_file, stdout_tail, stderr_tail))

    if return_code not in (None, 0):
        return _failed_job_outcome(
            JobError(
                code=JobErrorCode.WORKER_EXITED,
                message="Worker process exited unsuccessfully",
                stdout=stdout_tail,
                stderr=stderr_tail,
                exit_code=return_code,
            )
        )

    result_file = await _read_result_file(job_path)
    if result_file.state == ResultFileState.MISSING:
        return _failed_job_outcome(
            JobError(
                code=JobErrorCode.WORKER_RESULT_MISSING,
                message="Worker did not produce a result file",
                stdout=stdout_tail,
                stderr=stderr_tail,
            )
        )
    if result_file.state == ResultFileState.INVALID:
        return _failed_job_outcome(
            JobError(
                code=JobErrorCode.WORKER_RESULT_INVALID,
                message=result_file.message,
                stdout=stdout_tail,
                stderr=stderr_tail,
            )
        )

    worker_result = result_file.result
    if isinstance(worker_result, WorkerCompleted):
        return JobExecutionOutcome(JobStatus.COMPLETED, worker_result.result)
    if worker_result is None:
        raise RuntimeError("Loaded worker result is unexpectedly empty")
    return _failed_job_outcome(worker_result.error)


async def _complete_worker_process(
    process_context: WorkerProcessContext,
    job_path: str,
    job_id: str,
) -> tuple[JobExecutionOutcome, dict[str, str]]:
    process_output = await _wait_for_worker_process(process_context, job_id)
    process_logs = {
        "stdout": process_output.stdout_tail,
        "stderr": process_output.stderr_tail,
    }
    if process_output.stderr_tail:
        logger.warning("Job %s wrote to stderr: %s", job_id, process_output.stderr_tail)

    final_outcome = await _resolve_worker_outcome(
        job_path,
        process_output.return_code,
        process_output.timed_out,
        process_output.stdout_tail,
        process_output.stderr_tail,
    )
    return final_outcome, process_logs


async def _collect_cancelled_process_logs(
    process_context: WorkerProcessContext,
    job_id: str,
) -> tuple[dict[str, str], str, str]:
    stdout_tail, stderr_tail = await _drain_output_tasks(
        process_context.stdout_task,
        process_context.stderr_task,
        process_context.process,
        job_id,
    )
    return {"stdout": stdout_tail, "stderr": stderr_tail}, stdout_tail, stderr_tail


def _cancelled_job_outcome(stdout_tail: str, stderr_tail: str) -> JobExecutionOutcome:
    return JobExecutionOutcome(
        status=JobStatus.CANCELLED,
        result=JobError(
            code=JobErrorCode.JOB_CANCELLED,
            message="Job was cancelled",
            stdout=stdout_tail,
            stderr=stderr_tail,
        ),
    )


def _processing_failure_outcome(
    exception: Exception,
    jobname: str,
    job_id: str,
    process_logs: dict[str, str] | None,
) -> JobExecutionOutcome:
    logger.error(
        "System error while processing job %s (%s): %s",
        jobname,
        job_id,
        exception,
        exc_info=True,
    )
    stdout_tail = process_logs["stdout"] if process_logs is not None else ""
    stderr_tail = process_logs["stderr"] if process_logs is not None else ""
    return _failed_job_outcome(
        JobError(
            code=JobErrorCode.PROCESSING_FAILED,
            message=str(exception),
            traceback=traceback.format_exc(),
            stdout=stdout_tail,
            stderr=stderr_tail,
        )
    )


async def _cancelled_process_outcome(
    process_context: WorkerProcessContext | None,
    job_id: str,
) -> tuple[JobExecutionOutcome, dict[str, str] | None]:
    if process_context is None:
        return _cancelled_job_outcome("", ""), None

    process_logs, stdout_tail, stderr_tail = await _collect_cancelled_process_logs(
        process_context,
        job_id,
    )
    return _cancelled_job_outcome(stdout_tail, stderr_tail), process_logs


async def _persist_job_outcome(
    *,
    job_id: str,
    jobname: str,
    final_outcome: JobExecutionOutcome,
    process_logs: dict[str, str] | None,
    started_at: float,
) -> None:
    metrics.active_jobs.dec()
    duration_seconds = time.time() - started_at
    logger.info(
        "Job %s (%s) finished with status %s in %.2fs.",
        jobname,
        job_id,
        final_outcome.status.value,
        duration_seconds,
    )

    if final_outcome.status == JobStatus.COMPLETED:
        metrics.jobs_completed.inc()
    elif final_outcome.status == JobStatus.FAILED:
        metrics.jobs_failed.inc()

    await state.update_job_status(
        job_id,
        final_outcome.status,
        final_outcome.result,
        duration_seconds,
        process_logs,
    )


async def _process_job_internal(script_path: str, jobname: str, job_id: str) -> None:
    """Run one job in an isolated subprocess and persist its terminal outcome."""
    metrics.active_jobs.inc()
    started_at = time.time()
    logger.info("Starting job %s (%s) in a subprocess.", jobname, job_id)

    job_path = os.path.dirname(script_path)
    final_outcome = _failed_job_outcome(
        JobError(
            code=JobErrorCode.PROCESSING_FAILED,
            message="Worker processing ended without a terminal outcome",
        )
    )
    process_logs: dict[str, str] | None = None
    process_context: WorkerProcessContext | None = None

    try:
        await state.update_job_status(job_id, JobStatus.RUNNING)
        process_context = await _start_worker_process(script_path, job_id)
        final_outcome, process_logs = await _complete_worker_process(
            process_context,
            job_path,
            job_id,
        )
    except asyncio.CancelledError:
        final_outcome, process_logs = await _cancelled_process_outcome(
            process_context,
            job_id,
        )
        raise
    except Exception as exception:
        final_outcome = _processing_failure_outcome(
            exception,
            jobname,
            job_id,
            process_logs,
        )
    finally:
        await _persist_job_outcome(
            job_id=job_id,
            jobname=jobname,
            final_outcome=final_outcome,
            process_logs=process_logs,
            started_at=started_at,
        )


async def _dispatch_job(job: QueuedJob) -> None:
    """Track and await one queued job so API cancellation can find it."""
    process_task = asyncio.create_task(
        _process_job_internal(job.script_path, job.jobname, job.job_id),
        name=f"JobProcess-{job.job_id}",
    )
    _running_job_tasks[job.job_id] = process_task
    try:
        await process_task
    except asyncio.CancelledError:
        logger.info("Job %s (%s) was cancelled.", job.jobname, job.job_id)
        current_task = asyncio.current_task()
        if current_task is not None and current_task.cancelling():
            raise
    except Exception as exception:
        logger.exception("Critical dispatch error for job %s.", job.jobname)
        await state.update_job_status(
            job.job_id,
            JobStatus.FAILED,
            JobError(code=JobErrorCode.DISPATCH_FAILED, message=str(exception)),
        )
    finally:
        _running_job_tasks.pop(job.job_id, None)


async def cancel_running_job(job_id: str) -> bool:
    task = _running_job_tasks.get(job_id)
    if task is None or task.done() or not task.cancel():
        return False
    await asyncio.gather(task, return_exceptions=True)
    return True


def is_job_running(job_id: str) -> bool:
    task = _running_job_tasks.get(job_id)
    return task is not None and not task.done()


async def _worker() -> None:
    worker_name = asyncio.current_task().get_name()
    logger.info("Worker task started: %s", worker_name)

    while True:
        queued_job = await job_queue.get_job()
        if queued_job is None:
            job_queue.task_done()
            break
        if not job_queue.claim_job(queued_job.job_id):
            job_queue.task_done()
            continue

        try:
            await _dispatch_job(queued_job)
        except Exception:
            logger.exception("Unhandled exception in worker loop for job %s.", queued_job.job_id)
        finally:
            job_queue.release_job(queued_job.job_id)
            job_queue.task_done()

    logger.info("Worker task stopped: %s", worker_name)


def start_workers() -> None:
    global _workers

    if MAX_CONCURRENT_TASKS < 1:
        raise ValueError("MAX_CONCURRENT_TASKS must be at least 1")
    if _workers:
        return

    _workers = [
        asyncio.create_task(_worker(), name=f"Worker-{worker_number}")
        for worker_number in range(1, MAX_CONCURRENT_TASKS + 1)
    ]
    logger.info("Started %d workers.", len(_workers))


async def stop_workers(drain: bool = True) -> None:
    global _workers

    if not _workers:
        return

    if not drain:
        logger.error("Cancelling workers immediately because the shared browser is unavailable.")
        for worker_task in _workers:
            worker_task.cancel()
        await asyncio.gather(*_workers, return_exceptions=True)
        _workers = []
        return

    await job_queue.put_shutdown_signal(len(_workers))
    try:
        await job_queue.join(timeout=30.0)
    except asyncio.TimeoutError:
        logger.error("Worker queue did not drain within 30 seconds; cancelling workers.")
        for worker_task in _workers:
            worker_task.cancel()
    finally:
        await asyncio.gather(*_workers, return_exceptions=True)
        _workers = []
