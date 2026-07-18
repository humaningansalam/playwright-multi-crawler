import asyncio
import logging
import time
import os
import sys
import json
import signal
import traceback
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

from pydantic import ValidationError

from src.core import state_manager as state
from src.core import job_queue
from src.config import MAX_CONCURRENT_TASKS, PROJECT_ROOT, JOB_TIMEOUT_SECONDS
from src.common.metrics import metrics
from src.models.job import (
    JobError,
    JobErrorCode,
    JobStatus,
    QueuedJob,
    WORKER_RESULT_ADAPTER,
    WorkerCompleted,
    WorkerResult,
)

# 워커 스크립트의 절대 경로
JOB_RUNNER_PATH = os.path.join(PROJECT_ROOT, "src", "worker", "job_runner.py")
RESULT_FILENAME = "result.json"
STDOUT_LOG_FILENAME = "stdout.log"
STDERR_LOG_FILENAME = "stderr.log"
LOG_READ_CHUNK_BYTES = 64 * 1024
LOG_TAIL_BYTES = 64 * 1024
LOG_DRAIN_TIMEOUT_SECONDS = 5.0

_workers: List[asyncio.Task] = []
_running_job_tasks: Dict[str, asyncio.Task] = {}


class ResultFileState(str, Enum):
    LOADED = "LOADED"
    MISSING = "MISSING"
    INVALID = "INVALID"


@dataclass(frozen=True)
class ResultFileRead:
    state: ResultFileState
    result: WorkerResult | None = None
    message: str = ""


async def _run_file_operation(function, *args):
    operation = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError as cancellation:
        try:
            await operation
        except Exception:
            logging.exception("File operation failed while job cancellation was pending.")
        raise cancellation


async def _open_file(*args, **kwargs):
    operation = asyncio.create_task(asyncio.to_thread(open, *args, **kwargs))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError as cancellation:
        try:
            opened_file = await operation
        except Exception:
            logging.exception("File open failed while job cancellation was pending.")
        else:
            try:
                await asyncio.to_thread(opened_file.close)
            except Exception:
                logging.exception("Failed to close file opened during job cancellation.")
        raise cancellation


def _load_result_file(result_path: str) -> object:
    with open(result_path, "r", encoding="utf-8") as result_file:
        return json.load(result_file)


async def _read_result_file(job_path: str) -> ResultFileRead:
    result_path = os.path.join(job_path, RESULT_FILENAME)
    try:
        payload = await _run_file_operation(_load_result_file, result_path)
        return ResultFileRead(
            ResultFileState.LOADED,
            result=WORKER_RESULT_ADAPTER.validate_python(payload),
        )
    except FileNotFoundError:
        return ResultFileRead(ResultFileState.MISSING)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        logging.error("Failed to read valid %s in %s: %s", RESULT_FILENAME, job_path, exc)
        return ResultFileRead(ResultFileState.INVALID, message=str(exc))


def _write_log_chunk(log_file, chunk: bytes) -> None:
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
            logging.exception("Failed to close job log while another error was pending.")
        raise
    else:
        await _run_file_operation(log_file.close)
    return tail.decode("utf-8", errors="replace")


async def _terminate_process(proc: asyncio.subprocess.Process, job_id: str, force: bool = False) -> None:
    logging.warning(f"Terminating job subprocess group for {job_id}.")
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGKILL if force else signal.SIGTERM)
        else:
            if proc.returncode is not None:
                return
            if force:
                proc.kill()
            else:
                proc.terminate()
    except ProcessLookupError:
        return

    if proc.returncode is not None:
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=5.0)
    except asyncio.TimeoutError:
        logging.warning(f"Job {job_id} did not exit after SIGTERM. Killing subprocess group.")
        try:
            if os.name == "posix":
                os.killpg(proc.pid, signal.SIGKILL)
            else:
                proc.kill()
        except ProcessLookupError:
            return
        await proc.wait()


async def _drain_output_tasks(
    stdout_task: asyncio.Task,
    stderr_task: asyncio.Task,
    proc: asyncio.subprocess.Process,
    job_id: str,
) -> tuple[str, str]:
    output_tasks = asyncio.gather(stdout_task, stderr_task)
    try:
        output = await asyncio.wait_for(asyncio.shield(output_tasks), timeout=LOG_DRAIN_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        logging.warning(f"Job {job_id} output pipes did not close; killing its process group.")
        await _terminate_process(proc, job_id, force=True)
        output = await asyncio.wait_for(output_tasks, timeout=LOG_DRAIN_TIMEOUT_SECONDS)
    return tuple(output)


async def _process_job_internal(script_path: str, jobname: str, job_id: str):
    """서브프로세스를 통해 작업을 격리 실행"""
    metrics.active_jobs.inc()

    start_time = time.time()
    logging.info(f"Starting job '{jobname}' (ID: {job_id}) via subprocess")
    
    job_path = os.path.dirname(script_path)
    
    # 실행할 명령어: python src/core/job_runner.py <id> <script> <path>
    cmd = [
        sys.executable,
        "-u",
        JOB_RUNNER_PATH,
        job_id,
        script_path,
        job_path
    ]

    final_status = JobStatus.FAILED
    result_data = None
    stdout_decoded = ""
    stderr_decoded = ""
    logs: Optional[Dict[str, str]] = None
    timed_out = False
    proc: Optional[asyncio.subprocess.Process] = None
    stdout_task: Optional[asyncio.Task] = None
    stderr_task: Optional[asyncio.Task] = None

    try:
        await state.update_job_status(job_id, JobStatus.RUNNING)

        # 서브프로세스 실행 (비동기)
        subprocess_options = {
            "cwd": job_path,
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
        }
        if os.name == "posix":
            subprocess_options["start_new_session"] = True
        proc = await asyncio.create_subprocess_exec(*cmd, **subprocess_options)

        if proc.stdout is None or proc.stderr is None:
            raise RuntimeError("Worker subprocess pipes were not created")
        stdout_task = asyncio.create_task(
            _stream_output_to_log(proc.stdout, os.path.join(job_path, STDOUT_LOG_FILENAME))
        )
        stderr_task = asyncio.create_task(
            _stream_output_to_log(proc.stderr, os.path.join(job_path, STDERR_LOG_FILENAME))
        )
        try:
            try:
                await asyncio.wait_for(proc.wait(), timeout=JOB_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                timed_out = True
                await _terminate_process(proc, job_id)
            except asyncio.CancelledError:
                await _terminate_process(proc, job_id)
                raise
            stdout_decoded, stderr_decoded = await _drain_output_tasks(
                stdout_task, stderr_task, proc, job_id
            )
        finally:
            # The direct runner may exit while descendants keep its process group alive.
            await _terminate_process(proc, job_id, force=True)

        logs = {"stdout": stdout_decoded, "stderr": stderr_decoded}

        if stderr_decoded:
            logging.warning(f"Job {job_id} stderr: {stderr_decoded}")

        result_file = await _read_result_file(job_path)
        if timed_out:
            final_status = JobStatus.FAILED
            loaded_result = result_file.result if result_file.state == ResultFileState.LOADED else None
            result_data = JobError(
                code=JobErrorCode.WORKER_TIMED_OUT,
                message="Worker exceeded its execution timeout",
                stdout=stdout_decoded,
                stderr=stderr_decoded,
                timeout_seconds=JOB_TIMEOUT_SECONDS,
                worker_result=loaded_result.result if isinstance(loaded_result, WorkerCompleted) else None,
                worker_error=loaded_result.error if loaded_result is not None and not isinstance(loaded_result, WorkerCompleted) else None,
            )
        elif proc.returncode not in (None, 0):
            final_status = JobStatus.FAILED
            result_data = JobError(
                code=JobErrorCode.WORKER_EXITED,
                message="Worker process exited unsuccessfully",
                stdout=stdout_decoded,
                stderr=stderr_decoded,
                exit_code=proc.returncode,
            )
        elif result_file.state == ResultFileState.MISSING:
            final_status = JobStatus.FAILED
            result_data = JobError(
                code=JobErrorCode.WORKER_RESULT_MISSING,
                message="Worker did not produce a result file",
                stdout=stdout_decoded,
                stderr=stderr_decoded,
            )
        elif result_file.state == ResultFileState.INVALID:
            final_status = JobStatus.FAILED
            result_data = JobError(
                code=JobErrorCode.WORKER_RESULT_INVALID,
                message=result_file.message,
                stdout=stdout_decoded,
                stderr=stderr_decoded,
            )
        else:
            worker_result = result_file.result
            if isinstance(worker_result, WorkerCompleted):
                final_status = JobStatus.COMPLETED
                result_data = worker_result.result
            else:
                final_status = JobStatus.FAILED
                result_data = worker_result.error

    except asyncio.CancelledError:
        if stdout_task is not None and stderr_task is not None:
            stdout_decoded, stderr_decoded = await _drain_output_tasks(
                stdout_task, stderr_task, proc, job_id
            )
            logs = {"stdout": stdout_decoded, "stderr": stderr_decoded}
        final_status = JobStatus.CANCELLED
        result_data = JobError(
            code=JobErrorCode.JOB_CANCELLED,
            message="Job was cancelled",
            stdout=stdout_decoded,
            stderr=stderr_decoded,
        )
        raise
    except Exception as e:
        logging.error(f"System error processing job '{jobname}' (ID: {job_id}): {e}")
        logging.error(traceback.format_exc())
        result_data = JobError(
            code=JobErrorCode.PROCESSING_FAILED,
            message=str(e),
            traceback=traceback.format_exc(),
            stdout=stdout_decoded,
            stderr=stderr_decoded,
        )
    
    finally:
        metrics.active_jobs.dec()

        end_time = time.time()
        duration = end_time - start_time
        logging.info(f"Job '{jobname}' (ID: {job_id}) finished with status {final_status} in {duration:.2f}s")

        if final_status == JobStatus.COMPLETED:
            metrics.jobs_completed.inc()
        elif final_status == JobStatus.FAILED:
            metrics.jobs_failed.inc()

        await state.update_job_status(job_id, final_status, result_data, duration, logs)
        await state.remove_submitted_job(jobname)

async def _dispatch_job(job: QueuedJob):
    """큐에서 작업을 받아 서브프로세스 실행"""
    script_path = job.script_path
    jobname = job.jobname
    job_id = job.job_id

    # 기존의 context.new_page() 로직 제거됨.
    # Worker Process가 알아서 CDP로 접속하여 처리함.
    process_task = asyncio.create_task(
        _process_job_internal(script_path, jobname, job_id),
        name=f"JobProcess-{job_id}",
    )
    _running_job_tasks[job_id] = process_task
    try:
        await process_task
    except asyncio.CancelledError:
        logging.info(f"Job '{jobname}' (ID: {job_id}) was cancelled.")
        current_task = asyncio.current_task()
        if current_task is not None and current_task.cancelling():
            raise
    except Exception as e:
        logging.error(f"Critical dispatch error for job '{jobname}': {e}")
        await state.update_job_status(
            job_id,
            JobStatus.FAILED,
            JobError(code=JobErrorCode.DISPATCH_FAILED, message=str(e)),
        )
        await state.remove_submitted_job(jobname)
    finally:
        _running_job_tasks.pop(job_id, None)


async def cancel_running_job(job_id: str) -> bool:
    task = _running_job_tasks.get(job_id)
    if task is None or task.done():
        return False
    if not task.cancel():
        return False
    await asyncio.gather(task, return_exceptions=True)
    return True


def is_job_running(job_id: str) -> bool:
    task = _running_job_tasks.get(job_id)
    return task is not None and not task.done()

async def _worker():
    """큐에서 작업을 가져와 처리"""
    logging.info(f"Worker task started: {asyncio.current_task().get_name()}")
    while True:
        job = await job_queue.get_job()
        if job is None:
            job_queue.task_done()
            break
        if not job_queue.claim_job(job.job_id):
            job_queue.task_done()
            continue
        try:
            await _dispatch_job(job)
        except Exception as e:
            logging.error(f"Unhandled exception in worker loop: {e}", exc_info=True)
        finally:
            job_queue.release_job(job.job_id)
            job_queue.task_done()
    logging.info(f"Worker task stopped: {asyncio.current_task().get_name()}")

def start_workers():
    global _workers
    if MAX_CONCURRENT_TASKS < 1:
        raise ValueError("MAX_CONCURRENT_TASKS must be at least 1")
    if _workers:
        return
    _workers = [asyncio.create_task(_worker(), name=f"Worker-{i+1}") for i in range(MAX_CONCURRENT_TASKS)]
    logging.info(f"Started {len(_workers)} workers.")

async def stop_workers(drain: bool = True):
    global _workers
    if not _workers:
        return
    if not drain:
        logging.error("Cancelling workers immediately because the shared browser is unavailable.")
        for worker in _workers:
            worker.cancel()
        await asyncio.gather(*_workers, return_exceptions=True)
        _workers = []
        return

    await job_queue.put_shutdown_signal(len(_workers))
    try:
        await job_queue.join(timeout=30.0)
    except asyncio.TimeoutError:
        logging.error("Worker queue did not drain within 30 seconds; cancelling workers.")
        for worker in _workers:
            worker.cancel()
    finally:
        await asyncio.gather(*_workers, return_exceptions=True)
        _workers = []
