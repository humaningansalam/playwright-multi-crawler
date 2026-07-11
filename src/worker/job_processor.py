import asyncio
import logging
import time
import os
import sys
import json
import signal
import traceback
from typing import Dict, List, Any, Optional

from src.core import state_manager as state
from src.core import job_queue
from src.config import MAX_CONCURRENT_TASKS, PROJECT_ROOT, JOB_TIMEOUT_SECONDS
from src.common.metrics import metrics
from src.models.job import JobStatus

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


def _build_fallback_result(stdout: str, stderr: str, error: str, timeout_seconds: Optional[int] = None) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "error": error,
        "stdout": stdout,
        "stderr": stderr,
    }
    if timeout_seconds is not None:
        result["timeout_seconds"] = timeout_seconds
    return result


async def _read_result_file(job_path: str) -> Optional[Dict[str, Any]]:
    result_path = os.path.join(job_path, RESULT_FILENAME)
    if not os.path.exists(result_path):
        return None
    try:
        with open(result_path, "r", encoding="utf-8") as result_file:
            return json.load(result_file)
    except Exception as e:
        logging.error(f"Failed to read {RESULT_FILENAME} in {job_path}: {e}")
        return None


async def _stream_output_to_log(stream: asyncio.StreamReader, path: str) -> str:
    tail = bytearray()
    with open(path, "wb") as log_file:
        while chunk := await stream.read(LOG_READ_CHUNK_BYTES):
            log_file.write(chunk)
            log_file.flush()
            tail.extend(chunk)
            if len(tail) > LOG_TAIL_BYTES:
                del tail[:-LOG_TAIL_BYTES]
    return tail.decode("utf-8", errors="replace").strip()


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
    
    await state.update_job_status(job_id, JobStatus.RUNNING)
    
    job_path = os.path.dirname(script_path)
    
    # 실행할 명령어: python src/core/job_runner.py <id> <script> <path>
    cmd = [
        sys.executable, 
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
        # 서브프로세스 실행 (비동기)
        subprocess_options = {
            "cwd": job_path,
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
        }
        if os.name == "posix":
            subprocess_options["start_new_session"] = True
        proc = await asyncio.create_subprocess_exec(*cmd, **subprocess_options)

        stdout_stream = getattr(proc, "stdout", None)
        stderr_stream = getattr(proc, "stderr", None)
        if stdout_stream is not None and stderr_stream is not None:
            stdout_task = asyncio.create_task(
                _stream_output_to_log(stdout_stream, os.path.join(job_path, STDOUT_LOG_FILENAME))
            )
            stderr_task = asyncio.create_task(
                _stream_output_to_log(stderr_stream, os.path.join(job_path, STDERR_LOG_FILENAME))
            )
            try:
                await asyncio.wait_for(proc.wait(), timeout=JOB_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                timed_out = True
                await _terminate_process(proc, job_id)
            stdout_decoded, stderr_decoded = await _drain_output_tasks(
                stdout_task, stderr_task, proc, job_id
            )
        else:
            # Compatibility path for test doubles that do not expose stream readers.
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=JOB_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                timed_out = True
                await _terminate_process(proc, job_id)
                stdout, stderr = await proc.communicate()
            stdout_decoded = stdout.decode().strip()
            stderr_decoded = stderr.decode().strip()

        logs = {"stdout": stdout_decoded, "stderr": stderr_decoded}

        if stderr_decoded:
            logging.warning(f"Job {job_id} stderr: {stderr_decoded}")

        output_json = await _read_result_file(job_path)
        if timed_out:
            final_status = JobStatus.FAILED
            result_data = _build_fallback_result(
                stdout_decoded,
                stderr_decoded,
                "timeout",
                timeout_seconds=JOB_TIMEOUT_SECONDS
            )
            if output_json is not None:
                result_data["worker_result"] = output_json.get("result")
                result_data["worker_error"] = output_json.get("error")
        elif output_json is None:
            result_data = _build_fallback_result(
                stdout_decoded,
                stderr_decoded,
                "Worker output missing or invalid"
            )
        else:
            final_status = JobStatus(output_json.get("status", JobStatus.FAILED))
            result_data = output_json.get("result")
            error_info = output_json.get("error")

            if final_status == JobStatus.FAILED:
                result_data = error_info

        if final_status == JobStatus.COMPLETED:
            metrics.jobs_completed.inc()
        else:
            metrics.jobs_failed.inc()

    except asyncio.CancelledError:
        if proc is not None:
            await _terminate_process(proc, job_id)
        if stdout_task is not None and stderr_task is not None:
            stdout_decoded, stderr_decoded = await _drain_output_tasks(
                stdout_task, stderr_task, proc, job_id
            )
            logs = {"stdout": stdout_decoded, "stderr": stderr_decoded}
        final_status = JobStatus.CANCELLED
        result_data = {"error": "cancelled"}
        raise
    except Exception as e:
        logging.error(f"System error processing job '{jobname}' (ID: {job_id}): {e}")
        logging.error(traceback.format_exc())
        result_data = _build_fallback_result(
            stdout_decoded,
            stderr_decoded,
            str(e)
        )
    
    finally:
        metrics.active_jobs.dec()
        metrics.queued_jobs.set(job_queue.qsize())

        end_time = time.time()
        duration = end_time - start_time
        logging.info(f"Job '{jobname}' (ID: {job_id}) finished with status {final_status} in {duration:.2f}s")

        await state.update_job_status(job_id, final_status, result_data, duration, logs)
        await state.remove_submitted_job(jobname)

async def _dispatch_job(job: Dict[str, Any]):
    """큐에서 작업을 받아 서브프로세스 실행"""
    script_path = job['script_path']
    jobname = job['jobname']
    job_id = job['job_id']

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
    except Exception as e:
        logging.error(f"Critical dispatch error for job '{jobname}': {e}")
        await state.update_job_status(job_id, JobStatus.FAILED, {'error': str(e)})
        await state.remove_submitted_job(jobname)
    finally:
        _running_job_tasks.pop(job_id, None)


def cancel_running_job(job_id: str) -> bool:
    task = _running_job_tasks.get(job_id)
    if task is None or task.done():
        return False
    task.cancel()
    return True

async def _worker():
    """큐에서 작업을 가져와 처리"""
    logging.info(f"Worker task started: {asyncio.current_task().get_name()}")
    while True:
        job = await job_queue.get_job()
        if job is None:
            job_queue.task_done()
            break
        if not job_queue.claim_job(job["job_id"]):
            await state.remove_submitted_job(job["jobname"])
            job_queue.task_done()
            continue
        try:
            await _dispatch_job(job)
        except Exception as e:
            logging.error(f"Unhandled exception in worker loop: {e}", exc_info=True)
        finally:
            job_queue.release_job(job["job_id"])
            job_queue.task_done()
    logging.info(f"Worker task stopped: {asyncio.current_task().get_name()}")

def start_workers():
    global _workers
    if _workers: return
    _workers = [asyncio.create_task(_worker(), name=f"Worker-{i+1}") for i in range(MAX_CONCURRENT_TASKS)]
    logging.info(f"Started {len(_workers)} workers.")

async def stop_workers():
    global _workers
    if not _workers:
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
