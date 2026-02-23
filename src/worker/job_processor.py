import asyncio
import logging
import time
import os
import sys
import json
import traceback
from typing import Dict, List, Any, Optional

from src.core import state_manager as state
from src.core import job_queue
from src.config import MAX_CONCURRENT_TASKS, PROJECT_ROOT, JOB_TIMEOUT_SECONDS
from src.common.metrics import metrics

# 워커 스크립트의 절대 경로
JOB_RUNNER_PATH = os.path.join(PROJECT_ROOT, "src", "worker", "job_runner.py")
RESULT_FILENAME = "result.json"

_workers: List[asyncio.Task] = []


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
        with open(result_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to read {RESULT_FILENAME} in {job_path}: {e}")
        return None


async def _terminate_process(proc: asyncio.subprocess.Process, job_id: str) -> None:
    if proc.returncode is not None:
        return
    logging.warning(f"Job {job_id} exceeded timeout. Terminating subprocess.")
    try:
        proc.terminate()
    except ProcessLookupError:
        return

    try:
        await asyncio.wait_for(proc.wait(), timeout=5.0)
    except asyncio.TimeoutError:
        logging.warning(f"Job {job_id} did not exit after SIGTERM. Killing subprocess.")
        try:
            proc.kill()
        except ProcessLookupError:
            return
        await proc.wait()


async def _process_job_internal(script_path: str, jobname: str, job_id: str):
    """서브프로세스를 통해 작업을 격리 실행"""
    metrics.active_jobs.inc()

    start_time = time.time()
    logging.info(f"Starting job '{jobname}' (ID: {job_id}) via subprocess")
    
    await state.update_job_status(job_id, 'RUNNING')
    
    job_path = os.path.dirname(script_path)
    
    # 실행할 명령어: python src/core/job_runner.py <id> <script> <path>
    cmd = [
        sys.executable, 
        JOB_RUNNER_PATH,
        job_id,
        script_path,
        job_path
    ]

    final_status = 'FAILED'
    result_data = None
    stdout_decoded = ""
    stderr_decoded = ""
    timed_out = False

    try:
        # 서브프로세스 실행 (비동기)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        # 프로세스 완료 대기 및 출력 캡처 (타임아웃 적용)
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=JOB_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            timed_out = True
            await _terminate_process(proc, job_id)
            stdout, stderr = await proc.communicate()

        stdout_decoded = stdout.decode().strip()
        stderr_decoded = stderr.decode().strip()

        if stderr_decoded:
            logging.warning(f"Job {job_id} stderr: {stderr_decoded}")

        output_json = await _read_result_file(job_path)
        if timed_out:
            final_status = "FAILED"
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
            final_status = output_json.get("status", "FAILED")
            result_data = output_json.get("result")
            error_info = output_json.get("error")

            if final_status == "FAILED":
                result_data = error_info

        if final_status == 'COMPLETED':
            metrics.jobs_completed.inc()
        else:
            metrics.jobs_failed.inc()

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

        await state.update_job_status(job_id, final_status, result_data, duration)
        await state.remove_submitted_job(jobname)

async def _dispatch_job(job: Dict[str, Any]):
    """큐에서 작업을 받아 서브프로세스 실행"""
    script_path = job['script_path']
    jobname = job['jobname']
    job_id = job['job_id']

    # 기존의 context.new_page() 로직 제거됨.
    # Worker Process가 알아서 CDP로 접속하여 처리함.
    try:
        await _process_job_internal(script_path, jobname, job_id)
    except Exception as e:
        logging.error(f"Critical dispatch error for job '{jobname}': {e}")
        await state.update_job_status(job_id, 'FAILED', {'error': str(e)})
        await state.remove_submitted_job(jobname)

async def _worker():
    """큐에서 작업을 가져와 처리"""
    logging.info(f"Worker task started: {asyncio.current_task().get_name()}")
    while True:
        job = await job_queue.get_job()
        if job is None:
            job_queue.task_done()
            break
        try:
            await _dispatch_job(job)
        except Exception as e:
            logging.error(f"Unhandled exception in worker loop: {e}", exc_info=True)
        finally:
            job_queue.task_done()
    logging.info(f"Worker task stopped: {asyncio.current_task().get_name()}")

def start_workers():
    global _workers
    if _workers: return
    _workers = [asyncio.create_task(_worker(), name=f"Worker-{i+1}") for i in range(MAX_CONCURRENT_TASKS)]
    logging.info(f"Started {len(_workers)} workers.")

async def stop_workers():
    global _workers
    if not _workers: return
    await job_queue.put_shutdown_signal(len(_workers))
    await job_queue.join(timeout=30.0)
    await asyncio.gather(*_workers, return_exceptions=True)
    _workers = []