import asyncio
import logging
from typing import Any, Dict, Optional

_queue = asyncio.Queue()

async def add_job(job_data: Dict[str, Any]):
    """작업 큐에 작업 추가"""
    await _queue.put(job_data)
    logging.debug(f"Job {job_data.get('job_id', '')} added to queue.")

async def get_job() -> Optional[Dict[str, Any]]:
    """큐에서 작업 가져오기 """
    job = await _queue.get()
    if job is None:
        logging.debug("Shutdown signal received from queue.")
    else:
        logging.debug(f"Job {job.get('job_id', '')} retrieved from queue.")
    return job

def task_done():
    """큐의 작업 완료 알림"""
    try:
        _queue.task_done()
    except ValueError:
        logging.debug("task_done() called when queue count is already zero.")


async def join(timeout: Optional[float] = None):
    """큐의 모든 작업이 완료될 때까지 대기"""
    logging.info(f"Waiting for queue to join (timeout: {timeout}s)...")
    if timeout:
        await asyncio.wait_for(_queue.join(), timeout=timeout)
    else:
        await _queue.join()
    logging.info("Queue joined.")

async def put_shutdown_signal(num_signals: int):
    """워커 수만큼 종료 신호를 큐에 넣음"""
    for _ in range(num_signals):
        await _queue.put(None)
    logging.debug(f"Put {num_signals} shutdown signals into queue.")

def qsize() -> int:
    """현재 큐에 있는 항목 수 반환"""
    return _queue.qsize()