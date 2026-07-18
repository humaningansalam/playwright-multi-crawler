import asyncio
import logging
from typing import Optional

from src.common.metrics import metrics
from src.models.job import QueuedJob

_queue: asyncio.Queue[Optional[QueuedJob]] = asyncio.Queue()
_cancelled_job_ids: set[str] = set()
_claimed_job_ids: set[str] = set()
_queued_job_ids: set[str] = set()


async def add_job(job: QueuedJob) -> None:
    if not isinstance(job, QueuedJob):
        raise TypeError("job must be a QueuedJob")
    await _queue.put(job)
    _queued_job_ids.add(job.job_id)
    metrics.queued_jobs.set(qsize())
    logging.debug("Job %s added to queue.", job.job_id)


def cancel_job(job_id: str) -> bool:
    if job_id in _claimed_job_ids or job_id not in _queued_job_ids:
        return False
    _cancelled_job_ids.add(job_id)
    _queued_job_ids.remove(job_id)
    metrics.queued_jobs.set(qsize())
    return True


def consume_cancellation(job_id: str) -> bool:
    if job_id not in _cancelled_job_ids:
        return False
    _cancelled_job_ids.remove(job_id)
    return True


def claim_job(job_id: str) -> bool:
    if consume_cancellation(job_id):
        return False
    _queued_job_ids.discard(job_id)
    metrics.queued_jobs.set(qsize())
    _claimed_job_ids.add(job_id)
    return True


def release_job(job_id: str) -> None:
    _claimed_job_ids.discard(job_id)


async def get_job() -> Optional[QueuedJob]:
    job = await _queue.get()
    logging.debug("%s received from queue.", "Shutdown signal" if job is None else f"Job {job.job_id}")
    return job


def task_done() -> None:
    _queue.task_done()


async def join(timeout: Optional[float] = None) -> None:
    logging.info("Waiting for queue to join (timeout: %ss)...", timeout)
    if timeout is not None:
        await asyncio.wait_for(_queue.join(), timeout=timeout)
    else:
        await _queue.join()


async def put_shutdown_signal(num_signals: int) -> None:
    for _ in range(num_signals):
        await _queue.put(None)


def qsize() -> int:
    return len(_queued_job_ids)
