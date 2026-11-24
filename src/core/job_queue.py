import asyncio
import logging

from src.common.metrics import metrics
from src.models.job import QueuedJob

logger = logging.getLogger(__name__)

_queue: asyncio.Queue[QueuedJob | None] = asyncio.Queue()
_cancelled_job_ids: set[str] = set()
_claimed_job_ids: set[str] = set()
_queued_job_ids: set[str] = set()


async def add_job(job: QueuedJob) -> None:
    if not isinstance(job, QueuedJob):
        raise TypeError("job must be a QueuedJob")

    await _queue.put(job)
    _queued_job_ids.add(job.job_id)
    metrics.queued_jobs.set(qsize())
    logger.debug("Added job %s to the queue.", job.job_id)


def cancel_job(job_id: str) -> bool:
    """Mark an unclaimed queued job as cancelled."""
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


async def get_job() -> QueuedJob | None:
    queued_job = await _queue.get()
    description = "shutdown signal" if queued_job is None else f"job {queued_job.job_id}"
    logger.debug("Received %s from the queue.", description)
    return queued_job


def task_done() -> None:
    _queue.task_done()


async def join(timeout: float | None = None) -> None:
    logger.info("Waiting for the job queue to drain (timeout=%s seconds).", timeout)
    if timeout is None:
        await _queue.join()
        return
    await asyncio.wait_for(_queue.join(), timeout=timeout)


async def put_shutdown_signal(num_signals: int) -> None:
    for _ in range(num_signals):
        await _queue.put(None)


def qsize() -> int:
    """Return the number of live, unclaimed jobs rather than raw queue slots."""
    return len(_queued_job_ids)


def restore_jobs(jobs: list[QueuedJob]) -> None:
    restored_job_ids: set[str] = set()
    for job in jobs:
        if not isinstance(job, QueuedJob):
            raise TypeError("all restored jobs must be QueuedJob instances")
        if job.job_id in restored_job_ids:
            raise RuntimeError(f"Duplicate restored job ID: {job.job_id}")
        restored_job_ids.add(job.job_id)

    if not _queue.empty() or _queued_job_ids or _claimed_job_ids or _cancelled_job_ids:
        raise RuntimeError("Cannot restore jobs into non-empty queue state")

    for job in jobs:
        _queue.put_nowait(job)
    _queued_job_ids.update(restored_job_ids)
    metrics.queued_jobs.set(qsize())
    logger.info("Restored %d pending jobs into the queue.", len(jobs))
