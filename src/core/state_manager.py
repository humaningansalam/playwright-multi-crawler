import asyncio
import logging
from typing import Any, Dict, Optional, Set

from src.models.job import JobError, JobRecord, JobStatus


class InvalidJobTransitionError(Exception):
    def __init__(self, job_id: str, current: JobStatus, requested: JobStatus):
        self.job_id = job_id
        self.current = current
        self.requested = requested
        super().__init__(f"Invalid job transition for {job_id}: {current.value} -> {requested.value}")

_job_status_and_results: Dict[str, JobRecord] = {}
_submitted_jobs: Set[str] = set()

_job_status_lock = asyncio.Lock()
_submitted_jobs_lock = asyncio.Lock()


async def get_job_info(job_id: str) -> Optional[JobRecord]:
    async with _job_status_lock:
        job_info = _job_status_and_results.get(job_id)
    if job_info is None:
        return None
    return await asyncio.to_thread(job_info.model_copy, deep=True)


async def get_job_status(job_id: str) -> Optional[JobStatus]:
    async with _job_status_lock:
        job_info = _job_status_and_results.get(job_id)
        return job_info.status if job_info else None


async def get_active_job_ids() -> Set[str]:
    async with _job_status_lock:
        return {
            job_id
            for job_id, job_info in _job_status_and_results.items()
            if job_info.status.is_active
        }


async def set_initial_status(job_id: str, job_name: str, job_path: str) -> None:
    async with _job_status_lock:
        if job_id in _job_status_and_results:
            logging.warning("Replacing existing state for job ID %s during initialization.", job_id)
        _job_status_and_results[job_id] = JobRecord(
            job_id=job_id,
            jobname=job_name,
            job_path=job_path,
        )
    logging.debug("Initial status set for job %s: %s", job_id, JobStatus.PENDING)


async def update_job_status(
    job_id: str,
    status: JobStatus,
    result: Any = None,
    duration: Optional[float] = None,
    logs: Optional[Dict[str, str]] = None,
) -> bool:
    if not isinstance(status, JobStatus):
        raise TypeError("status must be a JobStatus")

    async with _job_status_lock:
        current = _job_status_and_results.get(job_id)
        if current is None:
            logging.warning("Attempted to update status for non-existent job ID: %s", job_id)
            return False

        if current.status.is_terminal and status != current.status:
            raise InvalidJobTransitionError(job_id, current.status, status)
        if status in (JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.INTERRUPTED):
            if not isinstance(result, JobError):
                raise TypeError(f"{status.value} state requires a JobError result")

        updates: Dict[str, Any] = {"status": status}
        if result is not None:
            updates["result"] = result
        if duration is not None:
            updates["duration_seconds"] = duration
        if logs is not None:
            updates["logs"] = logs
        _job_status_and_results[job_id] = current.model_copy(update=updates, deep=True)
        logging.debug("Status updated for job %s: %s", job_id, status)
        return True


async def remove_job_state(job_id: str) -> None:
    async with _job_status_lock:
        if _job_status_and_results.pop(job_id, None) is not None:
            logging.info("Removed state for job ID: %s", job_id)


async def add_submitted_job(jobname: str) -> bool:
    async with _submitted_jobs_lock:
        if jobname in _submitted_jobs:
            logging.warning("Duplicate job submission detected for name: %s", jobname)
            return False
        _submitted_jobs.add(jobname)
        return True


async def remove_submitted_job(jobname: str) -> None:
    async with _submitted_jobs_lock:
        _submitted_jobs.discard(jobname)


async def is_job_submitted(jobname: str) -> bool:
    async with _submitted_jobs_lock:
        return jobname in _submitted_jobs
