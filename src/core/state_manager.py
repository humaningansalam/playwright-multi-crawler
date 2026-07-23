import asyncio
import json
import logging
import math
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Set

from pydantic import ValidationError

from src.models.job import (
    JOB_STATE_FILENAME,
    JOB_STATE_SCHEMA_VERSION,
    JSON_VALUE_ADAPTER,
    JobError,
    JobErrorCode,
    JobRecord,
    JobStatus,
    PersistedJobStateV1,
    QueuedJob,
)


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


def _canonicalize_json_value(value: Any) -> Any:
    if isinstance(value, JobError):
        payload = value.model_dump(
            mode="json",
            exclude={"worker_result", "worker_error"},
        )
        payload["worker_result"] = _canonicalize_json_value(value.worker_result)
        payload["worker_error"] = _canonicalize_json_value(value.worker_error)
        return JSON_VALUE_ADAPTER.validate_python(payload, strict=True)
    return JSON_VALUE_ADAPTER.validate_python(value, strict=True)


def _now() -> datetime:
    return datetime.now()


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Non-standard JSON constant: {value}")


def _validate_finite_json_numbers(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Persisted JSON contains a non-finite number")
    if isinstance(value, dict):
        for item in value.values():
            _validate_finite_json_numbers(item)
    elif isinstance(value, list):
        for item in value:
            _validate_finite_json_numbers(item)


def _build_persisted_state(record: JobRecord) -> PersistedJobStateV1:
    if record.status.is_active:
        if record.result is not None:
            raise TypeError(f"{record.status.value} state cannot persist a result")
        persisted_result = None
    elif record.status == JobStatus.COMPLETED:
        persisted_result = _canonicalize_json_value(record.result)
    else:
        if not isinstance(record.result, JobError):
            raise TypeError(f"{record.status.value} state requires a JobError result")
        persisted_result = _canonicalize_json_value(record.result)

    return PersistedJobStateV1(
        job_id=record.job_id,
        jobname=record.jobname,
        status=record.status,
        result=persisted_result,
        logs=record.logs,
        submitted_at=record.submitted_at,
        started_at=record.started_at,
        completed_at=record.completed_at,
        queue_wait_seconds=record.queue_wait_seconds,
        run_duration_seconds=record.run_duration_seconds,
        duration_seconds=record.duration_seconds,
    )


def _serialize_persisted_state(record: JobRecord) -> bytes:
    payload = _build_persisted_state(record).model_dump(mode="json")
    serialized = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    return f"{serialized}\n".encode("utf-8")


def _sync_directory(path: Path) -> None:
    try:
        directory_fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        logging.warning("Could not open directory %s for durability sync.", path, exc_info=True)
        return
    try:
        os.fsync(directory_fd)
    except OSError:
        logging.warning("Could not sync directory %s after state commit.", path, exc_info=True)
    finally:
        try:
            os.close(directory_fd)
        except OSError:
            logging.warning("Could not close synced directory %s.", path, exc_info=True)


def _write_state_file_atomic(record: JobRecord) -> None:
    content = _serialize_persisted_state(record)
    job_dir = Path(record.job_path)
    if not job_dir.is_dir():
        raise FileNotFoundError(f"Job directory does not exist: {job_dir}")

    state_path = job_dir / JOB_STATE_FILENAME
    file_descriptor, temporary_name = tempfile.mkstemp(
        dir=job_dir.parent,
        prefix=f".{job_dir.name}.state-",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)
    committed = False
    try:
        with os.fdopen(file_descriptor, "wb") as state_file:
            state_file.write(content)
            state_file.flush()
            os.fsync(state_file.fileno())
        os.replace(temporary_path, state_path)
        committed = True
        _sync_directory(job_dir)
    finally:
        if not committed:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                logging.warning("Could not remove temporary state file %s.", temporary_path, exc_info=True)


def _remove_state_file(record: JobRecord) -> None:
    state_path = Path(record.job_path) / JOB_STATE_FILENAME
    try:
        state_path.unlink()
    except FileNotFoundError:
        return
    _sync_directory(state_path.parent)


async def _run_committed_state_change(operation):
    task = asyncio.create_task(operation)
    cancellation = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as current_cancellation:
            if task.cancelled():
                raise
            if cancellation is None:
                cancellation = current_cancellation
        except Exception:
            if cancellation is not None:
                logging.exception("State change failed while caller cancellation was pending.")
                raise cancellation
            raise

    try:
        result = task.result()
    except Exception:
        if cancellation is not None:
            logging.exception("State change failed while caller cancellation was pending.")
            raise cancellation
        raise
    if cancellation is not None:
        raise cancellation
    return result


def _load_recovery_candidate(state_path: Path) -> Optional[JobRecord]:
    try:
        raw_payload = json.loads(
            state_path.read_text(encoding="utf-8"),
            parse_constant=_reject_json_constant,
        )
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        logging.warning("Skipping malformed persisted job state: %s", state_path)
        return None

    if not isinstance(raw_payload, dict):
        logging.warning("Skipping non-object persisted job state: %s", state_path)
        return None

    try:
        _validate_finite_json_numbers(raw_payload)
    except ValueError:
        logging.warning("Skipping persisted job state with a non-finite number: %s", state_path)
        return None

    schema_version = raw_payload.get("schema_version")
    if type(schema_version) is not int:
        logging.warning("Skipping persisted job state without an integer schema version: %s", state_path)
        return None
    if schema_version != JOB_STATE_SCHEMA_VERSION:
        raise RuntimeError(
            f"Unsupported persisted job state schema version {schema_version}: {state_path}"
        )

    try:
        persisted = PersistedJobStateV1.model_validate(raw_payload)
    except ValidationError:
        logging.warning("Skipping invalid V1 persisted job state: %s", state_path, exc_info=True)
        return None

    job_dir = state_path.parent
    if persisted.job_id != job_dir.name:
        logging.warning(
            "Skipping persisted job state whose ID does not match its directory: %s",
            state_path,
        )
        return None
    if not persisted.jobname.strip():
        logging.warning("Skipping persisted job state with a blank job name: %s", state_path)
        return None

    result: Any = persisted.result
    if persisted.status.is_active:
        if result is not None:
            logging.warning("Skipping active persisted job with a result: %s", state_path)
            return None
        if persisted.status == JobStatus.PENDING and not (job_dir / "script.py").is_file():
            logging.warning("Skipping pending persisted job without script.py: %s", state_path)
            return None
    elif persisted.status == JobStatus.COMPLETED:
        try:
            result = _canonicalize_json_value(result)
        except (TypeError, ValueError):
            logging.warning("Skipping completed persisted job with invalid JSON result: %s", state_path)
            return None
    else:
        try:
            result = JobError.model_validate(result)
        except ValidationError:
            logging.warning("Skipping terminal persisted job without a valid JobError: %s", state_path)
            return None

    return JobRecord(
        job_id=persisted.job_id,
        jobname=persisted.jobname,
        job_path=str(job_dir),
        status=persisted.status,
        result=result,
        logs=persisted.logs,
        submitted_at=persisted.submitted_at,
        started_at=persisted.started_at,
        completed_at=persisted.completed_at,
        queue_wait_seconds=persisted.queue_wait_seconds,
        run_duration_seconds=persisted.run_duration_seconds,
        duration_seconds=persisted.duration_seconds,
    )


async def recover_persisted_jobs(job_root: str | Path) -> list[QueuedJob]:
    root = Path(job_root)
    recovered: Dict[str, JobRecord] = {}
    running_job_ids: Set[str] = set()

    async with _job_status_lock:
        async with _submitted_jobs_lock:
            if _job_status_and_results or _submitted_jobs:
                raise RuntimeError("Cannot recover persisted jobs into non-empty runtime registries")

    for job_dir in sorted(root.iterdir(), key=lambda path: path.name):
        if not job_dir.is_dir():
            continue
        state_path = job_dir / JOB_STATE_FILENAME
        if not state_path.exists():
            continue
        record = await asyncio.to_thread(_load_recovery_candidate, state_path)
        if record is None:
            continue
        if record.job_id in recovered:
            raise RuntimeError(f"Duplicate recovered job ID: {record.job_id}")
        recovered[record.job_id] = record

    active_names: Set[str] = set()
    for record in recovered.values():
        if record.status == JobStatus.RUNNING:
            running_job_ids.add(record.job_id)
            completed_at = _now()
            run_duration = (
                max(0.0, (completed_at - record.started_at).total_seconds())
                if record.started_at is not None
                else None
            )
            record = record.model_copy(
                update={
                    "status": JobStatus.INTERRUPTED,
                    "result": JobError(
                        code=JobErrorCode.SERVICE_SHUTDOWN,
                        message=(
                            "Job was interrupted because the service restarted while it was running."
                        ),
                    ),
                    "completed_at": completed_at,
                    "run_duration_seconds": run_duration,
                    "duration_seconds": run_duration,
                },
                deep=True,
            )
            recovered[record.job_id] = record
        if record.status == JobStatus.PENDING:
            if record.jobname in active_names:
                raise RuntimeError(f"Duplicate active job name in recovered state: {record.jobname}")
            active_names.add(record.jobname)

    for job_id in sorted(running_job_ids):
        await asyncio.to_thread(_write_state_file_atomic, recovered[job_id])

    async with _job_status_lock:
        async with _submitted_jobs_lock:
            if _job_status_and_results or _submitted_jobs:
                raise RuntimeError("Cannot recover persisted jobs into non-empty runtime registries")
            _job_status_and_results.update(recovered)
            _submitted_jobs.update(active_names)

    pending_jobs = [
        QueuedJob(
            job_id=record.job_id,
            jobname=record.jobname,
            script_path=str(Path(record.job_path) / "script.py"),
        )
        for record in recovered.values()
        if record.status == JobStatus.PENDING
    ]
    logging.info(
        "Recovered %d persisted jobs (%d pending).",
        len(recovered),
        len(pending_jobs),
    )
    return pending_jobs


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
    async def commit() -> None:
        async with _job_status_lock:
            if job_id in _job_status_and_results:
                logging.warning("Replacing existing state for job ID %s during initialization.", job_id)
            candidate = JobRecord(
                job_id=job_id,
                jobname=job_name,
                job_path=job_path,
            )
            await asyncio.to_thread(_write_state_file_atomic, candidate)
            _job_status_and_results[job_id] = candidate

    await _run_committed_state_change(commit())
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

    async def commit() -> bool:
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
            transition_time = _now()
            if status == JobStatus.RUNNING and current.started_at is None:
                updates["started_at"] = transition_time
                updates["queue_wait_seconds"] = max(
                    0.0,
                    (transition_time - current.submitted_at).total_seconds(),
                )
            if status.is_terminal and current.completed_at is None:
                updates["completed_at"] = transition_time
                run_duration = duration
                if run_duration is None and current.started_at is not None:
                    run_duration = max(
                        0.0,
                        (transition_time - current.started_at).total_seconds(),
                    )
                updates["run_duration_seconds"] = run_duration
                updates["duration_seconds"] = run_duration
            if result is not None:
                updates["result"] = result
            if duration is not None and not status.is_terminal:
                updates["duration_seconds"] = duration
            if logs is not None:
                updates["logs"] = logs
            candidate = current.model_copy(update=updates, deep=True)
            await asyncio.to_thread(_write_state_file_atomic, candidate)
            _job_status_and_results[job_id] = candidate
            logging.debug("Status updated for job %s: %s", job_id, status)
            return True

    return await _run_committed_state_change(commit())


async def remove_job_state(job_id: str) -> None:
    async def commit() -> None:
        async with _job_status_lock:
            current = _job_status_and_results.get(job_id)
            if current is None:
                return
            await asyncio.to_thread(_remove_state_file, current)
            _job_status_and_results.pop(job_id, None)
            logging.info("Removed state for job ID: %s", job_id)

    await _run_committed_state_change(commit())


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
