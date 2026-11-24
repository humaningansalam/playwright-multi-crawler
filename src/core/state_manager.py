import asyncio
import json
import logging
import math
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

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

logger = logging.getLogger(__name__)


class InvalidJobTransitionError(Exception):
    def __init__(self, job_id: str, current: JobStatus, requested: JobStatus) -> None:
        self.job_id = job_id
        self.current = current
        self.requested = requested
        super().__init__(
            f"Invalid job transition for {job_id}: {current.value} -> {requested.value}"
        )


_job_status_and_results: dict[str, JobRecord] = {}
_submitted_jobs: set[str] = set()
_state_change_lock = asyncio.Lock()


def _canonicalize_json_value(json_candidate: Any) -> Any:
    if isinstance(json_candidate, JobError):
        error_payload = json_candidate.model_dump(
            mode="json",
            exclude={"worker_result", "worker_error"},
        )
        error_payload["worker_result"] = _canonicalize_json_value(json_candidate.worker_result)
        error_payload["worker_error"] = _canonicalize_json_value(json_candidate.worker_error)
        return JSON_VALUE_ADAPTER.validate_python(error_payload, strict=True)
    return JSON_VALUE_ADAPTER.validate_python(json_candidate, strict=True)


def _now() -> datetime:
    return datetime.now()


def _reject_json_constant(constant_name: str) -> None:
    raise ValueError(f"Non-standard JSON constant: {constant_name}")


def _validate_finite_json_numbers(json_candidate: Any) -> None:
    if isinstance(json_candidate, float) and not math.isfinite(json_candidate):
        raise ValueError("Persisted JSON contains a non-finite number")
    if isinstance(json_candidate, dict):
        for nested_value in json_candidate.values():
            _validate_finite_json_numbers(nested_value)
    elif isinstance(json_candidate, list):
        for nested_value in json_candidate:
            _validate_finite_json_numbers(nested_value)


def _build_persisted_state(job_record: JobRecord) -> PersistedJobStateV1:
    if job_record.status.is_active:
        if job_record.result is not None:
            raise TypeError(f"{job_record.status.value} state cannot persist a result")
        persisted_result = None
    elif job_record.status == JobStatus.COMPLETED:
        persisted_result = _canonicalize_json_value(job_record.result)
    else:
        if not isinstance(job_record.result, JobError):
            raise TypeError(f"{job_record.status.value} state requires a JobError result")
        persisted_result = _canonicalize_json_value(job_record.result)

    return PersistedJobStateV1(
        job_id=job_record.job_id,
        jobname=job_record.jobname,
        status=job_record.status,
        result=persisted_result,
        logs=job_record.logs,
        submitted_at=job_record.submitted_at,
        started_at=job_record.started_at,
        completed_at=job_record.completed_at,
        queue_wait_seconds=job_record.queue_wait_seconds,
        run_duration_seconds=job_record.run_duration_seconds,
        duration_seconds=job_record.duration_seconds,
    )


def _serialize_persisted_state(job_record: JobRecord) -> bytes:
    state_payload = _build_persisted_state(job_record).model_dump(mode="json")
    serialized_state = json.dumps(
        state_payload,
        allow_nan=False,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    return f"{serialized_state}\n".encode("utf-8")


def _sync_directory(directory_path: Path) -> None:
    try:
        directory_descriptor = os.open(
            directory_path,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
    except OSError:
        logger.warning(
            "Could not open directory %s for durability sync.",
            directory_path,
            exc_info=True,
        )
        return

    try:
        os.fsync(directory_descriptor)
    except OSError:
        logger.warning(
            "Could not sync directory %s after state commit.",
            directory_path,
            exc_info=True,
        )
    finally:
        try:
            os.close(directory_descriptor)
        except OSError:
            logger.warning(
                "Could not close synced directory %s.",
                directory_path,
                exc_info=True,
            )


def _write_state_file_atomic(job_record: JobRecord) -> None:
    serialized_state = _serialize_persisted_state(job_record)
    job_directory = Path(job_record.job_path)
    if not job_directory.is_dir():
        raise FileNotFoundError(f"Job directory does not exist: {job_directory}")

    state_path = job_directory / JOB_STATE_FILENAME
    file_descriptor, temporary_name = tempfile.mkstemp(
        dir=job_directory.parent,
        prefix=f".{job_directory.name}.state-",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)
    committed = False
    try:
        with os.fdopen(file_descriptor, "wb") as state_file:
            state_file.write(serialized_state)
            state_file.flush()
            os.fsync(state_file.fileno())
        os.replace(temporary_path, state_path)
        committed = True
        _sync_directory(job_directory)
    finally:
        if not committed:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                logger.warning(
                    "Could not remove temporary state file %s.",
                    temporary_path,
                    exc_info=True,
                )


def _remove_state_file(job_record: JobRecord) -> None:
    state_path = Path(job_record.job_path) / JOB_STATE_FILENAME
    try:
        state_path.unlink()
    except FileNotFoundError:
        return
    _sync_directory(state_path.parent)


async def _run_committed_state_change(operation):
    """Finish a started state commit before propagating caller cancellation."""
    commit_task = asyncio.create_task(operation)
    pending_cancellation: asyncio.CancelledError | None = None

    while not commit_task.done():
        try:
            await asyncio.shield(commit_task)
        except asyncio.CancelledError as cancellation:
            if commit_task.cancelled():
                raise
            if pending_cancellation is None:
                pending_cancellation = cancellation
        except Exception as commit_error:
            if pending_cancellation is not None:
                logger.exception("State change failed while caller cancellation was pending.")
                raise pending_cancellation from commit_error
            raise

    try:
        commit_result = commit_task.result()
    except Exception as commit_error:
        if pending_cancellation is not None:
            logger.exception("State change failed while caller cancellation was pending.")
            raise pending_cancellation from commit_error
        raise

    if pending_cancellation is not None:
        raise pending_cancellation
    return commit_result


def _parse_persisted_state(state_path: Path) -> PersistedJobStateV1 | None:
    try:
        raw_state = json.loads(
            state_path.read_text(encoding="utf-8"),
            parse_constant=_reject_json_constant,
        )
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        logger.warning("Skipping malformed persisted job state: %s", state_path)
        return None

    if not isinstance(raw_state, dict):
        logger.warning("Skipping non-object persisted job state: %s", state_path)
        return None

    try:
        _validate_finite_json_numbers(raw_state)
    except ValueError:
        logger.warning("Skipping persisted job state with a non-finite number: %s", state_path)
        return None

    schema_version = raw_state.get("schema_version")
    if type(schema_version) is not int:
        logger.warning(
            "Skipping persisted job state without an integer schema version: %s",
            state_path,
        )
        return None
    if schema_version != JOB_STATE_SCHEMA_VERSION:
        raise RuntimeError(
            f"Unsupported persisted job state schema version {schema_version}: {state_path}"
        )

    try:
        return PersistedJobStateV1.model_validate(raw_state)
    except ValidationError:
        logger.warning(
            "Skipping invalid V1 persisted job state: %s",
            state_path,
            exc_info=True,
        )
        return None


def _has_valid_recovery_identity(
    persisted_state: PersistedJobStateV1,
    state_path: Path,
) -> bool:
    if persisted_state.job_id != state_path.parent.name:
        logger.warning(
            "Skipping persisted job state whose ID does not match its directory: %s",
            state_path,
        )
        return False
    if not persisted_state.jobname.strip():
        logger.warning("Skipping persisted job state with a blank job name: %s", state_path)
        return False
    return True


def _normalize_recovered_result(
    persisted_state: PersistedJobStateV1,
    state_path: Path,
) -> tuple[bool, Any]:
    recovered_result = persisted_state.result
    if persisted_state.status.is_active:
        if recovered_result is not None:
            logger.warning("Skipping active persisted job with a result: %s", state_path)
            return False, None
        if (
            persisted_state.status == JobStatus.PENDING
            and not (state_path.parent / "script.py").is_file()
        ):
            logger.warning("Skipping pending persisted job without script.py: %s", state_path)
            return False, None
        return True, None

    if persisted_state.status == JobStatus.COMPLETED:
        try:
            return True, _canonicalize_json_value(recovered_result)
        except (TypeError, ValueError):
            logger.warning(
                "Skipping completed persisted job with invalid JSON result: %s",
                state_path,
            )
            return False, None

    try:
        return True, JobError.model_validate(recovered_result)
    except ValidationError:
        logger.warning(
            "Skipping terminal persisted job without a valid JobError: %s",
            state_path,
        )
        return False, None


def _load_recovery_candidate(state_path: Path) -> JobRecord | None:
    persisted_state = _parse_persisted_state(state_path)
    if persisted_state is None or not _has_valid_recovery_identity(persisted_state, state_path):
        return None

    result_is_valid, recovered_result = _normalize_recovered_result(persisted_state, state_path)
    if not result_is_valid:
        return None

    return JobRecord(
        job_id=persisted_state.job_id,
        jobname=persisted_state.jobname,
        job_path=str(state_path.parent),
        status=persisted_state.status,
        result=recovered_result,
        logs=persisted_state.logs,
        submitted_at=persisted_state.submitted_at,
        started_at=persisted_state.started_at,
        completed_at=persisted_state.completed_at,
        queue_wait_seconds=persisted_state.queue_wait_seconds,
        run_duration_seconds=persisted_state.run_duration_seconds,
        duration_seconds=persisted_state.duration_seconds,
    )


def _load_recovery_candidates(job_root: Path) -> dict[str, JobRecord]:
    recovered_records: dict[str, JobRecord] = {}
    for job_directory in sorted(job_root.iterdir(), key=lambda candidate: candidate.name):
        if not job_directory.is_dir():
            continue

        state_path = job_directory / JOB_STATE_FILENAME
        if not state_path.exists():
            continue

        recovered_record = _load_recovery_candidate(state_path)
        if recovered_record is None:
            continue
        if recovered_record.job_id in recovered_records:
            raise RuntimeError(f"Duplicate recovered job ID: {recovered_record.job_id}")
        recovered_records[recovered_record.job_id] = recovered_record

    return recovered_records


def _interrupted_recovery_record(job_record: JobRecord) -> JobRecord:
    completed_at = _now()
    run_duration_seconds = (
        max(0.0, (completed_at - job_record.started_at).total_seconds())
        if job_record.started_at is not None
        else None
    )
    return job_record.model_copy(
        update={
            "status": JobStatus.INTERRUPTED,
            "result": JobError(
                code=JobErrorCode.SERVICE_SHUTDOWN,
                message="Job was interrupted because the service restarted while it was running.",
            ),
            "completed_at": completed_at,
            "run_duration_seconds": run_duration_seconds,
            "duration_seconds": run_duration_seconds,
        },
        deep=True,
    )


def _prepare_recovered_records(
    recovered_records: dict[str, JobRecord],
) -> tuple[set[str], set[str]]:
    interrupted_job_ids: set[str] = set()
    active_job_names: set[str] = set()

    for job_id, recovered_record in list(recovered_records.items()):
        if recovered_record.status == JobStatus.RUNNING:
            interrupted_job_ids.add(job_id)
            recovered_record = _interrupted_recovery_record(recovered_record)
            recovered_records[job_id] = recovered_record

        if recovered_record.status != JobStatus.PENDING:
            continue
        if recovered_record.jobname in active_job_names:
            raise RuntimeError(
                f"Duplicate active job name in recovered state: {recovered_record.jobname}"
            )
        active_job_names.add(recovered_record.jobname)

    return interrupted_job_ids, active_job_names


async def recover_persisted_jobs(job_root: str | Path) -> list[QueuedJob]:
    root_path = Path(job_root)

    async with _state_change_lock:
        if _job_status_and_results or _submitted_jobs:
            raise RuntimeError("Cannot recover persisted jobs into non-empty runtime registries")

    recovered_records = await asyncio.to_thread(_load_recovery_candidates, root_path)
    interrupted_job_ids, active_job_names = _prepare_recovered_records(recovered_records)

    for job_id in sorted(interrupted_job_ids):
        await asyncio.to_thread(_write_state_file_atomic, recovered_records[job_id])

    async with _state_change_lock:
        if _job_status_and_results or _submitted_jobs:
            raise RuntimeError("Cannot recover persisted jobs into non-empty runtime registries")
        _job_status_and_results.update(recovered_records)
        _submitted_jobs.update(active_job_names)

    pending_jobs = [
        QueuedJob(
            job_id=job_record.job_id,
            jobname=job_record.jobname,
            script_path=str(Path(job_record.job_path) / "script.py"),
        )
        for job_record in recovered_records.values()
        if job_record.status == JobStatus.PENDING
    ]
    logger.info(
        "Recovered %d persisted jobs (%d pending).",
        len(recovered_records),
        len(pending_jobs),
    )
    return pending_jobs


async def get_job_info(job_id: str) -> JobRecord | None:
    job_record = _job_status_and_results.get(job_id)
    if job_record is None:
        return None
    return await asyncio.to_thread(job_record.model_copy, deep=True)


async def get_job_status(job_id: str) -> JobStatus | None:
    job_record = _job_status_and_results.get(job_id)
    return job_record.status if job_record else None


async def get_active_job_ids() -> set[str]:
    return {
        job_id
        for job_id, job_record in _job_status_and_results.items()
        if job_record.status.is_active
    }


async def set_initial_status(job_id: str, job_name: str, job_path: str) -> None:
    async def commit() -> None:
        async with _state_change_lock:
            if job_id in _job_status_and_results:
                logger.warning(
                    "Replacing existing state for job ID %s during initialization.", job_id
                )

            initial_record = JobRecord(
                job_id=job_id,
                jobname=job_name,
                job_path=job_path,
            )
            await asyncio.to_thread(_write_state_file_atomic, initial_record)
            _job_status_and_results[job_id] = initial_record

    await _run_committed_state_change(commit())
    logger.debug("Initial status set for job %s: %s", job_id, JobStatus.PENDING.value)


def _build_status_updates(
    current_record: JobRecord,
    requested_status: JobStatus,
    job_result: Any,
    duration: float | None,
    logs: dict[str, str] | None,
) -> dict[str, Any]:
    updates: dict[str, Any] = {"status": requested_status}
    transition_time = _now()

    if requested_status == JobStatus.RUNNING and current_record.started_at is None:
        updates["started_at"] = transition_time
        updates["queue_wait_seconds"] = max(
            0.0,
            (transition_time - current_record.submitted_at).total_seconds(),
        )

    if requested_status.is_terminal and current_record.completed_at is None:
        updates["completed_at"] = transition_time
        run_duration_seconds = duration
        if run_duration_seconds is None and current_record.started_at is not None:
            run_duration_seconds = max(
                0.0,
                (transition_time - current_record.started_at).total_seconds(),
            )
        updates["run_duration_seconds"] = run_duration_seconds
        updates["duration_seconds"] = run_duration_seconds

    if job_result is not None:
        updates["result"] = job_result
    if duration is not None and not requested_status.is_terminal:
        updates["duration_seconds"] = duration
    if logs is not None:
        updates["logs"] = logs

    return updates


async def update_job_status(
    job_id: str,
    status: JobStatus,
    result: Any = None,
    duration: float | None = None,
    logs: dict[str, str] | None = None,
) -> bool:
    if not isinstance(status, JobStatus):
        raise TypeError("status must be a JobStatus")

    async def commit() -> bool:
        async with _state_change_lock:
            current_record = _job_status_and_results.get(job_id)
            if current_record is None:
                logger.warning("Attempted to update status for non-existent job ID: %s", job_id)
                return False

            if current_record.status.is_terminal and status != current_record.status:
                raise InvalidJobTransitionError(job_id, current_record.status, status)
            if status in (
                JobStatus.FAILED,
                JobStatus.CANCELLED,
                JobStatus.INTERRUPTED,
            ) and not isinstance(result, JobError):
                raise TypeError(f"{status.value} state requires a JobError result")

            releases_job_name = current_record.status.is_active and status.is_terminal
            updated_record = current_record.model_copy(
                update=_build_status_updates(
                    current_record,
                    status,
                    result,
                    duration,
                    logs,
                ),
                deep=True,
            )
            await asyncio.to_thread(_write_state_file_atomic, updated_record)
            _job_status_and_results[job_id] = updated_record

            if releases_job_name:
                _submitted_jobs.discard(current_record.jobname)

            logger.debug("Status updated for job %s: %s", job_id, status.value)
            return True

    return await _run_committed_state_change(commit())


async def remove_job_state(job_id: str) -> None:
    async def commit() -> None:
        async with _state_change_lock:
            job_record = _job_status_and_results.get(job_id)
            if job_record is None:
                return
            await asyncio.to_thread(_remove_state_file, job_record)
            _job_status_and_results.pop(job_id, None)
            logger.info("Removed state for job ID: %s", job_id)

    await _run_committed_state_change(commit())


async def add_submitted_job(jobname: str) -> bool:
    async with _state_change_lock:
        if jobname in _submitted_jobs:
            logger.warning("Duplicate job submission detected for name: %s", jobname)
            return False
        _submitted_jobs.add(jobname)
        return True


async def remove_submitted_job(jobname: str) -> None:
    async with _state_change_lock:
        _submitted_jobs.discard(jobname)


async def is_job_submitted(jobname: str) -> bool:
    return jobname in _submitted_jobs
