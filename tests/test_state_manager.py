import pytest
import asyncio
import json
import threading
import time
from datetime import datetime, timedelta

# 테스트 대상 모듈 임포트
from src.core import state_manager as state
from src.models.job import (
    CleanupFailure,
    JobError,
    JobErrorCode,
    JobRecord,
    JobStatus,
    PersistedJobStateV1,
)


def _write_persisted_job(
    root,
    job_id,
    jobname,
    status,
    result=None,
    *,
    create_script=False,
):
    job_dir = root / job_id
    job_dir.mkdir()
    if create_script:
        (job_dir / "script.py").write_text("pass\n", encoding="utf-8")
    persisted = PersistedJobStateV1(
        job_id=job_id,
        jobname=jobname,
        status=status,
        result=result,
        submitted_at=datetime(2026, 7, 23, 1, 2, 3),
    )
    state_path = job_dir / "state.json"
    state_path.write_text(persisted.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return job_dir, state_path

@pytest.mark.asyncio
async def test_initial_status_and_update(tmp_path):
    """상태 초기화 및 업데이트 기능 테스트"""
    job_id = "test-job-123"
    job_name = "unit_test_job"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    job_path = str(job_dir)

    # 초기 상태 설정
    await state.set_initial_status(job_id, job_name, job_path)

    # 상태 확인
    initial_info = await state.get_job_info(job_id)
    assert initial_info is not None
    assert initial_info.status == JobStatus.PENDING
    assert initial_info.jobname == job_name
    assert initial_info.job_path == job_path

    # 상태 업데이트 (RUNNING)
    await state.update_job_status(job_id, JobStatus.RUNNING)
    running_status = await state.get_job_status(job_id)
    assert running_status == JobStatus.RUNNING

    # 상태 업데이트 (COMPLETED with result)
    result_data = {"key": "value"}
    duration = 5.5
    await state.update_job_status(job_id, JobStatus.COMPLETED, result=result_data, duration=duration)

    # 최종 정보 확인
    final_info = await state.get_job_info(job_id)
    assert final_info is not None
    assert final_info.status == JobStatus.COMPLETED
    assert final_info.result == result_data
    assert final_info.duration_seconds == duration


@pytest.mark.asyncio
async def test_state_manager_rejects_untyped_status(tmp_path):
    job_dir = tmp_path / "typed-status"
    job_dir.mkdir()
    await state.set_initial_status("typed-status", "typed", str(job_dir))
    with pytest.raises(TypeError):
        await state.update_job_status("typed-status", "COMPLETED")


@pytest.mark.asyncio
async def test_state_manager_enforces_terminal_error_and_transition_contracts(tmp_path):
    job_dir = tmp_path / "terminal-contract"
    job_dir.mkdir()
    await state.set_initial_status("terminal-contract", "typed", str(job_dir))

    with pytest.raises(TypeError):
        await state.update_job_status("terminal-contract", JobStatus.FAILED, {"error": "boom"})

    await state.update_job_status(
        "terminal-contract",
        JobStatus.FAILED,
        JobError(code=JobErrorCode.PROCESSING_FAILED, message="boom"),
    )
    with pytest.raises(state.InvalidJobTransitionError):
        await state.update_job_status("terminal-contract", JobStatus.RUNNING)


@pytest.mark.asyncio
async def test_get_job_info_deep_copy_does_not_block_event_loop(monkeypatch, tmp_path):
    job_id = "slow-deep-copy"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "slow_copy", str(job_dir))
    await state.update_job_status(job_id, JobStatus.COMPLETED, {"items": [1, 2, 3]})
    original_model_copy = JobRecord.model_copy

    def slow_model_copy(self, *, update=None, deep=False):
        time.sleep(0.1)
        return original_model_copy(self, update=update, deep=deep)

    monkeypatch.setattr(JobRecord, "model_copy", slow_model_copy)

    started = time.perf_counter()
    copy_task = asyncio.create_task(state.get_job_info(job_id))
    await asyncio.sleep(0.01)
    heartbeat_elapsed = time.perf_counter() - started
    copied = await copy_task

    assert heartbeat_elapsed < 0.05
    assert copied is not None
    assert copied.result == {"items": [1, 2, 3]}


@pytest.mark.asyncio
async def test_initial_status_persists_versioned_state(tmp_path):
    job_id = "persisted-pending"
    job_dir = tmp_path / job_id
    job_dir.mkdir()

    await state.set_initial_status(job_id, "persisted_pending", str(job_dir))

    job_info = await state.get_job_info(job_id)
    payload = json.loads((job_dir / "state.json").read_text(encoding="utf-8"))
    assert job_info is not None
    assert payload == {
        "completed_at": None,
        "duration_seconds": None,
        "job_id": job_id,
        "jobname": "persisted_pending",
        "logs": None,
        "queue_wait_seconds": None,
        "result": None,
        "run_duration_seconds": None,
        "schema_version": 1,
        "started_at": None,
        "status": "PENDING",
        "submitted_at": job_info.submitted_at.isoformat(),
    }
    assert "job_path" not in payload


@pytest.mark.asyncio
async def test_failed_atomic_update_preserves_disk_and_memory(monkeypatch, tmp_path):
    job_id = "failed-persist"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "failed_persist", str(job_dir))
    state_path = job_dir / "state.json"
    original_bytes = state_path.read_bytes()

    def fail_replace(_source, _destination):
        raise OSError("replace failed")

    monkeypatch.setattr(state.os, "replace", fail_replace)

    with pytest.raises(OSError, match="replace failed"):
        await state.update_job_status(job_id, JobStatus.RUNNING)

    job_info = await state.get_job_info(job_id)
    assert job_info is not None
    assert job_info.status == JobStatus.PENDING
    assert state_path.read_bytes() == original_bytes
    assert list(tmp_path.glob(f".{job_id}.state-*.tmp")) == []


@pytest.mark.asyncio
async def test_state_change_commits_before_cancellation_propagates(monkeypatch, tmp_path):
    job_id = "cancelled-persist"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "cancelled_persist", str(job_dir))
    original_writer = state._write_state_file_atomic
    started = threading.Event()
    release = threading.Event()

    def delayed_writer(record):
        if record.status == JobStatus.RUNNING:
            started.set()
            release.wait(timeout=5)
        original_writer(record)

    monkeypatch.setattr(state, "_write_state_file_atomic", delayed_writer)
    update_task = asyncio.create_task(state.update_job_status(job_id, JobStatus.RUNNING))
    assert await asyncio.to_thread(started.wait, 5)
    update_task.cancel()
    await asyncio.sleep(0)
    update_task.cancel()
    await asyncio.sleep(0)
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await update_task

    job_info = await state.get_job_info(job_id)
    payload = json.loads((job_dir / "state.json").read_text(encoding="utf-8"))
    assert job_info is not None
    assert job_info.status == JobStatus.RUNNING
    assert payload["status"] == "RUNNING"


@pytest.mark.asyncio
async def test_state_change_preserves_cancellation_when_writer_fails(monkeypatch, tmp_path):
    job_id = "cancelled-failed-persist"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "cancelled_failed_persist", str(job_dir))
    original_bytes = (job_dir / "state.json").read_bytes()
    started = threading.Event()
    release = threading.Event()

    def delayed_failure(_record):
        started.set()
        release.wait(timeout=5)
        raise OSError("state write failed")

    monkeypatch.setattr(state, "_write_state_file_atomic", delayed_failure)
    update_task = asyncio.create_task(state.update_job_status(job_id, JobStatus.RUNNING))
    assert await asyncio.to_thread(started.wait, 5)
    update_task.cancel()
    await asyncio.sleep(0)
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await update_task

    job_info = await state.get_job_info(job_id)
    assert job_info is not None
    assert job_info.status == JobStatus.PENDING
    assert (job_dir / "state.json").read_bytes() == original_bytes


@pytest.mark.asyncio
async def test_post_commit_directory_close_failure_keeps_state_consistent(
    monkeypatch,
    tmp_path,
):
    job_id = "post-commit-close"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "post_commit_close", str(job_dir))
    original_close = state.os.close

    def close_then_fail(file_descriptor):
        original_close(file_descriptor)
        raise OSError("directory close failed")

    monkeypatch.setattr(state.os, "close", close_then_fail)

    assert await state.update_job_status(job_id, JobStatus.RUNNING) is True
    job_info = await state.get_job_info(job_id)
    payload = json.loads((job_dir / "state.json").read_text(encoding="utf-8"))
    assert job_info is not None
    assert job_info.status == JobStatus.RUNNING
    assert payload["status"] == "RUNNING"


@pytest.mark.asyncio
async def test_non_json_result_fails_before_commit(tmp_path):
    job_id = "non-json-result"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "non_json_result", str(job_dir))
    state_path = job_dir / "state.json"
    original_bytes = state_path.read_bytes()

    with pytest.raises(ValueError):
        await state.update_job_status(
            job_id,
            JobStatus.COMPLETED,
            result={"unsupported"},
        )

    job_info = await state.get_job_info(job_id)
    assert job_info is not None
    assert job_info.status == JobStatus.PENDING
    assert state_path.read_bytes() == original_bytes
    assert list(tmp_path.glob(f".{job_id}.state-*.tmp")) == []


@pytest.mark.asyncio
async def test_persisted_completed_state_round_trips_v1(tmp_path):
    job_id = "completed-round-trip"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "completed_round_trip", str(job_dir))
    await state.update_job_status(
        job_id,
        JobStatus.COMPLETED,
        result={"items": [1, "two"]},
        duration=2.5,
        logs={"stdout": "done", "stderr": ""},
    )

    persisted = PersistedJobStateV1.model_validate_json(
        (job_dir / "state.json").read_text(encoding="utf-8")
    )
    assert persisted.job_id == job_id
    assert persisted.jobname == "completed_round_trip"
    assert persisted.status == JobStatus.COMPLETED
    assert persisted.result == {"items": [1, "two"]}
    assert persisted.duration_seconds == 2.5
    assert persisted.run_duration_seconds == 2.5
    assert persisted.completed_at is not None
    assert persisted.logs == {"stdout": "done", "stderr": ""}


@pytest.mark.asyncio
async def test_lifecycle_timestamps_are_fixed_at_first_transition(monkeypatch, tmp_path):
    job_id = "lifecycle-timestamps"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "lifecycle_timestamps", str(job_dir))
    initial = await state.get_job_info(job_id)
    assert initial is not None
    started_at = initial.submitted_at + timedelta(seconds=3)
    completed_at = started_at + timedelta(seconds=2)
    times = iter([started_at, completed_at, completed_at + timedelta(seconds=10)])
    monkeypatch.setattr(state, "_now", lambda: next(times))

    await state.update_job_status(job_id, JobStatus.RUNNING)
    await state.update_job_status(job_id, JobStatus.COMPLETED, result={"ok": True}, duration=2.0)
    await state.update_job_status(job_id, JobStatus.COMPLETED, result={"ok": True})

    record = await state.get_job_info(job_id)
    persisted = PersistedJobStateV1.model_validate_json(
        (job_dir / "state.json").read_text(encoding="utf-8")
    )
    assert record is not None
    assert record.started_at == started_at
    assert record.completed_at == completed_at
    assert record.queue_wait_seconds == 3.0
    assert record.run_duration_seconds == 2.0
    assert record.duration_seconds == 2.0
    assert persisted.started_at == started_at
    assert persisted.completed_at == completed_at
    assert persisted.queue_wait_seconds == 3.0
    assert persisted.run_duration_seconds == 2.0


@pytest.mark.asyncio
async def test_persisted_error_state_round_trips_v1(tmp_path):
    job_id = "failed-round-trip"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "failed_round_trip", str(job_dir))
    nested_worker_error = JobError(
        code=JobErrorCode.USER_SCRIPT_FAILED,
        message="nested worker failure",
        worker_result={"stage": "crawl"},
    )
    error = JobError(
        code=JobErrorCode.WORKER_EXECUTION_FAILED,
        message="worker failed",
        traceback="trace",
        cleanup_failures=[CleanupFailure(resource="browser", message="close failed")],
        exit_code=2,
        worker_result={"partial": [1, 2]},
        worker_error=nested_worker_error,
    )
    await state.update_job_status(job_id, JobStatus.FAILED, result=error)

    persisted = PersistedJobStateV1.model_validate_json(
        (job_dir / "state.json").read_text(encoding="utf-8")
    )
    restored_error = JobError.model_validate(persisted.result)
    assert persisted.status == JobStatus.FAILED
    assert restored_error.model_dump(mode="json") == error.model_dump(mode="json")
    assert JobError.model_validate(restored_error.worker_error) == nested_worker_error


@pytest.mark.asyncio
async def test_non_json_nested_error_value_fails_before_commit(tmp_path):
    job_id = "non-json-error"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "non_json_error", str(job_dir))
    state_path = job_dir / "state.json"
    original_bytes = state_path.read_bytes()
    error = JobError(
        code=JobErrorCode.PROCESSING_FAILED,
        message="bad worker payload",
        worker_result={"unsupported"},
    )

    with pytest.raises(ValueError):
        await state.update_job_status(job_id, JobStatus.FAILED, result=error)

    job_info = await state.get_job_info(job_id)
    assert job_info is not None
    assert job_info.status == JobStatus.PENDING
    assert state_path.read_bytes() == original_bytes


@pytest.mark.asyncio
async def test_concurrent_updates_preserve_disk_order(monkeypatch, tmp_path):
    job_id = "ordered-updates"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "ordered_updates", str(job_dir))
    original_writer = state._write_state_file_atomic
    running_started = threading.Event()
    completed_started = threading.Event()
    release_running = threading.Event()

    def ordered_writer(record):
        if record.status == JobStatus.RUNNING:
            running_started.set()
            release_running.wait(timeout=5)
        elif record.status == JobStatus.COMPLETED:
            completed_started.set()
        original_writer(record)

    monkeypatch.setattr(state, "_write_state_file_atomic", ordered_writer)
    running_task = asyncio.create_task(
        state.update_job_status(job_id, JobStatus.RUNNING)
    )
    assert await asyncio.to_thread(running_started.wait, 5)
    completed_task = asyncio.create_task(
        state.update_job_status(job_id, JobStatus.COMPLETED, result={"ok": True})
    )
    await asyncio.sleep(0.05)
    assert not completed_started.is_set()
    release_running.set()
    await asyncio.gather(running_task, completed_task)

    job_info = await state.get_job_info(job_id)
    payload = json.loads((job_dir / "state.json").read_text(encoding="utf-8"))
    assert job_info is not None
    assert job_info.status == JobStatus.COMPLETED
    assert payload["status"] == "COMPLETED"


@pytest.mark.asyncio
async def test_remove_job_state_deletes_persisted_state(tmp_path):
    job_id = "remove-persisted"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "remove_persisted", str(job_dir))

    await state.remove_job_state(job_id)

    assert not (job_dir / "state.json").exists()
    assert await state.get_job_info(job_id) is None


@pytest.mark.asyncio
async def test_remove_job_state_accepts_missing_persisted_file(tmp_path):
    job_id = "remove-missing-persisted"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "remove_missing_persisted", str(job_dir))
    (job_dir / "state.json").unlink()

    await state.remove_job_state(job_id)

    assert await state.get_job_info(job_id) is None


@pytest.mark.asyncio
async def test_remove_job_state_failure_preserves_disk_and_memory(monkeypatch, tmp_path):
    job_id = "remove-failure"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "remove_failure", str(job_dir))
    original_unlink = state.Path.unlink

    def fail_state_unlink(path, *args, **kwargs):
        if path.name == "state.json":
            raise OSError("unlink failed")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(state.Path, "unlink", fail_state_unlink)

    with pytest.raises(OSError, match="unlink failed"):
        await state.remove_job_state(job_id)

    assert (job_dir / "state.json").exists()
    assert await state.get_job_info(job_id) is not None


@pytest.mark.asyncio
async def test_post_delete_directory_close_failure_removes_state(monkeypatch, tmp_path):
    job_id = "post-delete-close"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "post_delete_close", str(job_dir))
    original_close = state.os.close

    def close_then_fail(file_descriptor):
        original_close(file_descriptor)
        raise OSError("directory close failed")

    monkeypatch.setattr(state.os, "close", close_then_fail)

    await state.remove_job_state(job_id)

    assert not (job_dir / "state.json").exists()
    assert await state.get_job_info(job_id) is None


@pytest.mark.asyncio
async def test_state_persistence_does_not_block_event_loop(monkeypatch, tmp_path):
    job_id = "slow-persistence"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    original_writer = state._write_state_file_atomic

    def slow_writer(record):
        time.sleep(0.1)
        original_writer(record)

    monkeypatch.setattr(state, "_write_state_file_atomic", slow_writer)
    started = time.perf_counter()
    persistence_task = asyncio.create_task(
        state.set_initial_status(job_id, "slow_persistence", str(job_dir))
    )
    await asyncio.sleep(0.01)
    heartbeat_elapsed = time.perf_counter() - started
    await persistence_task

    assert heartbeat_elapsed < 0.05


@pytest.mark.asyncio
async def test_recovery_restores_terminal_and_pending_jobs(tmp_path):
    completed_dir, _ = _write_persisted_job(
        tmp_path,
        "completed-job",
        "completed_name",
        JobStatus.COMPLETED,
        {"items": [1, 2]},
    )
    failed_error = JobError(code=JobErrorCode.USER_SCRIPT_FAILED, message="boom")
    _write_persisted_job(
        tmp_path,
        "failed-job",
        "failed_name",
        JobStatus.FAILED,
        failed_error.model_dump(mode="json"),
    )
    pending_dir, _ = _write_persisted_job(
        tmp_path,
        "pending-job",
        "pending_name",
        JobStatus.PENDING,
        create_script=True,
    )

    pending_jobs = await state.recover_persisted_jobs(tmp_path)

    assert [job.job_id for job in pending_jobs] == ["pending-job"]
    assert pending_jobs[0].script_path == str(pending_dir / "script.py")
    completed = await state.get_job_info("completed-job")
    failed = await state.get_job_info("failed-job")
    assert completed is not None
    assert completed.job_path == str(completed_dir)
    assert completed.result == {"items": [1, 2]}
    assert failed is not None
    assert failed.status == JobStatus.FAILED
    assert isinstance(failed.result, JobError)
    assert failed.result.code == JobErrorCode.USER_SCRIPT_FAILED
    assert await state.is_job_submitted("pending_name")
    assert not await state.is_job_submitted("completed_name")
    assert not await state.is_job_submitted("failed_name")


@pytest.mark.asyncio
async def test_recovery_persists_running_job_as_interrupted(tmp_path):
    _, state_path = _write_persisted_job(
        tmp_path,
        "running-job",
        "running_name",
        JobStatus.RUNNING,
    )

    pending_jobs = await state.recover_persisted_jobs(tmp_path)

    recovered = await state.get_job_info("running-job")
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert pending_jobs == []
    assert recovered is not None
    assert recovered.status == JobStatus.INTERRUPTED
    assert recovered.result.code == JobErrorCode.SERVICE_SHUTDOWN
    assert payload["status"] == JobStatus.INTERRUPTED
    assert payload["result"]["code"] == JobErrorCode.SERVICE_SHUTDOWN
    assert not await state.is_job_submitted("running_name")


@pytest.mark.asyncio
async def test_recovery_write_failure_aborts_before_registry_install(monkeypatch, tmp_path):
    _write_persisted_job(tmp_path, "running-job", "running_name", JobStatus.RUNNING)

    def fail_write(_record):
        raise OSError("disk full")

    monkeypatch.setattr(state, "_write_state_file_atomic", fail_write)

    with pytest.raises(OSError, match="disk full"):
        await state.recover_persisted_jobs(tmp_path)

    assert await state.get_job_info("running-job") is None
    assert not await state.is_job_submitted("running_name")


@pytest.mark.asyncio
async def test_recovery_skips_invalid_job_without_modifying_file(tmp_path):
    job_dir = tmp_path / "invalid-job"
    job_dir.mkdir()
    state_path = job_dir / "state.json"
    original = b'{"schema_version": 1, "job_id": "other-job"}\n'
    state_path.write_bytes(original)

    assert await state.recover_persisted_jobs(tmp_path) == []

    assert state_path.read_bytes() == original
    assert await state.get_job_info("invalid-job") is None


@pytest.mark.asyncio
async def test_recovery_rejects_unsupported_schema_version(tmp_path):
    job_dir = tmp_path / "future-job"
    job_dir.mkdir()
    (job_dir / "state.json").write_text(
        json.dumps({"schema_version": 2, "job_id": "future-job"}),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="Unsupported persisted job state schema version 2"):
        await state.recover_persisted_jobs(tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize("schema_fragment", ["", '"schema_version": true,', '"schema_version": 1.0,'])
async def test_recovery_skips_missing_or_non_integer_schema_version_unchanged(
    tmp_path,
    schema_fragment,
):
    job_dir = tmp_path / "invalid-version"
    job_dir.mkdir()
    (job_dir / "script.py").write_text("pass\n", encoding="utf-8")
    original = (
        "{"
        f"{schema_fragment}"
        '"job_id":"invalid-version",'
        '"jobname":"invalid_version",'
        '"status":"PENDING",'
        '"result":null,'
        '"logs":null,'
        '"submitted_at":"2026-07-23T01:02:03",'
        '"duration_seconds":null'
        "}\n"
    ).encode()
    state_path = job_dir / "state.json"
    state_path.write_bytes(original)

    assert await state.recover_persisted_jobs(tmp_path) == []
    assert state_path.read_bytes() == original
    assert await state.get_job_info("invalid-version") is None
    assert not await state.is_job_submitted("invalid_version")


@pytest.mark.asyncio
@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
async def test_recovery_skips_non_standard_json_constants_unchanged(tmp_path, constant):
    job_dir = tmp_path / "non-standard-json"
    job_dir.mkdir()
    original = (
        "{"
        '"schema_version":1,'
        '"job_id":"non-standard-json",'
        '"jobname":"non_standard_json",'
        '"status":"COMPLETED",'
        f'"result":{{"value":{constant}}},'
        '"logs":null,'
        '"submitted_at":"2026-07-23T01:02:03",'
        '"duration_seconds":null'
        "}\n"
    ).encode()
    state_path = job_dir / "state.json"
    state_path.write_bytes(original)

    assert await state.recover_persisted_jobs(tmp_path) == []
    assert state_path.read_bytes() == original
    assert await state.get_job_info("non-standard-json") is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("result", "duration"),
    [
        ('{"nested":[1e400]}', "null"),
        ('{"nested":[-1e400]}', "null"),
        ("null", "1e400"),
        ("null", "-1e400"),
    ],
)
async def test_recovery_skips_overflowed_non_finite_numbers_unchanged(
    tmp_path,
    result,
    duration,
):
    job_dir = tmp_path / "overflowed-number"
    job_dir.mkdir()
    original = (
        "{"
        '"schema_version":1,'
        '"job_id":"overflowed-number",'
        '"jobname":"overflowed_number",'
        '"status":"COMPLETED",'
        f'"result":{result},'
        '"logs":null,'
        '"submitted_at":"2026-07-23T01:02:03",'
        f'"duration_seconds":{duration}'
        "}\n"
    ).encode()
    state_path = job_dir / "state.json"
    state_path.write_bytes(original)

    assert await state.recover_persisted_jobs(tmp_path) == []
    assert state_path.read_bytes() == original
    assert await state.get_job_info("overflowed-number") is None
    assert not await state.is_job_submitted("overflowed_number")


@pytest.mark.asyncio
async def test_recovery_rejects_duplicate_active_names_before_publication(tmp_path):
    _write_persisted_job(
        tmp_path,
        "pending-a",
        "same_name",
        JobStatus.PENDING,
        create_script=True,
    )
    _write_persisted_job(
        tmp_path,
        "pending-b",
        "same_name",
        JobStatus.PENDING,
        create_script=True,
    )

    with pytest.raises(RuntimeError, match="Duplicate active job name"):
        await state.recover_persisted_jobs(tmp_path)

    assert await state.get_job_info("pending-a") is None
    assert not await state.is_job_submitted("same_name")


@pytest.mark.asyncio
async def test_recovery_requires_empty_runtime_registries(tmp_path):
    _, state_path = _write_persisted_job(
        tmp_path,
        "running-job",
        "running_name",
        JobStatus.RUNNING,
    )
    original = state_path.read_bytes()
    existing_dir = tmp_path / "existing-job"
    existing_dir.mkdir()
    await state.set_initial_status("existing-job", "existing_name", str(existing_dir))

    with pytest.raises(RuntimeError, match="non-empty runtime registries"):
        await state.recover_persisted_jobs(tmp_path)

    assert await state.get_job_info("running-job") is None
    assert state_path.read_bytes() == original

@pytest.mark.asyncio
async def test_submitted_job_tracking():
    """중복 작업 제출 방지 로직 테스트"""
    job_name = "tracking_test"

    # 처음 추가 시 성공 (True 반환)
    assert await state.add_submitted_job(job_name) is True
    # 현재 제출된 상태인지 확인
    assert await state.is_job_submitted(job_name) is True

    # 다시 추가 시 실패 (False 반환)
    assert await state.add_submitted_job(job_name) is False

    # 제거
    await state.remove_submitted_job(job_name)
    # 제거 후 제출되지 않은 상태인지 확인
    assert await state.is_job_submitted(job_name) is False
