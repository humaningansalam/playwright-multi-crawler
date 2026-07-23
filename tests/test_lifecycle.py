import asyncio
import os
import signal

import pytest

from src import main as main_module
from src.core import job_queue, playwright_manager, state_manager as state
from src.models.job import JobError, JobErrorCode, JobStatus, WorkerCompleted
from src.worker import job_processor


class _Gauge:
    def inc(self):
        pass

    def dec(self):
        pass


class _Counter:
    def inc(self):
        pass


class _Process:
    returncode = 0
    stdout = object()
    stderr = object()

    async def wait(self):
        return self.returncode


async def _prepare_processor(monkeypatch, result_read):
    process = _Process()

    async def create_process(*_args, **_kwargs):
        return process

    async def stream_output(*_args, **_kwargs):
        return ""

    async def drain(*_args, **_kwargs):
        return "stdout tail", "stderr tail"

    async def terminate(*_args, **_kwargs):
        return None

    async def read_result(_job_path):
        return result_read

    monkeypatch.setattr(job_processor.asyncio, "create_subprocess_exec", create_process)
    monkeypatch.setattr(job_processor, "_stream_output_to_log", stream_output)
    monkeypatch.setattr(job_processor, "_drain_output_tasks", drain)
    monkeypatch.setattr(job_processor, "_terminate_process", terminate)
    monkeypatch.setattr(job_processor, "_read_result_file", read_result)
    monkeypatch.setattr(job_processor.metrics, "active_jobs", _Gauge())
    monkeypatch.setattr(job_processor.metrics, "jobs_completed", _Counter())
    monkeypatch.setattr(job_processor.metrics, "jobs_failed", _Counter())
    return process


@pytest.mark.asyncio
async def test_completion_persists_full_lifecycle(monkeypatch, tmp_path):
    job_id = "lifecycle-completed"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    script = job_dir / "script.py"
    script.write_text("pass\n", encoding="utf-8")
    await state.set_initial_status(job_id, "lifecycle_completed", str(job_dir))
    await state.add_submitted_job("lifecycle_completed")
    await _prepare_processor(
        monkeypatch,
        job_processor.ResultFileRead(
            job_processor.ResultFileState.LOADED,
            WorkerCompleted(result={"ok": True}),
        ),
    )

    await job_processor._process_job_internal(str(script), "lifecycle_completed", job_id)

    record = await state.get_job_info(job_id)
    assert record is not None
    assert record.status == JobStatus.COMPLETED
    assert record.started_at is not None
    assert record.completed_at is not None
    assert record.queue_wait_seconds is not None
    assert record.run_duration_seconds is not None
    assert record.duration_seconds == record.run_duration_seconds
    assert not await state.is_job_submitted("lifecycle_completed")


@pytest.mark.asyncio
async def test_timeout_persists_failed_lifecycle(monkeypatch, tmp_path):
    job_id = "lifecycle-timeout"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    script = job_dir / "script.py"
    script.write_text("pass\n", encoding="utf-8")
    await state.set_initial_status(job_id, "lifecycle_timeout", str(job_dir))
    await state.add_submitted_job("lifecycle_timeout")
    await _prepare_processor(
        monkeypatch,
        job_processor.ResultFileRead(job_processor.ResultFileState.MISSING),
    )

    async def timeout(awaitable, timeout):
        assert timeout == job_processor.JOB_TIMEOUT_SECONDS
        awaitable.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(job_processor.asyncio, "wait_for", timeout)

    await job_processor._process_job_internal(str(script), "lifecycle_timeout", job_id)

    record = await state.get_job_info(job_id)
    assert record is not None
    assert record.status == JobStatus.FAILED
    assert record.result.code == JobErrorCode.WORKER_TIMED_OUT
    assert record.completed_at is not None
    assert record.run_duration_seconds is not None
    assert not await state.is_job_submitted("lifecycle_timeout")


@pytest.mark.asyncio
async def test_cancelled_job_remains_cancelled_after_recovery(tmp_path):
    job_id = "lifecycle-cancelled"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    await state.set_initial_status(job_id, "lifecycle_cancelled", str(job_dir))
    await state.update_job_status(job_id, JobStatus.RUNNING)
    await state.update_job_status(
        job_id,
        JobStatus.CANCELLED,
        JobError(code=JobErrorCode.JOB_CANCELLED, message="cancelled"),
        duration=1.0,
    )

    async with state._job_status_lock:
        state._job_status_and_results.clear()
    async with state._submitted_jobs_lock:
        state._submitted_jobs.clear()
    assert await state.recover_persisted_jobs(tmp_path) == []

    recovered = await state.get_job_info(job_id)
    assert recovered is not None
    assert recovered.status == JobStatus.CANCELLED
    assert recovered.result.code == JobErrorCode.JOB_CANCELLED
    assert recovered.completed_at is not None
    assert recovered.run_duration_seconds == 1.0


@pytest.mark.asyncio
async def test_browser_loss_uses_non_draining_shutdown(monkeypatch):
    calls = []

    class _Monitor:
        def __init__(self, **_kwargs):
            pass

        def start(self):
            calls.append("monitor_start")

        def stop(self):
            calls.append("monitor_stop")

    async def recover(_root):
        return []

    async def browser_start():
        calls.append("browser_start")

    async def browser_shutdown():
        calls.append("browser_shutdown")

    async def stop_workers(*, drain=True):
        calls.append(("stop_workers", drain))

    monkeypatch.setattr(main_module, "ResourceMonitor", _Monitor)
    monkeypatch.setattr(main_module.tool_utils, "ensure_job_folder", lambda: None)
    monkeypatch.setattr(main_module.state_manager, "recover_persisted_jobs", recover)
    monkeypatch.setattr(main_module.job_queue, "restore_jobs", lambda _jobs: None)
    monkeypatch.setattr(main_module.tool_utils, "start_display", lambda: True)
    monkeypatch.setattr(main_module.tool_utils, "stop_display", lambda: calls.append("display_stop"))
    monkeypatch.setattr(main_module.playwright_manager, "start", browser_start)
    monkeypatch.setattr(main_module.playwright_manager, "shutdown", browser_shutdown)
    monkeypatch.setattr(main_module.job_processor, "start_workers", lambda: calls.append("workers_start"))
    monkeypatch.setattr(main_module.job_processor, "stop_workers", stop_workers)
    monkeypatch.setattr(main_module.tool_utils, "periodic_cleanup", lambda: asyncio.sleep(0))
    monkeypatch.setattr(playwright_manager, "_signal_process", lambda pid, sig: calls.append((pid, sig)))
    monkeypatch.setattr(playwright_manager, "_shutting_down", False)
    monkeypatch.setattr(playwright_manager, "_requested_exit_code", None)
    monkeypatch.setenv("RUN_HEAVY_STARTUP", "true")

    async with main_module.app.router.lifespan_context(main_module.app):
        playwright_manager._on_browser_disconnected(object())
        assert playwright_manager.requested_exit_code() == 1

    assert (os.getpid(), signal.SIGTERM) in calls
    assert ("stop_workers", False) in calls
    assert "browser_shutdown" in calls
    assert "display_stop" in calls


@pytest.mark.asyncio
async def test_recovery_requeues_pending_and_interrupts_running_with_timestamps(tmp_path):
    pending_dir = tmp_path / "pending"
    pending_dir.mkdir()
    (pending_dir / "script.py").write_text("pass\n", encoding="utf-8")
    running_dir = tmp_path / "running"
    running_dir.mkdir()
    await state.set_initial_status("pending", "pending_name", str(pending_dir))
    await state.set_initial_status("running", "running_name", str(running_dir))
    await state.update_job_status("running", JobStatus.RUNNING)

    async with state._job_status_lock:
        state._job_status_and_results.clear()
    async with state._submitted_jobs_lock:
        state._submitted_jobs.clear()
    pending = await state.recover_persisted_jobs(tmp_path)
    job_queue.restore_jobs(pending)

    interrupted = await state.get_job_info("running")
    assert interrupted is not None
    assert interrupted.status == JobStatus.INTERRUPTED
    assert interrupted.completed_at is not None
    assert interrupted.run_duration_seconds is not None
    assert job_queue.qsize() == 1
    queued = await job_queue.get_job()
    assert queued is not None
    assert queued.job_id == "pending"
    assert await state.is_job_submitted("pending_name")
    assert not await state.is_job_submitted("running_name")
