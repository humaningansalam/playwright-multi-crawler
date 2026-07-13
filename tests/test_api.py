import asyncio
import json
import os
import socket
import tomllib
import time
from pathlib import Path

import httpx
import pytest
from fastapi import HTTPException

from src.config import JOB_FOLDER
from src.api import jobs as jobs_api
from src.api.jobs import download_file_endpoint, stream_job_logs_endpoint
from src.core import state_manager as state
from src.core import job_queue
from src.main import app
from src.worker import job_processor
from src.core import playwright_manager
from src.common import tool_utils
from src.models.job import JobStatus


def test_job_api_status_sets_use_shared_enum():
    assert jobs_api.ACTIVE_JOB_STATUSES == frozenset({JobStatus.PENDING, JobStatus.RUNNING})
    assert jobs_api.TERMINAL_JOB_STATUSES == frozenset({
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
        JobStatus.INTERRUPTED,
    })


def test_browser_launch_options_omit_executable_by_default(monkeypatch):
    monkeypatch.setattr(playwright_manager, "BROWSER_EXECUTABLE_PATH", None)

    options = playwright_manager._browser_launch_options()

    assert options["headless"] is False
    assert options["handle_sigint"] is False
    assert options["handle_sigterm"] is False
    assert options["handle_sighup"] is False
    assert "--remote-debugging-address=127.0.0.1" in options["args"]
    assert "executable_path" not in options


def test_browser_launch_options_use_configured_executable(monkeypatch):
    monkeypatch.setattr(playwright_manager, "BROWSER_EXECUTABLE_PATH", "/usr/bin/google-chrome")

    options = playwright_manager._browser_launch_options()

    assert options["executable_path"] == "/usr/bin/google-chrome"


class _FakeBrowser:
    def __init__(self, calls):
        self.calls = calls

    def on(self, event, _callback):
        self.calls.append(f"browser.on:{event}")

    async def close(self):
        self.calls.append("browser.close")
        playwright_manager._on_browser_disconnected()

    def is_connected(self):
        return True


class _FakeChromium:
    def __init__(self, browser, calls):
        self.browser = browser
        self.calls = calls

    async def launch(self, **_options):
        self.calls.append("chromium.launch")
        return self.browser


class _FakePlaywright:
    def __init__(self, browser, calls):
        self.chromium = _FakeChromium(browser, calls)
        self.calls = calls

    async def stop(self):
        self.calls.append("playwright.stop")


class _FakePlaywrightStarter:
    def __init__(self, playwright, calls):
        self.playwright = playwright
        self.calls = calls

    async def start(self):
        self.calls.append("playwright.start")
        return self.playwright


def _install_fake_playwright(monkeypatch, calls):
    browser = _FakeBrowser(calls)
    playwright = _FakePlaywright(browser, calls)
    starter = _FakePlaywrightStarter(playwright, calls)
    monkeypatch.setattr(playwright_manager, "async_playwright", lambda: starter)
    monkeypatch.setattr(playwright_manager, "_playwright", None)
    monkeypatch.setattr(playwright_manager, "_browser", None)
    monkeypatch.setattr(playwright_manager, "_shutting_down", False)
    return browser, playwright


@pytest.mark.asyncio
async def test_browser_start_rejects_occupied_ipv4_cdp_port(monkeypatch):
    calls = []

    class _UnexpectedPlaywrightStarter:
        async def start(self):
            calls.append("playwright.start")
            raise AssertionError(
                "Playwright must not start while the CDP port is occupied"
            )

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    occupied_port = listener.getsockname()[1]
    monkeypatch.setattr(playwright_manager, "CDP_PORT", occupied_port)
    monkeypatch.setattr(
        playwright_manager,
        "async_playwright",
        lambda: _UnexpectedPlaywrightStarter(),
    )
    monkeypatch.setattr(playwright_manager, "_playwright", None)
    monkeypatch.setattr(playwright_manager, "_browser", None)

    try:
        with pytest.raises(
            RuntimeError,
            match=f"127.0.0.1:{occupied_port}.*already in use",
        ):
            await playwright_manager.start()
    finally:
        listener.close()

    assert calls == []


@pytest.mark.asyncio
async def test_cdp_readiness_probe_requires_websocket_url(monkeypatch):
    async def fetch_invalid_version():
        return {"Browser": "Chrome/150"}

    monkeypatch.setattr(
        playwright_manager,
        "_fetch_cdp_version",
        fetch_invalid_version,
        raising=False,
    )
    monkeypatch.setattr(
        playwright_manager,
        "CDP_READY_TIMEOUT_SECONDS",
        0,
        raising=False,
    )

    with pytest.raises(RuntimeError, match="webSocketDebuggerUrl"):
        await playwright_manager._wait_for_cdp_ready()


@pytest.mark.asyncio
async def test_browser_start_cleans_up_after_cdp_probe_failure(monkeypatch):
    calls = []
    _install_fake_playwright(monkeypatch, calls)
    monkeypatch.setattr(
        playwright_manager,
        "_assert_cdp_port_available",
        lambda: calls.append("cdp.preflight"),
        raising=False,
    )

    async def fail_cdp_probe():
        calls.append("cdp.probe")
        raise RuntimeError("CDP endpoint did not become ready")

    exit_codes = []
    monkeypatch.setattr(
        playwright_manager,
        "_wait_for_cdp_ready",
        fail_cdp_probe,
        raising=False,
    )
    monkeypatch.setattr(playwright_manager, "_exit_process", lambda code: exit_codes.append(code))

    with pytest.raises(RuntimeError, match="CDP endpoint did not become ready"):
        await playwright_manager.start()

    assert calls == [
        "cdp.preflight",
        "playwright.start",
        "chromium.launch",
        "cdp.probe",
        "browser.close",
        "playwright.stop",
    ]
    assert exit_codes == []
    assert playwright_manager._browser is None
    assert playwright_manager._playwright is None
    assert playwright_manager._shutting_down is True


@pytest.mark.asyncio
async def test_browser_start_registers_disconnect_handler_after_cdp_probe(monkeypatch):
    calls = []
    browser, playwright = _install_fake_playwright(monkeypatch, calls)
    monkeypatch.setattr(
        playwright_manager,
        "_assert_cdp_port_available",
        lambda: calls.append("cdp.preflight"),
        raising=False,
    )

    async def pass_cdp_probe():
        calls.append("cdp.probe")

    monkeypatch.setattr(
        playwright_manager,
        "_wait_for_cdp_ready",
        pass_cdp_probe,
        raising=False,
    )

    await playwright_manager.start()

    assert calls == [
        "cdp.preflight",
        "playwright.start",
        "chromium.launch",
        "cdp.probe",
        "browser.on:disconnected",
    ]
    assert playwright_manager._browser is browser
    assert playwright_manager._playwright is playwright
    assert playwright_manager._shutting_down is False

    await playwright_manager.shutdown()
    assert calls[-2:] == ["browser.close", "playwright.stop"]


# 테스트용 간단한 스크립트 파일 내용
DUMMY_SCRIPT_CONTENT = """
import asyncio
async def crawl(page, context, job_path):
    print("Dummy crawl running")
    await asyncio.sleep(0.1) # 아주 짧은 작업 시간
    return {'status': 'success'}
"""


def _pyproject_version() -> str:
    pyproject_path = Path(__file__).resolve().parents[1] / "pyproject.toml"
    with pyproject_path.open("rb") as handle:
        return tomllib.load(handle)["project"]["version"]


@pytest.mark.asyncio
async def test_upload_save_reads_bounded_chunks(tmp_path):
    class _ChunkedUpload:
        def __init__(self):
            self.read_sizes = []
            self.chunks = iter((b"first", b"second", b""))

        async def read(self, size):
            self.read_sizes.append(size)
            return next(self.chunks)

    upload = _ChunkedUpload()
    destination = tmp_path / "upload.bin"

    await jobs_api._save_upload_file(upload, str(destination))

    assert upload.read_sizes == [jobs_api.UPLOAD_CHUNK_BYTES] * 3
    assert destination.read_bytes() == b"firstsecond"


@pytest.mark.asyncio
async def test_upload_save_does_not_block_event_loop(monkeypatch):
    class _Upload:
        def __init__(self):
            self.chunks = iter((b"payload", b""))

        async def read(self, _size):
            return next(self.chunks)

    class _SlowFile:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def write(self, _chunk):
            time.sleep(0.1)

        def close(self):
            return None

    monkeypatch.setattr(jobs_api, "open", lambda *_args, **_kwargs: _SlowFile(), raising=False)

    started = time.perf_counter()
    save_task = asyncio.create_task(jobs_api._save_upload_file(_Upload(), "unused"))
    await asyncio.sleep(0.01)
    heartbeat_elapsed = time.perf_counter() - started
    await save_task

    assert heartbeat_elapsed < 0.05


@pytest.mark.asyncio
@pytest.mark.parametrize("slow_stage", ["open", "close"])
async def test_upload_file_lifecycle_does_not_block_event_loop(monkeypatch, slow_stage):
    class _Upload:
        async def read(self, _size):
            return b""

    class _File:
        def close(self):
            if slow_stage == "close":
                time.sleep(0.1)

    def open_file(*_args, **_kwargs):
        if slow_stage == "open":
            time.sleep(0.1)
        return _File()

    monkeypatch.setattr(jobs_api, "open", open_file, raising=False)

    started = time.perf_counter()
    save_task = asyncio.create_task(jobs_api._save_upload_file(_Upload(), "unused"))
    await asyncio.sleep(0.01)
    heartbeat_elapsed = time.perf_counter() - started
    await save_task

    assert heartbeat_elapsed < 0.05


@pytest.mark.asyncio
async def test_upload_close_failure_does_not_replace_cancellation(monkeypatch):
    read_started = asyncio.Event()

    class _Upload:
        async def read(self, _size):
            read_started.set()
            await asyncio.Event().wait()

    class _File:
        def close(self):
            raise OSError("close failed")

    monkeypatch.setattr(jobs_api, "open", lambda *_args, **_kwargs: _File(), raising=False)

    save_task = asyncio.create_task(jobs_api._save_upload_file(_Upload(), "unused"))
    await read_started.wait()
    save_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await save_task


@pytest.mark.asyncio
async def test_request_file_operation_preserves_cancellation_after_disk_error():
    def delayed_disk_error():
        time.sleep(0.05)
        raise OSError("disk failed")

    operation = asyncio.create_task(jobs_api._run_file_operation(delayed_disk_error))
    await asyncio.sleep(0.01)
    operation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await operation


@pytest.mark.asyncio
async def test_submission_rollback_does_not_block_event_loop(monkeypatch, tmp_path):
    job_path = tmp_path / "partial-job"
    job_path.mkdir()

    def slow_rmtree(_path):
        time.sleep(0.1)

    monkeypatch.setattr(jobs_api.shutil, "rmtree", slow_rmtree)

    started = time.perf_counter()
    rollback_task = asyncio.create_task(
        jobs_api._rollback_job_submission("partial-job", "partial-job", str(job_path))
    )
    await asyncio.sleep(0.01)
    heartbeat_elapsed = time.perf_counter() - started
    await rollback_task

    assert heartbeat_elapsed < 0.05


@pytest.mark.asyncio
async def test_submit_cancellation_rolls_back_reserved_name_and_job_folder(
    monkeypatch,
    tmp_path,
):
    class _CancelledUpload:
        filename = "crawl.py"

        def __init__(self):
            self.closed = False

        async def read(self, _size):
            raise asyncio.CancelledError()

        async def close(self):
            self.closed = True

    class _Request:
        app = app

    upload = _CancelledUpload()
    job_name = "cancelled-upload"
    monkeypatch.setattr(jobs_api, "JOB_FOLDER", str(tmp_path))

    with pytest.raises(asyncio.CancelledError):
        await jobs_api.submit_job_endpoint(_Request(), job_name, upload, [])

    assert upload.closed is True
    assert not await state.is_job_submitted(job_name)
    assert list(tmp_path.iterdir()) == []


def test_browser_disconnect_requests_service_restart(monkeypatch):
    exit_codes = []
    monkeypatch.setattr(playwright_manager, "_exit_process", lambda code: exit_codes.append(code))
    monkeypatch.setattr(playwright_manager, "_shutting_down", False)

    playwright_manager._on_browser_disconnected()

    assert exit_codes == [1]


def test_browser_disconnect_during_shutdown_does_not_exit(monkeypatch):
    exit_codes = []
    monkeypatch.setattr(playwright_manager, "_exit_process", lambda code: exit_codes.append(code))
    monkeypatch.setattr(playwright_manager, "_shutting_down", True)

    playwright_manager._on_browser_disconnected()

    assert exit_codes == []

@pytest.mark.asyncio
async def test_health_check_reports_ready_service(client: httpx.AsyncClient, monkeypatch):
    monkeypatch.setattr("src.core.playwright_manager.is_browser_connected", lambda: True)

    response = await client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["browser_connected"] is True
    assert data["workers_ready"] is True
    assert data["queued_tasks"] == 0


@pytest.mark.asyncio
async def test_health_check_reports_unavailable_browser(client: httpx.AsyncClient, monkeypatch):
    monkeypatch.setattr("src.core.playwright_manager.is_browser_connected", lambda: False)

    response = await client.get("/health")

    assert response.status_code == 503
    assert response.json() == {
        "status": "unavailable",
        "browser_connected": False,
        "workers_ready": True,
        "queued_tasks": 0,
    }


@pytest.mark.asyncio
async def test_health_check_reports_unavailable_workers(client: httpx.AsyncClient, monkeypatch):
    monkeypatch.setattr("src.core.playwright_manager.is_browser_connected", lambda: True)
    app.state.job_submission_enabled = False

    response = await client.get("/health")

    assert response.status_code == 503
    assert response.json() == {
        "status": "unavailable",
        "browser_connected": True,
        "workers_ready": False,
        "queued_tasks": 0,
    }


@pytest.mark.asyncio
async def test_lifespan_stops_resource_monitor(monkeypatch):
    calls = []

    class _FakeMonitor:
        def __init__(self, **_kwargs):
            pass

        def start(self):
            calls.append("start")

        def stop(self):
            calls.append("stop")

    monkeypatch.setattr("src.main.ResourceMonitor", _FakeMonitor)
    monkeypatch.setattr("src.common.tool_utils.ensure_job_folder", lambda: None)
    monkeypatch.setattr("src.common.tool_utils.periodic_cleanup", lambda: asyncio.sleep(0))
    monkeypatch.setenv("RUN_HEAVY_STARTUP", "false")

    async with app.router.lifespan_context(app):
        pass

    assert calls == ["start", "stop"]


@pytest.mark.asyncio
async def test_periodic_cleanup_offloads_file_cleanup(monkeypatch):
    calls = []

    async def fake_to_thread(function, excluded_job_ids):
        calls.append((function, excluded_job_ids))
        raise asyncio.CancelledError()

    monkeypatch.setattr("src.common.tool_utils.asyncio.to_thread", fake_to_thread)

    with pytest.raises(asyncio.CancelledError):
        await tool_utils.periodic_cleanup()

    assert calls == [(tool_utils.clean_old_jobs, set())]


@pytest.mark.asyncio
async def test_periodic_cleanup_removes_state_for_deleted_job_folders(monkeypatch):
    removed_job_ids = []

    async def fake_to_thread(function, excluded_job_ids):
        assert function is tool_utils.clean_old_jobs
        assert excluded_job_ids == set()
        return ["expired-job-1", "expired-job-2"]

    async def fake_remove_job_state(job_id):
        removed_job_ids.append(job_id)

    async def stop_after_one_cleanup(_seconds):
        raise asyncio.CancelledError()

    monkeypatch.setattr("src.common.tool_utils.asyncio.to_thread", fake_to_thread)
    monkeypatch.setattr("src.common.tool_utils.state.remove_job_state", fake_remove_job_state)
    monkeypatch.setattr("src.common.tool_utils.asyncio.sleep", stop_after_one_cleanup)

    with pytest.raises(asyncio.CancelledError):
        await tool_utils.periodic_cleanup()

    assert removed_job_ids == ["expired-job-1", "expired-job-2"]


@pytest.mark.asyncio
async def test_periodic_cleanup_preserves_old_active_job(monkeypatch, tmp_path):
    job_id = "old-pending-job"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    (job_dir / "script.py").write_text("pass\n", encoding="utf-8")
    old_timestamp = time.time() - (tool_utils.JOB_RETENTION_DAYS + 1) * 24 * 3600
    os.utime(job_dir, (old_timestamp, old_timestamp))
    await state.set_initial_status(job_id, "old_pending", str(job_dir))

    async def stop_after_one_cleanup(_seconds):
        raise asyncio.CancelledError()

    monkeypatch.setattr(tool_utils, "JOB_FOLDER", str(tmp_path))
    monkeypatch.setattr(tool_utils.asyncio, "sleep", stop_after_one_cleanup)

    with pytest.raises(asyncio.CancelledError):
        await tool_utils.periodic_cleanup()

    assert job_dir.is_dir()
    assert await state.get_job_status(job_id) == JobStatus.PENDING


def test_cleanup_does_not_report_folder_deleted_when_rmtree_fails(monkeypatch, tmp_path):
    job_dir = tmp_path / "expired-job"
    job_dir.mkdir()
    old_timestamp = time.time() - (tool_utils.JOB_RETENTION_DAYS + 1) * 24 * 3600
    os.utime(job_dir, (old_timestamp, old_timestamp))

    def fail_rmtree(_path):
        raise OSError("disk error")

    monkeypatch.setattr(tool_utils, "JOB_FOLDER", str(tmp_path))
    monkeypatch.setattr(tool_utils.shutil, "rmtree", fail_rmtree)

    assert tool_utils.clean_old_jobs() == []
    assert job_dir.is_dir()


@pytest.mark.asyncio
async def test_metrics_endpoint(client: httpx.AsyncClient):
    response = await client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "python_info" in response.text


@pytest.mark.asyncio
async def test_openapi_metrics_200_content_type():
    responses = app.openapi()["paths"]["/metrics"]["get"]["responses"]["200"]["content"]
    assert "text/plain" in responses


@pytest.mark.asyncio
async def test_openapi_job_download_200_content_type():
    responses = app.openapi()["paths"]["/api/jobs/download/{job_id}/{filename}"]["get"]["responses"]["200"]["content"]
    assert "application/octet-stream" in responses


def test_openapi_job_results_200_documents_response_models():
    schema = app.openapi()["paths"]["/api/jobs/results/{job_id}"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]

    refs = {entry["$ref"] for entry in schema["anyOf"]}
    assert refs == {
        "#/components/schemas/JobProcessingResponse",
        "#/components/schemas/JobResultResponse",
    }


def test_openapi_job_routes_document_error_responses():
    openapi = app.openapi()

    assert {"400", "409", "503"} <= set(openapi["paths"]["/api/jobs/submit"]["post"]["responses"])
    assert "404" in openapi["paths"]["/api/jobs/status/{job_id}"]["get"]["responses"]
    assert "404" in openapi["paths"]["/api/jobs/results/{job_id}"]["get"]["responses"]
    assert {"403", "404"} <= set(openapi["paths"]["/api/jobs/download/{job_id}/{filename}"]["get"]["responses"])
    assert {"404", "409"} <= set(openapi["paths"]["/api/jobs/{job_id}/cancel"]["post"]["responses"])

    detail_schema = openapi["paths"]["/api/jobs/status/{job_id}"]["get"]["responses"]["404"]["content"]["application/json"]["schema"]
    assert detail_schema["required"] == ["detail"]
    assert detail_schema["properties"]["detail"]["type"] == "string"


def test_openapi_stream_and_health_error_content_types():
    openapi = app.openapi()
    stream_response = openapi["paths"]["/api/jobs/logs/{job_id}"]["get"]["responses"]["200"]
    health_response = openapi["paths"]["/health"]["get"]["responses"]["503"]

    assert set(stream_response["content"]) == {"text/event-stream"}
    assert stream_response["content"]["text/event-stream"]["schema"] == {"type": "string"}
    assert health_response["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/HealthResponse"
    }


def test_openapi_info_version_matches_pyproject():
    assert app.openapi()["info"]["version"] == _pyproject_version()

@pytest.mark.asyncio
async def test_submit_job_accepted(client: httpx.AsyncClient):
    """작업 제출 시 202 Accepted 와 job_id 반환 테스트"""
    job_name = f"submit_test_{int(time.time())}"
    files = {'script_file': ('dummy_script.py', DUMMY_SCRIPT_CONTENT, 'text/x-python')}
    data = {'jobname': job_name}

    response = await client.post("/api/jobs/submit", data=data, files=files)

    assert response.status_code == 202
    result = response.json()
    assert "job_id" in result
    assert isinstance(result["job_id"], str)
    assert result["status"] == "PENDING"

@pytest.mark.asyncio
async def test_get_initial_status(client: httpx.AsyncClient):
    """작업 제출 직후 상태가 PENDING 인지 테스트"""
    job_name = f"status_test_{int(time.time())}"
    files = {'script_file': ('dummy_script.py', DUMMY_SCRIPT_CONTENT, 'text/x-python')}
    data = {'jobname': job_name}

    submit_response = await client.post("/api/jobs/submit", data=data, files=files)
    assert submit_response.status_code == 202
    job_id = submit_response.json()["job_id"]

    # 제출 직후 상태 확인
    status_response = await client.get(f"/api/jobs/status/{job_id}")
    assert status_response.status_code == 200
    assert status_response.json()["status"] == "PENDING"

@pytest.mark.asyncio
async def test_get_results_for_pending_job(client: httpx.AsyncClient):
    """처리 중인 작업 결과 요청 시 적절한 응답 반환 테스트"""
    job_name = f"results_pending_test_{int(time.time())}"
    files = {'script_file': ('dummy_script.py', DUMMY_SCRIPT_CONTENT, 'text/x-python')}
    data = {'jobname': job_name}

    submit_response = await client.post("/api/jobs/submit", data=data, files=files)
    assert submit_response.status_code == 202
    job_id = submit_response.json()["job_id"]

    # 처리 중일 때 결과 요청
    results_response = await client.get(f"/api/jobs/results/{job_id}")
    assert results_response.status_code == 200 # 서버는 200 OK 와 함께 상태 메시지 반환
    result_data = results_response.json()
    assert result_data["job_id"] == job_id
    assert result_data["status"] in ["PENDING", "RUNNING"] # PENDING 또는 RUNNING 상태 기대
    assert "message" in result_data


@pytest.mark.asyncio
async def test_cancel_pending_job_marks_it_cancelled_and_removes_submission(
    client: httpx.AsyncClient,
    monkeypatch,
    tmp_path,
):
    job_id = "cancel-pending-test"
    job_name = "cancel_pending_test"

    class _Gauge:
        def __init__(self):
            self.value = 0

        def set(self, value):
            self.value = value

    queued_jobs = _Gauge()
    monkeypatch.setattr(job_queue, "_queue", asyncio.Queue())
    monkeypatch.setattr(job_queue, "_cancelled_job_ids", set())
    monkeypatch.setattr(job_queue, "_claimed_job_ids", set())
    monkeypatch.setattr(job_queue, "_queued_job_ids", set())
    monkeypatch.setattr(job_processor.metrics, "queued_jobs", queued_jobs)
    await state.set_initial_status(job_id, job_name, str(tmp_path))
    await state.add_submitted_job(job_name)
    await job_queue.add_job({"job_id": job_id, "jobname": job_name, "script_path": str(tmp_path / "script.py")})
    response = await client.post(f"/api/jobs/{job_id}/cancel")

    assert response.status_code == 200
    assert response.json() == {"job_id": job_id, "status": "CANCELLED"}
    assert await state.get_job_status(job_id) == "CANCELLED"
    assert not await state.is_job_submitted(job_name)
    assert job_queue.qsize() == 0
    assert queued_jobs.value == 0
    assert job_processor.job_queue.consume_cancellation(job_id)


@pytest.mark.asyncio
async def test_cancel_running_job_waits_for_terminal_metadata(client: httpx.AsyncClient, tmp_path):
    job_id = "cancel-running-test"
    job_name = "cancel_running_test"
    cleanup_started = asyncio.Event()
    finish_cleanup = asyncio.Event()

    async def running_job():
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await finish_cleanup.wait()
            await state.update_job_status(
                job_id,
                JobStatus.CANCELLED,
                {"error": "cancelled"},
                duration=1.25,
                logs={"stdout": "tail output", "stderr": ""},
            )

    running_task = asyncio.create_task(running_job())
    job_processor._running_job_tasks[job_id] = running_task
    await state.set_initial_status(job_id, job_name, str(tmp_path))
    await state.update_job_status(job_id, JobStatus.RUNNING)

    response_task = asyncio.create_task(client.post(f"/api/jobs/{job_id}/cancel"))
    try:
        await cleanup_started.wait()
        await asyncio.sleep(0)
        assert not response_task.done()

        finish_cleanup.set()
        response = await response_task
        results = await client.get(f"/api/jobs/results/{job_id}")

        assert response.status_code == 200
        assert response.json() == {"job_id": job_id, "status": "CANCELLED"}
        assert results.json()["logs"] == {"stdout": "tail output", "stderr": ""}
        assert results.json()["duration_seconds"] == 1.25
    finally:
        finish_cleanup.set()
        await asyncio.gather(response_task, running_task, return_exceptions=True)
        job_processor._running_job_tasks.pop(job_id, None)


@pytest.mark.asyncio
async def test_cancel_rejects_terminal_or_unknown_job(client: httpx.AsyncClient, tmp_path):
    unknown = await client.post("/api/jobs/missing/cancel")
    assert unknown.status_code == 404

    await state.set_initial_status("completed-job", "completed_test", str(tmp_path))
    await state.update_job_status("completed-job", "COMPLETED", {"ok": True})
    terminal = await client.post("/api/jobs/completed-job/cancel")
    assert terminal.status_code == 409


@pytest.mark.asyncio
async def test_stream_job_logs_replays_completed_job_output(tmp_path):
    job_id = "stream-logs-test"
    await state.set_initial_status(job_id, "stream_logs", str(tmp_path))
    (tmp_path / "stdout.log").write_text("hello stdout\n", encoding="utf-8")
    (tmp_path / "stderr.log").write_text("hello stderr\n", encoding="utf-8")
    await state.update_job_status(job_id, "COMPLETED", {"ok": True})

    response = await stream_job_logs_endpoint(job_id)
    body = "".join([chunk async for chunk in response.body_iterator])

    assert response.media_type == "text/event-stream"
    assert 'event: stdout\ndata: "hello stdout\\n"\n\n' in body
    assert 'event: stderr\ndata: "hello stderr\\n"\n\n' in body


@pytest.mark.asyncio
async def test_stream_job_logs_chunks_large_terminal_backlog(tmp_path):
    job_id = "chunked-log-stream"
    content = "x" * (jobs_api.LOG_STREAM_CHUNK_CHARS * 2 + 17)
    await state.set_initial_status(job_id, "chunked_logs", str(tmp_path))
    (tmp_path / "stdout.log").write_text(content, encoding="utf-8")
    await state.update_job_status(job_id, JobStatus.COMPLETED, {"ok": True})

    stream = jobs_api._stream_job_logs(job_id, str(tmp_path))
    events = [event async for event in stream]
    payloads = [json.loads(event.split("data: ", maxsplit=1)[1]) for event in events]

    assert len(payloads) == 3
    assert all(len(payload) <= jobs_api.LOG_STREAM_CHUNK_CHARS for payload in payloads)
    assert "".join(payloads) == content


@pytest.mark.asyncio
async def test_stream_job_logs_waits_for_cancelled_process_output_drain(tmp_path):
    job_id = "cancelled-stream-tail"
    await state.set_initial_status(job_id, "cancelled_stream_tail", str(tmp_path))
    (tmp_path / "stdout.log").write_text("before cancel\n", encoding="utf-8")

    process_done = asyncio.Event()
    process_task = asyncio.create_task(process_done.wait())
    job_processor._running_job_tasks[job_id] = process_task
    stream = jobs_api._stream_job_logs(job_id, str(tmp_path))
    try:
        assert await anext(stream) == 'event: stdout\ndata: "before cancel\\n"\n\n'
        await state.update_job_status(job_id, JobStatus.CANCELLED, {"error": "cancelled"})

        next_chunk = asyncio.create_task(anext(stream))
        await asyncio.sleep(0.15)
        assert not next_chunk.done()

        with (tmp_path / "stdout.log").open("a", encoding="utf-8") as log_file:
            log_file.write("after cancel\n")
        process_done.set()
        await process_task

        assert await next_chunk == 'event: stdout\ndata: "after cancel\\n"\n\n'
        with pytest.raises(StopAsyncIteration):
            await anext(stream)
    finally:
        process_done.set()
        await process_task
        job_processor._running_job_tasks.pop(job_id, None)


@pytest.mark.asyncio
async def test_interrupted_job_is_a_terminal_result_and_stops_log_stream(client: httpx.AsyncClient, tmp_path):
    job_id = "interrupted-job"
    await state.set_initial_status(job_id, "interrupted", str(tmp_path))
    await state.update_job_status(job_id, JobStatus.INTERRUPTED, {"error": "shutdown"})

    results_response = await client.get(f"/api/jobs/results/{job_id}")

    assert results_response.status_code == 200
    assert results_response.json()["status"] == JobStatus.INTERRUPTED
    assert results_response.json()["result"] == {"error": "shutdown"}

    stream = jobs_api._stream_job_logs(job_id, str(tmp_path))
    with pytest.raises(StopAsyncIteration):
        await anext(stream)


@pytest.mark.asyncio
async def test_worker_skips_cancelled_queued_job_without_releasing_resubmitted_name(monkeypatch):
    job = {"job_id": "skip-cancelled-job", "jobname": "skip_cancelled", "script_path": "/tmp/script.py"}
    queue_items = iter((job, None))
    dispatched = []
    task_done_calls = []
    monkeypatch.setattr(job_queue, "_queued_job_ids", {job["job_id"]})
    monkeypatch.setattr(job_queue, "_cancelled_job_ids", set())
    monkeypatch.setattr(job_queue, "_claimed_job_ids", set())
    await state.add_submitted_job(job["jobname"])
    assert job_processor.job_queue.cancel_job(job["job_id"])
    await state.remove_submitted_job(job["jobname"])
    assert await state.add_submitted_job(job["jobname"])

    async def fake_get_job():
        return next(queue_items)

    async def fake_dispatch_job(received_job):
        dispatched.append(received_job)

    monkeypatch.setattr(job_processor.job_queue, "get_job", fake_get_job)
    monkeypatch.setattr(job_processor.job_queue, "task_done", lambda: task_done_calls.append(True))
    monkeypatch.setattr(job_processor, "_dispatch_job", fake_dispatch_job)

    await job_processor._worker()

    assert dispatched == []
    assert task_done_calls == [True, True]
    assert await state.is_job_submitted(job["jobname"])


@pytest.mark.asyncio
async def test_worker_claim_updates_logical_queue_metric(monkeypatch):
    job = {"job_id": "metric-dequeue", "jobname": "metric_dequeue", "script_path": "/tmp/script.py"}
    observed = []

    class _Gauge:
        def __init__(self):
            self.value = 0

        def set(self, value):
            self.value = value

    queued_jobs = _Gauge()
    monkeypatch.setattr(job_queue, "_queue", asyncio.Queue())
    monkeypatch.setattr(job_queue, "_queued_job_ids", set())
    monkeypatch.setattr(job_queue, "_cancelled_job_ids", set())
    monkeypatch.setattr(job_queue, "_claimed_job_ids", set())
    monkeypatch.setattr(job_processor.metrics, "queued_jobs", queued_jobs)

    async def capture_dispatch(_job):
        observed.append((job_queue.qsize(), queued_jobs.value))

    monkeypatch.setattr(job_processor, "_dispatch_job", capture_dispatch)
    await job_queue.add_job(job)
    await job_queue.put_shutdown_signal(1)

    await job_processor._worker()

    assert observed == [(0, 0)]


@pytest.mark.asyncio
async def test_cancel_claimed_pending_job_cancels_registered_dispatch_task(
    client: httpx.AsyncClient,
    monkeypatch,
    tmp_path,
):
    job_id = "claimed-pending-cancel"
    job_name = "claimed_pending_cancel"
    monkeypatch.setattr(job_queue, "_queued_job_ids", {job_id})
    monkeypatch.setattr(job_queue, "_cancelled_job_ids", set())
    monkeypatch.setattr(job_queue, "_claimed_job_ids", set())
    await state.set_initial_status(job_id, job_name, str(tmp_path))
    assert job_queue.claim_job(job_id)

    task = asyncio.create_task(asyncio.Event().wait())
    job_processor._running_job_tasks[job_id] = task
    try:
        response = await client.post(f"/api/jobs/{job_id}/cancel")
        await asyncio.sleep(0)

        assert response.json() == {"job_id": job_id, "status": "CANCELLED"}
        assert task.cancelled()
        assert await state.get_job_status(job_id) == "CANCELLED"
    finally:
        job_processor._running_job_tasks.pop(job_id, None)
        job_queue.release_job(job_id)


@pytest.mark.asyncio
async def test_get_completed_results_lists_downloadable_files(client: httpx.AsyncClient, tmp_path):
    job_id = "completed-download-test"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    result_file = job_dir / "output.txt"
    result_file.write_text("crawl output", encoding="utf-8")

    await state.set_initial_status(job_id, "completed_download_test", str(job_dir))
    await state.update_job_status(
        job_id,
        "COMPLETED",
        result={"ok": True},
        duration=1.25,
        logs={"stdout": "tail output", "stderr": ""},
    )

    results_response = await client.get(f"/api/jobs/results/{job_id}")

    assert results_response.status_code == 200
    result_data = results_response.json()
    assert result_data["status"] == "COMPLETED"
    assert result_data["result"] == {"ok": True}
    assert result_data["logs"] == {"stdout": "tail output", "stderr": ""}
    assert result_data["files"] == {
        "output.txt": f"/api/jobs/download/{job_id}/output.txt"
    }

    download_response = await client.get(f"/api/jobs/download/{job_id}/output.txt")
    assert download_response.status_code == 200
    assert download_response.content == b"crawl output"


@pytest.mark.asyncio
async def test_result_file_listing_does_not_block_event_loop(monkeypatch, tmp_path):
    job_id = "slow-listing-test"
    await state.set_initial_status(job_id, "slow_listing", str(tmp_path))
    await state.update_job_status(job_id, JobStatus.COMPLETED, result={"ok": True})
    original_listdir = jobs_api.os.listdir

    def slow_listdir(path):
        time.sleep(0.1)
        return original_listdir(path)

    monkeypatch.setattr(jobs_api.os, "listdir", slow_listdir)

    started = time.perf_counter()
    result_task = asyncio.create_task(jobs_api.get_job_results_endpoint(job_id))
    await asyncio.sleep(0.01)
    heartbeat_elapsed = time.perf_counter() - started
    await result_task

    assert heartbeat_elapsed < 0.05


@pytest.mark.asyncio
async def test_completed_result_encodes_reserved_filename_download_link(client: httpx.AsyncClient, tmp_path):
    job_id = "encoded-download-test"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    filename = "report?.txt"
    (job_dir / filename).write_text("reserved filename", encoding="utf-8")

    await state.set_initial_status(job_id, "encoded_download_test", str(job_dir))
    await state.update_job_status(job_id, "COMPLETED", result={"ok": True})

    results_response = await client.get(f"/api/jobs/results/{job_id}")

    assert results_response.status_code == 200
    download_url = results_response.json()["files"][filename]
    assert download_url == f"/api/jobs/download/{job_id}/report%3F.txt"

    download_response = await client.get(download_url)

    assert download_response.status_code == 200
    assert download_response.content == b"reserved filename"


@pytest.mark.asyncio
async def test_get_status_not_found(client: httpx.AsyncClient):
    """존재하지 않는 job_id 상태 조회 시 404 반환 테스트"""
    response = await client.get("/api/jobs/status/non-existent-job-id")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_get_results_not_found(client: httpx.AsyncClient):
    """존재하지 않는 job_id 결과 조회 시 404 반환 테스트"""
    response = await client.get("/api/jobs/results/non-existent-job-id")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_submit_duplicate_job(client: httpx.AsyncClient):
    """중복 작업 제출 시 409 Conflict 반환 테스트"""
    job_name = f"duplicate_test_{int(time.time())}"
    files = {'script_file': ('dummy_script.py', DUMMY_SCRIPT_CONTENT, 'text/x-python')}
    data = {'jobname': job_name}

    # 첫 번째 제출
    response1 = await client.post("/api/jobs/submit", data=data, files=files)
    assert response1.status_code == 202

    # 잠시 대기 (상태가 바로 정리되지 않을 수 있으므로)
    await asyncio.sleep(0.1)

    # 두 번째 제출
    response2 = await client.post("/api/jobs/submit", data=data, files=files)
    assert response2.status_code == 409

@pytest.mark.asyncio
async def test_submit_additional_file_rejects_path_traversal(client: httpx.AsyncClient):
    job_name = f"traversal_test_{int(time.time())}"
    escaped_target = Path(JOB_FOLDER) / "evil.txt"
    if escaped_target.exists():
        escaped_target.unlink()

    files = [
        ('script_file', ('dummy_script.py', DUMMY_SCRIPT_CONTENT, 'text/x-python')),
        ('additional_files', ('../evil.txt', 'malicious payload', 'text/plain')),
    ]
    data = {'jobname': job_name}

    response = await client.post("/api/jobs/submit", data=data, files=files)

    assert response.status_code == 400
    assert "Invalid additional file name" in response.json()["detail"]
    assert not escaped_target.exists()
    if escaped_target.exists():
        escaped_target.unlink()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "filename",
    ["script.py", "result.json", "result.json.tmp", "stdout.log", "stderr.log"],
)
async def test_submit_rejects_reserved_additional_filenames(client: httpx.AsyncClient, filename):
    response = await client.post(
        "/api/jobs/submit",
        data={"jobname": f"reserved-{filename}"},
        files=[
            ("script_file", ("crawl.py", DUMMY_SCRIPT_CONTENT, "text/x-python")),
            ("additional_files", (filename, "payload", "text/plain")),
        ],
    )

    assert response.status_code == 400
    assert response.json() == {"detail": f"Reserved additional file name: {filename}"}


@pytest.mark.asyncio
@pytest.mark.parametrize("jobname", [" ", "\t", "\r\n"])
async def test_submit_rejects_blank_job_names(client: httpx.AsyncClient, jobname):
    response = await client.post(
        "/api/jobs/submit",
        data={"jobname": jobname},
        files=[("script_file", ("crawl.py", DUMMY_SCRIPT_CONTENT, "text/x-python"))],
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Jobname and script file are required"}


@pytest.mark.asyncio
async def test_submit_rejects_duplicate_additional_filenames(client: httpx.AsyncClient):
    response = await client.post(
        "/api/jobs/submit",
        data={"jobname": "duplicate-additional-filename"},
        files=[
            ("script_file", ("crawl.py", DUMMY_SCRIPT_CONTENT, "text/x-python")),
            ("additional_files", ("helper.py", "first", "text/x-python")),
            ("additional_files", ("helper.py", "second", "text/x-python")),
        ],
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Duplicate additional file name: helper.py"}


@pytest.mark.asyncio
async def test_download_rejects_sibling_prefix_traversal(tmp_path):
    job_id = "download-prefix-traversal"
    job_dir = tmp_path / "jobA"
    sibling_dir = tmp_path / "jobA2"
    job_dir.mkdir()
    sibling_dir.mkdir()
    (sibling_dir / "secret.txt").write_text("secret", encoding="utf-8")

    await state.set_initial_status(job_id, "download_prefix_test", str(job_dir))

    with pytest.raises(HTTPException) as exc:
        await download_file_endpoint(job_id, "../jobA2/secret.txt")

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_lightweight_lifespan_skips_workers(monkeypatch):
    calls = []

    monkeypatch.setattr("src.common.tool_utils.ensure_job_folder", lambda: calls.append("ensure_job_folder"))
    monkeypatch.setattr("src.common.tool_utils.start_display", lambda: calls.append("start_display") or True)
    monkeypatch.setattr("src.common.tool_utils.stop_display", lambda: calls.append("stop_display"))
    async def fake_playwright_start():
        calls.append("playwright_start")

    async def fake_playwright_shutdown():
        calls.append("playwright_shutdown")

    monkeypatch.setattr("src.core.playwright_manager.start", fake_playwright_start)
    monkeypatch.setattr("src.core.playwright_manager.shutdown", fake_playwright_shutdown)
    monkeypatch.setattr("src.worker.job_processor.start_workers", lambda: calls.append("start_workers"))
    async def fake_stop_workers():
        calls.append("stop_workers")

    monkeypatch.setattr("src.worker.job_processor.stop_workers", fake_stop_workers)
    monkeypatch.setattr("src.common.tool_utils.periodic_cleanup", lambda: asyncio.sleep(0))
    monkeypatch.setenv("RUN_HEAVY_STARTUP", "false")

    import src.main as main

    app = main.app
    async with app.router.lifespan_context(app):
        assert calls == ["ensure_job_folder"]

    assert calls == ["ensure_job_folder"]


@pytest.mark.asyncio
async def test_submit_is_rejected_when_workers_are_disabled(monkeypatch):
    monkeypatch.setattr("src.common.tool_utils.ensure_job_folder", lambda: None)
    monkeypatch.setattr("src.common.tool_utils.periodic_cleanup", lambda: asyncio.sleep(0))
    monkeypatch.setenv("RUN_HEAVY_STARTUP", "false")

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as disabled_client:
            response = await disabled_client.post(
                "/api/jobs/submit",
                data={"jobname": "workers-disabled"},
                files={"script_file": ("crawl.py", DUMMY_SCRIPT_CONTENT, "text/x-python")},
            )

    assert response.status_code == 503
    assert response.json() == {"detail": "Job workers are unavailable"}
    assert not await state.is_job_submitted("workers-disabled")
    assert not os.path.exists(JOB_FOLDER) or not any(os.scandir(JOB_FOLDER))


@pytest.mark.asyncio
async def test_heavy_lifespan_starts_workers(monkeypatch):
    calls = []

    monkeypatch.setattr("src.common.tool_utils.ensure_job_folder", lambda: calls.append("ensure_job_folder"))
    monkeypatch.setattr("src.common.tool_utils.start_display", lambda: calls.append("start_display") or True)
    monkeypatch.setattr("src.common.tool_utils.stop_display", lambda: calls.append("stop_display"))
    async def fake_playwright_start():
        calls.append("playwright_start")

    async def fake_playwright_shutdown():
        calls.append("playwright_shutdown")

    monkeypatch.setattr("src.core.playwright_manager.start", fake_playwright_start)
    monkeypatch.setattr("src.core.playwright_manager.shutdown", fake_playwright_shutdown)
    monkeypatch.setattr("src.worker.job_processor.start_workers", lambda: calls.append("start_workers"))
    async def fake_stop_workers():
        calls.append("stop_workers")

    monkeypatch.setattr("src.worker.job_processor.stop_workers", fake_stop_workers)
    monkeypatch.setattr("src.common.tool_utils.periodic_cleanup", lambda: asyncio.sleep(0))
    monkeypatch.setenv("RUN_HEAVY_STARTUP", "true")

    import src.main as main

    app = main.app
    async with app.router.lifespan_context(app):
        assert calls == ["ensure_job_folder", "start_display", "playwright_start", "start_workers"]

    assert calls == ["ensure_job_folder", "start_display", "playwright_start", "start_workers", "stop_workers", "playwright_shutdown", "stop_display"]


@pytest.mark.asyncio
async def test_lifespan_completes_teardown_when_worker_shutdown_fails(monkeypatch):
    calls = []

    monkeypatch.setattr("src.common.tool_utils.ensure_job_folder", lambda: calls.append("ensure_job_folder"))
    monkeypatch.setattr("src.common.tool_utils.start_display", lambda: calls.append("start_display") or True)
    monkeypatch.setattr("src.common.tool_utils.stop_display", lambda: calls.append("stop_display"))
    monkeypatch.setattr("src.worker.job_processor.start_workers", lambda: calls.append("start_workers"))
    monkeypatch.setattr("src.common.tool_utils.periodic_cleanup", lambda: asyncio.sleep(0))
    monkeypatch.setenv("RUN_HEAVY_STARTUP", "true")

    async def fake_playwright_start():
        calls.append("playwright_start")

    async def fake_playwright_shutdown():
        calls.append("playwright_shutdown")

    async def failing_stop_workers():
        calls.append("stop_workers")
        raise asyncio.TimeoutError()

    monkeypatch.setattr("src.core.playwright_manager.start", fake_playwright_start)
    monkeypatch.setattr("src.core.playwright_manager.shutdown", fake_playwright_shutdown)
    monkeypatch.setattr("src.worker.job_processor.stop_workers", failing_stop_workers)

    async with app.router.lifespan_context(app):
        pass

    assert calls == ["ensure_job_folder", "start_display", "playwright_start", "start_workers", "stop_workers", "playwright_shutdown", "stop_display"]


@pytest.mark.asyncio
async def test_heavy_lifespan_refuses_headful_browser_when_display_fails(monkeypatch):
    calls = []

    monkeypatch.setattr("src.common.tool_utils.ensure_job_folder", lambda: calls.append("ensure_job_folder"))
    monkeypatch.setattr("src.common.tool_utils.start_display", lambda: calls.append("start_display") or False)
    monkeypatch.setattr("src.worker.job_processor.start_workers", lambda: calls.append("start_workers"))
    monkeypatch.setenv("RUN_HEAVY_STARTUP", "true")

    async def fake_playwright_start():
        calls.append("playwright_start")

    monkeypatch.setattr("src.core.playwright_manager.start", fake_playwright_start)

    with pytest.raises(RuntimeError, match="Virtual display startup failed"):
        async with app.router.lifespan_context(app):
            pass

    assert calls == ["ensure_job_folder", "start_display"]


@pytest.mark.asyncio
async def test_heavy_lifespan_unwinds_resources_when_browser_start_fails(monkeypatch):
    calls = []

    class FakeMonitor:
        def __init__(self, **_kwargs):
            pass
        def start(self):
            calls.append("monitor_start")
        def stop(self):
            calls.append("monitor_stop")

    async def failing_playwright_start():
        calls.append("playwright_start")
        raise RuntimeError("browser launch failed")

    async def fake_playwright_shutdown():
        calls.append("playwright_shutdown")

    monkeypatch.setattr("src.main.ResourceMonitor", FakeMonitor)
    monkeypatch.setattr("src.common.tool_utils.ensure_job_folder", lambda: calls.append("ensure_job_folder"))
    monkeypatch.setattr("src.common.tool_utils.start_display", lambda: calls.append("start_display") or True)
    monkeypatch.setattr("src.common.tool_utils.stop_display", lambda: calls.append("stop_display"))
    monkeypatch.setattr("src.core.playwright_manager.start", failing_playwright_start)
    monkeypatch.setattr("src.core.playwright_manager.shutdown", fake_playwright_shutdown)
    monkeypatch.setenv("RUN_HEAVY_STARTUP", "true")

    with pytest.raises(RuntimeError, match="browser launch failed"):
        async with app.router.lifespan_context(app):
            pass

    assert calls == ["monitor_start", "ensure_job_folder", "start_display", "playwright_start", "playwright_shutdown", "stop_display", "monitor_stop"]


@pytest.mark.asyncio
async def test_stop_workers_cancels_workers_after_queue_timeout(monkeypatch):
    worker = asyncio.create_task(asyncio.Event().wait())
    job_processor._workers = [worker]

    async def fake_put_shutdown_signal(_count):
        return None

    async def timeout_join(*_args, **_kwargs):
        raise asyncio.TimeoutError()

    monkeypatch.setattr(job_processor.job_queue, "put_shutdown_signal", fake_put_shutdown_signal)
    monkeypatch.setattr(job_processor.job_queue, "join", timeout_join)

    await job_processor.stop_workers()

    assert worker.cancelled()
    assert job_processor._workers == []


def test_systemd_deploy_serializes_production_updates():
    workflow = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "systemd-deploy.yaml"
    service = Path(__file__).resolve().parents[1] / "example" / "playwright-multi-crawler.service"
    content = workflow.read_text(encoding="utf-8")
    service_content = service.read_text(encoding="utf-8")

    assert "concurrency:" in content
    assert "group: playwright-multi-crawler-production" in content
    assert "cancel-in-progress: false" in content
    assert "KillMode=mixed" in content
    assert "KillMode=mixed" in service_content

@pytest.mark.asyncio
async def test_submit_additional_file_accepts_safe_filename(client: httpx.AsyncClient):
    job_name = f"safe_additional_test_{int(time.time())}"
    additional_name = "fixtures.txt"
    additional_content = "safe payload"

    files = [
        ('script_file', ('dummy_script.py', DUMMY_SCRIPT_CONTENT, 'text/x-python')),
        ('additional_files', (additional_name, additional_content, 'text/plain')),
    ]
    data = {'jobname': job_name}

    response = await client.post("/api/jobs/submit", data=data, files=files)

    assert response.status_code == 202
    job_id = response.json()["job_id"]
    job_dir = Path(JOB_FOLDER) / job_id
    saved_file = job_dir / additional_name
    assert saved_file.exists()
    assert saved_file.read_text() == additional_content
