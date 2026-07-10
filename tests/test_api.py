import asyncio
import os
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
from src.main import app
from src.worker import job_processor
from src.core import playwright_manager
from src.common import tool_utils

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

    async def fake_to_thread(function):
        calls.append(function)
        raise asyncio.CancelledError()

    monkeypatch.setattr("src.common.tool_utils.asyncio.to_thread", fake_to_thread)

    with pytest.raises(asyncio.CancelledError):
        await tool_utils.periodic_cleanup()

    assert calls == [tool_utils.clean_old_jobs]


@pytest.mark.asyncio
async def test_periodic_cleanup_removes_state_for_deleted_job_folders(monkeypatch):
    removed_job_ids = []

    async def fake_to_thread(function):
        assert function is tool_utils.clean_old_jobs
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
async def test_cancel_pending_job_marks_it_cancelled_and_removes_submission(client: httpx.AsyncClient, tmp_path):
    job_id = "cancel-pending-test"
    job_name = "cancel_pending_test"
    await state.set_initial_status(job_id, job_name, str(tmp_path))
    await state.add_submitted_job(job_name)

    response = await client.post(f"/api/jobs/{job_id}/cancel")

    assert response.status_code == 200
    assert response.json() == {"job_id": job_id, "status": "CANCELLED"}
    assert await state.get_job_status(job_id) == "CANCELLED"
    assert not await state.is_job_submitted(job_name)
    assert job_processor.job_queue.consume_cancellation(job_id)


@pytest.mark.asyncio
async def test_cancel_running_job_cancels_only_the_job_task(client: httpx.AsyncClient, tmp_path):
    job_id = "cancel-running-test"
    job_name = "cancel_running_test"
    running_task = asyncio.create_task(asyncio.Event().wait())
    job_processor._running_job_tasks[job_id] = running_task
    await state.set_initial_status(job_id, job_name, str(tmp_path))
    await state.update_job_status(job_id, "RUNNING")

    response = await client.post(f"/api/jobs/{job_id}/cancel")

    with pytest.raises(asyncio.CancelledError):
        await running_task
    assert response.status_code == 200
    assert response.json() == {"job_id": job_id, "status": "CANCELLED"}
    assert await state.get_job_status(job_id) == "CANCELLED"
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
async def test_worker_skips_cancelled_queued_job(monkeypatch):
    job = {"job_id": "skip-cancelled-job", "jobname": "skip_cancelled", "script_path": "/tmp/script.py"}
    queue_items = iter((job, None))
    dispatched = []
    task_done_calls = []
    await state.add_submitted_job(job["jobname"])
    job_processor.job_queue.cancel_job(job["job_id"])

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
    assert not await state.is_job_submitted(job["jobname"])


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
@pytest.mark.parametrize("filename", ["script.py", "result.json", "result.json.tmp"])
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
