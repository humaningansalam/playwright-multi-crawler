import asyncio
import os
import tomllib
import time
from pathlib import Path

import httpx
import pytest
from fastapi import HTTPException

from src.config import JOB_FOLDER
from src.api.jobs import download_file_endpoint
from src.core import state_manager as state
from src.main import app

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
async def test_health_check(client: httpx.AsyncClient):
    """/health 엔드포인트 기본 응답 테스트"""
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["browser_connected"] is False
    assert data["queued_tasks"] == 0


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
async def test_get_completed_results_lists_downloadable_files(client: httpx.AsyncClient, tmp_path):
    job_id = "completed-download-test"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    result_file = job_dir / "output.txt"
    result_file.write_text("crawl output", encoding="utf-8")

    await state.set_initial_status(job_id, "completed_download_test", str(job_dir))
    await state.update_job_status(job_id, "COMPLETED", result={"ok": True}, duration=1.25)

    results_response = await client.get(f"/api/jobs/results/{job_id}")

    assert results_response.status_code == 200
    result_data = results_response.json()
    assert result_data["status"] == "COMPLETED"
    assert result_data["result"] == {"ok": True}
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
    monkeypatch.setattr("src.common.tool_utils.start_display", lambda: calls.append("start_display"))
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
    monkeypatch.setattr("src.common.tool_utils.start_display", lambda: calls.append("start_display"))
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
