import pytest
import pytest_asyncio
import httpx
import asyncio
import os
import time
from pathlib import Path

from src.config import JOB_FOLDER

# 테스트용 간단한 스크립트 파일 내용
DUMMY_SCRIPT_CONTENT = """
import asyncio
async def crawl(page, context, job_path):
    print("Dummy crawl running")
    await asyncio.sleep(0.1) # 아주 짧은 작업 시간
    return {'status': 'success'}
"""

@pytest.mark.asyncio
async def test_health_check(client: httpx.AsyncClient):
    """/health 엔드포인트 기본 응답 테스트"""
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"

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
