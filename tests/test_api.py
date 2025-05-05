import pytest
import pytest_asyncio
import httpx
import asyncio
import os
import time

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