import pytest
import pytest_asyncio
import asyncio

# 테스트 대상 모듈 임포트
from src.core import state_manager as state

@pytest.mark.asyncio
async def test_initial_status_and_update():
    """상태 초기화 및 업데이트 기능 테스트"""
    job_id = "test-job-123"
    job_name = "unit_test_job"
    job_path = "/tmp/fake/path"

    # 초기 상태 설정
    await state.set_initial_status(job_id, job_name, job_path)

    # 상태 확인
    initial_info = await state.get_job_info(job_id)
    assert initial_info is not None
    assert initial_info["status"] == "PENDING"
    assert initial_info["jobname"] == job_name
    assert initial_info["job_path"] == job_path

    # 상태 업데이트 (RUNNING)
    await state.update_job_status(job_id, "RUNNING")
    running_status = await state.get_job_status(job_id)
    assert running_status == "RUNNING"

    # 상태 업데이트 (COMPLETED with result)
    result_data = {"key": "value"}
    duration = 5.5
    await state.update_job_status(job_id, "COMPLETED", result=result_data, duration=duration)

    # 최종 정보 확인
    final_info = await state.get_job_info(job_id)
    assert final_info is not None
    assert final_info["status"] == "COMPLETED"
    assert final_info["result"] == result_data
    assert final_info["duration"] == duration

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