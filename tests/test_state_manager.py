import pytest
import asyncio
import time

# 테스트 대상 모듈 임포트
from src.core import state_manager as state
from src.models.job import JobError, JobErrorCode, JobRecord, JobStatus

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
async def test_state_manager_rejects_untyped_status():
    await state.set_initial_status("typed-status", "typed", "/tmp/typed")
    with pytest.raises(TypeError):
        await state.update_job_status("typed-status", "COMPLETED")


@pytest.mark.asyncio
async def test_state_manager_enforces_terminal_error_and_transition_contracts():
    await state.set_initial_status("terminal-contract", "typed", "/tmp/typed")

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
async def test_get_job_info_deep_copy_does_not_block_event_loop(monkeypatch):
    job_id = "slow-deep-copy"
    await state.set_initial_status(job_id, "slow_copy", "/tmp/slow-copy")
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
