import asyncio
from datetime import datetime
import logging
from typing import Any, Dict, Optional, Set

# --- 상태 변수 및 락 ---
# 작업 상태 및 결과 저장 
_job_status_and_results: Dict[str, Dict[str, Any]] = {}
# 현재 큐 또는 실행 중인 작업 이름 추적용 
_submitted_jobs: Set[str] = set()

# Lock 객체
_job_status_lock = asyncio.Lock()
_submitted_jobs_lock = asyncio.Lock()

# --- 상태 관리 함수 ---
async def get_job_info(job_id: str) -> Optional[Dict[str, Any]]:
    """특정 작업의 전체 정보 조회"""
    async with _job_status_lock:
        job_info = _job_status_and_results.get(job_id)
        return job_info.copy() if job_info else None

async def get_job_status(job_id: str) -> Optional[str]:
    """특정 작업의 상태 문자열 조회"""
    async with _job_status_lock:
        job_info = _job_status_and_results.get(job_id)
        return job_info['status'] if job_info else None

async def set_initial_status(job_id: str, job_name: str, job_path: str):
    """작업 상태 초기화 (PENDING)"""
    async with _job_status_lock:
        if job_id in _job_status_and_results:
            logging.warning(f"Job ID {job_id} already exists in status dict during initialization.")
        _job_status_and_results[job_id] = {
            'status': 'PENDING',
            'result': None,
            'job_path': job_path,
            'jobname': job_name,
            'submitted_at': datetime.now().isoformat(),
            'duration': None
        }
    logging.debug(f"Initial status set for job {job_id}: PENDING")

async def update_job_status(job_id: str, status: str, result: Any = None, duration: Optional[float] = None):
    """작업 상태 및 결과 업데이트"""
    async with _job_status_lock:
        if job_id in _job_status_and_results:
            _job_status_and_results[job_id]['status'] = status
            if result is not None:
                _job_status_and_results[job_id]['result'] = result
            if duration is not None:
                _job_status_and_results[job_id]['duration'] = duration
            logging.debug(f"Status updated for job {job_id}: {status}")
        else:
            logging.warning(f"Attempted to update status for non-existent job ID: {job_id}")

async def remove_job_state(job_id: str):
    """오래된 작업 상태 정보 삭제"""
    async with _job_status_lock:
        if job_id in _job_status_and_results:
            del _job_status_and_results[job_id]
            logging.info(f"Removed state for job ID: {job_id}")

# --- 중복 작업 관리 함수 ---
async def add_submitted_job(jobname: str) -> bool:
    """중복 체크 및 submitted_jobs 세트에 추가. 성공 시 True 반환."""
    async with _submitted_jobs_lock:
        if jobname in _submitted_jobs:
            logging.warning(f"Duplicate job submission detected for name: {jobname}")
            return False
        _submitted_jobs.add(jobname)
        logging.debug(f"Job name '{jobname}' added to submitted set.")
        return True

async def remove_submitted_job(jobname: str):
    """submitted_jobs 세트에서 작업 이름 제거"""
    async with _submitted_jobs_lock:
        _submitted_jobs.discard(jobname)
        logging.debug(f"Job name '{jobname}' removed from submitted set.")

async def is_job_submitted(jobname: str) -> bool:
    """작업 이름이 현재 제출/처리 중인지 확인"""
    async with _submitted_jobs_lock:
        return jobname in _submitted_jobs