import logging
from fastapi import APIRouter, Depends

# core 모듈 임포트
from src.core import playwright_manager
from src.core import job_queue
# models 임포트 
from src.models.job import HealthResponse

router = APIRouter(
    prefix="/health",
    tags=["Health"],
)

logger = logging.getLogger(__name__)

@router.get("", response_model=HealthResponse)
async def health_check_endpoint():
    """애플리케이션 상태 확인"""
    browser_connected = playwright_manager.is_browser_connected()
    queued_tasks = job_queue.qsize()

    return HealthResponse(
        browser_connected=browser_connected,
        queued_tasks=queued_tasks
    )
