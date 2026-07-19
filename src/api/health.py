import logging
from fastapi import APIRouter, Request, Response, status

# core 모듈 임포트
from src.core import playwright_manager
from src.core import job_queue
# models 임포트 
from src.models.job import HealthResponse, HealthStatus

router = APIRouter(
    prefix="/health",
    tags=["Health"],
)

logger = logging.getLogger(__name__)

@router.get(
    "",
    response_model=HealthResponse,
    responses={
        503: {
            "model": HealthResponse,
            "description": "Browser or worker pool is unavailable",
        }
    },
)
async def health_check_endpoint(request: Request, response: Response):
    """애플리케이션 상태 확인"""
    browser_connected = playwright_manager.is_browser_connected()
    workers_ready = bool(getattr(request.app.state, "job_submission_enabled", False))
    queued_tasks = job_queue.qsize()

    ready = browser_connected and workers_ready
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthResponse(
        status=HealthStatus.OK if ready else HealthStatus.UNAVAILABLE,
        browser_connected=browser_connected,
        workers_ready=workers_ready,
        queued_tasks=queued_tasks
    )
