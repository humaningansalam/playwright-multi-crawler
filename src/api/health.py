from fastapi import APIRouter, Request, Response, status

from src.core import job_queue, playwright_manager
from src.models.job import HealthResponse, HealthStatus

router = APIRouter(prefix="/health", tags=["Health"])


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
async def health_check_endpoint(request: Request, http_response: Response) -> HealthResponse:
    """Report whether the shared browser and worker pool accept jobs."""
    browser_connected = playwright_manager.is_browser_connected()
    workers_ready = bool(getattr(request.app.state, "job_submission_enabled", False))
    queued_tasks = job_queue.qsize()
    is_ready = browser_connected and workers_ready

    if not is_ready:
        http_response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthResponse(
        status=HealthStatus.OK if is_ready else HealthStatus.UNAVAILABLE,
        browser_connected=browser_connected,
        workers_ready=workers_ready,
        queued_tasks=queued_tasks,
    )
