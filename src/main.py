import asyncio
import logging
import os
import signal
import tomllib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path
from types import FrameType

import uvicorn
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from his_mon import ResourceMonitor, setup_logging
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.api import health as health_api
from src.api import jobs as jobs_api
from src.api import metrics as metrics_api
from src.common import tool_utils
from src.common.metrics import metrics
from src.config import HOST, JOB_FOLDER, LOG_FILE_PATH, LOG_LEVEL, LOKI_TAGS, LOKI_URL, PORT
from src.core import job_queue, playwright_manager, state_manager
from src.models.job import ApiErrorCode, ApiErrorDetail, ApiErrorResponse
from src.worker import job_processor

logger = logging.getLogger(__name__)
_logging_configured = False


def _configure_logging() -> None:
    """Configure process logging once, when the application runtime starts."""
    global _logging_configured

    if _logging_configured:
        return

    Path(LOG_FILE_PATH).parent.mkdir(parents=True, exist_ok=True)
    setup_logging(
        level=LOG_LEVEL,
        loki_url=LOKI_URL,
        tags=LOKI_TAGS,
        log_file=LOG_FILE_PATH,
    )
    _logging_configured = True


@dataclass
class RuntimeResources:
    monitor: ResourceMonitor | None = None
    cleanup_task: asyncio.Task[None] | None = None
    display_started: bool = False
    playwright_start_attempted: bool = False
    workers_started: bool = False


def _get_app_version() -> str:
    """Load the package version, falling back to the local project metadata."""
    try:
        return package_version("playwright-multi-crawler")
    except PackageNotFoundError:
        pass

    pyproject_path = Path(__file__).resolve().parent.parent / "pyproject.toml"
    try:
        with pyproject_path.open("rb") as pyproject_file:
            project_config = tomllib.load(pyproject_file)
        return project_config["project"]["version"]
    except (OSError, KeyError, tomllib.TOMLDecodeError):
        return "0.0.0"


def _heavy_startup_enabled() -> bool:
    return os.getenv("RUN_HEAVY_STARTUP", "true").lower() == "true"


async def _start_runtime(
    app: FastAPI,
    resources: RuntimeResources,
    *,
    heavy_startup: bool,
) -> None:
    resources.monitor = ResourceMonitor(metrics_obj=metrics, interval=5)
    resources.monitor.start()
    app.state.monitor = resources.monitor

    tool_utils.ensure_job_folder()
    pending_jobs = await state_manager.recover_persisted_jobs(JOB_FOLDER)

    if heavy_startup:
        if not tool_utils.start_display():
            raise RuntimeError("Virtual display startup failed; refusing to launch headful browser")
        resources.display_started = True
        resources.playwright_start_attempted = True
        await playwright_manager.start()
        job_queue.restore_jobs(pending_jobs)
        job_processor.start_workers()
        resources.workers_started = True
        app.state.job_submission_enabled = True
    else:
        job_queue.restore_jobs(pending_jobs)
        logger.info("Skipping display, Chromium, and workers (RUN_HEAVY_STARTUP=false).")

    resources.cleanup_task = asyncio.create_task(
        tool_utils.periodic_cleanup(),
        name="PeriodicCleanupTask",
    )
    app.state.cleanup_task = resources.cleanup_task


async def _stop_workers(resources: RuntimeResources) -> None:
    if not resources.workers_started:
        return

    try:
        if playwright_manager.requested_exit_code() is None:
            await job_processor.stop_workers()
        else:
            await job_processor.stop_workers(drain=False)
    except Exception:
        logger.exception("Worker shutdown failed; continuing resource teardown.")


async def _stop_playwright(resources: RuntimeResources) -> None:
    if not resources.playwright_start_attempted:
        return

    try:
        await playwright_manager.shutdown()
    except Exception:
        logger.exception("Playwright shutdown failed.")


async def _cancel_cleanup_task(cleanup_task: asyncio.Task[None] | None) -> None:
    if cleanup_task is None or cleanup_task.done():
        return

    cleanup_task.cancel()
    logger.info("Cancelled periodic cleanup task.")
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass


def _stop_resource_monitor(monitor: ResourceMonitor | None) -> None:
    if monitor is None:
        return

    try:
        monitor.stop()
    except Exception:
        logger.exception("Resource monitor shutdown failed.")


async def _stop_runtime(app: FastAPI, resources: RuntimeResources) -> None:
    app.state.job_submission_enabled = False

    await _stop_workers(resources)
    await _stop_playwright(resources)
    if resources.display_started:
        tool_utils.stop_display()
    await _cancel_cleanup_task(resources.cleanup_task)
    _stop_resource_monitor(resources.monitor)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start and tear down the service runtime around the ASGI application."""
    _configure_logging()
    logger.info("Application startup sequence initiated.")
    resources = RuntimeResources()
    app.state.job_submission_enabled = False

    try:
        await _start_runtime(app, resources, heavy_startup=_heavy_startup_enabled())
        logger.info("Application startup sequence completed.")
        yield
    finally:
        logger.info("Application shutdown sequence initiated.")
        await _stop_runtime(app, resources)
        logger.info("Application shutdown sequence completed.")


app = FastAPI(
    title="Playwright Job Runner API",
    description="API for submitting and managing Playwright browser automation jobs.",
    version=_get_app_version(),
    lifespan=lifespan,
)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(
    request: Request,
    exception: StarletteHTTPException,
) -> JSONResponse:
    detail = exception.detail
    if (
        exception.status_code == 400
        and request.url.path == "/api/jobs/submit"
        and not isinstance(detail, dict)
    ):
        detail = ApiErrorDetail(
            code=ApiErrorCode.INVALID_SUBMISSION,
            message="Invalid multipart form data",
            context={"reason": str(exception.detail)},
        ).model_dump(mode="json")

    return JSONResponse(
        status_code=exception.status_code,
        content={"detail": jsonable_encoder(detail)},
        headers=exception.headers,
    )


@app.exception_handler(RequestValidationError)
async def request_validation_error_handler(
    _request: Request,
    exception: RequestValidationError,
) -> JSONResponse:
    error_response = ApiErrorResponse(
        detail=ApiErrorDetail(
            code=ApiErrorCode.REQUEST_VALIDATION_FAILED,
            message="Request validation failed",
            context={"violations": jsonable_encoder(exception.errors())},
        )
    )
    return JSONResponse(status_code=422, content=error_response.model_dump(mode="json"))


@app.middleware("http")
async def disable_job_response_caching(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/api/jobs/"):
        response.headers["Cache-Control"] = "no-store"
        response.headers.add_vary_header("X-Job-Token")
    return response


app.include_router(jobs_api.router)
app.include_router(health_api.router)
app.include_router(metrics_api.router)


@app.get("/")
async def read_root() -> dict[str, str]:
    return {"message": "Welcome to the Playwright Job Runner API!"}


def run_server() -> None:
    reload_enabled = os.getenv("UVICORN_RELOAD", "false").lower() == "true"
    previous_sigterm_handler = signal.getsignal(signal.SIGTERM)

    def preserve_requested_exit_code(
        received_signal: int,
        frame: FrameType | None,
    ) -> None:
        if playwright_manager.requested_exit_code() is not None:
            return
        if callable(previous_sigterm_handler):
            previous_sigterm_handler(received_signal, frame)
        elif previous_sigterm_handler != signal.SIG_IGN:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.raise_signal(signal.SIGTERM)

    signal.signal(signal.SIGTERM, preserve_requested_exit_code)
    try:
        uvicorn.run(
            "src.main:app",
            host=HOST,
            port=PORT,
            reload=reload_enabled,
            log_level="info",
        )
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm_handler)

    exit_code = playwright_manager.requested_exit_code()
    if exit_code is not None:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    run_server()
