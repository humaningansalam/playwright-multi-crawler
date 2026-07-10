import asyncio
import logging
import os
import tomllib
from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager

# API 라우터 임포트
from src.api import health as health_api
from src.api import jobs as jobs_api
from src.api import metrics as metrics_api
from src.common import tool_utils
# 핵심 모듈 임포트 (이벤트 핸들러에서 사용)
from src.core import playwright_manager
from src.worker import job_processor
# 설정 및 로깅 설정 함수 임포트
from src.config import HOST, LOG_LEVEL, PORT, LOKI_URL, LOKI_TAGS, LOG_FILE_PATH

from his_mon import setup_logging, ResourceMonitor
from src.common.metrics import metrics

# 로깅 설정 실행
setup_logging(
    level=LOG_LEVEL,
    loki_url=LOKI_URL,
    tags=LOKI_TAGS,
    log_file=LOG_FILE_PATH
)


def _get_app_version() -> str:
    """Load API version from installed package metadata with a local fallback."""
    try:
        return package_version("playwright-multi-crawler")
    except PackageNotFoundError:
        pass

    pyproject_path = Path(__file__).resolve().parent.parent / "pyproject.toml"
    try:
        with pyproject_path.open("rb") as handle:
            project_config = tomllib.load(handle)
        return project_config["project"]["version"]
    except (OSError, KeyError, tomllib.TOMLDecodeError):
        return "0.0.0"

# --- FastAPI 앱 생성 ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 시 실행"""
    logging.info("Application startup sequence initiated...")
    heavy_startup = os.getenv("RUN_HEAVY_STARTUP", "true").lower() == "true"
    monitor = None
    display_started = False
    playwright_start_attempted = False
    workers_started = False
    cleanup_task = None
    app.state.job_submission_enabled = False

    try:
        monitor = ResourceMonitor(metrics_obj=metrics, interval=5)
        monitor.start()
        app.state.monitor = monitor

        tool_utils.ensure_job_folder()
        if heavy_startup:
            if not tool_utils.start_display():
                raise RuntimeError("Virtual display startup failed; refusing to launch headful browser")
            display_started = True
            playwright_start_attempted = True
            await playwright_manager.start()
            job_processor.start_workers()
            workers_started = True
            app.state.job_submission_enabled = True
        else:
            logging.info("Skipping display/Playwright startup (RUN_HEAVY_STARTUP=false)")
            logging.info("Skipping worker startup (RUN_HEAVY_STARTUP=false)")

        cleanup_task = asyncio.create_task(tool_utils.periodic_cleanup(), name="PeriodicCleanupTask")
        app.state.cleanup_task = cleanup_task
        logging.info("Application startup sequence completed.")
        yield
    finally:
        logging.info("Application shutdown sequence initiated...")
        app.state.job_submission_enabled = False
        if workers_started:
            try:
                await job_processor.stop_workers()
            except Exception:
                logging.exception("Worker shutdown failed; continuing resource teardown.")
        if playwright_start_attempted:
            try:
                await playwright_manager.shutdown()
            except Exception:
                logging.exception("Playwright shutdown failed.")
        if display_started:
            tool_utils.stop_display()

        if cleanup_task and not cleanup_task.done():
            cleanup_task.cancel()
            logging.info("Cancelled periodic cleanup task.")
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass

        if monitor:
            try:
                monitor.stop()
            except Exception:
                logging.exception("Resource monitor shutdown failed.")

        logging.info("Application shutdown sequence completed.")


app = FastAPI(
    title="Playwright Job Runner API",
    description="API for submitting and managing Playwright browser automation jobs.",
    version=_get_app_version(),
    lifespan=lifespan
)

# --- API 라우터 포함 ---
app.include_router(jobs_api.router) 
app.include_router(health_api.router) 
app.include_router(metrics_api.router)

# --- 루트 경로  ---
@app.get("/")
async def read_root():
    return {"message": "Welcome to the Playwright Job Runner API!"}

# --- 실행 ---
if __name__ == "__main__":
    reload_enabled = os.getenv("UVICORN_RELOAD", "false").lower() == "true"
    logging.info(f"Starting Uvicorn server on {HOST}:{PORT}")
    uvicorn.run("src.main:app", host=HOST, port=PORT, reload=reload_enabled, log_level="info")
