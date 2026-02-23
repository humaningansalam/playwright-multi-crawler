import asyncio
import logging
import os

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

# --- FastAPI 앱 생성 ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 시 실행"""
    logging.info("Application startup sequence initiated...")

    # 리소스 모니터 시작
    monitor = ResourceMonitor(metrics_obj=metrics, interval=5)
    monitor.start()
    app.state.monitor = monitor

    # 작업 폴더 확인/생성
    tool_utils.ensure_job_folder()
    # 가상 디스플레이 시작
    if os.getenv("RUN_HEAVY_STARTUP", "true").lower() == "true":
        tool_utils.start_display()
        # Playwright 시작 및 브라우저/컨텍스트 준비
        await playwright_manager.start()
    else:
        logging.info("Skipping display/Playwright startup (RUN_HEAVY_STARTUP=false)")
    # 워커 태스크 시작
    job_processor.start_workers()
    # 주기적 정리 작업 시작
    app.state.cleanup_task = asyncio.create_task(
        tool_utils.periodic_cleanup(),
        name="PeriodicCleanupTask"
    )

    logging.info("Application startup sequence completed.")

    try:
        yield
    finally:
        logging.info("Application shutdown sequence initiated...")
        # 워커 종료
        await job_processor.stop_workers()

        if os.getenv("RUN_HEAVY_STARTUP", "true").lower() == "true":
            # Playwright 종료
            await playwright_manager.shutdown()
            # 가상 디스플레이 종료
            tool_utils.stop_display()

        cleanup_task = getattr(app.state, "cleanup_task", None)
        if cleanup_task and not cleanup_task.done():
            cleanup_task.cancel()
            logging.info("Cancelled periodic cleanup task.")
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass

        logging.info("Application shutdown sequence completed.")


app = FastAPI(
    title="Playwright Job Runner API",
    description="API for submitting and managing Playwright browser automation jobs.",
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
    logging.info(f"Starting Uvicorn server on {HOST}:{PORT}")
    uvicorn.run("src.main:app", host=HOST, port=PORT, reload=True, log_level="info")
