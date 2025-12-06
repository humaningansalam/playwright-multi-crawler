import asyncio
import logging
from typing import Optional

import uvicorn
from fastapi import FastAPI
from playwright.async_api import Page

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
app = FastAPI(
    title="Playwright Job Runner API",
    description="API for submitting and managing Playwright browser automation jobs."
)

# --- 이벤트 핸들러 ---
@app.on_event("startup")
async def startup_event():
    """애플리케이션 시작 시 실행"""
    logging.info("Application startup sequence initiated...")

    # 리소스 모니터 시작
    monitor = ResourceMonitor(metrics_obj=metrics, interval=5)
    monitor.start()
    # 작업 폴더 확인/생성
    tool_utils.ensure_job_folder()
    # 가상 디스플레이 시작
    tool_utils.start_display()
    # Playwright 시작 및 브라우저/컨텍스트 준비
    await playwright_manager.start()
    # 워커 태스크 시작
    job_processor.start_workers()
    # 주기적 정리 작업 시작드
    asyncio.create_task(tool_utils.periodic_cleanup(), name="PeriodicCleanupTask")

    logging.info("Application startup sequence completed.")

@app.on_event("shutdown")
async def shutdown_event():
    """애플리케이션 종료 시 실행"""
    logging.info("Application shutdown sequence initiated...")
    # 워커 종료 
    await job_processor.stop_workers()
    # Playwright 종료 
    await playwright_manager.shutdown()
    # 가상 디스플레이 종료
    tool_utils.stop_display()
    # 주기적 정리 태스크 등 다른 백그라운드 태스크도 여기서 취소/정리 가능
    for task in asyncio.all_tasks():
        if task.get_name() == "PeriodicCleanupTask" and not task.done():
             task.cancel()
             logging.info("Cancelled periodic cleanup task.")
             # await task # 취소 완료 대기 

    logging.info("Application shutdown sequence completed.")

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
