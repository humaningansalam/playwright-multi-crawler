import asyncio
import logging
import os
import importlib.util
import uuid
import traceback
import shutil
from datetime import datetime, timedelta
from typing import List
from pyvirtualdisplay import Display
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse, FileResponse
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext
import uvicorn
import time
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

os.environ['TZ'] = 'Asia/Seoul'
time.tzset() 

# 설정
logging.basicConfig(level=logging.INFO)
MAX_CONCURRENT_TASKS = 3
JOB_FOLDER = 'submitted_jobs'
JOB_RETENTION_DAYS = 3  # 작업 폴더 보존 기간 (일)
CLEANUP_INTERVAL_HOURS = 24  # 폴더 정리 주기 (시간 단위)

# 큐 및 중복 작업 추적
queue = asyncio.Queue()
submitted_jobs = set()
job_results = {}
job_futures = {}
submitted_jobs_lock = asyncio.Lock()  # 중복 작업 방지용 Lock

app = FastAPI()

# Playwright 관련 변수
playwright: Playwright = None
browser: Browser = None
context: BrowserContext = None
workers = []
display = Display(visible=1, backend="xephyr", size=(1920, 1080))

# 오래된 작업 폴더 삭제 함수
def clean_old_jobs():
    cutoff = datetime.now() - timedelta(days=JOB_RETENTION_DAYS)
    for jobname in os.listdir(JOB_FOLDER):
        job_path = os.path.join(JOB_FOLDER, jobname)
        if os.path.isdir(job_path) and datetime.fromtimestamp(os.path.getmtime(job_path)) < cutoff:
            shutil.rmtree(job_path, ignore_errors=True)
            logging.info(f"Deleted old job folder: {job_path}")

# 주기적으로 하루에 한 번 폴더 정리하는 비동기 작업
async def periodic_cleanup():
    while True:
        clean_old_jobs()
        await asyncio.sleep(CLEANUP_INTERVAL_HOURS * 3600)  # 하루 대기 후 다시 실행

# 외부 크롤링 스크립트를 동적으로 불러와 실행하는 함수
async def process_job(page, script_path, jobname, job_id):
    try:
        logging.info(f"Starting job '{jobname}' with script: {script_path}")

        # 외부 스크립트 동적 임포트
        spec = importlib.util.spec_from_file_location("external_module", script_path)
        external_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(external_module)

        if hasattr(external_module, 'crawl'):
            result = await external_module.crawl(page)
            job_results[job_id] = result
            logging.info(f"Job '{jobname}' completed with result: {result}")
            if job_id in job_futures:
                job_futures[job_id].set_result(result)
        else:
            raise AttributeError(f"The script {script_path} does not have a 'crawl' function.")

        logging.info(f"Completed job '{jobname}'")
    except Exception as e:
        error_result = {'error': str(e)}
        job_results[job_id] = error_result
        if job_id in job_futures:
            job_futures[job_id].set_result(error_result)
        logging.error(f"Error processing job '{jobname}' with script {script_path}: {e}")
        logging.error(traceback.format_exc())  # 전체 오류 스택 추적 추가
    finally:
        async with submitted_jobs_lock:
            submitted_jobs.discard(jobname)

# 작업 디스패처
async def dispatch_job(job):
    script_path = job['script_path']
    jobname = job['jobname']
    job_id = job['job_id']

    page = await context.new_page()
    logging.debug(f"New tab created for job '{jobname}'")
    await process_job(page, script_path, jobname, job_id)
    await page.close()

# 워커 함수
async def worker():
    while True:
        job = await queue.get()
        if job is None:
            logging.debug("Worker received shutdown signal")
            break
        await dispatch_job(job)
        queue.task_done()

@app.on_event("startup")
async def startup_event():
    global playwright, browser, context, workers

    if not os.path.exists(JOB_FOLDER):
        os.makedirs(JOB_FOLDER)

    # 오래된 작업 폴더 주기적으로 정리
    asyncio.create_task(periodic_cleanup())

    display.start()  # 가상 디스플레이 시작
    playwright = await async_playwright().start()
    logging.info("Launching Playwright browser...")
    browser = await playwright.chromium.launch(headless=False)
    context = await browser.new_context()

    page = await context.new_page()
    await page.goto('https://example.com', wait_until='networkidle')
    logging.info("Initial page loaded: https://example.com")
    await page.close()

    workers = [asyncio.create_task(worker()) for _ in range(MAX_CONCURRENT_TASKS)]

@app.on_event("shutdown")
async def shutdown_event():
    for _ in range(MAX_CONCURRENT_TASKS):
        await queue.put(None)
    await asyncio.gather(*workers)

    await context.close()
    await browser.close()
    await playwright.stop()
    display.stop()  # 가상 디스플레이 종료

@app.post("/submit")
async def submit_job(
    jobname: str = Form(...),
    script_file: UploadFile = File(...),
    additional_files: List[UploadFile] = File(default=[])
):
    if not jobname or not script_file:
        return JSONResponse({'error': 'Jobname and script file are required'}, status_code=400)

    async with submitted_jobs_lock:
        if jobname in submitted_jobs:
            return JSONResponse({'status': 'Duplicate job, ignored'}, status_code=409)
        submitted_jobs.add(jobname)

    job_id = str(uuid.uuid4())
    job_path = os.path.join(JOB_FOLDER, job_id)
    os.makedirs(job_path, exist_ok=True)

    logging.info(f"Processing job {job_id}: Saving script file {script_file.filename}")
    
    # script_file 저장
    script_path = os.path.join(job_path, 'script.py') 
    script_contents = await script_file.read()
    with open(script_path, 'wb') as f:
        f.write(script_contents)

    # additional_files 저장
    for add_file in additional_files:
        logging.info(f"Saving additional file: {add_file.filename}")
        content = await add_file.read()
        add_file_path = os.path.join(job_path, add_file.filename)
        with open(add_file_path, 'wb') as f:
            f.write(content)

    future = asyncio.Future()
    job_futures[job_id] = future

    job = {'script_path': script_path, 'jobname': jobname, 'job_id': job_id}
    await queue.put(job)
    logging.info(f"Job '{jobname}' queued with ID: {job_id}")

    # 작업 완료 대기
    result = await future
    del job_futures[job_id]

    # 실제 파일만 포함하여 다운로드 URL 생성
    files = {
        filename: f"/api/download/{job_id}/{filename}"
        for filename in os.listdir(job_path)
        if os.path.isfile(os.path.join(job_path, filename))
    }
    
    return {
        'status': 'Job completed', 
        'result': result, 
        'job_id': job_id, 
        'files': files
    }

@app.get("/api/download/{job_id}/{filename}")
async def download_file(job_id: str, filename: str):
    """개별 파일 다운로드 처리"""
    file_path = os.path.join(JOB_FOLDER, job_id, filename)
    if not os.path.isfile(file_path):
        return JSONResponse({'error': 'File not found'}, status_code=404)
    return FileResponse(file_path, media_type="application/octet-stream", filename=filename)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    uvicorn.run(app, host="0.0.0.0", port=port)
