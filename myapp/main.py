import asyncio
import logging
import os
import importlib.util
import uuid
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext
import uvicorn

# 설정
logging.basicConfig(level=logging.INFO)
MAX_CONCURRENT_TASKS = 3
JOB_FOLDER = 'submitted_jobs'

# 큐 및 중복 작업 추적
queue = asyncio.Queue()
submitted_jobs = set()
job_results = {}
job_futures = {}

app = FastAPI()

# Playwright 관련 변수
playwright: Playwright = None
browser: Browser = None
context: BrowserContext = None
workers = []

# 외부 크롤링 스크립트를 동적으로 불러와 실행하는 함수
async def process_job(page, script_path, jobname, job_id):
    try:
        logging.info(f"Starting job '{jobname}' with script: {script_path}")

        # 외부 스크립트 동적 임포트
        spec = importlib.util.spec_from_file_location("external_module", script_path)
        external_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(external_module)

        # 스크립트에 'crawl' 함수가 있는지 확인 후 실행
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
    finally:
        submitted_jobs.discard(jobname)

# 작업 디스패처
async def dispatch_job(job):
    script_path = job['script_path']
    jobname = job['jobname']
    job_id = job['job_id']

    # 페이지 생성 및 작업 처리
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

# FastAPI 이벤트 핸들러: 애플리케이션 시작 시 실행
@app.on_event("startup")
async def startup_event():
    global playwright, browser, context, workers

    if not os.path.exists(JOB_FOLDER):
        os.makedirs(JOB_FOLDER)

    playwright = await async_playwright().start()
    logging.info("Launching Playwright browser...")
    browser = await playwright.chromium.launch(headless=False)
    context = await browser.new_context()

    page = await context.new_page()
    await page.goto('https://example.com', wait_until='networkidle')
    logging.info("Initial page loaded: https://example.com")
    await page.close()

    workers = [asyncio.create_task(worker()) for _ in range(MAX_CONCURRENT_TASKS)]

# FastAPI 이벤트 핸들러: 애플리케이션 종료 시 실행
@app.on_event("shutdown")
async def shutdown_event():
    for _ in range(MAX_CONCURRENT_TASKS):
        await queue.put(None)
    await asyncio.gather(*workers)

    await context.close()
    await browser.close()
    await playwright.stop()

# 작업 제출 후 결과 대기
@app.post("/submit")
async def submit_job(
    jobname: str = Form(...),
    script_file: UploadFile = File(...),
):
    if not jobname or not script_file:
        return JSONResponse({'error': 'Jobname and script file are required'}, status_code=400)

    if jobname in submitted_jobs:
        return JSONResponse({'status': 'Duplicate job, ignored'}, status_code=409)

    job_id = str(uuid.uuid4())
    submitted_jobs.add(jobname)
    script_path = os.path.join(JOB_FOLDER, script_file.filename)

    contents = await script_file.read()
    with open(script_path, 'wb') as f:
        f.write(contents)

    future = asyncio.Future()
    job_futures[job_id] = future

    job = {'script_path': script_path, 'jobname': jobname, 'job_id': job_id}
    await queue.put(job)
    logging.debug(f"Job '{jobname}' added to the queue with ID: {job_id}")

    result = await future
    del job_futures[job_id]

    return {'status': 'Job completed', 'result': result, 'job_id': job_id}

# `__main__` 블록에서 FastAPI 애플리케이션 실행
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
