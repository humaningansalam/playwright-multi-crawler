import asyncio
import logging
import os
import importlib.util
from flask import Flask, request, jsonify
from threading import Thread
from playwright.async_api import async_playwright

# 설정
logging.basicConfig(level=logging.INFO)  # 로깅 설정
MAX_CONCURRENT_TASKS = 3  # 동시에 처리할 최대 작업 수
JOB_FOLDER = 'submitted_jobs'  # 스크립트 파일이 저장될 폴더

# 큐 및 중복 작업 추적
queue = asyncio.PriorityQueue()  # 우선순위 큐 사용
submitted_jobs = set()  # URL 중복 방지를 위한 집합
app = Flask(__name__)

# 전역 이벤트 루프
loop = asyncio.get_event_loop()

# 외부 크롤링 스크립트를 동적으로 불러와 실행하는 함수
async def process_job(page, script_path, url, priority):
    try:
        logging.info(f"Starting job (Priority: {priority}) for URL: {url} with script: {script_path}")
        
        # 외부 스크립트 동적 임포트
        spec = importlib.util.spec_from_file_location("external_module", script_path)
        external_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(external_module)

        # 스크립트에 'crawl' 함수가 있는지 확인 후 실행
        if hasattr(external_module, 'crawl'):
            await external_module.crawl(page, url)
        else:
            raise AttributeError(f"The script {script_path} does not have a 'crawl' function.")
        
        logging.info(f"Completed job for URL: {url}")
    except Exception as e:
        logging.error(f"Error processing job for URL {url} with script {script_path}: {e}")

# 작업 디스패처
async def dispatch_job(browser, job):
    script_path = job['script_path']
    url = job['url']
    priority = job['priority']
    
    # 페이지 생성 및 작업 처리
    context = await browser.new_context()
    page = await context.new_page()
    await process_job(page, script_path, url, priority)
    await page.close()
    await context.close()

# 워커 함수: 단일 브라우저 인스턴스를 공유하여 리소스를 절약
async def worker(queue, browser):
    while True:
        job = await queue.get()
        if job is None:  # 종료 신호
            break
        await dispatch_job(browser, job)
        queue.task_done()

# Flask 엔드포인트: 작업 제출
@app.route('/submit', methods=['POST'])
def submit_job():
    url = request.form.get('url')
    script_file = request.files['script']
    priority = int(request.form.get('priority', 10))  # 기본 우선순위는 10

    if not url or not script_file:
        return jsonify({'error': 'URL and script file are required'}), 400

    # 중복 작업 방지: 동일한 URL이 이미 제출되었는지 확인
    if url in submitted_jobs:
        return jsonify({'status': 'Duplicate job, ignored'}), 409

    submitted_jobs.add(url)  # 중복 방지 집합에 추가
    script_path = os.path.join(JOB_FOLDER, script_file.filename)
    script_file.save(script_path)

    # 큐에 작업 추가
    job = {'script_path': script_path, 'url': url, 'priority': priority}
    asyncio.run_coroutine_threadsafe(queue.put((priority, job)), loop)

    return jsonify({'status': 'Job submitted', 'job': job}), 200

# Flask 서버 스레드 실행
def run_flask():
    app.run(host='0.0.0.0', port=5000)

if __name__ == '__main__':
    # 제출된 스크립트 폴더가 존재하지 않으면 생성
    if not os.path.exists(JOB_FOLDER):
        os.makedirs(JOB_FOLDER)

    # Flask 서버 스레드 시작
    flask_thread = Thread(target=run_flask)
    flask_thread.start()

    # Playwright 브라우저 인스턴스 생성 및 워커 실행
    async def main():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            # 제한된 수의 워커 생성
            workers = [asyncio.create_task(worker(queue, browser)) for _ in range(MAX_CONCURRENT_TASKS)]
            
            # 모든 워커 태스크 완료 대기
            await asyncio.gather(*workers)
            await browser.close()

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        for _ in range(MAX_CONCURRENT_TASKS):
            asyncio.run_coroutine_threadsafe(queue.put(None), loop)  # 워커 종료 신호
    finally:
        flask_thread.join()
