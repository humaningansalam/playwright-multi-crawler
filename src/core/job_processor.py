

import asyncio
import importlib
import logging
import time
import os
import traceback
from typing import Dict, List, Optional, Any
from playwright.async_api import Page, BrowserContext

# 다른 core 모듈 및 config 임포트
from src.core import state_manager as state
from src.core import job_queue
from src.core import playwright_manager
from src.config import MAX_CONCURRENT_TASKS

_workers: List[asyncio.Task] = [] # 워커 태스크 저장 리스트

async def _process_job_internal(page: Page, script_path: str, jobname: str, job_id: str):
    """개별 작업을 실제로 처리하는 내부 함수"""
    start_time = time.time()
    logging.info(f"Starting job '{jobname}' (ID: {job_id}) with script: {script_path}")
    # 상태: RUNNING 업데이트
    await state.update_job_status(job_id, 'RUNNING')

    result_data = None
    error_occurred = False
    try:
        # 동적으로 스크립트 로드 및 실행
        # 모듈 이름 충돌 방지를 위해 job_id 사용
        module_name = f"job_module_{job_id}"
        spec = importlib.util.spec_from_file_location(module_name, script_path)
        if spec is None or spec.loader is None:
             raise ImportError(f"Could not create module spec for {script_path}")

        external_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(external_module)

        if hasattr(external_module, 'crawl') and asyncio.iscoroutinefunction(external_module.crawl):
            # crawl 함수에 필요한 인자 전달 (page, context, job_path)
            job_context: BrowserContext = page.context
            job_path = os.path.dirname(script_path)
            # crawl 함수 호출
            result_data = await external_module.crawl(page, job_context, job_path)
            logging.info(f"Job '{jobname}' (ID: {job_id}) completed successfully.")
        else:
            raise AttributeError(f"The script {script_path} must contain an async function named 'crawl'.")

    except Exception as e:
        error_occurred = True
        error_message = f"Error processing job '{jobname}' (ID: {job_id}): {e}"
        logging.error(error_message)
        # 상세 traceback 로깅
        logging.error(traceback.format_exc())
        # 결과에 에러 정보 포함
        result_data = {'error': str(e), 'traceback': traceback.format_exc()}
    finally:
        end_time = time.time()
        duration = end_time - start_time
        logging.info(f"Job '{jobname}' (ID: {job_id}) finished in {duration:.2f} seconds.")

        # 상태 업데이트: COMPLETED 또는 FAILED
        final_status = 'FAILED' if error_occurred else 'COMPLETED'
        await state.update_job_status(job_id, final_status, result_data, duration)
        # 작업 완료 후 submitted_jobs 세트에서 제거
        await state.remove_submitted_job(jobname)
        # 사용한 모듈 정리 
        # if module_name in sys.modules:
        #     del sys.modules[module_name]


async def _dispatch_job(job: Dict[str, Any]):
    """큐에서 작업을 받아 페이지를 할당하고 _process_job_internal 호출"""
    script_path = job['script_path']
    jobname = job['jobname']
    job_id = job['job_id']

    page: Optional[Page] = None
    browser_context = await playwright_manager.get_context()

    if not browser_context:
        logging.error(f"Cannot dispatch job {job_id}, browser context is not available.")
        await state.update_job_status(job_id, 'FAILED', {'error': 'Browser context unavailable'})
        await state.remove_submitted_job(jobname) 
        return

    try:
        # 작업별로 새 페이지 사용 
        page = await browser_context.new_page()
        logging.debug(f"New page created for job '{jobname}' (ID: {job_id})")
        # 실제 작업 처리 함수 호출
        await _process_job_internal(page, script_path, jobname, job_id)
    except Exception as e:
        # dispatch 단계에서의 예외 처리 
        error_message = f"Critical error dispatching job '{jobname}' (ID: {job_id}): {e}"
        logging.error(error_message)
        logging.error(traceback.format_exc())
        await state.update_job_status(job_id, 'FAILED', {'error': f"Dispatch error: {e}", 'traceback': traceback.format_exc()})
        await state.remove_submitted_job(jobname)
    finally:
        if page:
            try:
                # 페이지 닫기
                await page.close()
                logging.debug(f"Page closed for job '{jobname}' (ID: {job_id})")
            except Exception as page_close_e:
                # 페이지 닫기 실패는 경고 수준으로 로깅
                logging.warning(f"Error closing page for job {job_id}: {page_close_e}")


async def _worker():
    """큐에서 작업을 가져와 디스패처에게 전달하는 워커"""
    logging.info(f"Worker started: {asyncio.current_task().get_name()}")
    while True:
        job = await job_queue.get_job()
        if job is None:
            # 종료 신호 수신
            job_queue.task_done()
            break
        try:
            # 작업 디스패치
            await _dispatch_job(job)
        except Exception as e:
            # 워커 루프 내에서 예상치 못한 예외 발생 시 로깅 및 상태 업데이트 시도
            job_id = job.get('job_id', 'unknown')
            jobname = job.get('jobname', 'unknown')
            logging.error(f"Unhandled exception in worker loop for job '{jobname}' (ID: {job_id}): {e}", exc_info=True)
            if job_id != 'unknown':
                await state.update_job_status(job_id, 'FAILED', {'error': f"Worker loop error: {e}", 'traceback': traceback.format_exc()})
                await state.remove_submitted_job(jobname)
        finally:
            if job is not None:
                job_queue.task_done()
    logging.info(f"Worker stopped: {asyncio.current_task().get_name()}")


def start_workers():
    """설정된 수만큼 워커 태스크를 시작"""
    global _workers
    if _workers:
        logging.warning("Workers already started.")
        return
    _workers = [asyncio.create_task(_worker(), name=f"Worker-{i+1}") for i in range(MAX_CONCURRENT_TASKS)]
    logging.info(f"Started {len(_workers)} workers.")

async def stop_workers():
    """모든 워커 태스크를 정상적으로 종료"""
    global _workers
    if not _workers:
        logging.info("No workers to stop.")
        return

    logging.info("Stopping workers...")
    # 워커들에게 종료 신호 전송
    await job_queue.put_shutdown_signal(len(_workers))

    # 큐의 모든 작업 완료 대기 
    try:
        await job_queue.join(timeout=30.0)
    except asyncio.TimeoutError:
        logging.warning("Queue join timed out during shutdown. Some tasks might be interrupted.")

    # 워커 태스크 종료 대기 
    results = await asyncio.gather(*_workers, return_exceptions=True)
    for i, result in enumerate(results):
        if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
            logging.error(f"Worker {i+1} finished with error: {result}")
        elif isinstance(result, asyncio.CancelledError):
             logging.info(f"Worker {i+1} was cancelled.")

    logging.info(f"All {len(_workers)} workers stopped.")
    _workers = [] 