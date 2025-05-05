import asyncio
import logging
import os
import shutil
import logging
from datetime import datetime, timedelta
from typing import Optional
from pyvirtualdisplay import Display

from src.config import JOB_FOLDER, JOB_RETENTION_DAYS, CLEANUP_INTERVAL_HOURS
from src.core import state_manager as state 

_display_available = True
_display: Optional[Display] = None

def start_display() -> bool:
    """가상 디스플레이 시작 (Linux 환경, pyvirtualdisplay 설치 시)"""
    global _display
    if not _display_available or Display is None:
        logging.debug("Virtual display not available or not installed.")
        return False

    if _display:
        logging.warning("Virtual display already started.")
        return True

    try:
        logging.info("Starting virtual display...")
        _display = Display(visible=1, backend="xephyr",  size=(1920, 1080)) # visible=0 for headless
        _display.start()
        logging.info("Virtual display started successfully.")
        return True
    except Exception as e:
        # 디스플레이 시작 실패는 경고로 처리하고 계속 진행
        logging.warning(f"Could not initialize/start virtual display: {e}. Running without it.")
        _display = None
        return False

def stop_display():
    """가상 디스플레이 종료"""
    global _display
    if _display:
        logging.info("Stopping virtual display...")
        try:
            _display.stop()
            logging.info("Virtual display stopped.")
        except Exception as e:
            logging.error(f"Failed to stop virtual display: {e}")
        _display = None # 종료 후 None으로 설정

def clean_old_jobs():
    """오래된 작업 폴더 삭제 및 관련 상태 정보 정리 요청"""
    cutoff = datetime.now() - timedelta(days=JOB_RETENTION_DAYS)
    logging.info(f"Running cleanup for jobs older than {cutoff.isoformat()} in {JOB_FOLDER}")

    if not os.path.exists(JOB_FOLDER):
        logging.debug(f"Job folder {JOB_FOLDER} does not exist. Skipping cleanup.")
        return

    deleted_job_ids = []
    try:
        for item_name in os.listdir(JOB_FOLDER):
            item_path = os.path.join(JOB_FOLDER, item_name)
            try:
                if os.path.isdir(item_path): 
                    mod_time = datetime.fromtimestamp(os.path.getmtime(item_path))
                    if mod_time < cutoff:
                        logging.info(f"Deleting old job folder: {item_path} (modified: {mod_time})")
                        shutil.rmtree(item_path, ignore_errors=True)
                        deleted_job_ids.append(item_name)
            except FileNotFoundError:
                 logging.warning(f"File not found during cleanup scan: {item_path}")
                 continue # 다음 파일/폴더로 진행
            except Exception as e:
                logging.error(f"Error processing item {item_path} during cleanup: {e}")
    except Exception as e:
         logging.error(f"Error listing directory {JOB_FOLDER} during cleanup: {e}")


    # 상태 딕셔너리에서도 삭제된 job_id 정리 
    if deleted_job_ids:
        async def cleanup_state_task():
            logging.info(f"Requesting state cleanup for {len(deleted_job_ids)} old jobs.")
            for job_id in deleted_job_ids:
                await state.remove_job_state(job_id)
        try:
            # 현재 실행 중인 이벤트 루프에서 태스크 생성
            loop = asyncio.get_running_loop()
            loop.create_task(cleanup_state_task())
            logging.debug("Scheduled state cleanup task for old jobs.")
        except RuntimeError:
            logging.warning("Event loop not running, cannot schedule state cleanup task.")


async def periodic_cleanup():
    """주기적으로 오래된 작업 정리"""
    logging.info("Periodic cleanup task started.")
    while True:
        try:
            clean_old_jobs()
        except Exception as e:
            logging.error(f"Error during periodic cleanup execution: {e}", exc_info=True)

        # 다음 실행까지 대기
        sleep_duration = CLEANUP_INTERVAL_HOURS * 3600
        logging.debug(f"Periodic cleanup finished. Sleeping for {sleep_duration} seconds.")
        await asyncio.sleep(sleep_duration)

def ensure_job_folder():
    """JOB_FOLDER 존재 확인 및 생성"""
    if not os.path.exists(JOB_FOLDER):
        logging.info(f"Job folder {JOB_FOLDER} not found. Creating...")
        try:
            os.makedirs(JOB_FOLDER)
            logging.info(f"Created job folder: {JOB_FOLDER}")
        except OSError as e:
            logging.error(f"Failed to create job folder {JOB_FOLDER}: {e}")
            raise

def set_logging(log_level):
    """
    setting logging
    """
    # 로그 생성
    logger = logging.getLogger()
    # 로그 레벨 문자열을 적절한 로깅 상수로 변환
    log_level_constant = getattr(logging, log_level, logging.INFO)
    # 로그의 출력 기준 설정
    logger.setLevel(log_level_constant)
    # log 출력 형식
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # log를 console에 출력
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    # log를 파일에 출력
    #file_handler = logging.FileHandler('GoogleTrendsBot.log')
    #file_handler.setFormatter(formatter)
    #logger.addHandler(file_handler)