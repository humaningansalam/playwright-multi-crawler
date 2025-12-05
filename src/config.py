import os
import logging
import time
from dotenv import load_dotenv

# .env 파일 로드 
dotenv_path = os.path.join(os.path.dirname(__file__), '../.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path)
    print(f"Loaded .env file from: {dotenv_path}") 
else:
    logging.warning(f".env file not found at {dotenv_path}")

# 시간대 설정
os.environ['TZ'] = os.getenv('TZ', 'Asia/Seoul') 
if hasattr(time, 'tzset'):
    time.tzset()
    logging.info(f"Timezone set to {os.environ['TZ']}")
else:
    logging.warning("time.tzset() not available on this system.")

# 설정 상수
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", 3))
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
JOB_FOLDER = os.path.join(PROJECT_ROOT, os.getenv('JOB_FOLDER', 'submitted_jobs'))
#CONTEXT_PATH = os.path.join(PROJECT_ROOT, os.getenv('CONTEXT_PATH', 'browser_context.json'))
JOB_RETENTION_DAYS = int(os.getenv("JOB_RETENTION_DAYS", 3))
CLEANUP_INTERVAL_HOURS = int(os.getenv("CLEANUP_INTERVAL_HOURS", 24))
PORT = int(os.environ.get("PORT", 5000))
HOST = os.environ.get("HOST", "0.0.0.0")
CDP_PORT = int(os.getenv("CDP_PORT", 9222))
CDP_URL = f"http://127.0.0.1:{CDP_PORT}"