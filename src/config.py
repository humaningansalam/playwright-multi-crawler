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
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
LOKI_URL = os.getenv("LOKI_URL")
LOKI_TAGS = {
    "app": os.getenv("APP_NAME", "playwright-runner"),
    "env": os.getenv("ENV", "production")
}

LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE_PATH = os.path.join(LOG_DIR, "app.log")

MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", 3))
JOB_FOLDER = os.path.join(PROJECT_ROOT, os.getenv('JOB_FOLDER', 'submitted_jobs'))
#CONTEXT_PATH = os.path.join(PROJECT_ROOT, os.getenv('CONTEXT_PATH', 'browser_context.json'))
JOB_RETENTION_DAYS = int(os.getenv("JOB_RETENTION_DAYS", 3))
CLEANUP_INTERVAL_HOURS = int(os.getenv("CLEANUP_INTERVAL_HOURS", 24))
JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", 3600))
PORT = int(os.environ.get("PORT", 5000))
HOST = os.environ.get("HOST", "0.0.0.0")
CDP_PORT = int(os.getenv("CDP_PORT", 9222))
CDP_URL = f"http://127.0.0.1:{CDP_PORT}"