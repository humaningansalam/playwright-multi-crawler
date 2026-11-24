import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

PROJECT_ROOT_PATH = Path(__file__).resolve().parent.parent
DOTENV_PATH = PROJECT_ROOT_PATH / ".env"

if DOTENV_PATH.is_file():
    load_dotenv(dotenv_path=DOTENV_PATH)
    logger.debug("Loaded environment variables from %s", DOTENV_PATH)
else:
    logger.debug("No .env file found at %s", DOTENV_PATH)

os.environ["TZ"] = os.getenv("TZ", "Asia/Seoul")
if hasattr(time, "tzset"):
    time.tzset()

PROJECT_ROOT = str(PROJECT_ROOT_PATH)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOKI_URL = os.getenv("LOKI_URL")
LOKI_TAGS = {
    "app": os.getenv("APP_NAME", "playwright-runner"),
    "env": os.getenv("APP_ENV", "dev"),
}

LOG_DIR = PROJECT_ROOT_PATH / "logs"
LOG_FILE_PATH = str(LOG_DIR / "app.log")

MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", "3"))
JOB_FOLDER = str(PROJECT_ROOT_PATH / os.getenv("JOB_FOLDER", "submitted_jobs"))
JOB_RETENTION_DAYS = int(os.getenv("JOB_RETENTION_DAYS", "3"))
CLEANUP_INTERVAL_HOURS = int(os.getenv("CLEANUP_INTERVAL_HOURS", "24"))
JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", "3600"))
PORT = int(os.getenv("PORT", "5000"))
HOST = os.getenv("HOST", "0.0.0.0")
CDP_PORT = int(os.getenv("CDP_PORT", "9222"))
CDP_URL = f"http://127.0.0.1:{CDP_PORT}"
BROWSER_EXECUTABLE_PATH = os.getenv("BROWSER_EXECUTABLE_PATH") or None

VIRTUAL_DISPLAY_BACKEND = os.getenv("VIRTUAL_DISPLAY_BACKEND", "xvfb").strip().lower()
