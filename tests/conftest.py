import asyncio
import os
import shutil

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

# src 폴더를 sys.path에 추가하여 src 내부 모듈 임포트 가능하게 함
# (테스트 실행 환경에 따라 필요 없을 수도 있음)
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# FastAPI 애플리케이션 임포트
# src.main 에서 app 객체를 가져옵니다.
os.environ.setdefault("RUN_HEAVY_STARTUP", "false")
os.environ.setdefault("PYTHONPATH", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config import JOB_FOLDER
from src.common.metrics import metrics
from src.core import job_queue, state_manager


@pytest.fixture(autouse=True)
def mark_test_app_workers_ready(monkeypatch):
    """API unit tests mock queue behavior and therefore opt into submission."""
    from src.main import app

    monkeypatch.setattr("src.core.playwright_manager.is_browser_connected", lambda: True)
    app.state.job_submission_enabled = True
    yield
    app.state.job_submission_enabled = False

@pytest.fixture(scope="session")
def event_loop():
    """
    pytest-asyncio가 세션 범위에서 이벤트 루프를 사용하도록 설정합니다.
    모든 비동기 테스트는 이 루프 위에서 실행됩니다.
    """
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="session")
async def client():
    """
    FastAPI 애플리케이션에 대한 비동기 테스트 클라이언트를 생성하는 픽스처입니다.
    'session' 범위는 테스트 세션 동안 클라이언트가 한 번만 생성되도록 합니다.
    """
    from src.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as test_client:
        yield test_client


@pytest_asyncio.fixture(autouse=True)
async def cleanup_job_state_and_folder():
    """
    각 테스트 함수 실행 *전후*에 상태와 작업 폴더를 정리하는 픽스처입니다.
    autouse=True 이므로 모든 테스트 함수에 자동으로 적용됩니다.
    """
    async def reset_test_state():
        async with state_manager._job_status_lock:
            state_manager._job_status_and_results.clear()
        async with state_manager._submitted_jobs_lock:
            state_manager._submitted_jobs.clear()

        job_queue._queue = asyncio.Queue()
        job_queue._cancelled_job_ids.clear()
        job_queue._claimed_job_ids.clear()
        job_queue._queued_job_ids.clear()
        metrics.queued_jobs.set(0)

        if os.path.exists(JOB_FOLDER):
            for item in os.listdir(JOB_FOLDER):
                item_path = os.path.join(JOB_FOLDER, item)
                try:
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    elif os.path.isfile(item_path):
                        os.unlink(item_path)
                except Exception as e:
                    print(f"Warning: Error cleaning up {item_path}: {e}")

    await reset_test_state()
    yield
    await reset_test_state()
