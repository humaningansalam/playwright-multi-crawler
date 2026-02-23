import pytest
import pytest_asyncio # 비동기 픽스처를 위해 필요
import asyncio
import os
import shutil
from httpx import AsyncClient, ASGITransport

# src 폴더를 sys.path에 추가하여 src 내부 모듈 임포트 가능하게 함
# (테스트 실행 환경에 따라 필요 없을 수도 있음)
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# FastAPI 애플리케이션 임포트
# src.main 에서 app 객체를 가져옵니다.
try:
    from src.main import app # FastAPI 앱 객체 임포트
    from src.config import JOB_FOLDER
    from src.core import state_manager
    from src.common import tool_utils
except ImportError as e:
    print(f"Error importing application modules: {e}")
    print("Ensure your project structure and PYTHONPATH are correct.")
    pytest.exit(f"Failed to import application modules: {e}", returncode=1)

tool_utils.set_logging("DEBUG")
os.environ.setdefault("RUN_HEAVY_STARTUP", "false")

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
    transport = ASGITransport(app=app, lifespan="on")
    # 'async with'를 사용하여 클라이언트의 생명주기를 관리합니다.
    # base_url은 테스트 시 요청 경로를 완전한 URL로 만들어줍니다.
    async with AsyncClient(transport=transport, base_url="http://testserver") as test_client:
        print("Test client created")
        yield test_client # 테스트 함수에서 이 클라이언트를 사용할 수 있도록 전달
        print("Test client closing")
    # 'async with' 블록이 끝나면 클라이언트가 자동으로 정리됩니다.

@pytest_asyncio.fixture(autouse=True)
async def cleanup_job_state_and_folder():
    """
    각 테스트 함수 실행 *전후*에 상태와 작업 폴더를 정리하는 픽스처입니다.
    autouse=True 이므로 모든 테스트 함수에 자동으로 적용됩니다.
    """
    # --- 테스트 실행 전 ---
    # 상태 딕셔너리 초기화 (테스트 간 독립성 보장)
    async with state_manager._job_status_lock:
        state_manager._job_status_and_results.clear()
    async with state_manager._submitted_jobs_lock:
        state_manager._submitted_jobs.clear()
    # 작업 폴더 내용 삭제 (폴더 자체는 유지)
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

    yield # --- 테스트 실행 ---

    # --- 테스트 실행 후 ---
    # (필요하다면 추가 정리 작업)
    print("Job state and folder cleaned up after test.")
