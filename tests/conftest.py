import asyncio
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("RUN_HEAVY_STARTUP", "false")
# Set before importing configuration so tests never clean the operator job directory.
_test_job_root = TemporaryDirectory(prefix="crawler-tests-")
os.environ["JOB_FOLDER"] = _test_job_root.name

from src.common.metrics import metrics  # noqa: E402
from src.config import JOB_FOLDER  # noqa: E402
from src.core import job_queue, state_manager  # noqa: E402


@pytest.fixture(autouse=True)
def mark_test_app_workers_ready(monkeypatch):
    """Allow API tests to submit jobs without starting the real browser runtime."""
    from src.main import app

    monkeypatch.setattr("src.core.playwright_manager.is_browser_connected", lambda: True)
    app.state.job_submission_enabled = True
    yield
    app.state.job_submission_enabled = False


@pytest.fixture(scope="session")
def event_loop():
    """Provide one event loop for session-scoped asynchronous fixtures."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def client():
    """Create an in-process asynchronous client for the FastAPI application."""
    from src.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as test_client:
        yield test_client


def _remove_test_job_artifacts() -> None:
    job_root = Path(JOB_FOLDER)
    if not job_root.exists():
        return

    for artifact_path in job_root.iterdir():
        if artifact_path.is_symlink() or artifact_path.is_file():
            artifact_path.unlink()
        elif artifact_path.is_dir():
            shutil.rmtree(artifact_path)


@pytest_asyncio.fixture(autouse=True)
async def cleanup_job_state_and_folder():
    """Reset in-memory and on-disk job state around every test."""

    async def reset_test_state() -> None:
        async with state_manager._state_change_lock:
            state_manager._job_status_and_results.clear()
            state_manager._submitted_jobs.clear()

        job_queue._queue = asyncio.Queue()
        job_queue._cancelled_job_ids.clear()
        job_queue._claimed_job_ids.clear()
        job_queue._queued_job_ids.clear()
        metrics.queued_jobs.set(0)
        _remove_test_job_artifacts()

    await reset_test_state()
    yield
    await reset_test_state()
