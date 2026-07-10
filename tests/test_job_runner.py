from pathlib import Path

import pytest

from src.worker import job_runner
from src.worker import job_processor


class _FakePage:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


class _FakeContext:
    def __init__(self):
        self.closed = False
        self.page = _FakePage()

    async def new_page(self):
        return self.page

    async def close(self):
        self.closed = True


class _FakeBrowser:
    def __init__(self):
        self.close_called = False
        self.context = _FakeContext()

    async def new_context(self):
        return self.context

    async def close(self):
        self.close_called = True


class _FakeChromium:
    def __init__(self, browser):
        self.browser = browser

    async def connect_over_cdp(self, _url):
        return self.browser


class _FakePlaywright:
    def __init__(self, browser):
        self.chromium = _FakeChromium(browser)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_job_runner_does_not_close_shared_cdp_browser(monkeypatch, tmp_path):
    script_path = tmp_path / "script.py"
    script_path.write_text(
        "async def crawl(page, context, job_path):\n"
        "    return {'ok': True}\n",
        encoding="utf-8",
    )
    browser = _FakeBrowser()

    monkeypatch.setattr(job_runner, "async_playwright", lambda: _FakePlaywright(browser))

    await job_runner.run_user_script("job-1", str(script_path), str(tmp_path))

    assert browser.context.page.closed is True
    assert browser.context.closed is True
    assert browser.close_called is False
    result_path = Path(tmp_path) / job_runner.RESULT_FILENAME
    assert result_path.exists()


@pytest.mark.asyncio
async def test_job_runner_keeps_shared_browser_open_when_crawl_fails(monkeypatch, tmp_path):
    script_path = tmp_path / "script.py"
    script_path.write_text(
        "async def crawl(page, context, job_path):\n"
        "    raise RuntimeError('boom')\n",
        encoding="utf-8",
    )
    browser = _FakeBrowser()

    monkeypatch.setattr(job_runner, "async_playwright", lambda: _FakePlaywright(browser))

    await job_runner.run_user_script("job-1", str(script_path), str(tmp_path))

    assert browser.context.page.closed is True
    assert browser.context.closed is True
    assert browser.close_called is False
    assert (Path(tmp_path) / job_runner.RESULT_FILENAME).exists()


@pytest.mark.asyncio
async def test_job_processor_runs_subprocess_in_job_directory(monkeypatch, tmp_path):
    script_path = tmp_path / "script.py"
    script_path.write_text("# test script\n", encoding="utf-8")
    subprocess_kwargs = {}

    class _FakeProcess:
        returncode = 0

        async def communicate(self):
            return b"", b""

    async def fake_create_subprocess_exec(*_args, **kwargs):
        subprocess_kwargs.update(kwargs)
        return _FakeProcess()

    async def fake_read_result_file(_job_path):
        return {"status": "COMPLETED", "result": {"ok": True}, "error": None}

    async def ignore_state_update(*_args, **_kwargs):
        return None

    monkeypatch.setattr(job_processor.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(job_processor, "_read_result_file", fake_read_result_file)
    monkeypatch.setattr(job_processor.state, "update_job_status", ignore_state_update)
    monkeypatch.setattr(job_processor.state, "remove_submitted_job", ignore_state_update)

    await job_processor._process_job_internal(str(script_path), "cwd-test", "job-1")

    assert subprocess_kwargs["cwd"] == str(tmp_path)
