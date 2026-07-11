from pathlib import Path
import asyncio
import json
import os
import signal
import sys

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
async def test_job_runner_writes_result_when_page_cleanup_fails(monkeypatch, tmp_path):
    script_path = tmp_path / "script.py"
    script_path.write_text(
        "async def crawl(page, context, job_path):\n"
        "    return {'ok': True}\n",
        encoding="utf-8",
    )
    browser = _FakeBrowser()

    async def fail_close():
        raise RuntimeError("page close failed")

    browser.context.page.close = fail_close
    monkeypatch.setattr(job_runner, "async_playwright", lambda: _FakePlaywright(browser))

    await job_runner.run_user_script("job-1", str(script_path), str(tmp_path))

    result = json.loads((tmp_path / job_runner.RESULT_FILENAME).read_text(encoding="utf-8"))
    assert result["status"] == "FAILED"
    assert result["error"] == {
        "error": "Browser cleanup failed",
        "cleanup_errors": [{"resource": "page", "error": "page close failed"}],
    }
    assert browser.context.closed is True


@pytest.mark.asyncio
async def test_job_runner_writes_result_when_context_cleanup_fails(monkeypatch, tmp_path):
    script_path = tmp_path / "script.py"
    script_path.write_text(
        "async def crawl(page, context, job_path):\n"
        "    return {'ok': True}\n",
        encoding="utf-8",
    )
    browser = _FakeBrowser()

    async def fail_close():
        raise RuntimeError("context close failed")

    browser.context.close = fail_close
    monkeypatch.setattr(job_runner, "async_playwright", lambda: _FakePlaywright(browser))

    await job_runner.run_user_script("job-1", str(script_path), str(tmp_path))

    result = json.loads((tmp_path / job_runner.RESULT_FILENAME).read_text(encoding="utf-8"))
    assert result["status"] == "FAILED"
    assert result["error"] == {
        "error": "Browser cleanup failed",
        "cleanup_errors": [{"resource": "context", "error": "context close failed"}],
    }
    assert browser.context.page.closed is True


@pytest.mark.asyncio
async def test_job_runner_imports_helper_from_job_directory(monkeypatch, tmp_path):
    helper_module = "job_helper_for_import_test"
    script_path = tmp_path / "script.py"
    (tmp_path / f"{helper_module}.py").write_text(
        "def value():\n"
        "    return 'from helper'\n",
        encoding="utf-8",
    )
    script_path.write_text(
        f"from {helper_module} import value\n\n"
        "async def crawl(page, context, job_path):\n"
        "    return {'value': value()}\n",
        encoding="utf-8",
    )
    browser = _FakeBrowser()
    monkeypatch.setattr(job_runner, "async_playwright", lambda: _FakePlaywright(browser))
    sys.modules.pop(helper_module, None)

    await job_runner.run_user_script("job-1", str(script_path), str(tmp_path))

    result = json.loads((tmp_path / job_runner.RESULT_FILENAME).read_text(encoding="utf-8"))
    assert result == {"status": "COMPLETED", "result": {"value": "from helper"}, "error": None}


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
    if os.name == "posix":
        assert subprocess_kwargs["start_new_session"] is True


@pytest.mark.asyncio
async def test_job_processor_streams_output_to_log_and_bounds_tail(tmp_path):
    stream = asyncio.StreamReader()
    stream.feed_data(b"prefix-" + b"x" * (job_processor.LOG_TAIL_BYTES + 10))
    stream.feed_eof()
    log_path = tmp_path / job_processor.STDOUT_LOG_FILENAME

    tail = await job_processor._stream_output_to_log(stream, str(log_path))

    assert log_path.read_bytes().startswith(b"prefix-")
    assert log_path.stat().st_size == len(b"prefix-") + job_processor.LOG_TAIL_BYTES + 10
    assert len(tail.encode("utf-8")) == job_processor.LOG_TAIL_BYTES


@pytest.mark.asyncio
async def test_job_processor_reads_completed_result_file(tmp_path):
    expected = {"status": "COMPLETED", "result": {"items": ["one"]}, "error": None}
    (tmp_path / job_processor.RESULT_FILENAME).write_text(json.dumps(expected), encoding="utf-8")

    result = await job_processor._read_result_file(str(tmp_path))

    assert result == expected


@pytest.mark.asyncio
async def test_job_processor_terminates_process_group(monkeypatch):
    signals = []

    class _FakeProcess:
        pid = 4321
        returncode = None

        async def wait(self):
            return 0

        def terminate(self):
            raise AssertionError("POSIX process groups must use killpg")

    process = _FakeProcess()
    if os.name == "posix":
        monkeypatch.setattr(job_processor.os, "killpg", lambda pid, sig: signals.append((pid, sig)))

    await job_processor._terminate_process(process, "job-1")

    if os.name == "posix":
        assert signals == [(process.pid, signal.SIGTERM)]


@pytest.mark.asyncio
async def test_job_processor_terminates_group_after_direct_runner_exits(monkeypatch):
    signals = []

    class _ExitedProcess:
        pid = 4321
        returncode = 0

    process = _ExitedProcess()
    if os.name == "posix":
        monkeypatch.setattr(job_processor.os, "killpg", lambda pid, sig: signals.append((pid, sig)))

    await job_processor._terminate_process(process, "job-1")

    if os.name == "posix":
        assert signals == [(process.pid, signal.SIGTERM)]


@pytest.mark.asyncio
async def test_job_processor_escalates_when_output_pipes_do_not_close(monkeypatch):
    release_streams = asyncio.Event()
    termination_calls = []

    async def delayed_tail(value):
        await release_streams.wait()
        return value

    class _ExitedProcess:
        returncode = 0

    async def fake_terminate(process, job_id, force=False):
        termination_calls.append((process, job_id, force))
        release_streams.set()

    monkeypatch.setattr(job_processor, "LOG_DRAIN_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(job_processor, "_terminate_process", fake_terminate)

    stdout_task = asyncio.create_task(delayed_tail("stdout"))
    stderr_task = asyncio.create_task(delayed_tail("stderr"))
    process = _ExitedProcess()
    result = await job_processor._drain_output_tasks(stdout_task, stderr_task, process, "job-1")

    assert result == ("stdout", "stderr")
    assert termination_calls == [(process, "job-1", True)]


@pytest.mark.asyncio
async def test_job_processor_terminates_process_group_when_cancelled(monkeypatch, tmp_path):
    script_path = tmp_path / "script.py"
    script_path.write_text("# test script\n", encoding="utf-8")
    communicate_started = asyncio.Event()
    terminated = []

    class _FakeProcess:
        returncode = None

        async def communicate(self):
            communicate_started.set()
            await asyncio.Event().wait()

    process = _FakeProcess()

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return process

    async def fake_terminate(received_process, job_id):
        terminated.append((received_process, job_id))

    async def ignore_state_update(*_args, **_kwargs):
        return None

    monkeypatch.setattr(job_processor.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(job_processor, "_terminate_process", fake_terminate)
    monkeypatch.setattr(job_processor.state, "update_job_status", ignore_state_update)
    monkeypatch.setattr(job_processor.state, "remove_submitted_job", ignore_state_update)

    task = asyncio.create_task(job_processor._process_job_internal(str(script_path), "cancel-test", "job-1"))
    await communicate_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert terminated == [(process, "job-1")]


@pytest.mark.asyncio
async def test_job_processor_retains_log_tails_when_cancelled(monkeypatch, tmp_path):
    script_path = tmp_path / "script.py"
    script_path.write_text("# test script\n", encoding="utf-8")
    wait_started = asyncio.Event()
    state_updates = []

    class _FakeProcess:
        returncode = None

        def __init__(self):
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            self.stdout.feed_data(b"stdout before cancel\n")
            self.stderr.feed_data(b"stderr before cancel\n")
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        async def wait(self):
            wait_started.set()
            await asyncio.Event().wait()

    process = _FakeProcess()

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return process

    async def fake_terminate(_process, _job_id):
        return None

    async def capture_state_update(*args, **kwargs):
        state_updates.append((args, kwargs))

    async def ignore_remove_submitted_job(*_args, **_kwargs):
        return None

    monkeypatch.setattr(job_processor.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(job_processor, "_terminate_process", fake_terminate)
    monkeypatch.setattr(job_processor.state, "update_job_status", capture_state_update)
    monkeypatch.setattr(job_processor.state, "remove_submitted_job", ignore_remove_submitted_job)

    task = asyncio.create_task(job_processor._process_job_internal(str(script_path), "cancel-logs", "job-1"))
    await wait_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    final_update = state_updates[-1]
    assert final_update[0][1] == job_processor.JobStatus.CANCELLED
    assert final_update[0][4] == {
        "stdout": "stdout before cancel",
        "stderr": "stderr before cancel",
    }
