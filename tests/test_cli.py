import argparse
import json
from pathlib import Path

import httpx
import pytest

from src import cli
from src.models.job import JobStatus


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_run_submits_follows_logs_and_downloads(tmp_path, capsys):
    script = tmp_path / "crawl.py"
    script.write_text("async def crawl(*args): pass\n", encoding="utf-8")
    extra = tmp_path / "input.txt"
    extra.write_text("input", encoding="utf-8")
    requests = []

    def handler(request):
        requests.append((request.method, request.url.path))
        if request.url.path.endswith("/submit"):
            body = request.read()
            assert b'name="jobname"' in body
            assert b"\r\n\r\ndemo\r\n" in body
            assert b'filename="crawl.py"' in body
            assert b'filename="input.txt"' in body
            return httpx.Response(202, json={"job_id": "job-1", "status": "PENDING"})
        if request.url.path.endswith("/logs/job-1"):
            return httpx.Response(
                200,
                text='event: stdout\ndata: "hello\\n"\n\nevent: stderr\ndata: "warn\\n"\n\n',
            )
        if request.url.path.endswith("/results/job-1"):
            return httpx.Response(
                200,
                json={
                    "job_id": "job-1",
                    "status": "COMPLETED",
                    "result": {"ok": True},
                    "files": {"output.txt": "/api/jobs/download/job-1/output.txt"},
                },
            )
        if request.url.path.endswith("/download/job-1/output.txt"):
            return httpx.Response(200, content=b"artifact")
        raise AssertionError(request.url)

    args = argparse.Namespace(
        script=script,
        job_name="demo",
        additional=[extra],
        server="http://service.local",
        output=tmp_path / "downloads",
    )
    with _client(handler) as client:
        assert cli.run(args, client) == 0

    captured = capsys.readouterr()
    assert captured.out.startswith("hello\n")
    assert json.loads(captured.out[captured.out.index("{"):])["status"] == JobStatus.COMPLETED
    assert "job_id=job-1" in captured.err
    assert "warn" in captured.err
    assert (tmp_path / "downloads" / "job-1" / "output.txt").read_bytes() == b"artifact"
    assert requests == [
        ("POST", "/api/jobs/submit"),
        ("GET", "/api/jobs/logs/job-1"),
        ("GET", "/api/jobs/results/job-1"),
        ("GET", "/api/jobs/download/job-1/output.txt"),
    ]


def test_run_returns_failure_for_terminal_error(tmp_path):
    script = tmp_path / "crawl.py"
    script.write_text("pass\n", encoding="utf-8")

    def handler(request):
        if request.url.path.endswith("/submit"):
            return httpx.Response(202, json={"job_id": "job-1", "status": "PENDING"})
        if request.url.path.endswith("/logs/job-1"):
            return httpx.Response(200, text="")
        return httpx.Response(
            200,
            json={
                "job_id": "job-1",
                "status": "FAILED",
                "result": {"code": "PROCESSING_FAILED", "message": "failed"},
            },
        )

    args = argparse.Namespace(
        script=script,
        job_name="demo",
        additional=[],
        server="http://service.local",
        output=tmp_path,
    )
    with _client(handler) as client:
        assert cli.run(args, client) == 1


def test_download_rejects_other_origin():
    with pytest.raises(cli.CrawlerCliError, match="outside"):
        cli._download_url("http://service.local", "http://other.local/file")


def test_run_rejects_missing_script(tmp_path):
    args = argparse.Namespace(
        script=tmp_path / "missing.py",
        job_name="demo",
        additional=[],
        server="http://service.local",
        output=tmp_path,
    )
    with _client(lambda _request: pytest.fail("network must not be called")) as client:
        with pytest.raises(cli.CrawlerCliError, match="Script file not found"):
            cli.run(args, client)


def test_run_cancels_remote_job_when_log_follow_is_interrupted(tmp_path, monkeypatch, capsys):
    script = tmp_path / "crawl.py"
    script.write_text("pass\n", encoding="utf-8")
    requests = []

    def handler(request):
        requests.append((request.method, request.url.path))
        if request.url.path.endswith("/submit"):
            return httpx.Response(202, json={"job_id": "job-1", "status": "PENDING"})
        if request.url.path.endswith("/cancel"):
            return httpx.Response(200, json={"job_id": "job-1", "status": "CANCELLED"})
        raise AssertionError(request.url)

    monkeypatch.setattr(cli, "follow_logs", lambda *_args: (_ for _ in ()).throw(KeyboardInterrupt()))
    args = argparse.Namespace(
        script=script,
        job_name="demo",
        additional=[],
        server="http://service.local",
        output=tmp_path,
    )

    with _client(handler) as client:
        assert cli.run(args, client) == 130

    assert requests == [
        ("POST", "/api/jobs/submit"),
        ("POST", "/api/jobs/job-1/cancel"),
    ]
    assert "Cancelling remote job job-1" in capsys.readouterr().err
