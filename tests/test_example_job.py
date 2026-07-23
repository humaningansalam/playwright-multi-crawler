import json
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from example import job
from src.models.job import JobProcessingResponse, JobStatus


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_bundled_client_uploads_crawl_script_and_additional_file(tmp_path):
    extra = tmp_path / "input.txt"
    extra.write_text("input", encoding="utf-8")

    def handler(request):
        assert request.method == "POST"
        assert request.url.path == "/api/jobs/submit"
        body = request.read()
        assert b'filename="crawl.py"' in body
        assert b'filename="input.txt"' in body
        assert b"example-job" in body
        return httpx.Response(202, json={"job_id": "job-1", "status": "PENDING"})

    with _client(handler) as client:
        job_id = job.submit_job(
            client,
            job.default_crawl_script_path(),
            "example-job",
            [extra],
            server="http://service.local",
        )

    assert job_id == "job-1"


@pytest.mark.parametrize("status", [JobStatus.CANCELLED, JobStatus.INTERRUPTED])
def test_bundled_client_stops_polling_for_terminal_statuses(status):
    def handler(_request):
        return httpx.Response(200, json={"job_id": "job-1", "status": status})

    with _client(handler) as client:
        result = job.poll_job_status(
            client,
            "job-1",
            server="http://service.local",
            interval_seconds=0,
        )

    assert result == job.PollResult(job.PollOutcome.TERMINAL, status)


def test_bundled_client_reports_unknown_status_as_invalid_response():
    def handler(_request):
        return httpx.Response(200, json={"job_id": "job-1", "status": "ALMOST_DONE"})

    with _client(handler) as client:
        result = job.poll_job_status(
            client,
            "job-1",
            server="http://service.local",
            interval_seconds=0,
        )

    assert result == job.PollResult(job.PollOutcome.INVALID_RESPONSE)


def test_bundled_client_cancels_remote_job():
    def handler(request):
        assert request.method == "POST"
        assert request.url.path == "/api/jobs/job-1/cancel"
        return httpx.Response(200, json={"job_id": "job-1", "status": "CANCELLED"})

    with _client(handler) as client:
        result = job.cancel_job(client, "job-1", server="http://service.local")

    assert result.status == JobStatus.CANCELLED


def test_bundled_client_downloads_result_files(tmp_path):
    requests = []

    def handler(request):
        requests.append(request.url.path)
        if request.url.path == "/api/jobs/results/job-1":
            return httpx.Response(
                200,
                json={
                    "job_id": "job-1",
                    "status": "COMPLETED",
                    "result": {"ok": True},
                    "files": {"screenshot.png": "/api/jobs/download/job-1/screenshot.png"},
                },
            )
        if request.url.path == "/api/jobs/download/job-1/screenshot.png":
            return httpx.Response(200, content=b"screenshot")
        raise AssertionError(request.url)

    with _client(handler) as client:
        result = job.get_job_results(client, "job-1", server="http://service.local")
        assert result is not None
        assert not isinstance(result, JobProcessingResponse)
        assert result.files is not None
        job.download_files(
            client,
            "job-1",
            result.files,
            server="http://service.local",
            output_dir=tmp_path,
        )

    assert requests == [
        "/api/jobs/results/job-1",
        "/api/jobs/download/job-1/screenshot.png",
    ]
    assert (tmp_path / "job-1" / "screenshot.png").read_bytes() == b"screenshot"


def test_bundled_client_is_importable_from_its_script_directory():
    example_dir = Path(job.__file__).parent
    result = subprocess.run(
        [sys.executable, "-c", "import runpy; runpy.run_path('job.py', run_name='not_main')"],
        cwd=example_dir,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert job.default_crawl_script_path().is_file()


def test_bundled_client_run_prints_terminal_result(monkeypatch, capsys):
    terminal_payload = {
        "job_id": "job-1",
        "status": "COMPLETED",
        "result": {"items": ["one"]},
    }
    monkeypatch.setattr(job, "submit_job", lambda *_args, **_kwargs: "job-1")
    monkeypatch.setattr(
        job,
        "poll_job_status",
        lambda *_args, **_kwargs: job.PollResult(job.PollOutcome.TERMINAL, JobStatus.COMPLETED),
    )
    monkeypatch.setattr(
        job,
        "get_job_results",
        lambda *_args, **_kwargs: job.JOB_RESULTS_RESPONSE_ADAPTER.validate_python(terminal_payload),
    )

    with _client(lambda _request: pytest.fail("network must not be called")) as client:
        assert job.run(client) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["job_id"] == terminal_payload["job_id"]
    assert output["status"] == terminal_payload["status"]
    assert output["result"] == terminal_payload["result"]
    assert "submitted_at" in output
    assert "run_duration_seconds" in output
