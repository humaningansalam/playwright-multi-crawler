import json
import subprocess
import sys
from pathlib import Path

import pytest

from example import job
from src.models.job import ApiErrorCode, JobCompletedResponse, JobResultResponse, JobStatus


class _Response:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


@pytest.mark.parametrize("status", [JobStatus.CANCELLED, JobStatus.INTERRUPTED])
def test_bundled_client_stops_polling_for_terminal_statuses(monkeypatch, status):
    calls = []

    def get(_url, timeout):
        calls.append(timeout)
        return _Response({"job_id": "terminal-job", "status": status})

    monkeypatch.setattr(job.requests, "get", get)
    monkeypatch.setattr(job.time, "sleep", lambda _seconds: pytest.fail("terminal status must not sleep"))

    result = job.poll_job_status("terminal-job")

    assert result == job.PollResult(job.PollOutcome.TERMINAL, status)
    assert calls == [10]


def test_bundled_client_reports_unknown_status_as_invalid_response(monkeypatch):
    monkeypatch.setattr(
        job.requests,
        "get",
        lambda _url, timeout: _Response({"job_id": "job-1", "status": "ALMOST_DONE"}),
    )

    assert job.poll_job_status("job-1") == job.PollResult(job.PollOutcome.INVALID_RESPONSE)


@pytest.mark.parametrize(
    "payload, expected_type",
    [
        ({"job_id": "job-1", "status": JobStatus.COMPLETED}, JobCompletedResponse),
        (
            {"job_id": "job-1", "status": JobStatus.PENDING, "message": "still running"},
            job.JobProcessingResponse,
        ),
    ],
)
def test_bundled_client_discriminates_result_shape_by_status(monkeypatch, payload, expected_type):
    monkeypatch.setattr(job.requests, "get", lambda _url, timeout: _Response(payload))

    result = job.get_job_results("job-1")

    assert isinstance(result, expected_type)


@pytest.mark.parametrize(
    "payload",
    [
        {"job_id": "job-1", "status": JobStatus.PENDING, "result": {"premature": True}},
        {"job_id": "job-1", "status": JobStatus.FAILED, "message": "still processing"},
        {"job_id": "job-1", "status": JobStatus.FAILED},
        {
            "job_id": "job-1",
            "status": JobStatus.CANCELLED,
            "result": {"unexpected": True},
        },
    ],
)
def test_bundled_client_rejects_status_shape_mismatch(monkeypatch, payload):
    monkeypatch.setattr(job.requests, "get", lambda _url, timeout: _Response(payload))

    assert job.get_job_results("job-1") is None


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


def test_bundled_client_downloads_artifact_named_error(monkeypatch):
    files = {"error": "/api/jobs/download/job-1/error"}
    downloaded = []
    response = _Response({"job_id": "job-1", "status": JobStatus.COMPLETED, "files": files})

    monkeypatch.setattr(job.requests, "get", lambda _url, timeout: response)
    monkeypatch.setattr(job, "download_files", lambda job_id, listing: downloaded.append((job_id, listing)))

    result = job.get_job_results("job-1")

    assert isinstance(result, JobResultResponse)
    assert result.files == files
    assert downloaded == [("job-1", files)]


def test_bundled_client_reports_structured_file_listing_error(monkeypatch):
    response = _Response(
        {
            "job_id": "job-1",
            "status": JobStatus.COMPLETED,
            "files_error": {
                "code": ApiErrorCode.RESULT_FILES_UNAVAILABLE,
                "message": "Could not list result files",
            },
        }
    )
    monkeypatch.setattr(job.requests, "get", lambda _url, timeout: response)

    result = job.get_job_results("job-1")

    assert isinstance(result, JobResultResponse)
    assert result.files is None
    assert result.files_error.code == ApiErrorCode.RESULT_FILES_UNAVAILABLE


def test_bundled_cli_prints_terminal_result(monkeypatch, capsys):
    terminal_result = JobCompletedResponse(
        job_id="job-1",
        status=JobStatus.COMPLETED,
        result={"items": ["one"]},
        jobname="example-job",
        duration_seconds=1.25,
    )
    monkeypatch.setattr(job, "submit_job", lambda *_args, **_kwargs: "job-1")
    monkeypatch.setattr(
        job,
        "poll_job_status",
        lambda _job_id: job.PollResult(job.PollOutcome.TERMINAL, JobStatus.COMPLETED),
    )
    monkeypatch.setattr(job, "get_job_results", lambda _job_id: terminal_result)

    job.main()

    assert json.loads(capsys.readouterr().out) == terminal_result.model_dump(mode="json")
