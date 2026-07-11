import pytest

from example import job
from src.models.job import JobStatus


class _StatusResponse:
    status_code = 200

    def __init__(self, status):
        self._status = status

    def raise_for_status(self):
        return None

    def json(self):
        return {"status": self._status}


@pytest.mark.parametrize("status", [JobStatus.CANCELLED.value, JobStatus.INTERRUPTED.value])
def test_bundled_client_stops_polling_for_all_non_success_terminal_statuses(monkeypatch, status):
    calls = []

    def get(_url, timeout):
        calls.append(timeout)
        return _StatusResponse(status)

    monkeypatch.setattr(job.requests, "get", get)
    monkeypatch.setattr(job.time, "sleep", lambda _seconds: pytest.fail("terminal status must not sleep"))

    assert job.poll_job_status("terminal-job") == status
    assert calls == [10]


def test_bundled_client_terminal_set_matches_public_job_status_enum():
    assert job.TERMINAL_JOB_STATUSES == {
        JobStatus.COMPLETED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
        JobStatus.INTERRUPTED.value,
    }
