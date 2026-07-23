import json
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import BinaryIO, Iterator, Sequence
from urllib.parse import urljoin, urlparse

import httpx
from pydantic import ValidationError

from src.models.job import (
    JOB_RESULTS_RESPONSE_ADAPTER,
    JobProcessingResponse,
    JobResultsResponse,
    JobStatus,
    JobStatusResponse,
    JobSubmitResponse,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SERVER_URL = os.getenv("PLAYWRIGHT_URL", "http://localhost:5000")
POLL_INTERVAL_SECONDS = 1.0
MAX_POLL_ATTEMPTS = 600


class PollOutcome(str, Enum):
    TERMINAL = "TERMINAL"
    NOT_FOUND = "NOT_FOUND"
    INVALID_RESPONSE = "INVALID_RESPONSE"
    TIMEOUT = "TIMEOUT"


@dataclass(frozen=True)
class PollResult:
    outcome: PollOutcome
    status: JobStatus | None = None


def default_crawl_script_path() -> Path:
    return Path(__file__).with_name("crawl.py")


def _api_url(server: str, path: str) -> str:
    return urljoin(f"{server.rstrip('/')}/", path.lstrip("/"))


@contextmanager
def _open_uploads(script: Path, additional: Sequence[Path]) -> Iterator[list[tuple]]:
    opened: list[BinaryIO] = []
    files: list[tuple] = []
    try:
        script_handle = script.open("rb")
        opened.append(script_handle)
        files.append(("script_file", (script.name, script_handle, "text/x-python")))
        for path in additional:
            handle = path.open("rb")
            opened.append(handle)
            files.append(("additional_files", (path.name, handle, "application/octet-stream")))
        yield files
    finally:
        for handle in opened:
            handle.close()


def submit_job(
    client: httpx.Client,
    script_path: Path,
    job_name: str,
    additional_files: Sequence[Path] = (),
    *,
    server: str = SERVER_URL,
) -> str:
    if not script_path.is_file():
        raise FileNotFoundError(f"Crawl script not found: {script_path}")
    missing = [path for path in additional_files if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Additional file not found: {missing[0]}")

    with _open_uploads(script_path, additional_files) as files:
        response = client.post(
            _api_url(server, "/api/jobs/submit"),
            data={"jobname": job_name},
            files=files,
        )
    response.raise_for_status()
    submitted = JobSubmitResponse.model_validate(response.json())
    logging.info("Job submitted: id=%s status=%s", submitted.job_id, submitted.status.value)
    return submitted.job_id


def poll_job_status(
    client: httpx.Client,
    job_id: str,
    *,
    server: str = SERVER_URL,
    max_attempts: int = MAX_POLL_ATTEMPTS,
    interval_seconds: float = POLL_INTERVAL_SECONDS,
) -> PollResult:
    status_url = _api_url(server, f"/api/jobs/status/{job_id}")
    for attempt in range(max_attempts):
        try:
            response = client.get(status_url)
            if response.status_code == 404:
                return PollResult(PollOutcome.NOT_FOUND)
            response.raise_for_status()
            status_response = JobStatusResponse.model_validate(response.json())
        except httpx.TimeoutException:
            time.sleep(interval_seconds)
            continue
        except ValidationError:
            return PollResult(PollOutcome.INVALID_RESPONSE)

        logging.info(
            "Attempt %s/%s: job status=%s",
            attempt + 1,
            max_attempts,
            status_response.status.value,
        )
        if status_response.status.is_terminal:
            return PollResult(PollOutcome.TERMINAL, status_response.status)
        time.sleep(interval_seconds)
    return PollResult(PollOutcome.TIMEOUT)


def cancel_job(
    client: httpx.Client,
    job_id: str,
    *,
    server: str = SERVER_URL,
) -> JobStatusResponse:
    response = client.post(_api_url(server, f"/api/jobs/{job_id}/cancel"))
    response.raise_for_status()
    return JobStatusResponse.model_validate(response.json())


def get_job_results(
    client: httpx.Client,
    job_id: str,
    *,
    server: str = SERVER_URL,
) -> JobResultsResponse | None:
    response = client.get(_api_url(server, f"/api/jobs/results/{job_id}"))
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return JOB_RESULTS_RESPONSE_ADAPTER.validate_python(response.json())


def _download_url(server: str, path: str) -> str:
    base = urlparse(server)
    candidate = urlparse(urljoin(f"{server.rstrip('/')}/", path))
    if candidate.scheme != base.scheme or candidate.netloc != base.netloc:
        raise ValueError(f"Server returned a download URL outside {base.netloc}")
    if not candidate.path.startswith("/api/jobs/download/"):
        raise ValueError(f"Server returned an invalid download URL: {path}")
    return candidate.geturl()


def download_files(
    client: httpx.Client,
    job_id: str,
    files: dict[str, str],
    *,
    server: str = SERVER_URL,
    output_dir: Path = Path("downloads"),
) -> None:
    job_output = output_dir / job_id
    job_output.mkdir(parents=True, exist_ok=True)
    for filename, path in files.items():
        response = client.get(_download_url(server, path))
        response.raise_for_status()
        (job_output / filename).write_bytes(response.content)


def run(client: httpx.Client) -> int:
    job_id = submit_job(client, default_crawl_script_path(), "example-domain")
    try:
        poll_result = poll_job_status(client, job_id)
    except KeyboardInterrupt:
        logging.info("Cancelling remote job %s", job_id)
        cancel_job(client, job_id)
        return 130

    if poll_result.outcome != PollOutcome.TERMINAL:
        logging.error("Job did not finish: %s", poll_result.outcome.value)
        return 1

    result = get_job_results(client, job_id)
    if result is None or isinstance(result, JobProcessingResponse):
        logging.error("Terminal job result is unavailable")
        return 1
    if result.files:
        download_files(client, job_id, result.files)
    print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2))
    return 0 if result.status == JobStatus.COMPLETED else 1


def main() -> None:
    try:
        with httpx.Client(timeout=httpx.Timeout(30, read=60)) as client:
            raise SystemExit(run(client))
    except (FileNotFoundError, OSError, ValueError, ValidationError, httpx.HTTPError) as exc:
        logging.error("Example client failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
