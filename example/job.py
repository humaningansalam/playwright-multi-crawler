import json
import logging
import os
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import BinaryIO
from urllib.parse import urljoin, urlparse

import httpx
from pydantic import ValidationError

from src.common.download_paths import output_child, write_download
from src.models.job import (
    JOB_RESULTS_RESPONSE_ADAPTER,
    JobProcessingResponse,
    JobResultsResponse,
    JobStatus,
    JobStatusResponse,
    JobSubmitResponse,
)

logger = logging.getLogger(__name__)

SERVER_URL = os.getenv("PLAYWRIGHT_URL", "http://localhost:5000")
POLL_INTERVAL_SECONDS = 1.0
MAX_POLL_ATTEMPTS = 600

MultipartFile = tuple[str, tuple[str, BinaryIO, str]]


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
def _open_uploads(
    script_path: Path,
    additional_paths: Sequence[Path],
) -> Iterator[list[MultipartFile]]:
    opened_files: list[BinaryIO] = []
    multipart_files: list[MultipartFile] = []
    try:
        script_file = script_path.open("rb")
        opened_files.append(script_file)
        multipart_files.append(("script_file", (script_path.name, script_file, "text/x-python")))

        for additional_path in additional_paths:
            additional_file = additional_path.open("rb")
            opened_files.append(additional_file)
            multipart_files.append(
                (
                    "additional_files",
                    (additional_path.name, additional_file, "application/octet-stream"),
                )
            )

        yield multipart_files
    finally:
        for opened_file in opened_files:
            opened_file.close()


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

    missing_paths = [path for path in additional_files if not path.is_file()]
    if missing_paths:
        raise FileNotFoundError(f"Additional file not found: {missing_paths[0]}")

    with _open_uploads(script_path, additional_files) as multipart_files:
        submission_response = client.post(
            _api_url(server, "/api/jobs/submit"),
            data={"jobname": job_name},
            files=multipart_files,
        )

    submission_response.raise_for_status()
    submitted_job = JobSubmitResponse.model_validate(submission_response.json())
    logger.info(
        "Job submitted: id=%s status=%s",
        submitted_job.job_id,
        submitted_job.status.value,
    )
    return submitted_job.job_id


def poll_job_status(
    client: httpx.Client,
    job_id: str,
    *,
    server: str = SERVER_URL,
    max_attempts: int = MAX_POLL_ATTEMPTS,
    interval_seconds: float = POLL_INTERVAL_SECONDS,
) -> PollResult:
    status_url = _api_url(server, f"/api/jobs/status/{job_id}")
    for attempt_index in range(max_attempts):
        try:
            status_http_response = client.get(status_url)
            if status_http_response.status_code == 404:
                return PollResult(PollOutcome.NOT_FOUND)
            status_http_response.raise_for_status()
            job_status = JobStatusResponse.model_validate(status_http_response.json())
        except httpx.TimeoutException:
            time.sleep(interval_seconds)
            continue
        except ValidationError:
            return PollResult(PollOutcome.INVALID_RESPONSE)

        logger.info(
            "Attempt %s/%s: job status=%s",
            attempt_index + 1,
            max_attempts,
            job_status.status.value,
        )
        if job_status.status.is_terminal:
            return PollResult(PollOutcome.TERMINAL, job_status.status)
        time.sleep(interval_seconds)

    return PollResult(PollOutcome.TIMEOUT)


def cancel_job(
    client: httpx.Client,
    job_id: str,
    *,
    server: str = SERVER_URL,
) -> JobStatusResponse:
    cancellation_response = client.post(_api_url(server, f"/api/jobs/{job_id}/cancel"))
    cancellation_response.raise_for_status()
    return JobStatusResponse.model_validate(cancellation_response.json())


def get_job_results(
    client: httpx.Client,
    job_id: str,
    *,
    server: str = SERVER_URL,
) -> JobResultsResponse | None:
    results_response = client.get(_api_url(server, f"/api/jobs/results/{job_id}"))
    if results_response.status_code == 404:
        return None
    results_response.raise_for_status()
    return JOB_RESULTS_RESPONSE_ADAPTER.validate_python(results_response.json())


def _download_url(server: str, download_path: str) -> str:
    server_url = urlparse(server)
    candidate_url = urlparse(urljoin(f"{server.rstrip('/')}/", download_path))
    if candidate_url.scheme != server_url.scheme or candidate_url.netloc != server_url.netloc:
        raise ValueError(f"Server returned a download URL outside {server_url.netloc}")
    if not candidate_url.path.startswith("/api/jobs/download/"):
        raise ValueError(f"Server returned an invalid download URL: {download_path}")
    return candidate_url.geturl()


def _download_file(
    client: httpx.Client,
    download_url: str,
    destination: Path,
) -> None:
    with client.stream("GET", download_url) as download_response:
        download_response.raise_for_status()
        write_download(destination, download_response.iter_bytes())


def download_files(
    client: httpx.Client,
    job_id: str,
    files: dict[str, str],
    *,
    server: str = SERVER_URL,
    output_dir: Path = Path("downloads"),
) -> None:
    job_output = output_child(output_dir, job_id)
    planned_downloads = [
        (
            _download_url(server, download_path),
            output_child(job_output, filename),
        )
        for filename, download_path in files.items()
    ]

    job_output.mkdir(parents=True, exist_ok=True)
    for download_url, destination in planned_downloads:
        _download_file(client, download_url, destination)


def run(client: httpx.Client) -> int:
    job_id = submit_job(client, default_crawl_script_path(), "example-domain")
    try:
        poll_result = poll_job_status(client, job_id)
    except KeyboardInterrupt:
        logger.info("Cancelling remote job %s", job_id)
        cancel_job(client, job_id)
        return 130

    if poll_result.outcome != PollOutcome.TERMINAL:
        logger.error("Job did not finish: %s", poll_result.outcome.value)
        return 1

    job_result = get_job_results(client, job_id)
    if job_result is None or isinstance(job_result, JobProcessingResponse):
        logger.error("Terminal job result is unavailable")
        return 1
    if job_result.files:
        download_files(client, job_id, job_result.files)

    print(json.dumps(job_result.model_dump(mode="json"), ensure_ascii=False, indent=2))
    return 0 if job_result.status == JobStatus.COMPLETED else 1


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    try:
        with httpx.Client(timeout=httpx.Timeout(30, read=60)) as client:
            raise SystemExit(run(client))
    except (FileNotFoundError, OSError, ValueError, ValidationError, httpx.HTTPError) as exception:
        logger.error("Example client failed: %s", exception)
        raise SystemExit(1) from exception


if __name__ == "__main__":
    main()
