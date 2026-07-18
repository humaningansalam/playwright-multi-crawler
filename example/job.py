import logging
import os
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from pydantic import ValidationError

from src.models.job import (
    JOB_RESULTS_RESPONSE_ADAPTER,
    JobProcessingResponse,
    JobResultResponse,
    JobResultsResponse,
    JobStatus,
    JobStatusResponse,
    JobSubmitResponse,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SERVER_URL = os.getenv("PLAYWRIGHT_URL", "http://localhost:5000")
POLL_INTERVAL_SECONDS = 10
MAX_POLL_ATTEMPTS = 60


class PollOutcome(str, Enum):
    TERMINAL = "TERMINAL"
    NOT_FOUND = "NOT_FOUND"
    INVALID_RESPONSE = "INVALID_RESPONSE"
    TIMEOUT = "TIMEOUT"


@dataclass(frozen=True)
class PollResult:
    outcome: PollOutcome
    status: Optional[JobStatus] = None


def default_crawl_script_path() -> str:
    return str(Path(__file__).with_name("crawl.py"))


def submit_job(
    script_path: str,
    job_name: str,
    additional_files_info: Optional[List[Dict[str, str]]] = None,
) -> Optional[str]:
    if not os.path.exists(script_path):
        logging.error("Script file not found at %s", script_path)
        return None

    files_to_upload = []
    opened_files = []
    try:
        script_file_obj = open(script_path, "rb")
        opened_files.append(script_file_obj)
        files_to_upload.append(
            ("script_file", (os.path.basename(script_path), script_file_obj, "text/x-python"))
        )

        for file_info in additional_files_info or []:
            path = file_info.get("path")
            name = file_info.get("name")
            if not path or not name or not os.path.exists(path):
                logging.error("Invalid additional file descriptor: %s", file_info)
                return None
            add_file_obj = open(path, "rb")
            opened_files.append(add_file_obj)
            files_to_upload.append(
                ("additional_files", (name, add_file_obj, "application/octet-stream"))
            )

        response = requests.post(
            f"{SERVER_URL}/api/jobs/submit",
            files=files_to_upload,
            data={"jobname": job_name},
            timeout=30,
        )
        response.raise_for_status()
        submitted = JobSubmitResponse.model_validate(response.json())
        logging.info("Job submitted: id=%s status=%s", submitted.job_id, submitted.status.value)
        return submitted.job_id
    except (OSError, requests.exceptions.RequestException, ValidationError) as exc:
        logging.error("Job submission failed: %s", exc)
        return None
    finally:
        for opened_file in opened_files:
            try:
                opened_file.close()
            except OSError:
                logging.exception("Failed to close upload file")


def poll_job_status(job_id: str) -> PollResult:
    status_url = f"{SERVER_URL}/api/jobs/status/{job_id}"
    for attempt in range(MAX_POLL_ATTEMPTS):
        try:
            response = requests.get(status_url, timeout=10)
            if response.status_code == 404:
                return PollResult(PollOutcome.NOT_FOUND)
            response.raise_for_status()
            status_response = JobStatusResponse.model_validate(response.json())
            current_status = status_response.status
            logging.info(
                "Attempt %s/%s: Job status = %s",
                attempt + 1,
                MAX_POLL_ATTEMPTS,
                current_status.value,
            )
            if current_status.is_terminal:
                return PollResult(PollOutcome.TERMINAL, current_status)
            time.sleep(POLL_INTERVAL_SECONDS)
        except requests.exceptions.Timeout:
            time.sleep(POLL_INTERVAL_SECONDS)
        except requests.exceptions.RequestException as exc:
            logging.error("Status request failed: %s", exc)
            time.sleep(POLL_INTERVAL_SECONDS * 2)
        except ValidationError as exc:
            logging.error("Server returned an invalid status response: %s", exc)
            return PollResult(PollOutcome.INVALID_RESPONSE)

    return PollResult(PollOutcome.TIMEOUT)


def get_job_results(job_id: str) -> Optional[JobResultsResponse]:
    try:
        response = requests.get(f"{SERVER_URL}/api/jobs/results/{job_id}", timeout=30)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        result = JOB_RESULTS_RESPONSE_ADAPTER.validate_python(response.json())
    except (requests.exceptions.RequestException, ValidationError) as exc:
        logging.error("Failed to fetch job results: %s", exc)
        return None

    if isinstance(result, JobProcessingResponse):
        logging.info("Job %s is still processing", job_id)
        return result

    logging.info("Job %s finished with status %s", job_id, result.status.value)
    if result.files_error is not None:
        logging.error("Result files unavailable: %s", result.files_error.code.value)
    elif result.files:
        download_files(job_id, result.files)
    return result


def _download_url(path: str) -> Optional[str]:
    base = urlparse(SERVER_URL)
    candidate = urlparse(urljoin(f"{SERVER_URL}/", path))
    if candidate.scheme != base.scheme or candidate.netloc != base.netloc:
        return None
    expected_prefix = "/api/jobs/download/"
    if not candidate.path.startswith(expected_prefix):
        return None
    return candidate.geturl()


def download_files(job_id: str, files: Dict[str, str]) -> None:
    download_dir = Path("downloads") / job_id
    try:
        download_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        logging.exception("Failed to create download directory %s", download_dir)
        return

    for filename, path in files.items():
        download_url = _download_url(path)
        if download_url is None:
            logging.error("Server returned an invalid download URL for %s", filename)
            continue
        try:
            response = requests.get(download_url, stream=True, timeout=60)
            response.raise_for_status()
            with open(download_dir / filename, "wb") as output_file:
                for chunk in response.iter_content(chunk_size=8192):
                    output_file.write(chunk)
        except (OSError, requests.exceptions.RequestException):
            logging.exception("Failed to download %s", filename)


def main() -> None:
    submitted_job_id = submit_job(default_crawl_script_path(), "naver_news_crawl")
    if submitted_job_id is None:
        raise SystemExit("Job submission failed")

    poll_result = poll_job_status(submitted_job_id)
    if poll_result.outcome == PollOutcome.TERMINAL:
        result = get_job_results(submitted_job_id)
        if result is not None:
            print(result.model_dump_json(indent=2))
    elif poll_result.outcome == PollOutcome.TIMEOUT:
        logging.warning("Job polling timed out")
    elif poll_result.outcome == PollOutcome.INVALID_RESPONSE:
        logging.error("Server returned an invalid job status response")
    else:
        logging.error("Job was not found")


if __name__ == "__main__":
    main()
