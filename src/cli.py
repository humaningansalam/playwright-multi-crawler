import argparse
import json
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO
from urllib.parse import urljoin, urlparse

import httpx

from src.common.download_paths import output_child, write_download
from src.models.job import (
    JOB_RESULTS_RESPONSE_ADAPTER,
    JobCompletedResponse,
    JobErrorResponse,
    JobProcessingResponse,
    JobStatus,
)

MultipartFile = tuple[str, tuple[str, BinaryIO, str]]
TerminalJobResponse = JobCompletedResponse | JobErrorResponse


class CrawlerCliError(RuntimeError):
    """A user-facing CLI failure that does not need a traceback."""


def _raise_for_status(http_response: httpx.Response) -> None:
    if http_response.is_success:
        return

    http_response.read()
    try:
        response_payload = http_response.json()
    except ValueError:
        http_response.raise_for_status()
        return

    error_detail = response_payload.get("detail") if isinstance(response_payload, dict) else None
    if isinstance(error_detail, dict):
        error_code = error_detail.get("code")
        error_message = error_detail.get("message")
        if isinstance(error_code, str) and isinstance(error_message, str):
            raise CrawlerCliError(f"{error_code}: {error_message}")

    http_response.raise_for_status()


def _api_url(server: str, path: str) -> str:
    return urljoin(f"{server.rstrip('/')}/", path.lstrip("/"))


@contextmanager
def _open_uploads(
    script_path: Path,
    additional_paths: list[Path],
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
    server: str,
    script_path: Path,
    job_name: str,
    additional_paths: list[Path],
) -> str:
    with _open_uploads(script_path, additional_paths) as multipart_files:
        response = client.post(
            _api_url(server, "/api/jobs/submit"),
            data={"jobname": job_name},
            files=multipart_files,
        )

    _raise_for_status(response)
    response_payload = response.json()
    job_id = response_payload.get("job_id") if isinstance(response_payload, dict) else None
    if not isinstance(job_id, str) or not job_id:
        raise CrawlerCliError("Server returned an invalid submission response")
    return job_id


def follow_logs(client: httpx.Client, server: str, job_id: str) -> None:
    event_name = "stdout"
    with client.stream("GET", _api_url(server, f"/api/jobs/logs/{job_id}")) as response:
        _raise_for_status(response)
        for line in response.iter_lines():
            if line.startswith("event:"):
                event_name = line.removeprefix("event:").strip()
                continue
            if not line.startswith("data:"):
                continue

            event_content = json.loads(line.removeprefix("data:").strip())
            output_stream = sys.stderr if event_name == "stderr" else sys.stdout
            print(event_content, end="", file=output_stream, flush=True)


def cancel_job(client: httpx.Client, server: str, job_id: str) -> None:
    response = client.post(_api_url(server, f"/api/jobs/{job_id}/cancel"))
    _raise_for_status(response)


def _download_url(server: str, download_path: str) -> str:
    server_url = urlparse(server)
    candidate_url = urlparse(urljoin(f"{server.rstrip('/')}/", download_path))
    if candidate_url.scheme != server_url.scheme or candidate_url.netloc != server_url.netloc:
        raise CrawlerCliError(f"Server returned a download URL outside {server_url.netloc}")
    if not candidate_url.path.startswith("/api/jobs/download/"):
        raise CrawlerCliError(f"Server returned an invalid download URL: {download_path}")
    return candidate_url.geturl()


def _safe_output_child(parent: Path, name: str) -> Path:
    try:
        return output_child(parent, name)
    except ValueError as exception:
        raise CrawlerCliError(str(exception)) from exception


def _download_file(
    client: httpx.Client,
    download_url: str,
    destination: Path,
) -> None:
    with client.stream("GET", download_url) as response:
        _raise_for_status(response)
        write_download(destination, response.iter_bytes())


def fetch_result_and_download(
    client: httpx.Client,
    server: str,
    job_id: str,
    output_dir: Path,
) -> TerminalJobResponse:
    job_output = _safe_output_child(output_dir, job_id)
    response = client.get(_api_url(server, f"/api/jobs/results/{job_id}"))
    _raise_for_status(response)
    job_response = JOB_RESULTS_RESPONSE_ADAPTER.validate_python(response.json())
    if isinstance(job_response, JobProcessingResponse):
        raise CrawlerCliError("Log stream ended before the job reached a terminal state")

    if job_response.files:
        planned_downloads = [
            (
                _download_url(server, download_path),
                _safe_output_child(job_output, filename),
            )
            for filename, download_path in job_response.files.items()
        ]
        job_output.mkdir(parents=True, exist_ok=True)
        for download_url, destination in planned_downloads:
            _download_file(client, download_url, destination)

    return job_response


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crawler",
        description="Submit a crawler script and stream its output until completion.",
    )
    parser.add_argument(
        "script", type=Path, help="Path to the Python script containing async crawl()"
    )
    parser.add_argument("--job-name", required=True, help="Active-job uniqueness key")
    parser.add_argument(
        "--file",
        action="append",
        default=[],
        type=Path,
        dest="additional",
        help="Additional file to upload; repeat for multiple files",
    )
    parser.add_argument("--server", default="http://localhost:5000", help="Crawler API base URL")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("downloads"),
        help="Directory for downloaded job artifacts",
    )
    return parser


def run(arguments: argparse.Namespace, client: httpx.Client) -> int:
    script_path: Path = arguments.script
    additional_paths: list[Path] = arguments.additional

    if not script_path.is_file():
        raise CrawlerCliError(f"Script file not found: {script_path}")

    missing_paths = [path for path in additional_paths if not path.is_file()]
    if missing_paths:
        raise CrawlerCliError(f"Additional file not found: {missing_paths[0]}")

    job_id = submit_job(
        client,
        arguments.server,
        script_path,
        arguments.job_name,
        additional_paths,
    )
    print(f"job_id={job_id}", file=sys.stderr)

    try:
        follow_logs(client, arguments.server, job_id)
    except KeyboardInterrupt:
        print(f"Cancelling remote job {job_id}...", file=sys.stderr)
        cancel_job(client, arguments.server, job_id)
        return 130

    job_response = fetch_result_and_download(
        client,
        arguments.server,
        job_id,
        arguments.output,
    )
    print(job_response.model_dump_json(indent=2))
    return 0 if job_response.status == JobStatus.COMPLETED else 1


def main() -> None:
    arguments = build_parser().parse_args()
    try:
        with httpx.Client(timeout=httpx.Timeout(60, read=None)) as client:
            raise SystemExit(run(arguments, client))
    except (CrawlerCliError, httpx.HTTPError, OSError, ValueError) as exception:
        print(f"crawler: {exception}", file=sys.stderr)
        raise SystemExit(1) from exception
