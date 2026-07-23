import argparse
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO
from urllib.parse import urljoin, urlparse

import httpx

from src.models.job import JOB_RESULTS_RESPONSE_ADAPTER, JobProcessingResponse, JobStatus


class CrawlerCliError(RuntimeError):
    pass


def _api_url(server: str, path: str) -> str:
    return urljoin(f"{server.rstrip('/')}/", path.lstrip("/"))


@contextmanager
def _open_uploads(script: Path, additional: list[Path]):
    opened: list[BinaryIO] = []
    files = []
    try:
        script_file = script.open("rb")
        opened.append(script_file)
        files.append(("script_file", (script.name, script_file, "text/x-python")))
        for path in additional:
            additional_file = path.open("rb")
            opened.append(additional_file)
            files.append(
                ("additional_files", (path.name, additional_file, "application/octet-stream"))
            )
        yield files
    finally:
        for handle in opened:
            handle.close()


def submit_job(
    client: httpx.Client,
    server: str,
    script: Path,
    job_name: str,
    additional: list[Path],
) -> str:
    with _open_uploads(script, additional) as files:
        response = client.post(
            _api_url(server, "/api/jobs/submit"),
            data={"jobname": job_name},
            files=files,
        )
    response.raise_for_status()
    payload = response.json()
    job_id = payload.get("job_id")
    if not isinstance(job_id, str) or not job_id:
        raise CrawlerCliError("Server returned an invalid submission response")
    return job_id


def follow_logs(client: httpx.Client, server: str, job_id: str) -> None:
    event_name = "stdout"
    with client.stream("GET", _api_url(server, f"/api/jobs/logs/{job_id}")) as response:
        response.raise_for_status()
        for line in response.iter_lines():
            if line.startswith("event:"):
                event_name = line.removeprefix("event:").strip()
            elif line.startswith("data:"):
                content = json.loads(line.removeprefix("data:").strip())
                stream = sys.stderr if event_name == "stderr" else sys.stdout
                print(content, end="", file=stream, flush=True)


def cancel_job(client: httpx.Client, server: str, job_id: str) -> None:
    response = client.post(_api_url(server, f"/api/jobs/{job_id}/cancel"))
    response.raise_for_status()


def _download_url(server: str, path: str) -> str:
    base = urlparse(server)
    candidate = urlparse(urljoin(f"{server.rstrip('/')}/", path))
    if candidate.scheme != base.scheme or candidate.netloc != base.netloc:
        raise CrawlerCliError(f"Server returned a download URL outside {base.netloc}")
    return candidate.geturl()


def fetch_result_and_download(
    client: httpx.Client,
    server: str,
    job_id: str,
    output_dir: Path,
):
    response = client.get(_api_url(server, f"/api/jobs/results/{job_id}"))
    response.raise_for_status()
    result = JOB_RESULTS_RESPONSE_ADAPTER.validate_python(response.json())
    if isinstance(result, JobProcessingResponse):
        raise CrawlerCliError("Log stream ended before the job reached a terminal state")

    if result.files:
        job_output = output_dir / job_id
        job_output.mkdir(parents=True, exist_ok=True)
        for filename, path in result.files.items():
            download = client.get(_download_url(server, path))
            download.raise_for_status()
            (job_output / filename).write_bytes(download.content)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="crawler", description="Run a crawler job over HTTP")
    parser.add_argument("script", type=Path)
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--file", action="append", default=[], type=Path, dest="additional")
    parser.add_argument("--server", default="http://localhost:5000")
    parser.add_argument("--output", type=Path, default=Path("downloads"))
    return parser


def run(args: argparse.Namespace, client: httpx.Client) -> int:
    if not args.script.is_file():
        raise CrawlerCliError(f"Script file not found: {args.script}")
    missing = [path for path in args.additional if not path.is_file()]
    if missing:
        raise CrawlerCliError(f"Additional file not found: {missing[0]}")

    job_id = submit_job(client, args.server, args.script, args.job_name, args.additional)
    print(f"job_id={job_id}", file=sys.stderr)
    try:
        follow_logs(client, args.server, job_id)
    except KeyboardInterrupt:
        print(f"Cancelling remote job {job_id}...", file=sys.stderr)
        cancel_job(client, args.server, job_id)
        return 130
    result = fetch_result_and_download(client, args.server, job_id, args.output)
    print(result.model_dump_json(indent=2))
    return 0 if result.status == JobStatus.COMPLETED else 1


def main() -> None:
    args = build_parser().parse_args()
    try:
        with httpx.Client(timeout=httpx.Timeout(60, read=None)) as client:
            raise SystemExit(run(args, client))
    except (CrawlerCliError, httpx.HTTPError, OSError, ValueError) as exc:
        print(f"crawler: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
