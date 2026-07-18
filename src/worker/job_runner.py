import asyncio
import importlib.util
import json
import math
import os
import sys
import traceback
from typing import Any

from playwright.async_api import async_playwright
from pydantic import JsonValue, ValidationError

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from src.config import CDP_URL
from src.models.job import (
    CleanupFailure,
    JSON_VALUE_ADAPTER,
    JobError,
    JobErrorCode,
    WorkerCompleted,
    WorkerFailed,
    WorkerResult,
)

RESULT_FILENAME = "result.json"


def _validate_json_source(value: Any, active_containers: set[int]) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Crawl result contains a non-finite float")
        return
    if not isinstance(value, (list, tuple, dict)):
        raise TypeError(f"{type(value).__name__} is not JSON-serializable")

    container_id = id(value)
    if container_id in active_containers:
        raise ValueError("Crawl result contains a circular reference")
    active_containers.add(container_id)
    try:
        if isinstance(value, dict):
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError("JSON object keys must be strings")
                _validate_json_source(item, active_containers)
        else:
            for item in value:
                _validate_json_source(item, active_containers)
    finally:
        active_containers.remove(container_id)


def _normalize_crawl_result(value: Any) -> JsonValue:
    _validate_json_source(value, set())
    payload = json.dumps(value, allow_nan=False)
    return JSON_VALUE_ADAPTER.validate_json(payload)


def _write_result_atomic(job_path: str, output: WorkerResult) -> None:
    result_path = os.path.join(job_path, RESULT_FILENAME)
    tmp_path = f"{result_path}.tmp"
    payload = json.dumps(output.model_dump(mode="json"))
    replaced = False
    try:
        with open(tmp_path, "w", encoding="utf-8") as result_file:
            result_file.write(payload)
            result_file.flush()
            os.fsync(result_file.fileno())
        os.replace(tmp_path, result_path)
        replaced = True
    except OSError as exc:
        sys.stderr.write(f"Failed to write {RESULT_FILENAME}: {exc}\n")
    finally:
        if not replaced:
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass
            except OSError as exc:
                sys.stderr.write(f"Failed to clean temporary {RESULT_FILENAME}: {exc}\n")


def _exception_error(exc: BaseException) -> JobError:
    return JobError(
        code=JobErrorCode.WORKER_EXECUTION_FAILED,
        message=str(exc),
        traceback=traceback.format_exc(),
    )


async def run_user_script(job_id: str, script_path: str, job_path: str) -> None:
    del job_id
    result_data = None
    execution_error: JobError | None = None
    if job_path not in sys.path:
        sys.path.insert(0, job_path)

    try:
        async with async_playwright() as playwright:
            context = None
            page = None
            try:
                browser = await playwright.chromium.connect_over_cdp(CDP_URL)
                context = await browser.new_context()
                page = await context.new_page()

                spec = importlib.util.spec_from_file_location("user_module", script_path)
                if spec is None or spec.loader is None:
                    raise ImportError(f"Could not load script from {script_path}")

                user_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(user_module)
                crawl = getattr(user_module, "crawl", None)
                if not asyncio.iscoroutinefunction(crawl):
                    raise AttributeError("The script must contain an async function named 'crawl'.")
                raw_result = await crawl(page, context, job_path)
                try:
                    result_data = _normalize_crawl_result(raw_result)
                except (ValidationError, TypeError, ValueError, OverflowError, RecursionError) as exc:
                    execution_error = JobError(
                        code=JobErrorCode.WORKER_RESULT_INVALID,
                        message=f"Crawl result is not JSON-serializable: {exc}",
                    )
            except BaseException as exc:
                execution_error = _exception_error(exc)
            finally:
                cleanup_failures = []
                for name, resource in (("page", page), ("context", context)):
                    if resource is None:
                        continue
                    try:
                        await resource.close()
                    except Exception as exc:
                        cleanup_failures.append(CleanupFailure(resource=name, message=str(exc)))

                if cleanup_failures:
                    if execution_error is None:
                        execution_error = JobError(
                            code=JobErrorCode.BROWSER_CLEANUP_FAILED,
                            message="Browser resource cleanup failed",
                        )
                    execution_error.cleanup_failures.extend(cleanup_failures)
    except BaseException as exc:
        if execution_error is None:
            execution_error = _exception_error(exc)
    finally:
        output: WorkerResult
        if execution_error is None:
            output = WorkerCompleted(result=result_data)
        else:
            output = WorkerFailed(error=execution_error)
        _write_result_atomic(job_path, output)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.stderr.write("Invalid arguments provided to worker\n")
        sys.exit(1)

    try:
        asyncio.run(run_user_script(sys.argv[1], sys.argv[2], sys.argv[3]))
    except Exception as exc:
        sys.stderr.write(f"Worker runtime error: {exc}\n")
