import asyncio
import importlib.util
import json
import math
import os
import sys
import traceback
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright
from pydantic import JsonValue, ValidationError

# The worker is launched by file path with its working directory set to the job folder.
# Add the repository root before importing the service package.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import CDP_URL
from src.models.job import (
    JSON_VALUE_ADAPTER,
    CleanupFailure,
    JobError,
    JobErrorCode,
    WorkerCompleted,
    WorkerFailed,
    WorkerResult,
)

RESULT_FILENAME = "result.json"


def _validate_json_source(candidate: Any, active_containers: set[int]) -> None:
    if candidate is None or isinstance(candidate, (str, bool, int)):
        return
    if isinstance(candidate, float):
        if not math.isfinite(candidate):
            raise ValueError("Crawl result contains a non-finite float")
        return
    if not isinstance(candidate, (list, tuple, dict)):
        raise TypeError(f"{type(candidate).__name__} is not JSON-serializable")

    container_id = id(candidate)
    if container_id in active_containers:
        raise ValueError("Crawl result contains a circular reference")

    active_containers.add(container_id)
    try:
        if isinstance(candidate, dict):
            for key, nested_value in candidate.items():
                if not isinstance(key, str):
                    raise TypeError("JSON object keys must be strings")
                _validate_json_source(nested_value, active_containers)
        else:
            for nested_value in candidate:
                _validate_json_source(nested_value, active_containers)
    finally:
        active_containers.remove(container_id)


def _normalize_crawl_result(crawl_result: Any) -> JsonValue:
    _validate_json_source(crawl_result, set())
    serialized_result = json.dumps(crawl_result, allow_nan=False)
    return JSON_VALUE_ADAPTER.validate_json(serialized_result)


def _write_result_atomic(job_path: str, worker_result: WorkerResult) -> None:
    result_path = os.path.join(job_path, RESULT_FILENAME)
    temporary_path = f"{result_path}.tmp"
    serialized_result = json.dumps(worker_result.model_dump(mode="json"))
    committed = False

    try:
        with open(temporary_path, "w", encoding="utf-8") as result_file:
            result_file.write(serialized_result)
            result_file.flush()
            os.fsync(result_file.fileno())
        os.replace(temporary_path, result_path)
        committed = True
    except OSError as exception:
        sys.stderr.write(f"Failed to write {RESULT_FILENAME}: {exception}\n")
    finally:
        if not committed:
            try:
                os.remove(temporary_path)
            except FileNotFoundError:
                pass
            except OSError as exception:
                sys.stderr.write(f"Failed to clean temporary {RESULT_FILENAME}: {exception}\n")


def _exception_error(exception: BaseException) -> JobError:
    return JobError(
        code=JobErrorCode.WORKER_EXECUTION_FAILED,
        message=str(exception),
        traceback=traceback.format_exc(),
    )


def _load_crawl_function(job_id: str, script_path: str):
    safe_job_id = "".join(character if character.isalnum() else "_" for character in job_id)
    module_name = f"crawler_job_{safe_job_id or 'unknown'}"
    module_spec = importlib.util.spec_from_file_location(module_name, script_path)
    if module_spec is None or module_spec.loader is None:
        raise ImportError(f"Could not load script from {script_path}")

    user_module = importlib.util.module_from_spec(module_spec)
    previous_module = sys.modules.get(module_name)
    sys.modules[module_name] = user_module
    try:
        module_spec.loader.exec_module(user_module)
    except BaseException:
        if previous_module is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous_module
        raise

    crawl_function = getattr(user_module, "crawl", None)
    if not asyncio.iscoroutinefunction(crawl_function):
        raise AttributeError("The script must contain an async function named 'crawl'.")
    return crawl_function


async def _close_browser_resources(page: Any, context: Any) -> list[CleanupFailure]:
    cleanup_failures: list[CleanupFailure] = []
    for resource_name, resource in (("page", page), ("context", context)):
        if resource is None:
            continue
        try:
            await resource.close()
        except Exception as exception:
            cleanup_failures.append(CleanupFailure(resource=resource_name, message=str(exception)))
    return cleanup_failures


async def _execute_user_script(
    job_id: str,
    script_path: str,
    job_path: str,
) -> tuple[JsonValue | None, JobError | None]:
    normalized_result: JsonValue | None = None
    execution_error: JobError | None = None

    async with async_playwright() as playwright:
        context = None
        page = None
        try:
            browser = await playwright.chromium.connect_over_cdp(CDP_URL)
            context = await browser.new_context()
            page = await context.new_page()
            crawl_function = _load_crawl_function(job_id, script_path)
            raw_result = await crawl_function(page, context, job_path)
            try:
                normalized_result = _normalize_crawl_result(raw_result)
            except (
                ValidationError,
                TypeError,
                ValueError,
                OverflowError,
                RecursionError,
            ) as exception:
                execution_error = JobError(
                    code=JobErrorCode.WORKER_RESULT_INVALID,
                    message=f"Crawl result is not JSON-serializable: {exception}",
                )
        except BaseException as exception:
            execution_error = _exception_error(exception)
        finally:
            cleanup_failures = await _close_browser_resources(page, context)
            if cleanup_failures:
                if execution_error is None:
                    execution_error = JobError(
                        code=JobErrorCode.BROWSER_CLEANUP_FAILED,
                        message="Browser resource cleanup failed",
                    )
                execution_error.cleanup_failures.extend(cleanup_failures)

    return normalized_result, execution_error


async def run_user_script(job_id: str, script_path: str, job_path: str) -> None:
    """Execute one trusted crawl function and atomically persist its worker result."""
    if job_path not in sys.path:
        sys.path.insert(0, job_path)

    normalized_result: JsonValue | None = None
    execution_error: JobError | None = None
    try:
        normalized_result, execution_error = await _execute_user_script(
            job_id,
            script_path,
            job_path,
        )
    except BaseException as exception:
        execution_error = _exception_error(exception)
    finally:
        worker_result: WorkerResult
        if execution_error is None:
            worker_result = WorkerCompleted(result=normalized_result)
        else:
            worker_result = WorkerFailed(error=execution_error)
        _write_result_atomic(job_path, worker_result)


def main(arguments: list[str] | None = None) -> int:
    command_arguments = sys.argv if arguments is None else arguments
    if len(command_arguments) < 4:
        sys.stderr.write("Invalid arguments provided to worker\n")
        return 1

    try:
        asyncio.run(
            run_user_script(
                command_arguments[1],
                command_arguments[2],
                command_arguments[3],
            )
        )
    except Exception as exception:
        sys.stderr.write(f"Worker runtime error: {exception}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
