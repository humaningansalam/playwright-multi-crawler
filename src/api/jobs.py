import asyncio
import errno
import json
import logging
import os
import shutil
import stat
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Annotated, Any
from urllib.parse import quote

from fastapi import APIRouter, File, Form, Header, HTTPException, Request, UploadFile, status
from fastapi.responses import FileResponse, StreamingResponse

from src.common.metrics import metrics
from src.config import JOB_FOLDER
from src.core import job_queue, playwright_manager
from src.core import state_manager as state
from src.core.private_jobs import (
    JOB_TOKEN_HEADER,
    LEGACY_TISTORY_JOB_NAME,
    PRIVATE_INPUT_DIRECTORY,
    can_access_job,
    token_digest,
)
from src.models.job import (
    JOB_STATE_FILENAME,
    ApiErrorCode,
    ApiErrorDetail,
    ApiErrorResponse,
    JobCompletedResponse,
    JobError,
    JobErrorCode,
    JobErrorResponse,
    JobProcessingResponse,
    JobRecord,
    JobResultsResponse,
    JobStatus,
    JobStatusResponse,
    JobSubmitResponse,
    QueuedJob,
)
from src.worker import job_processor

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/jobs", tags=["Jobs"])

RESERVED_JOB_FILENAMES = {
    "script.py",
    "result.json",
    "result.json.tmp",
    "stdout.log",
    "stderr.log",
    JOB_STATE_FILENAME,
    PRIVATE_INPUT_DIRECTORY,
}
JobToken = Annotated[str | None, Header(alias=JOB_TOKEN_HEADER)]
UPLOAD_CHUNK_BYTES = 1024 * 1024
LOG_FILENAMES = ("stdout.log", "stderr.log")
LOG_STREAM_CHUNK_CHARS = 64 * 1024


def _error_response(description: str) -> dict[str, object]:
    return {"model": ApiErrorResponse, "description": description}


def _raise_api_error(
    status_code: int,
    code: ApiErrorCode,
    message: str,
    **context: object,
) -> None:
    detail = ApiErrorDetail(code=code, message=message, context=context)
    raise HTTPException(status_code=status_code, detail=detail.model_dump(mode="json"))


@dataclass(frozen=True)
class AdditionalFilenameViolation:
    code: ApiErrorCode
    filename: str


class JobFileLookupState(str, Enum):
    AVAILABLE = "AVAILABLE"
    OUTSIDE_JOB = "OUTSIDE_JOB"
    MISSING = "MISSING"


@dataclass(frozen=True)
class JobFileLookup:
    state: JobFileLookupState
    path: Path


@dataclass(frozen=True)
class OpenedJobFile:
    file_descriptor: int
    stat_result: os.stat_result


class PinnedFileResponse(FileResponse):
    def __init__(self, file_descriptor: int, **kwargs: Any) -> None:
        self._file_descriptor: int | None = file_descriptor
        super().__init__(f"/proc/self/fd/{file_descriptor}", **kwargs)

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            file_descriptor, self._file_descriptor = self._file_descriptor, None
            if file_descriptor is not None:
                os.close(file_descriptor)


def _is_valid_additional_filename(filename: str) -> bool:
    if not filename or os.path.isabs(filename):
        return False

    posix_path = PurePosixPath(filename)
    windows_path = PureWindowsPath(filename)

    if posix_path.name != filename or windows_path.name != filename:
        return False

    if posix_path.is_absolute() or windows_path.is_absolute():
        return False

    return ".." not in posix_path.parts and ".." not in windows_path.parts


def _validate_additional_filenames(
    additional_files: list[UploadFile],
) -> AdditionalFilenameViolation | None:
    seen_filenames: set[str] = set()
    for additional_file in additional_files:
        filename = additional_file.filename
        if not filename:
            continue
        if not _is_valid_additional_filename(filename):
            return AdditionalFilenameViolation(ApiErrorCode.INVALID_ADDITIONAL_FILENAME, filename)
        if filename in RESERVED_JOB_FILENAMES:
            return AdditionalFilenameViolation(ApiErrorCode.RESERVED_ADDITIONAL_FILENAME, filename)
        if filename in seen_filenames:
            return AdditionalFilenameViolation(ApiErrorCode.DUPLICATE_ADDITIONAL_FILENAME, filename)
        seen_filenames.add(filename)
    return None


async def _run_file_operation(function, *args):
    operation = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError as cancellation:
        try:
            await operation
        except Exception:
            logger.exception("File operation failed while request cancellation was pending.")
        raise cancellation


async def _open_job_file_for_response(job_path: str, filename: str):
    operation = asyncio.create_task(asyncio.to_thread(_open_job_file, job_path, filename))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError as cancellation:
        try:
            opened_file = await operation
        except Exception:
            logger.exception("Job file open failed while request cancellation was pending.")
        else:
            if isinstance(opened_file, OpenedJobFile):
                try:
                    os.close(opened_file.file_descriptor)
                except OSError:
                    logger.exception("Failed to close job file after request cancellation.")
        raise cancellation


async def _open_file(*args, **kwargs):
    operation = asyncio.create_task(asyncio.to_thread(open, *args, **kwargs))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError as cancellation:
        try:
            opened_file = await operation
        except Exception:
            logger.exception("File open failed while request cancellation was pending.")
        else:
            try:
                await asyncio.to_thread(opened_file.close)
            except Exception:
                logger.exception("Failed to close file opened during request cancellation.")
        raise cancellation


async def _save_upload_file(upload_file: UploadFile, destination: str) -> None:
    output_file = await _open_file(destination, "wb")
    try:
        while chunk := await upload_file.read(UPLOAD_CHUNK_BYTES):
            await _run_file_operation(output_file.write, chunk)
    except BaseException:
        try:
            await _run_file_operation(output_file.close)
        except Exception:
            logger.exception("Failed to close upload file while another error was pending.")
        raise
    else:
        await _run_file_operation(output_file.close)


def _read_log_content(path: str, offset: int) -> tuple[str, int]:
    with open(path, encoding="utf-8", errors="replace") as log_file:
        log_file.seek(offset)
        return log_file.read(LOG_STREAM_CHUNK_CHARS), log_file.tell()


def _lookup_resolved_job_file(job_dir: Path, filename: str) -> JobFileLookup:
    candidate = job_dir / filename
    try:
        file_path = candidate.resolve()
    except (OSError, RuntimeError, ValueError):
        return JobFileLookup(JobFileLookupState.MISSING, candidate)
    if not file_path.is_relative_to(job_dir):
        return JobFileLookup(JobFileLookupState.OUTSIDE_JOB, file_path)
    relative_path = file_path.relative_to(job_dir)
    if relative_path.parts and relative_path.parts[0] in {
        PRIVATE_INPUT_DIRECTORY,
        JOB_STATE_FILENAME,
    }:
        return JobFileLookup(JobFileLookupState.MISSING, file_path)
    if not file_path.is_file():
        return JobFileLookup(JobFileLookupState.MISSING, file_path)
    return JobFileLookup(JobFileLookupState.AVAILABLE, file_path)


def _lookup_job_file(job_path: str, filename: str) -> JobFileLookup:
    candidate = Path(job_path)
    try:
        job_dir = candidate.resolve()
    except (OSError, RuntimeError, ValueError):
        return JobFileLookup(JobFileLookupState.MISSING, candidate / filename)
    return _lookup_resolved_job_file(job_dir, filename)


def _open_job_file(job_path: str, filename: str) -> JobFileLookup | OpenedJobFile:
    lookup = _lookup_job_file(job_path, filename)
    if lookup.state != JobFileLookupState.AVAILABLE:
        return lookup

    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        file_descriptor = os.open(lookup.path, flags)
    except FileNotFoundError:
        return JobFileLookup(JobFileLookupState.MISSING, lookup.path)
    except OSError as exception:
        if exception.errno in {errno.ELOOP, errno.ENOTDIR}:
            return JobFileLookup(JobFileLookupState.OUTSIDE_JOB, lookup.path)
        raise

    file_stat = os.fstat(file_descriptor)
    if not stat.S_ISREG(file_stat.st_mode):
        os.close(file_descriptor)
        return JobFileLookup(JobFileLookupState.MISSING, lookup.path)
    return OpenedJobFile(file_descriptor, file_stat)


def _list_job_files(job_path: str, base_download_url: str) -> dict[str, str] | None:
    job_dir = Path(job_path).resolve()
    if not job_dir.is_dir():
        return None
    files = {}
    for filename in os.listdir(job_dir):
        if filename in {JOB_STATE_FILENAME, PRIVATE_INPUT_DIRECTORY}:
            continue
        lookup = _lookup_resolved_job_file(job_dir, filename)
        if lookup.state == JobFileLookupState.AVAILABLE:
            files[filename] = f"{base_download_url}/{quote(filename, safe='')}"
    return files


def _remove_job_path(job_path: str) -> None:
    if os.path.exists(job_path):
        shutil.rmtree(job_path)


def _create_job_path(job_path: str) -> None:
    os.makedirs(job_path, exist_ok=True)


async def _rollback_job_submission_inner(jobname: str, job_id: str, job_path: str) -> None:
    state_removed = False
    try:
        await state.remove_job_state(job_id)
        state_removed = True
    except Exception:
        logger.exception("Failed to remove job state during rollback for %s.", job_id)

    directory_removed = False
    try:
        await _run_file_operation(_remove_job_path, job_path)
        directory_removed = True
    except OSError:
        logger.exception("Failed to remove partial job directory %s during rollback.", job_path)

    if not state_removed and directory_removed:
        try:
            await state.remove_job_state(job_id)
            state_removed = True
        except Exception:
            logger.exception("Failed to clear job state after rollback removed %s.", job_path)

    if state_removed:
        await state.remove_submitted_job(jobname)
    else:
        logger.error(
            "Keeping active name reservation for %s because rollback did not remove job state %s.",
            jobname,
            job_id,
        )


async def _rollback_job_submission(jobname: str, job_id: str, job_path: str) -> None:
    rollback_task = asyncio.create_task(_rollback_job_submission_inner(jobname, job_id, job_path))
    cancellation = None
    while not rollback_task.done():
        try:
            await asyncio.shield(rollback_task)
        except asyncio.CancelledError as current_cancellation:
            if rollback_task.cancelled():
                raise
            if cancellation is None:
                cancellation = current_cancellation
        except Exception as rollback_error:
            if cancellation is not None:
                logger.exception("Submission rollback failed while cancellation was pending.")
                raise cancellation from rollback_error
            raise

    try:
        rollback_task.result()
    except Exception as rollback_error:
        if cancellation is not None:
            logger.exception("Submission rollback failed while cancellation was pending.")
            raise cancellation from rollback_error
        raise
    if cancellation is not None:
        raise cancellation


async def _save_submission_upload(
    upload_file: UploadFile,
    destination: str,
    *,
    job_id: str,
    role: str,
    filename: str | None = None,
) -> None:
    try:
        await _save_upload_file(upload_file, destination)
    except Exception:
        logger.exception("Failed to save %s upload for job %s.", role, job_id)
        error_context: dict[str, object] = {"role": role}
        if filename is not None:
            error_context["filename"] = filename
        _raise_api_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            ApiErrorCode.FILE_SAVE_FAILED,
            "Failed to save uploaded file",
            **error_context,
        )
    finally:
        await upload_file.close()


async def _save_submission_files(
    job_id: str,
    job_path: str,
    script_file: UploadFile,
    additional_files: list[UploadFile],
    private_files: list[UploadFile],
) -> str:
    script_path = os.path.join(job_path, "script.py")
    await _save_submission_upload(
        script_file,
        script_path,
        job_id=job_id,
        role="script",
    )
    logger.info("Saved script upload for job %s to %s.", job_id, script_path)

    for additional_file in additional_files:
        filename = additional_file.filename
        if not filename:
            logger.warning("Ignoring an additional upload without a filename for job %s.", job_id)
            await additional_file.close()
            continue

        additional_path = os.path.join(job_path, filename)
        await _save_submission_upload(
            additional_file,
            additional_path,
            job_id=job_id,
            role="additional",
            filename=filename,
        )
        logger.info("Saved additional upload %s for job %s.", filename, job_id)

    if private_files:
        private_directory = Path(job_path) / PRIVATE_INPUT_DIRECTORY
        await _run_file_operation(private_directory.mkdir, 0o700)
        for private_file in private_files:
            if not private_file.filename:
                await private_file.close()
                continue
            destination = private_directory / private_file.filename
            await _save_submission_upload(
                private_file, str(destination), job_id=job_id, role="private"
            )
            await _run_file_operation(destination.chmod, 0o600)

    return script_path


def _validate_submission_request(
    request: Request,
    jobname: str,
    script_file: UploadFile,
    additional_files: list[UploadFile],
    private_files: list[UploadFile],
) -> None:
    if not jobname.strip() or script_file.filename is None:
        _raise_api_error(
            status.HTTP_400_BAD_REQUEST,
            ApiErrorCode.INVALID_SUBMISSION,
            "Job name and script file are required",
        )

    submissions_enabled = bool(getattr(request.app.state, "job_submission_enabled", False))
    if not submissions_enabled or not playwright_manager.is_browser_connected():
        _raise_api_error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            ApiErrorCode.WORKERS_UNAVAILABLE,
            "Job workers are unavailable",
        )

    filename_violation = _validate_additional_filenames(additional_files + private_files)
    if filename_violation is not None:
        _raise_api_error(
            status.HTTP_400_BAD_REQUEST,
            filename_violation.code,
            "Additional file name is not allowed",
            filename=filename_violation.filename,
        )


def _validate_private_submission(
    jobname: str,
    additional_files: list[UploadFile],
    private_files: list[UploadFile],
    job_token: str | None,
) -> str | None:
    digest = token_digest(job_token)
    if (
        job_token is not None or private_files or jobname == LEGACY_TISTORY_JOB_NAME
    ) and digest is None:
        _raise_api_error(
            status.HTTP_400_BAD_REQUEST,
            ApiErrorCode.INVALID_SUBMISSION,
            "Private jobs require a valid job access token",
        )
    if jobname == LEGACY_TISTORY_JOB_NAME and any(
        upload.filename in {"config.json", "cookies.json"} for upload in additional_files
    ):
        _raise_api_error(
            status.HTTP_400_BAD_REQUEST,
            ApiErrorCode.INVALID_SUBMISSION,
            "Tistory account inputs must use private_files",
        )
    return digest


async def _reserve_job_name(jobname: str) -> None:
    if await state.add_submitted_job(jobname):
        return
    _raise_api_error(
        status.HTTP_409_CONFLICT,
        ApiErrorCode.DUPLICATE_JOB_NAME,
        "A job with this name is already active",
        jobname=jobname,
    )


async def _create_and_enqueue_job(
    *,
    job_id: str,
    jobname: str,
    job_path: str,
    script_file: UploadFile,
    additional_files: list[UploadFile],
    private_files: list[UploadFile],
    access_token_sha256: str | None,
) -> None:
    await _run_file_operation(_create_job_path, job_path)
    logger.info("Accepted job %s (%s); storing inputs in %s.", jobname, job_id, job_path)
    script_path = await _save_submission_files(
        job_id,
        job_path,
        script_file,
        additional_files,
        private_files,
    )
    if access_token_sha256 is None:
        await state.set_initial_status(job_id, jobname, job_path)
    else:
        await state.set_initial_status(
            job_id, jobname, job_path, access_token_sha256=access_token_sha256
        )
    await job_queue.add_job(QueuedJob(script_path=script_path, jobname=jobname, job_id=job_id))


async def _prepare_submission_with_rollback(
    *,
    job_id: str,
    jobname: str,
    job_path: str,
    script_file: UploadFile,
    additional_files: list[UploadFile],
    private_files: list[UploadFile],
    access_token_sha256: str | None,
) -> None:
    try:
        await _create_and_enqueue_job(
            job_id=job_id,
            jobname=jobname,
            job_path=job_path,
            script_file=script_file,
            additional_files=additional_files,
            private_files=private_files,
            access_token_sha256=access_token_sha256,
        )
    except asyncio.CancelledError:
        await _rollback_job_submission(jobname, job_id, job_path)
        logger.info(
            "Cancelled submission for job %s (%s); partial state was rolled back.",
            jobname,
            job_id,
        )
        raise
    except HTTPException:
        await _rollback_job_submission(jobname, job_id, job_path)
        raise
    except Exception:
        await _rollback_job_submission(jobname, job_id, job_path)
        logger.exception("Failed to prepare submission for job %s (%s).", jobname, job_id)
        _raise_api_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            ApiErrorCode.SUBMISSION_FAILED,
            "Failed to process job submission",
        )


@router.get("/capabilities")
async def get_job_capabilities() -> dict[str, int]:
    return {"private_jobs": 1}


@router.post(
    "/submit",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobSubmitResponse,
    responses={
        400: _error_response("Invalid job submission"),
        409: _error_response("Duplicate active job name"),
        422: _error_response("Request validation failed"),
        500: _error_response("Job submission setup failed"),
        503: _error_response("Job workers are unavailable"),
    },
)
async def submit_job_endpoint(
    request: Request,
    jobname: str = Form(...),
    script_file: UploadFile = File(...),
    additional_files: list[UploadFile] = File(default=[]),
    private_files: Annotated[list[UploadFile] | None, File()] = None,
    job_token: JobToken = None,
) -> JobSubmitResponse:
    """Validate, persist, and enqueue a new crawler job."""
    private_uploads = private_files or []
    access_token_sha256 = _validate_private_submission(
        jobname, additional_files, private_uploads, job_token
    )
    _validate_submission_request(request, jobname, script_file, additional_files, private_uploads)
    await _reserve_job_name(jobname)

    job_id = str(uuid.uuid4())
    job_path = os.path.join(JOB_FOLDER, job_id)
    await _prepare_submission_with_rollback(
        job_id=job_id,
        jobname=jobname,
        job_path=job_path,
        script_file=script_file,
        additional_files=additional_files,
        private_files=private_uploads,
        access_token_sha256=access_token_sha256,
    )

    metrics.jobs_submitted.inc()
    logger.info("Queued job %s (%s).", jobname, job_id)
    return JobSubmitResponse(job_id=job_id)


async def _get_accessible_job(job_id: str, job_token: str | None) -> JobRecord:
    job_record = await state.get_job_info(job_id)
    if job_record is None or not can_access_job(
        job_record.jobname, job_record.access_token_sha256, job_token
    ):
        _raise_api_error(status.HTTP_404_NOT_FOUND, ApiErrorCode.JOB_NOT_FOUND, "Job not found")
    return job_record


@router.post(
    "/{job_id}/cancel",
    response_model=JobStatusResponse,
    response_model_exclude_none=True,
    responses={
        404: _error_response("Job not found"),
        409: _error_response("Job is already terminal"),
    },
)
async def cancel_job_endpoint(job_id: str, job_token: JobToken = None) -> JobStatusResponse:
    job_record = await _get_accessible_job(job_id, job_token)
    current_status = job_record.status

    if current_status.is_terminal:
        _raise_api_error(
            status.HTTP_409_CONFLICT,
            ApiErrorCode.JOB_ALREADY_TERMINAL,
            "Job is already terminal",
            status=current_status.value,
        )

    cancelled_from_queue = current_status == JobStatus.PENDING and job_queue.cancel_job(job_id)
    if not cancelled_from_queue:
        cancellation_requested = await job_processor.cancel_running_job(job_id)
        post_cancel_status = await state.get_job_status(job_id)
        if post_cancel_status is None:
            _raise_api_error(status.HTTP_404_NOT_FOUND, ApiErrorCode.JOB_NOT_FOUND, "Job not found")
        if post_cancel_status == JobStatus.CANCELLED:
            return JobStatusResponse(job_id=job_id, status=JobStatus.CANCELLED)
        if post_cancel_status.is_terminal or not cancellation_requested:
            _raise_api_error(
                status.HTTP_409_CONFLICT,
                ApiErrorCode.JOB_ALREADY_TERMINAL,
                "Job is already terminal",
                status=post_cancel_status.value,
            )

    await state.update_job_status(
        job_id,
        JobStatus.CANCELLED,
        JobError(code=JobErrorCode.JOB_CANCELLED, message="Job was cancelled"),
    )

    return JobStatusResponse(job_id=job_id, status=JobStatus.CANCELLED)


@router.get(
    "/status/{job_id}",
    response_model=JobStatusResponse,
    response_model_exclude_none=True,
    responses={404: _error_response("Job not found")},
)
async def get_job_status_endpoint(job_id: str, job_token: JobToken = None) -> JobStatusResponse:
    """Return the current lifecycle status for one job."""
    job_record = await _get_accessible_job(job_id, job_token)
    return JobStatusResponse(
        job_id=job_id,
        status=job_record.status,
        submitted_at=job_record.submitted_at,
        started_at=job_record.started_at,
        completed_at=job_record.completed_at,
        queue_wait_seconds=job_record.queue_wait_seconds,
        run_duration_seconds=job_record.run_duration_seconds,
    )


async def _list_result_files(
    job_id: str,
    job_path: str,
) -> tuple[dict[str, str] | None, ApiErrorDetail | None]:
    try:
        base_download_url = f"{router.prefix}/download/{job_id}"
        result_files = await _run_file_operation(
            _list_job_files,
            job_path,
            base_download_url,
        )
    except (OSError, RuntimeError, ValueError):
        logger.exception("Could not list result files for job %s in %s.", job_id, job_path)
        return None, ApiErrorDetail(
            code=ApiErrorCode.RESULT_FILES_UNAVAILABLE,
            message="Could not list result files",
        )
    return result_files, None


def _build_terminal_result_response(
    job_record: JobRecord,
    result_files: dict[str, str] | None,
    files_error: ApiErrorDetail | None,
) -> JobCompletedResponse | JobErrorResponse:
    response_type = (
        JobCompletedResponse if job_record.status == JobStatus.COMPLETED else JobErrorResponse
    )
    return response_type(
        job_id=job_record.job_id,
        status=job_record.status,
        result=job_record.result,
        logs=job_record.logs,
        files=result_files,
        files_error=files_error,
        jobname=job_record.jobname,
        submitted_at=job_record.submitted_at,
        started_at=job_record.started_at,
        completed_at=job_record.completed_at,
        queue_wait_seconds=job_record.queue_wait_seconds,
        run_duration_seconds=job_record.run_duration_seconds,
        duration_seconds=job_record.duration_seconds,
    )


@router.get(
    "/results/{job_id}",
    response_model=JobResultsResponse,
    responses={404: _error_response("Job not found")},
)
async def get_job_results_endpoint(job_id: str, job_token: JobToken = None) -> JobResultsResponse:
    """Return progress for an active job or its terminal result and files."""
    job_record = await _get_accessible_job(job_id, job_token)

    if job_record.status.is_active:
        return JobProcessingResponse(
            job_id=job_id,
            status=job_record.status,
            submitted_at=job_record.submitted_at,
            started_at=job_record.started_at,
            queue_wait_seconds=job_record.queue_wait_seconds,
        )

    result_files, files_error = await _list_result_files(job_id, job_record.job_path)
    return _build_terminal_result_response(job_record, result_files, files_error)


async def _stream_job_logs(job_id: str, job_path: str) -> AsyncIterator[str]:
    offsets = {filename: 0 for filename in LOG_FILENAMES}
    while True:
        backlog_remaining = False
        for filename in LOG_FILENAMES:
            log_path = os.path.join(job_path, filename)
            try:
                content, offsets[filename] = await _run_file_operation(
                    _read_log_content,
                    log_path,
                    offsets[filename],
                )
            except FileNotFoundError:
                continue

            if content:
                event_name = filename.removesuffix(".log")
                yield f"event: {event_name}\ndata: {json.dumps(content)}\n\n"
                backlog_remaining = backlog_remaining or len(content) == LOG_STREAM_CHUNK_CHARS

        job_status = await state.get_job_status(job_id)
        if job_status is None or (
            job_status.is_terminal
            and not job_processor.is_job_running(job_id)
            and not backlog_remaining
        ):
            return
        if backlog_remaining:
            continue
        await asyncio.sleep(0.1)


@router.get(
    "/logs/{job_id}",
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "Live stdout and stderr events",
            "content": {"text/event-stream": {"schema": {"type": "string"}}},
        },
        404: _error_response("Job not found"),
    },
)
async def stream_job_logs_endpoint(job_id: str, job_token: JobToken = None) -> StreamingResponse:
    job_record = await _get_accessible_job(job_id, job_token)

    return StreamingResponse(
        _stream_job_logs(job_id, job_record.job_path),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _open_download_file(
    job_id: str,
    job_path: str,
    filename: str,
) -> OpenedJobFile:
    opened_job_file = await _open_job_file_for_response(job_path, filename)
    if not isinstance(opened_job_file, JobFileLookup):
        return opened_job_file

    if opened_job_file.state == JobFileLookupState.OUTSIDE_JOB:
        logger.warning("Attempted directory traversal for %s in job %s.", filename, job_id)
        _raise_api_error(
            status.HTTP_403_FORBIDDEN,
            ApiErrorCode.ACCESS_DENIED,
            "Requested file is outside the job directory",
            filename=filename,
        )

    logger.warning("Requested file not found: %s", opened_job_file.path)
    _raise_api_error(
        status.HTTP_404_NOT_FOUND,
        ApiErrorCode.FILE_NOT_FOUND,
        "File not found",
        filename=filename,
    )


def _build_download_response(opened_job_file: OpenedJobFile, filename: str) -> FileResponse:
    # FileResponse reopens its path later, so serve the already validated inode.
    try:
        return PinnedFileResponse(
            opened_job_file.file_descriptor,
            media_type="application/octet-stream",
            filename=filename,
            stat_result=opened_job_file.stat_result,
            headers={"Cache-Control": "no-store"},
        )
    except BaseException:
        os.close(opened_job_file.file_descriptor)
        raise


@router.get(
    "/download/{job_id}/{filename}",
    response_class=FileResponse,
    responses={
        200: {
            "description": "Result file download",
            "content": {
                "application/octet-stream": {"schema": {"type": "string", "format": "binary"}}
            },
        },
        403: _error_response("Access denied"),
        404: _error_response("Job or file not found"),
    },
)
async def download_file_endpoint(
    job_id: str, filename: str, job_token: JobToken = None
) -> FileResponse:
    """Return one validated regular file from a job directory."""
    job_record = await _get_accessible_job(job_id, job_token)

    if filename in {JOB_STATE_FILENAME, PRIVATE_INPUT_DIRECTORY}:
        _raise_api_error(
            status.HTTP_404_NOT_FOUND,
            ApiErrorCode.FILE_NOT_FOUND,
            "File not found",
            filename=filename,
        )

    opened_job_file = await _open_download_file(job_id, job_record.job_path, filename)
    return _build_download_response(opened_job_file, filename)
