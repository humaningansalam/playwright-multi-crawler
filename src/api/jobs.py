import asyncio
import errno
import json
import logging
import os
import shutil
import stat
import uuid
from enum import Enum
from pathlib import Path, PurePosixPath, PureWindowsPath
from dataclasses import dataclass
from typing import List, Dict, Optional
from urllib.parse import quote

from fastapi import APIRouter, File, Request, UploadFile, Form, HTTPException, status, Depends
from fastapi.responses import FileResponse, StreamingResponse

# core 및 common 모듈 임포트
from src.core import state_manager as state
from src.core import job_queue
from src.core import playwright_manager
from src.worker import job_processor
from src.config import JOB_FOLDER
from src.common.metrics import metrics
# models 임포트 
from src.models.job import (
    ApiErrorCode,
    ApiErrorDetail,
    ApiErrorResponse,
    JobCompletedResponse,
    JobError,
    JobErrorCode,
    JobErrorResponse,
    JobProcessingResponse,
    JobResultsResponse,
    JobStatus,
    JobStatusResponse,
    JobSubmitResponse,
    QueuedJob,
)

router = APIRouter(
    prefix="/api/jobs", # API 경로 접두사 설정
    tags=["Jobs"],      # Swagger UI 그룹화 태그
)

RESERVED_JOB_FILENAMES = {
    "script.py",
    "result.json",
    "result.json.tmp",
    "stdout.log",
    "stderr.log",
}
UPLOAD_CHUNK_BYTES = 1024 * 1024
LOG_FILENAMES = ("stdout.log", "stderr.log")
LOG_STREAM_CHUNK_CHARS = 64 * 1024
def _error_response(description: str) -> dict:
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
    def __init__(self, file_descriptor: int, **kwargs):
        self._file_descriptor: Optional[int] = file_descriptor
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


def _validate_additional_filenames(additional_files: List[UploadFile]) -> Optional[AdditionalFilenameViolation]:
    seen_filenames = set()
    for add_file in additional_files:
        filename = add_file.filename
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
            logging.exception("File operation failed while request cancellation was pending.")
        raise cancellation


async def _open_job_file_for_response(job_path: str, filename: str):
    operation = asyncio.create_task(asyncio.to_thread(_open_job_file, job_path, filename))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError as cancellation:
        try:
            opened_file = await operation
        except Exception:
            logging.exception("Job file open failed while request cancellation was pending.")
        else:
            if isinstance(opened_file, OpenedJobFile):
                try:
                    os.close(opened_file.file_descriptor)
                except OSError:
                    logging.exception("Failed to close job file after request cancellation.")
        raise cancellation


async def _open_file(*args, **kwargs):
    operation = asyncio.create_task(asyncio.to_thread(open, *args, **kwargs))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError as cancellation:
        try:
            opened_file = await operation
        except Exception:
            logging.exception("File open failed while request cancellation was pending.")
        else:
            try:
                await asyncio.to_thread(opened_file.close)
            except Exception:
                logging.exception("Failed to close file opened during request cancellation.")
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
            logging.exception("Failed to close upload file while another error was pending.")
        raise
    else:
        await _run_file_operation(output_file.close)


def _read_log_content(path: str, offset: int) -> tuple[str, int]:
    with open(path, "r", encoding="utf-8", errors="replace") as log_file:
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
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            return JobFileLookup(JobFileLookupState.OUTSIDE_JOB, lookup.path)
        raise

    file_stat = os.fstat(file_descriptor)
    if not stat.S_ISREG(file_stat.st_mode):
        os.close(file_descriptor)
        return JobFileLookup(JobFileLookupState.MISSING, lookup.path)
    return OpenedJobFile(file_descriptor, file_stat)


def _list_job_files(job_path: str, base_download_url: str) -> Optional[Dict[str, str]]:
    job_dir = Path(job_path).resolve()
    if not job_dir.is_dir():
        return None
    files = {}
    for filename in os.listdir(job_dir):
        lookup = _lookup_resolved_job_file(job_dir, filename)
        if lookup.state == JobFileLookupState.AVAILABLE:
            files[filename] = f"{base_download_url}/{quote(filename, safe='')}"
    return files


def _remove_job_path(job_path: str) -> None:
    if os.path.exists(job_path):
        shutil.rmtree(job_path)


def _create_job_path(job_path: str) -> None:
    os.makedirs(job_path, exist_ok=True)


async def _rollback_job_submission(jobname: str, job_id: str, job_path: str) -> None:
    await state.remove_submitted_job(jobname)
    await state.remove_job_state(job_id)
    try:
        await _run_file_operation(_remove_job_path, job_path)
    except OSError:
        logging.exception("Failed to remove partial job directory %s during rollback.", job_path)


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
    additional_files: List[UploadFile] = File(default=[])
):
    """
    새로운 크롤링 작업을 제출
    - **jobname**: 작업의 고유 이름
    - **script_file**: 실행할 Python 크롤링 스크립트 
    - **additional_files**: 스크립트 실행에 필요한 추가 파일 목록
    """
    if not jobname.strip() or not script_file or script_file.filename is None:
        _raise_api_error(
            status.HTTP_400_BAD_REQUEST,
            ApiErrorCode.INVALID_SUBMISSION,
            "Job name and script file are required",
        )

    if (
        not getattr(request.app.state, "job_submission_enabled", False)
        or not playwright_manager.is_browser_connected()
    ):
        _raise_api_error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            ApiErrorCode.WORKERS_UNAVAILABLE,
            "Job workers are unavailable",
        )

    filename_violation = _validate_additional_filenames(additional_files)
    if filename_violation is not None:
        _raise_api_error(
            status.HTTP_400_BAD_REQUEST,
            filename_violation.code,
            "Additional file name is not allowed",
            filename=filename_violation.filename,
        )

    # 중복 작업 이름 체크 및 등록
    if not await state.add_submitted_job(jobname):
        _raise_api_error(
            status.HTTP_409_CONFLICT,
            ApiErrorCode.DUPLICATE_JOB_NAME,
            "A job with this name is already active",
            jobname=jobname,
        )

    job_id = str(uuid.uuid4())
    # JOB_FOLDER는 config에서 가져옴
    job_path = os.path.join(JOB_FOLDER, job_id)

    try:
        await _run_file_operation(_create_job_path, job_path)
        logging.info(f"Received job submission '{jobname}' -> Assigning ID: {job_id}, Path: {job_path}")

        # --- 파일 저장 로직 ---
        # script_file 저장
        script_filename = "script.py" # 일관성을 위해 고정된 이름 사용
        script_path = os.path.join(job_path, script_filename)
        try:
            await _save_upload_file(script_file, script_path)
            logging.info(f"Saved script file for job {job_id} to {script_path}")
        except Exception as e:
            logging.error(f"Failed to save script file for job {job_id}: {e}")
            _raise_api_error(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                ApiErrorCode.FILE_SAVE_FAILED,
                "Failed to save uploaded file",
                role="script",
            )
        finally:
             await script_file.close()

        # additional_files 저장
        for add_file in additional_files:
            if add_file.filename:
                add_file_path = os.path.join(job_path, add_file.filename)
                try:
                    logging.info(f"Saving additional file: {add_file.filename} for job {job_id}")
                    await _save_upload_file(add_file, add_file_path)
                except Exception as e:
                    logging.error(f"Failed to save additional file {add_file.filename} for job {job_id}: {e}")
                    _raise_api_error(
                        status.HTTP_500_INTERNAL_SERVER_ERROR,
                        ApiErrorCode.FILE_SAVE_FAILED,
                        "Failed to save uploaded file",
                        role="additional",
                        filename=add_file.filename,
                    )
                finally:
                    await add_file.close()
            else:
                 logging.warning(f"Received additional file without filename for job {job_id}. Skipping.")

        await state.set_initial_status(job_id, jobname, job_path)
        await job_queue.add_job(
            QueuedJob(script_path=script_path, jobname=jobname, job_id=job_id)
        )

    except asyncio.CancelledError:
        await _rollback_job_submission(jobname, job_id, job_path)
        logging.info(f"Cancelled job submission '{jobname}' (ID: {job_id}); rolled back partial state.")
        raise
    except Exception as e:
        await _rollback_job_submission(jobname, job_id, job_path)
        logging.error(f"Failed during job submission setup for '{jobname}' (ID: {job_id}): {e}")
        if isinstance(e, HTTPException):
            raise
        _raise_api_error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            ApiErrorCode.SUBMISSION_FAILED,
            "Failed to process job submission",
        )
    
    metrics.jobs_submitted.inc()
    
    logging.info(f"Job '{jobname}' (ID: {job_id}) successfully queued.")
    return JobSubmitResponse(job_id=job_id)


@router.post(
    "/{job_id}/cancel",
    response_model=JobStatusResponse,
    responses={
        404: _error_response("Job not found"),
        409: _error_response("Job is already terminal"),
    },
)
async def cancel_job_endpoint(job_id: str):
    job_info = await state.get_job_info(job_id)
    if not job_info:
        _raise_api_error(status.HTTP_404_NOT_FOUND, ApiErrorCode.JOB_NOT_FOUND, "Job not found")

    current_status = job_info.status
    if current_status.is_terminal:
        _raise_api_error(
            status.HTTP_409_CONFLICT,
            ApiErrorCode.JOB_ALREADY_TERMINAL,
            "Job is already terminal",
            status=current_status.value,
        )

    if current_status == JobStatus.PENDING and job_queue.cancel_job(job_id):
        await state.update_job_status(
            job_id,
            JobStatus.CANCELLED,
            JobError(code=JobErrorCode.JOB_CANCELLED, message="Job was cancelled"),
        )
        await state.remove_submitted_job(job_info.jobname)
        return JobStatusResponse(job_id=job_id, status=JobStatus.CANCELLED)

    cancellation_requested = await job_processor.cancel_running_job(job_id)
    post_cancel_info = await state.get_job_info(job_id)
    if not post_cancel_info:
        _raise_api_error(status.HTTP_404_NOT_FOUND, ApiErrorCode.JOB_NOT_FOUND, "Job not found")
    if post_cancel_info.status == JobStatus.CANCELLED:
        return JobStatusResponse(job_id=job_id, status=JobStatus.CANCELLED)
    if post_cancel_info.status.is_terminal or not cancellation_requested:
        _raise_api_error(
            status.HTTP_409_CONFLICT,
            ApiErrorCode.JOB_ALREADY_TERMINAL,
            "Job is already terminal",
            status=post_cancel_info.status.value,
        )

    await state.update_job_status(
        job_id,
        JobStatus.CANCELLED,
        JobError(code=JobErrorCode.JOB_CANCELLED, message="Job was cancelled"),
    )
    await state.remove_submitted_job(post_cancel_info.jobname)

    return JobStatusResponse(job_id=job_id, status=JobStatus.CANCELLED)

@router.get(
    "/status/{job_id}",
    response_model=JobStatusResponse,
    responses={404: _error_response("Job not found")},
)
async def get_job_status_endpoint(job_id: str):
    """특정 작업의 현재 상태를 조회합니다."""
    status_val = await state.get_job_status(job_id)
    if status_val is None:
        _raise_api_error(status.HTTP_404_NOT_FOUND, ApiErrorCode.JOB_NOT_FOUND, "Job not found")
    return JobStatusResponse(job_id=job_id, status=status_val)


@router.get(
    "/results/{job_id}",
    response_model=JobResultsResponse,
    responses={404: _error_response("Job not found")},
)
async def get_job_results_endpoint(job_id: str):
    """
    특정 작업의 결과를 조회합니다.
    작업이 완료되거나 실패한 경우 상세 결과와 파일 목록을 반환합니다.
    처리 중인 경우 현재 상태를 반환합니다.
    """
    job_info = await state.get_job_info(job_id) # 전체 정보 가져오기
    if not job_info:
        _raise_api_error(status.HTTP_404_NOT_FOUND, ApiErrorCode.JOB_NOT_FOUND, "Job not found")

    status_val = job_info.status

    if status_val.is_active:
        # 처리 중인 경우 
        return JobProcessingResponse(job_id=job_id, status=status_val)
    if status_val.is_terminal:
        # 완료 또는 실패한 경우
        result_val = job_info.result
        job_path = job_info.job_path
        files: Optional[Dict[str, str]] = None 
        files_error: Optional[ApiErrorDetail] = None

        if job_path:
            try:
                base_download_url = f"{router.prefix}/download/{job_id}"
                files = await _run_file_operation(
                    _list_job_files,
                    job_path,
                    base_download_url,
                )
            except (OSError, RuntimeError, ValueError) as e:
                logging.error(f"Error listing files for job {job_id} in {job_path}: {e}")
                files_error = ApiErrorDetail(
                    code=ApiErrorCode.RESULT_FILES_UNAVAILABLE,
                    message="Could not list result files",
                )

        response_model = (
            JobCompletedResponse
            if status_val == JobStatus.COMPLETED
            else JobErrorResponse
        )
        return response_model(
            job_id=job_id,
            status=status_val,
            result=result_val,
            logs=job_info.logs,
            files=files,
            files_error=files_error,
            jobname=job_info.jobname,
            submitted_at=job_info.submitted_at,
            duration_seconds=job_info.duration_seconds,
        )


async def _stream_job_logs(job_id: str, job_path: str):
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
        if (
            job_status is None
            or (
                job_status.is_terminal
                and not job_processor.is_job_running(job_id)
                and not backlog_remaining
            )
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
async def stream_job_logs_endpoint(job_id: str):
    job_info = await state.get_job_info(job_id)
    if not job_info:
        _raise_api_error(status.HTTP_404_NOT_FOUND, ApiErrorCode.JOB_NOT_FOUND, "Job not found")

    return StreamingResponse(
        _stream_job_logs(job_id, job_info.job_path),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get(
    "/download/{job_id}/{filename}",
    response_class=FileResponse,
    responses={
        200: {
            "description": "Result file download",
            "content": {
                "application/octet-stream": {
                    "schema": {"type": "string", "format": "binary"}
                }
            },
        },
        403: _error_response("Access denied"),
        404: _error_response("Job or file not found"),
    },
)
async def download_file_endpoint(job_id: str, filename: str):
    """개별 결과 파일을 다운로드합니다."""
    job_info = await state.get_job_info(job_id)
    if not job_info:
        _raise_api_error(status.HTTP_404_NOT_FOUND, ApiErrorCode.JOB_NOT_FOUND, "Job not found")

    opened_file = await _open_job_file_for_response(job_info.job_path, filename)

    if isinstance(opened_file, JobFileLookup) and opened_file.state == JobFileLookupState.OUTSIDE_JOB:
        logging.warning(f"Attempted directory traversal: {filename} for job {job_id}")
        _raise_api_error(
            status.HTTP_403_FORBIDDEN,
            ApiErrorCode.ACCESS_DENIED,
            "Requested file is outside the job directory",
            filename=filename,
        )

    if isinstance(opened_file, JobFileLookup):
        logging.warning(f"Requested file not found: {opened_file.path}")
        _raise_api_error(
            status.HTTP_404_NOT_FOUND,
            ApiErrorCode.FILE_NOT_FOUND,
            "File not found",
            filename=filename,
        )

    # FileResponse reopens its path later, so serve the already validated inode.
    try:
        return PinnedFileResponse(
            opened_file.file_descriptor,
            media_type="application/octet-stream",
            filename=filename,
            stat_result=opened_file.stat_result,
        )
    except BaseException:
        os.close(opened_file.file_descriptor)
        raise
