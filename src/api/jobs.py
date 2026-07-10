import logging
import os
import shutil
import uuid
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import List, Dict, Any, Optional, Union

from fastapi import APIRouter, File, Request, UploadFile, Form, HTTPException, status, Depends
from fastapi.responses import FileResponse

# core 및 common 모듈 임포트
from src.core import state_manager as state
from src.core import job_queue
from src.worker import job_processor
from src.config import JOB_FOLDER
from src.common.metrics import metrics
# models 임포트 
from src.models.job import JobSubmitResponse, JobStatusResponse, JobResultResponse, JobProcessingResponse

router = APIRouter(
    prefix="/api/jobs", # API 경로 접두사 설정
    tags=["Jobs"],      # Swagger UI 그룹화 태그
)

RESERVED_JOB_FILENAMES = {"script.py", "result.json", "result.json.tmp"}


def _error_response(description: str) -> Dict[str, Any]:
    return {
        "description": description,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["detail"],
                    "properties": {"detail": {"type": "string"}},
                }
            }
        },
    }


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


def _validate_additional_filenames(additional_files: List[UploadFile]) -> None:
    seen_filenames = set()
    for add_file in additional_files:
        filename = add_file.filename
        if not filename:
            continue
        if not _is_valid_additional_filename(filename):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid additional file name: {filename}",
            )
        if filename in RESERVED_JOB_FILENAMES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Reserved additional file name: {filename}",
            )
        if filename in seen_filenames:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Duplicate additional file name: {filename}",
            )
        seen_filenames.add(filename)


@router.post(
    "/submit",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobSubmitResponse,
    responses={
        400: _error_response("Invalid job submission"),
        409: _error_response("Duplicate active job name"),
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
    if not jobname or not script_file or script_file.filename is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Jobname and script file are required')

    if not getattr(request.app.state, "job_submission_enabled", False):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Job workers are unavailable",
        )

    _validate_additional_filenames(additional_files)

    # 중복 작업 이름 체크 및 등록
    if not await state.add_submitted_job(jobname):
         raise HTTPException(
             status_code=status.HTTP_409_CONFLICT,
             detail=f'Job with name "{jobname}" is already submitted and processing.'
         )

    job_id = str(uuid.uuid4())
    # JOB_FOLDER는 config에서 가져옴
    job_path = os.path.join(JOB_FOLDER, job_id)

    try:
        os.makedirs(job_path, exist_ok=True)
        logging.info(f"Received job submission '{jobname}' -> Assigning ID: {job_id}, Path: {job_path}")

        # --- 파일 저장 로직 ---
        # script_file 저장
        script_filename = "script.py" # 일관성을 위해 고정된 이름 사용
        script_path = os.path.join(job_path, script_filename)
        try:
            script_contents = await script_file.read()
            with open(script_path, 'wb') as f:
                f.write(script_contents)
            logging.info(f"Saved script file for job {job_id} to {script_path}")
        except Exception as e:
            logging.error(f"Failed to save script file for job {job_id}: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to save script file") from e
        finally:
             await script_file.close()

        # additional_files 저장
        for add_file in additional_files:
            if add_file.filename:
                add_file_path = os.path.join(job_path, add_file.filename)
                try:
                    logging.info(f"Saving additional file: {add_file.filename} for job {job_id}")
                    content = await add_file.read()
                    with open(add_file_path, 'wb') as f:
                        f.write(content)
                except Exception as e:
                    logging.error(f"Failed to save additional file {add_file.filename} for job {job_id}: {e}")
                    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to save additional file: {add_file.filename}") from e
                finally:
                    await add_file.close()
            else:
                 logging.warning(f"Received additional file without filename for job {job_id}. Skipping.")

    except Exception as e:
         # 파일 저장 중 에러 발생 시 롤백
         await state.remove_submitted_job(jobname) 
         if os.path.exists(job_path):
             shutil.rmtree(job_path, ignore_errors=True) 
         logging.error(f"Failed during file saving process for job '{jobname}' (ID: {job_id}): {e}")
         if not isinstance(e, HTTPException):
              raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to process job submission: {e}")
         else:
              raise e

    # 작업 상태 초기화 및 큐 추가
    await state.set_initial_status(job_id, jobname, job_path)
    job_data = {'script_path': script_path, 'jobname': jobname, 'job_id': job_id}
    await job_queue.add_job(job_data)
    
    metrics.jobs_submitted.inc()
    metrics.queued_jobs.set(job_queue.qsize())
    
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
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    current_status = job_info["status"]
    if current_status not in {"PENDING", "RUNNING"}:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Job is already terminal")

    if current_status == "RUNNING":
        if not job_processor.cancel_running_job(job_id):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Job is already terminal")
    else:
        job_queue.cancel_job(job_id)

    await state.update_job_status(job_id, "CANCELLED", {"error": "cancelled"})
    if current_status == "PENDING":
        await state.remove_submitted_job(job_info["jobname"])

    return JobStatusResponse(job_id=job_id, status="CANCELLED")

@router.get(
    "/status/{job_id}",
    response_model=JobStatusResponse,
    responses={404: _error_response("Job not found")},
)
async def get_job_status_endpoint(job_id: str):
    """특정 작업의 현재 상태를 조회합니다."""
    status_val = await state.get_job_status(job_id)
    if status_val is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return JobStatusResponse(job_id=job_id, status=status_val)


@router.get(
    "/results/{job_id}",
    response_model=Union[JobProcessingResponse, JobResultResponse],
    responses={
        404: _error_response("Job not found"),
        500: _error_response("Unknown job status"),
    },
)
async def get_job_results_endpoint(job_id: str):
    """
    특정 작업의 결과를 조회합니다.
    작업이 완료되거나 실패한 경우 상세 결과와 파일 목록을 반환합니다.
    처리 중인 경우 현재 상태를 반환합니다.
    """
    job_info = await state.get_job_info(job_id) # 전체 정보 가져오기
    if not job_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    status_val = job_info['status']

    if status_val in ['PENDING', 'RUNNING']:
        # 처리 중인 경우 
        return JobProcessingResponse(job_id=job_id, status=status_val)
    elif status_val in ['COMPLETED', 'FAILED', 'CANCELLED']:
        # 완료 또는 실패한 경우
        result_val = job_info.get('result')
        job_path = job_info.get('job_path')
        files: Optional[Dict[str, str]] = None 

        if job_path and os.path.isdir(job_path):
            try:
                # 파일 목록 생성 (다운로드 URL 포함)
                # API 경로 접두사를 고려하여 URL 생성
                base_download_url = f"{router.prefix}/download/{job_id}"
                files = {
                    filename: f"{base_download_url}/{filename}"
                    for filename in os.listdir(job_path)
                    if os.path.isfile(os.path.join(job_path, filename))
                }
            except OSError as e:
                logging.error(f"Error listing files for job {job_id} in {job_path}: {e}")
                files = {"error": f"Could not list result files: {e}"} 

        return JobResultResponse(
            job_id=job_id,
            status=status_val,
            result=result_val,
            files=files,
            jobname=job_info.get('jobname'),
            submitted_at=job_info.get('submitted_at'),
            duration_seconds=job_info.get('duration')
        )
    else:
        logging.error(f"Job {job_id} has unknown status: {status_val}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unknown job status: {status_val}")


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
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job ID not found")

    job_path = job_info.get('job_path')
    if not job_path:
         logging.error(f"Job path not found in state for job ID {job_id}")
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job path configuration missing")

    job_dir = Path(job_path).resolve()
    file_path = (job_dir / filename).resolve()

    if not file_path.is_relative_to(job_dir):
         logging.warning(f"Attempted directory traversal: {filename} for job {job_id}")
         raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    if not os.path.isfile(file_path):
        logging.warning(f"Requested file not found: {file_path}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")

    # FileResponse 사용하여 파일 스트리밍
    return FileResponse(
        str(file_path),
        media_type="application/octet-stream", # 일반적인 바이너리 파일 타입
        filename=filename # 다운로드 시 사용될 파일 이름
    )
