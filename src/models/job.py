from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

class JobSubmitResponse(BaseModel):
    """작업 제출 성공 시 응답 모델"""
    job_id: str
    status: str = "PENDING"
    message: str = "Job submitted successfully."

class JobStatusResponse(BaseModel):
    """작업 상태 조회 응답 모델"""
    job_id: str
    status: str # PENDING, RUNNING, COMPLETED, FAILED

class FileInfo(BaseModel):
    """결과 파일 정보 모델"""
    filename: str
    url: str

class JobResultResponse(BaseModel):
    """작업 결과 조회 응답 모델"""
    job_id: str
    status: str # COMPLETED, FAILED
    result: Optional[Any] = None # 크롤링 결과 또는 에러 정보
    files: Optional[Dict[str, str]] = None # 파일 이름: 다운로드 URL 맵
    jobname: Optional[str] = None
    submitted_at: Optional[str] = None
    duration_seconds: Optional[float] = None

class JobProcessingResponse(BaseModel):
     """처리 중인 작업 결과 조회 시 응답 모델"""
     job_id: str
     status: str # PENDING, RUNNING
     message: str = "Job is still processing."

class HealthResponse(BaseModel):
     """Health check 응답 모델"""
     status: str = "ok"
     browser_connected: bool
     queued_tasks: int