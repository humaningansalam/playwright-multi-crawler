from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Dict, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    INTERRUPTED = "INTERRUPTED"

    @property
    def is_active(self) -> bool:
        return self in (JobStatus.PENDING, JobStatus.RUNNING)

    @property
    def is_terminal(self) -> bool:
        return not self.is_active


class JobErrorCode(str, Enum):
    USER_SCRIPT_FAILED = "USER_SCRIPT_FAILED"
    WORKER_EXECUTION_FAILED = "WORKER_EXECUTION_FAILED"
    BROWSER_CLEANUP_FAILED = "BROWSER_CLEANUP_FAILED"
    WORKER_TIMED_OUT = "WORKER_TIMED_OUT"
    WORKER_EXITED = "WORKER_EXITED"
    WORKER_RESULT_MISSING = "WORKER_RESULT_MISSING"
    WORKER_RESULT_INVALID = "WORKER_RESULT_INVALID"
    JOB_CANCELLED = "JOB_CANCELLED"
    PROCESSING_FAILED = "PROCESSING_FAILED"
    DISPATCH_FAILED = "DISPATCH_FAILED"
    SERVICE_SHUTDOWN = "SERVICE_SHUTDOWN"


class CleanupFailure(BaseModel):
    resource: str
    message: str


class JobError(BaseModel):
    code: JobErrorCode
    message: str
    traceback: Optional[str] = None
    cleanup_failures: list[CleanupFailure] = Field(default_factory=list)
    exit_code: Optional[int] = None
    timeout_seconds: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    worker_result: Any = None
    worker_error: Any = None


class WorkerCompleted(BaseModel):
    status: Literal[JobStatus.COMPLETED] = JobStatus.COMPLETED
    result: JsonValue = None
    error: None = None


class WorkerFailed(BaseModel):
    status: Literal[JobStatus.FAILED] = JobStatus.FAILED
    result: None = None
    error: JobError


WorkerResult = Annotated[Union[WorkerCompleted, WorkerFailed], Field(discriminator="status")]
WORKER_RESULT_ADAPTER = TypeAdapter(WorkerResult)
JSON_VALUE_ADAPTER = TypeAdapter(JsonValue)


class QueuedJob(BaseModel):
    job_id: str
    jobname: str
    script_path: str


class JobRecord(BaseModel):
    job_id: str
    jobname: str
    job_path: str
    status: JobStatus = JobStatus.PENDING
    result: Any = None
    logs: Optional[Dict[str, str]] = None
    submitted_at: datetime = Field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    queue_wait_seconds: Optional[float] = None
    run_duration_seconds: Optional[float] = None
    duration_seconds: Optional[float] = None


JOB_STATE_FILENAME = "state.json"
JOB_STATE_SCHEMA_VERSION = 1


class PersistedJobStateV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = JOB_STATE_SCHEMA_VERSION
    job_id: str
    jobname: str
    status: JobStatus
    result: Any = None
    logs: Optional[Dict[str, str]] = None
    submitted_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    queue_wait_seconds: Optional[float] = None
    run_duration_seconds: Optional[float] = None
    duration_seconds: Optional[float] = None


class ApiErrorCode(str, Enum):
    REQUEST_VALIDATION_FAILED = "REQUEST_VALIDATION_FAILED"
    INVALID_SUBMISSION = "INVALID_SUBMISSION"
    INVALID_ADDITIONAL_FILENAME = "INVALID_ADDITIONAL_FILENAME"
    RESERVED_ADDITIONAL_FILENAME = "RESERVED_ADDITIONAL_FILENAME"
    DUPLICATE_ADDITIONAL_FILENAME = "DUPLICATE_ADDITIONAL_FILENAME"
    DUPLICATE_JOB_NAME = "DUPLICATE_JOB_NAME"
    WORKERS_UNAVAILABLE = "WORKERS_UNAVAILABLE"
    FILE_SAVE_FAILED = "FILE_SAVE_FAILED"
    SUBMISSION_FAILED = "SUBMISSION_FAILED"
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    JOB_ALREADY_TERMINAL = "JOB_ALREADY_TERMINAL"
    RESULT_FILES_UNAVAILABLE = "RESULT_FILES_UNAVAILABLE"
    ACCESS_DENIED = "ACCESS_DENIED"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"


class ApiErrorDetail(BaseModel):
    code: ApiErrorCode
    message: str
    context: Dict[str, Any] = Field(default_factory=dict)


class ApiErrorResponse(BaseModel):
    detail: ApiErrorDetail


class JobSubmitResponse(BaseModel):
    job_id: str
    status: Literal[JobStatus.PENDING] = JobStatus.PENDING
    message: str = "Job submitted successfully."


class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    submitted_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    queue_wait_seconds: Optional[float] = None
    run_duration_seconds: Optional[float] = None


class FileInfo(BaseModel):
    filename: str
    url: str


class JobResultResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str
    status: JobStatus
    result: Any = None
    logs: Optional[Dict[str, str]] = None
    files: Optional[Dict[str, str]] = None
    files_error: Optional[ApiErrorDetail] = None
    jobname: Optional[str] = None
    submitted_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    queue_wait_seconds: Optional[float] = None
    run_duration_seconds: Optional[float] = None
    duration_seconds: Optional[float] = None


class JobCompletedResponse(JobResultResponse):
    status: Literal[JobStatus.COMPLETED] = JobStatus.COMPLETED
    result: JsonValue = None


class JobErrorResponse(JobResultResponse):
    status: Literal[
        JobStatus.FAILED,
        JobStatus.CANCELLED,
        JobStatus.INTERRUPTED,
    ]
    result: JobError


class JobProcessingResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str
    status: Literal[JobStatus.PENDING, JobStatus.RUNNING]
    submitted_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    queue_wait_seconds: Optional[float] = None
    message: str = "Job is still processing."


JobResultsResponse = Annotated[
    Union[JobProcessingResponse, JobCompletedResponse, JobErrorResponse],
    Field(discriminator="status"),
]
JOB_RESULTS_RESPONSE_ADAPTER = TypeAdapter(JobResultsResponse)


class HealthStatus(str, Enum):
    OK = "ok"
    UNAVAILABLE = "unavailable"


class HealthResponse(BaseModel):
    status: HealthStatus = HealthStatus.OK
    browser_connected: bool
    workers_ready: bool
    queued_tasks: int
