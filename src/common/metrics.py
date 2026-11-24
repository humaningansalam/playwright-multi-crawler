from his_mon import BaseMetrics
from prometheus_client import Counter, Gauge


class JobRunnerMetrics(BaseMetrics):
    def __init__(self) -> None:
        super().__init__(app_name="playwright_runner")

        self.jobs_submitted = Counter("jobs_submitted_total", "Total jobs submitted")
        self.jobs_completed = Counter(
            "jobs_completed_total",
            "Total jobs completed successfully",
        )
        self.jobs_failed = Counter("jobs_failed_total", "Total jobs failed")
        self.active_jobs = Gauge("active_jobs_count", "Number of jobs currently running")
        self.queued_jobs = Gauge("queued_jobs_count", "Number of jobs waiting in queue")


metrics = JobRunnerMetrics()
