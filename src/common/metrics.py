from his_mon import BaseMetrics
from prometheus_client import Counter, Gauge

class JobRunnerMetrics(BaseMetrics):
    def __init__(self):
        super().__init__(app_name="playwright_runner")
        
        # 작업 처리량 카운터
        self.jobs_submitted = Counter('jobs_submitted_total', 'Total jobs submitted')
        self.jobs_completed = Counter('jobs_completed_total', 'Total jobs completed successfully')
        self.jobs_failed = Counter('jobs_failed_total', 'Total jobs failed')
        
        # 현재 상태 게이지
        self.active_jobs = Gauge('active_jobs_count', 'Number of jobs currently running')
        self.queued_jobs = Gauge('queued_jobs_count', 'Number of jobs waiting in queue')

# 싱글톤 인스턴스
metrics = JobRunnerMetrics()