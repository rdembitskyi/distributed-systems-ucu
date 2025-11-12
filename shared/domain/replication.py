from dataclasses import dataclass


@dataclass
class ReplicationResult:
    """Result of replication operation"""

    success: bool
    total_workers: int = 0
    failed_workers: list[str] | None = None
    retry_workers: list[str] | None = None
    error_message: str | None = None

    @property
    def success_count(self) -> int:
        failed = self.failed_workers or []
        retry = self.retry_workers or []
        troubled_workers = failed + retry
        if len(troubled_workers) == 0:
            return self.total_workers
        return self.total_workers - len(troubled_workers)

    def remove_retry_worker(self, worker_id: str) -> None:
        if self.retry_workers and worker_id in self.retry_workers:
            self.retry_workers.remove(worker_id)
