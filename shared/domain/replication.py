from dataclasses import dataclass, field


@dataclass
class ReplicationResult:
    """Result of replication operation"""

    success: bool
    total_workers: int = 0
    failed_workers: list[str] = field(default_factory=list)
    retry_workers: list[str] = field(default_factory=list)
    error_message: str | None = None

    @property
    def success_count(self) -> int:
        troubled_workers = self.failed_workers + self.retry_workers
        if len(troubled_workers) == 0:
            return self.total_workers
        return self.total_workers - len(troubled_workers)

    def remove_retry_worker(self, worker_id: str) -> None:
        if worker_id in self.retry_workers:
            self.retry_workers.remove(worker_id)
