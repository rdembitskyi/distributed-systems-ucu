from dataclasses import dataclass


@dataclass
class ReplicationResult:
    """Result of replication operation"""

    success: bool
    failed_workers: list[str] | None = None
    error_message: str | None = None
