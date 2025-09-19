from dataclasses import dataclass
from enum import Enum


class WorkerStatus(Enum):
    HEALTHY = "healthy"
    DEAD = "dead"
    SUSPECTED = "suspected"


@dataclass
class Worker:
    """Represents a secondary worker"""

    worker_id: str
    address: str
    port: int
    is_active: bool = True
    status: WorkerStatus = WorkerStatus.HEALTHY
