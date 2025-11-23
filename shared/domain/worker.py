from dataclasses import dataclass
from enum import Enum


class WorkerHealthState(Enum):
    """Health states for worker nodes"""

    HEALTHY = "healthy"
    SUSPECTED = "suspected"
    UNHEALTHY = "unhealthy"


@dataclass
class Worker:
    """Represents a secondary worker"""

    worker_id: str
    address: str
    port: int
    is_active: bool = True
    status: WorkerHealthState = WorkerHealthState.HEALTHY
