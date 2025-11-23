from enum import Enum


class ResponseStatus(str, Enum):
    SUCCESS = "success"
    ERROR = "error"


class HealthStatus(str, Enum):
    HEALTHY = "healthy"


class SyncStatus(str, Enum):
    """Status values for synchronization operations"""

    SYNCED = "synced"
    PARTIALLY_SYNCED = "partially_synced"
    FAILED_TO_SYNC = "failed"
