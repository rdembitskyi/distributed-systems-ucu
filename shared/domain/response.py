from enum import Enum


class ResponseStatus(str, Enum):
    SUCCESS = "success"
    ERROR = "error"


class HealthStatus(str, Enum):
    HEALTHY = "healthy"


class SyncStatus(str, Enum):
    SYNCED = "synced"
    FAILED = "failed"
