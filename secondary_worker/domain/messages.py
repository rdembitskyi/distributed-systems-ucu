from dataclasses import dataclass

from shared.domain.messages import Message
from shared.domain.worker import WorkerHealthState


@dataclass
class MasterMessageReplicaRequest:
    message: Message
    auth_token: str


@dataclass
class MasterMessageReplicaResponse:
    status: str
    status_code: int
    error_message: str | None
    worker_id: str | None = None

    def __bool__(self):
        return bool(self.status_code == 200)


@dataclass
class MasterHealthCheckRequest:
    auth_token: str


@dataclass
class MasterHealthCheckResponse:
    status: str


@dataclass
class RecoveryNotification:
    status: str
