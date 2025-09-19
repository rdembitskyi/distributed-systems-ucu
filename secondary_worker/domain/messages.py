from dataclasses import dataclass
from shared.domain.messages import Message


@dataclass
class MasterMessageReplicaRequest:
    message: Message
    auth_token: str


@dataclass
class MasterMessageReplicaResponse:
    status: str
    status_code: int
    error_message: str | None


@dataclass
class MasterHealthCheckRequest:
    auth_token: str


@dataclass
class MasterHealthCheckResponse:
    status: str
