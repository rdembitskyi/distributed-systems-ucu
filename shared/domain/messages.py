from dataclasses import dataclass
from enum import Enum


class MessageStatus(Enum):
    RECEIVED = "received"
    PENDING = "pending"
    MISSING_PARENT = "missing_parent"
    PROCESSING = "processing"
    DELIVERED = "delivered"
    FAILED = "failed"


@dataclass
class Message:
    message_id: str  # UUID
    content: str
    sequence_number: int
    parent_id: str
    timestamp: float
    status: MessageStatus = MessageStatus.PENDING
    signature: str | None = None

    def __str__(self):
        return f"Message(message_id={self.message_id}, content={self.content}, sequence_number={self.sequence_number}, parent_id={self.parent_id}, timestamp={self.timestamp})"
