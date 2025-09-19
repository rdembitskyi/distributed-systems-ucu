import logging
from datetime import datetime
from typing import Optional
from uuid import uuid4

from shared.domain.messages import Message, MessageStatus
from shared.storage.interface import MessageStoreInterface


logger = logging.getLogger(__name__)


class MessageBuilder:
    """
    Service layer for creating and managing messages with proper validation
    and business logic. Acts as an extension to MessageStore for message creation.
    """

    def __init__(self, store: MessageStoreInterface):
        self.store = store

    def create_message(self, content: str) -> Message | None:
        """
        Create and store a new message with proper validation and chaining.

        Args:
            content: The message content

        """
        if not self._validate_content(content):
            logger.warning("Invalid message content provided")
            return None

        try:
            message = self._build_message(content)

            if message.sequence_number in self.store.get_messages_ids():
                return None

            return message
        except Exception as e:
            logger.error(f"Failed to create message: {e}")
            return None

    def _validate_content(self, content: str) -> bool:
        """Validate message content"""
        if not content:
            return False

        if not isinstance(content, str):
            return False

        if not content.strip():
            return False

        # Add more validation rules as needed
        if len(content) > 1000:  # Max length check
            return False

        return True

    def _build_message(self, content: str) -> Message:
        """Build a Message object with proper metadata"""
        msg_id = str(uuid4())
        parent_id = self._get_parent_id()
        sequence_number = self._get_next_sequence_number()

        return Message(
            message_id=msg_id,
            content=content.strip(),
            sequence_number=sequence_number,
            parent_id=parent_id,
            timestamp=datetime.now().timestamp(),
            status=MessageStatus.PROCESSING,
        )

    def _get_parent_id(self) -> Optional[str]:
        """Get the parent ID for chaining messages"""
        latest = self.store.get_latest()
        return latest.message_id if latest else None

    def _get_next_sequence_number(self) -> int:
        """Get the next sequence number"""
        latest = self.store.get_latest()
        return latest.sequence_number + 1 if latest else 1

    def get_message_chain(self, msg_id: str) -> list[Message]:
        """Get the full message chain from a specific message"""
        return self.store.get_chain_from_message(msg_id)

    def get_recent_messages(self, count: int = 10) -> list[Message]:
        """Get recent messages with validation"""
        if count <= 0:
            return []

        return self.store.get_last_n_messages(min(count, 100))  # Cap at 100
