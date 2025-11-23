from collections import OrderedDict
import logging
from typing import Dict, List, Optional

from shared.domain.messages import Message, MessageStatus
from shared.storage.interface import MessageStoreInterface
from shared.utils.singleton import singleton


logger = logging.getLogger(__name__)


@singleton
class MessageStore(MessageStoreInterface):
    """
    Best approach: Combine OrderedDict with auxiliary indexes

    Time Complexity:
    - Add message: O(1)
    - Get latest: O(1)
    - Get by sequence: O(1)
    - Get by ID: O(1)
    - Get parent: O(1)
    - Get all children: O(1)
    """

    def __init__(self):
        # Primary storage - OrderedDict maintains insertion order
        # Key: sequence_number, Value: message
        self.messages = OrderedDict()

        # Auxiliary indexes for O(1) lookups
        self.id_to_sequence = {}  # msg_id -> sequence_number
        self.parent_index = {}  # child_id -> parent_id
        self.children_index = {}  # parent_id -> child_id : dict

        # Track latest for O(1) access
        self.latest_message = None

    def add_message(self, message: Message, missing_parent: bool) -> bool:
        """Add a message - O(1) operation"""
        self.set_message_status(
            message=message,
            status=MessageStatus.MISSING_PARENT
            if missing_parent
            else MessageStatus.DELIVERED,
        )
        logger.info(f"Adding message: {message}")
        # Store in primary structure
        self.messages[message.sequence_number] = message

        # Update indexes
        msg_id = message.message_id
        self.id_to_sequence[msg_id] = message.sequence_number

        # Update parent-child relationships
        if message.parent_id:
            self.parent_index[msg_id] = message.parent_id
            self.children_index[message.parent_id] = msg_id

        self.set_latest(message=message)

        self._process_waiting_children(message_id=message.message_id)

        return True

    def get_messages(self) -> list[Message]:
        return [
            message
            for message in self.messages.values()
            if message.status == MessageStatus.DELIVERED
        ]

    def get_messages_ids(self) -> list[dict]:
        return list(self.messages.keys())

    def get_latest(self) -> Message | None:
        """Get latest message - O(1)"""
        return self.latest_message

    def set_latest(self, message: Message):
        self.latest_message = message

    def get_by_sequence(self, seq: int) -> Optional[Dict]:
        """Get message by sequence number - O(1)"""
        return self.messages.get(seq)

    def get_by_id(self, msg_id: str) -> Optional[Message]:
        """Get message by ID - O(1)"""
        seq = self.id_to_sequence.get(msg_id)
        return self.messages.get(seq) if seq is not None else None

    def get_parent(self, msg_id: str) -> Optional[Message]:
        """Get parent message - O(1)"""
        parent_id = self.parent_index.get(msg_id)
        if parent_id:
            return self.get_by_id(parent_id)
        return None

    def get_child(self, msg_id: str) -> Message | None:
        """Get the direct child (single next message)"""
        child_id = self.children_index.get(msg_id)
        return self.get_by_id(child_id) if child_id else None

    def get_descendants(self, msg_id: str) -> List[Message]:
        """Get all descendants"""
        messages = []
        child_id = self.children_index.get(msg_id)
        while child_id:
            child_msg = self.get_by_id(child_id)
            messages.append(child_msg)
            child_id = self.children_index.get(child_id)
        return messages

    def set_message_status(self, message: Message, status: MessageStatus):
        message.status = status

    def _process_waiting_children(self, message_id: str):
        if not message_id:
            return
        orphan_descendants = self.get_descendants(message_id)
        for orphan_child in orphan_descendants:
            if orphan_child.status == MessageStatus.MISSING_PARENT:
                logger.info(
                    f"Found orphan children: {orphan_child} - making it visible"
                )
                self.set_message_status(
                    message=orphan_child, status=MessageStatus.DELIVERED
                )
