import logging
from collections import OrderedDict
from typing import Dict, List, Optional
from shared.domain.messages import Message, MessageStatus
from shared.storage.interface import MessageStoreInterface
from threading import Lock
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
        self.children_index = {}  # parent_id -> Set[child_ids]

        # Track latest for O(1) access
        self.latest_message = None

        # Thread safety
        self.lock = Lock()
        self.clients_write_status = {}

    def add_message(self, message: Message) -> bool:
        """Add a message - O(1) operation"""
        with self.lock:
            parent_id = ""
            sequence_number = 1
            if self.latest_message:
                logger.info(f"Found latest message: {self.latest_message}")
                parent_id = self.latest_message.message_id
                sequence_number = self.latest_message.sequence_number + 1

            self.set_message_status(message=message, status=MessageStatus.DELIVERED)
            logger.info(f"Adding message: {message}")
            # Store in primary structure
            self.messages[sequence_number] = message

            # Update indexes
            msg_id = message.message_id
            self.id_to_sequence[msg_id] = sequence_number

            # Update parent-child relationships
            if parent_id:
                self.parent_index[msg_id] = parent_id
                if parent_id not in self.children_index:
                    self.children_index[parent_id] = set()
                self.children_index[parent_id].add(msg_id)

            self.set_latest(message=message)

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

    def get_parent(self, msg_id: str) -> Optional[Dict]:
        """Get parent message - O(1)"""
        parent_id = self.parent_index.get(msg_id)
        if parent_id:
            return self.get_by_id(parent_id)
        return None

    def get_children(self, msg_id: str) -> List[Dict]:
        """Get all direct children - O(k) where k is number of children"""
        child_ids = self.children_index.get(msg_id, set())
        return [self.get_by_id(child_id) for child_id in child_ids]

    def set_message_status(self, message: Message, status: MessageStatus):
        """Set message status - O(1)"""
        message.status = status

    def get_chain_from_message(self, msg_id: str) -> List[Dict]:
        """Get full parent chain from a message - O(n) where n is chain length"""
        chain = []
        current_id = msg_id

        while current_id:
            msg = self.get_by_id(current_id)
            if not msg:
                break
            chain.append(msg)
            current_id = self.parent_index.get(current_id)

        return list(reversed(chain))  # Return from oldest to newest

    def get_last_n_messages(self, n: int) -> List[Dict]:
        """Get last n messages - O(n)"""
        # OrderedDict maintains insertion order
        sequences = list(self.messages.keys())[-n:]
        return [self.messages[seq] for seq in sequences]

    def set_node_write_status(self, client_id: str, status: bool):
        self.clients_write_status[client_id] = status

    def get_node_write_status(self, client_id: str) -> bool:
        # default is true, if status was not updated it means node is active for connections
        logger.info(f"here {self.clients_write_status.get(client_id, True)}")
        return self.clients_write_status.get(client_id, True)
