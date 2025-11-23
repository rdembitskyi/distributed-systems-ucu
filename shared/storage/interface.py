from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from shared.domain.messages import Message


class MessageStoreInterface(ABC):
    """
    Abstract interface for message storage implementations.

    Defines the contract for storing and retrieving messages in a chain-like structure
    where each message can have a parent and children, maintaining chronological order.
    """

    @abstractmethod
    def add_message(self, message: Message) -> bool:
        """
        Add a new message to the store.

        Args:
            message: Dictionary containing message data with 'content' key

        Returns:
            bool: True if message was added successfully, False otherwise
        """
        pass

    @abstractmethod
    def get_messages(self) -> bool:
        pass

    @abstractmethod
    def get_messages_ids(self) -> list[str]:
        pass

    @abstractmethod
    def get_latest(self) -> Optional[Message]:
        """
        Get the most recently added message.

        Returns:
            Optional[Message]: Latest message or None if store is empty
        """
        pass

    @abstractmethod
    def get_by_sequence(self, seq: int) -> Optional[Message]:
        """
        Get message by its sequence number.

        Args:
            seq: Sequence number of the message

        Returns:
            Optional[Message]: Message with given sequence number or None
        """
        pass

    @abstractmethod
    def get_by_id(self, msg_id: str) -> Optional[Message]:
        """
        Get message by its unique identifier.

        Args:
            msg_id: Unique message identifier

        Returns:
            Optional[Message]: Message with given ID or None
        """
        pass

    @abstractmethod
    def get_parent(self, msg_id: str) -> Optional[Message]:
        """
        Get the parent message of the specified message.

        Args:
            msg_id: Message ID to find parent for

        Returns:
            Optional[Message]: Parent message or None if no parent exists
        """
        pass

    @abstractmethod
    def get_children(self, msg_id: str) -> List[Message]:
        """
        Get all direct children of the specified message.

        Args:
            msg_id: Message ID to find children for

        Returns:
            List[Message]: List of child messages (empty if no children)
        """
        pass

    @abstractmethod
    def get_chain_from_message(self, msg_id: str) -> List[Message]:
        """
        Get the full parent chain starting from the specified message.

        Args:
            msg_id: Message ID to start chain from

        Returns:
            List[Message]: Chain of messages from root to specified message
        """
        pass

    @abstractmethod
    def get_last_n_messages(self, n: int) -> List[Message]:
        """
        Get the last n messages in chronological order.

        Args:
            n: Number of recent messages to retrieve

        Returns:
            List[Message]: Last n messages (may be fewer if store has less than n messages)
        """
        pass
