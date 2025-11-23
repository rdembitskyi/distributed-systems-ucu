from abc import ABC, abstractmethod
from typing import Any


class MasterTransportInterface(ABC):
    """Interface for transport layer implementations (HTTP, gRPC, etc.)"""

    @abstractmethod
    async def save_message(
        self, message_content: str, client_id: str, write_concern=3
    ) -> dict[str, Any]:
        """Append a message to the in-memory list

        Args:
            message_content: Message content
            client_id: Client ID
            write_concern: Write concern

        Returns:
            Dict containing status and any relevant data
        """
        pass

    @abstractmethod
    async def get_messages(self) -> list[str]:
        """Return all messages from the in-memory list

        Returns:
            List of all stored messages
        """
        pass
