from abc import ABC, abstractmethod
from typing import Any


class MasterTransportInterface(ABC):
    """Interface for transport layer implementations (HTTP, gRPC, etc.)"""

    @abstractmethod
    async def save_message(self, message: str) -> dict[str, Any]:
        """Append a message to the in-memory list

        Args:
            message: The message to append

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
