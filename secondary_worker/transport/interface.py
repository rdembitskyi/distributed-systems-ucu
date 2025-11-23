from abc import ABC, abstractmethod

from secondary_worker.domain.messages import Message


class SecondaryTransportInterface(ABC):
    """Interface for transport layer implementations (HTTP, gRPC, etc.)"""

    @abstractmethod
    async def get_messages(self) -> list[str]:
        """Return all messages from the in-memory list

        Returns:
            List of all stored messages
        """
        pass

    @abstractmethod
    async def replicate_message(self, message: Message, master_token: str):
        """Replicate a message from the master"""
        pass

    @abstractmethod
    async def report_health(self):
        pass
