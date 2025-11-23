import logging
import asyncio
from grpc import aio
from api.generated import master_messages_pb2, master_messages_pb2_grpc
from shared.storage.factory import get_messages_storage
from shared.security.auth import get_auth_token
from secondary_worker.services.replica_message_validation.replica_validation import (
    validate_message,
)
from secondary_worker.domain.messages import Message

logger = logging.getLogger(__name__)


class WorkerSyncService:
    """Service for worker to sync with master when it falls behind"""

    def __init__(self, master_address: str = "master", master_port: int = 50052):
        self.master_address = master_address
        self.master_port = master_port
        self._master_client = None
        self._initialize_master_client()

    def _initialize_master_client(self):
        """Initialize gRPC client to communicate with master"""
        channel = aio.insecure_channel(f"{self.master_address}:{self.master_port}")
        self._master_client = master_messages_pb2_grpc.MessageServiceStub(channel)
        logger.info(
            f"Replica sync: Initialized master client: {self.master_address}:{self.master_port}"
        )

    async def request_catchup(self) -> bool:
        """
        Request catch-up messages from master.
        Returns True if catch-up was successful, False otherwise.
        """
        try:
            storage = get_messages_storage()
            worker_latest_message = storage.get_latest()
            worker_last_seq = (
                worker_latest_message.sequence_number if worker_latest_message else 0
            )

            logger.info(f"Requesting sync: Worker last sequence: {worker_last_seq}")

            # Create request to master
            auth_token = get_auth_token()
            request = master_messages_pb2.CatchUpRequest(
                last_sequence_number=worker_last_seq,
                auth_token=auth_token,
            )

            # Call master's RequestCatchUp RPC
            response = await asyncio.wait_for(
                self._master_client.CatchUp(request), timeout=30.0
            )

            if response.status != "success":
                logger.error(
                    f"Replica sync: Catch-up request failed: {response.error_message}"
                )
                return False

            # Process received messages
            if not response.messages:
                logger.info("Replica sync: No missing messages, worker is up-to-date")
                return True

            logger.info(
                f"Replica sync: Received {len(response.messages)} messages from master"
            )

            # Validate and store each message
            for pb_msg in response.messages:
                logger.info(f"Catch-up request message: {pb_msg}")
                message = Message(
                    message_id=pb_msg.message_id,
                    content=pb_msg.content,
                    sequence_number=pb_msg.sequence_number,
                    parent_id=pb_msg.parent_id,
                    timestamp=pb_msg.timestamp,
                    signature=pb_msg.signature,
                )

                # Validate message
                validation_result = validate_message(message=message)
                if not validation_result.is_valid:
                    logger.error(
                        f"Replica sync: Catch-up message validation failed: {message.message_id} - {validation_result.error}"
                    )
                    # TODO
                    return False

                if validation_result.is_duplicated:
                    logger.warning(
                        f"Replica sync: Catch-up message already exists: {message.message_id}"
                    )
                    continue

                # Store message
                storage.add_message(message)

            logger.info(f"Replica sync: Catch-up request successful")
            return True

        except asyncio.TimeoutError:
            logger.error("Catch-up request timed out")
            return False
        except Exception as e:
            logger.error(f"Error during catch-up request: {e}", exc_info=True)
            return False
