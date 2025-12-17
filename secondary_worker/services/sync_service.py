import asyncio
import logging

from api.generated import master_messages_pb2
from secondary_worker.config import get_master_client
from secondary_worker.domain.messages import Message
from secondary_worker.domain.sync_service import SyncResult
from secondary_worker.services.replica_message_validation.replica_validation import (
    validate_message,
)
from shared.domain.constants import NO_MESSAGES, SYNC_TIMEOUT
from shared.domain.response import ResponseStatus, SyncStatus
from shared.security.auth import get_auth_token
from shared.storage.factory import get_messages_storage


logger = logging.getLogger(__name__)


class WorkerSyncService:
    """Service for worker to sync with master when it falls behind"""

    def __init__(self):
        """Initialize sync service with master client from config"""
        self._master_client = get_master_client()

    async def request_catchup(self) -> SyncResult:
        """
        Request catch-up messages from master.

        Returns:
            SyncResult: Status of sync operation with failed messages if any
        """
        storage = get_messages_storage()
        worker_latest_message = storage.get_latest_delivered()
        worker_last_seq = (
            worker_latest_message.sequence_number
            if worker_latest_message
            else NO_MESSAGES
        )

        logger.info(f"Requesting sync from master (last_seq={worker_last_seq})")

        # Create request to master
        request = master_messages_pb2.CatchUpRequest(
            last_sequence_number=worker_last_seq,
            auth_token=get_auth_token(),
        )

        try:
            # Call master's CatchUp RPC
            response = await asyncio.wait_for(
                self._master_client.CatchUp(request), timeout=SYNC_TIMEOUT
            )
        except asyncio.TimeoutError:
            logger.error(f"Sync request timed out after {SYNC_TIMEOUT}s")
            return SyncResult(status=SyncStatus.FAILED_TO_SYNC)

        if response.status != ResponseStatus.SUCCESS:
            logger.error(f"Sync request failed: {response.error_message}")
            return SyncResult(status=SyncStatus.FAILED_TO_SYNC)

        # No missing messages = already in sync
        if not response.messages:
            logger.info("No missing messages, worker is up-to-date")
            return SyncResult(status=SyncStatus.SYNCED)

        logger.info(f"Received {len(response.messages)} messages from master")

        try:
            # Validate and store each message
            for pb_msg in response.messages:
                logger.debug(
                    f"Processing sync message: {pb_msg.message_id} (seq={pb_msg.sequence_number})"
                )

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
                    # Validation failure during sync is CRITICAL
                    logger.critical(
                        f"CRITICAL: Sync validation failed for {message.message_id}: "
                        f"{validation_result.error}. Aborting sync - this indicates "
                        f"master data corruption or sync bug."
                    )
                    return SyncResult(
                        status=SyncStatus.FAILED_TO_SYNC,
                        failed_messages=[message.message_id],
                    )

                if validation_result.is_duplicated:
                    logger.info(
                        f"Message {message.message_id} already exists, skipping"
                    )
                    continue

                # Store message
                storage.add_message(message=message, missing_parent=False)

            logger.info(f"Sync successful: {len(response.messages)} messages synced")
            return SyncResult(status=SyncStatus.SYNCED)

        except Exception as e:
            logger.error(f"Sync failed with exception: {e}", exc_info=True)
            return SyncResult(status=SyncStatus.FAILED_TO_SYNC)
