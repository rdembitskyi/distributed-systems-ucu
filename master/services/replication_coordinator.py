import logging

from secondary_worker.domain.messages import MasterMessageReplicaResponse
from shared.domain.replication import ReplicationResult
from shared.domain.status_codes import StatusCodes


logger = logging.getLogger(__name__)


def handle_replication_response_from_workers(
    results: list[MasterMessageReplicaResponse],
):
    """
    Handle replication responses from workers and determine overall success.
    If any worker fails, the entire replication is considered failed.
    For future versions, we may want to retry failed workers.
    """
    retry_workers = []
    error_messages = []
    for i, result in enumerate(results):
        worker_id = result.worker_id
        if result.status_code == StatusCodes.UNAUTHORIZED.value:
            retry_workers.append(worker_id)
            error_str = f"Replication failed for worker {worker_id}: Unauthorized"
            logger.error(error_str)
            error_messages.append(error_str)
        elif result.status_code == StatusCodes.BAD_REQUEST.value:
            retry_workers.append(worker_id)
            error_str = f"Replication failed for worker {worker_id}: Bad Request - {result.error_message}"
            logger.error(error_str)
            error_messages.append(error_str)
        elif result.status_code == StatusCodes.DUPLICATE_RECEIVED.value:
            # If a duplicate message is received, we should finish replication and not retry the worker
            logger.warning(
                f"Replication warning for worker {worker_id}: Duplicated message - {result.error_message}"
            )
        elif result.status_code == StatusCodes.RATE_LIMITED.value:
            retry_workers.append(worker_id)
            logger.warning(
                f"Replication warning for worker {worker_id}: Rate limited - {result.error_message}"
            )
        elif result.status_code == StatusCodes.UNAVAILABLE.value:
            retry_workers.append(worker_id)
            logger.warning(f"Replication warning for worker {worker_id}: Unavailable - {result.error_message}")
        elif result.status_code == StatusCodes.INTERNAL_SERVER_ERROR.value:
            retry_workers.append(worker_id)
            error_str = f"Replication failed for worker {worker_id}: Internal Server Error - {result.error_message}"
            logger.warning(error_str)
            error_messages.append(error_str)

    if retry_workers:
        return ReplicationResult(
            success=False,
            total_workers=len(results),
            failed_workers=[],
            retry_workers=retry_workers,
            error_message=f"Replication failed for workers: {retry_workers}",
        )
    else:
        return ReplicationResult(success=True, total_workers=len(results))
