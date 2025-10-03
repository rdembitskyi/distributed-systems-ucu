import logging
from shared.domain.replication import ReplicationResult
from shared.domain.status_codes import StatusCodes
from grpc import RpcError
from secondary_worker.domain.messages import MasterMessageReplicaResponse

logger = logging.getLogger(__name__)


def handle_replication_response_from_workers(active_workers, results: list[MasterMessageReplicaResponse | Exception]):
    """
    Handle replication responses from workers and determine overall success.
    If any worker fails, the entire replication is considered failed.
    For future versions, we may want to retry failed workers.
    """
    failed_workers = []
    retry_workers = []
    error_messages = []
    for i, result in enumerate(results):
        worker_id = list(active_workers.keys())[i]
        if isinstance(result, Exception):
            if isinstance(result, RpcError):
                retry_workers.append(f"Connection error for worker {worker_id}")
            failed_workers.append(worker_id)
            error_str = f"Unexpected error: Replication failed for worker {worker_id}: {result}"
            logger.error(error_messages)
            error_messages.append(error_str)
        elif result.status_code == StatusCodes.UNAUTHORIZED.value:
            failed_workers.append(worker_id)
            error_str = f"Replication failed for worker {worker_id}: Unauthorized"
            logger.error(error_str)
            error_messages.append(error_str)
        elif result.status_code == StatusCodes.BAD_REQUEST.value:
            failed_workers.append(worker_id)
            error_str = f"Replication failed for worker {worker_id}: Bad Request - {result.error_message}"
            logger.error(error_str)
            error_messages.append(error_str)
        elif result.status_code == StatusCodes.DUPLICATE_RECEIVED.value:
            # If a duplicate message is received, we should finish replication and not retry the worker
            logger.warning(f"Replication warning for worker {worker_id}: Duplicated message - {result.error_message}")
        elif result.status_code == StatusCodes.RATE_LIMITED.value:
            retry_workers.append(worker_id)
            logger.warning(f"Replication warning for worker {worker_id}: Rate limited - {result.error_message}")
        elif result.status_code == StatusCodes.INTERNAL_SERVER_ERROR.value:
            failed_workers.append(worker_id)
            error_str = f"Replication failed for worker {worker_id}: Internal Server Error - {result.error_message}"
            logger.error(error_str)
            error_messages.append(error_str)

    if failed_workers or retry_workers:
        return ReplicationResult(
            success=False,
            failed_workers=failed_workers,
            retry_workers=retry_workers,
            error_message=f"Replication failed for workers: {failed_workers}",
        )
    else:
        return ReplicationResult(success=True)
