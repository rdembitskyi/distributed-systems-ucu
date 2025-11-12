import logging
import asyncio
from typing import List, Dict
from shared.domain.worker import Worker
from shared.domain.replication import ReplicationResult
from grpc import aio
from shared.domain.messages import Message, MessageStatus
from shared.domain.status_codes import StatusCodes
from api.generated import worker_messages_pb2, worker_messages_pb2_grpc
from shared.security.auth import get_auth_token
from secondary_worker.domain.messages import MasterMessageReplicaResponse
from master.services.replication_coordinator import (
    handle_replication_response_from_workers,
)
from master.services.retry_policy import RetryPolicy
from master.services.heartbeat import HeartBeatService
from shared.utils.concurrency import wait_for_quorum, QuorumNotReached

logger = logging.getLogger(__name__)
WORKERS_REGISTRY = [
    Worker(worker_id="worker1", address="worker1", port=50053),
    Worker(worker_id="worker2", address="worker2", port=50054),
]


class WorkersService:
    """Service to manage secondary workers and handle replication"""

    def __init__(self, replication_timeout: float = 15.0):
        self.replication_timeout = replication_timeout
        self.workers: Dict[str, Worker] = {}
        self.worker_clients: Dict[
            str, worker_messages_pb2_grpc.SecondaryWorkerServiceStub
        ] = {}
        self.heartbeat_service: HeartBeatService | None = None
        self._initialize_workers()

    def _initialize_workers(self):
        """Initialize static worker registry"""
        for worker in WORKERS_REGISTRY:
            self.workers[worker.worker_id] = worker

            # Create gRPC client for each worker
            channel = aio.insecure_channel(f"{worker.address}:{worker.port}")
            self.worker_clients[worker.worker_id] = (
                worker_messages_pb2_grpc.SecondaryWorkerServiceStub(channel)
            )
        self.heartbeat_service = HeartBeatService(
            workers=list(self.workers.values()),
            health_check_func=self.health_check_worker,
        )
        self.heartbeat_service.start_monitoring()

        logger.info(f"Initialized {len(self.workers)} workers")

    async def replicate_message_to_workers(
        self, message: Message, write_concern: int
    ) -> ReplicationResult:
        """
        Replicate message to active workers.
        Returns success only if quorum of workers acknowledge.
        """
        if not self.workers:
            return ReplicationResult(
                success=False, error_message="No workers available", total_workers=0
            )

        available_workers = self.heartbeat_service.get_available_workers()
        logger.info(f"Available workers: {available_workers}")

        if not available_workers:
            return ReplicationResult(
                success=False,
                error_message="No active workers available",
                total_workers=0,
            )

        logger.info(
            f"Replicating message {message.message_id} to {len(available_workers)} workers"
        )

        # Create replication tasks for all workers
        replication_tasks = []
        for worker_id in available_workers:
            task = self.replicate_to_worker(worker_id=worker_id, message=message)
            replication_tasks.append(task)

        try:
            # Wait for workers quorum
            quorum_count = write_concern - 1  # Subtract 1 for the master
            results = await wait_for_quorum(
                tasks=replication_tasks,
                required_count=quorum_count,
                timeout=self.replication_timeout,
            )

            # Check if all replications succeeded
            replication_statuses = handle_replication_response_from_workers(
                results=results
            )
            if not replication_statuses.success:
                self.replicate_message_to_remaining_workers(
                    message=message, replication_statuses=replication_statuses
                )

            logger.info(
                f"Successfully replicated message {message.message_id} to {quorum_count} workers"
            )
            return replication_statuses

        except asyncio.TimeoutError:
            logger.error(f"Replication timeout after {self.replication_timeout}s")
            return ReplicationResult(
                success=False,
                error_message=f"Replication timeout after {self.replication_timeout}s",
            )
        except QuorumNotReached as e:
            logger.warning(
                f"Replication: Failed to reach quorum of {quorum_count} workers"
            )
            replication_statuses = handle_replication_response_from_workers(
                results=e.completed_results
            )
            try:
                retry_result = await self.retry_replication_until_quorum_is_reached(
                    replication_statuses=replication_statuses,
                    quorum_count=quorum_count,
                    message=message,
                )
                if retry_result.success:
                    return ReplicationResult(success=True)
                if retry_result.success_count >= quorum_count:
                    self.replicate_message_to_remaining_workers(
                        message=message, replication_statuses=replication_statuses
                    )
                    return ReplicationResult(success=True)
            except QuorumNotReached:
                return ReplicationResult(
                    success=False,
                    error_message=f"Replication failed: Quorum not reached",
                )
        except Exception as e:
            logger.error(f"Unexpected error during replication: {e}")
            return ReplicationResult(
                success=False, error_message=f"Unexpected error: {str(e)}"
            )

    async def replicate_to_worker(
        self, worker_id: str, message: Message
    ) -> MasterMessageReplicaResponse:
        """Replicate message to a single worker"""
        client = self.worker_clients[worker_id]

        # Create protobuf request
        pb_message = worker_messages_pb2.MessageReplicaReceived(
            message_id=message.message_id,
            content=message.content,
            sequence_number=message.sequence_number,
            parent_id=message.parent_id,
            timestamp=message.timestamp,
            signature=message.signature,
            status=message.status.value,
        )

        auth_token = get_auth_token()

        try:
            request = worker_messages_pb2.MasterMessageReplicaRequest(
                message=pb_message, auth_token=auth_token
            )

            # Send replication request
            result = await client.ReplicateMessage(request)
            return MasterMessageReplicaResponse(
                status=result.status,
                status_code=result.status_code,
                error_message=result.error_message,
                worker_id=worker_id,
            )
        except Exception as e:
            logger.warning(f"Unexpected error during replication: {e}")
            return MasterMessageReplicaResponse(
                worker_id=worker_id,
                status=MessageStatus.FAILED.value,
                status_code=StatusCodes.INTERNAL_SERVER_ERROR.value,
                error_message=f"Unexpected error: {str(e)}",
            )

    def get_active_workers(self) -> List[Worker]:
        """Get list of active workers"""
        return [worker for worker in self.workers.values() if worker.is_active]

    def get_worker_count(self) -> int:
        """Get total number of active workers"""
        return len(self.get_active_workers())

    async def health_check_worker(self, worker_id) -> bool:
        """Check health of worker"""

        try:
            client = self.worker_clients[worker_id]
            request = worker_messages_pb2.MasterHealthCheckRequest(
                auth_token="master_token"
            )

            response = await asyncio.wait_for(client.ReportHealth(request), timeout=2.0)

            return response.status == "healthy"

        except Exception as e:
            logger.warning(f"Health check failed for worker {worker_id}: {e}")
            return False

    async def retry_replication_using_policy(
        self,
        worker_id: str,
        message: Message,
    ) -> ReplicationResult:
        """Retry replication to a worker based on the given retry policy"""
        state = self.heartbeat_service.get_worker_state(worker_id=worker_id)
        policy = RetryPolicy.for_health_state(state=state)
        last_result = None
        for attempt in range(policy.max_attempts):
            result = await self.replicate_to_worker(worker_id, message)
            replication_status = handle_replication_response_from_workers(
                results=[result]
            )
            if replication_status.success:
                return replication_status

            last_result = replication_status
            if attempt < policy.max_attempts - 1:
                delay = policy.calculate_delay(attempt=attempt)
                await asyncio.sleep(delay)

        return last_result

    async def retry_replication_until_quorum_is_reached(
        self, replication_statuses: ReplicationResult, quorum_count, message
    ):
        workers_to_retry = [
            worker_id for worker_id in replication_statuses.retry_workers
        ]
        retry_tasks = {
            asyncio.create_task(
                self.retry_replication_using_policy(
                    worker_id=worker_id, message=message
                )
            ): worker_id
            for worker_id in workers_to_retry
        }
        for coro in asyncio.as_completed(
            retry_tasks.keys(), timeout=self.replication_timeout
        ):
            retry_result = await coro
            worker_id = retry_tasks[coro]
            if retry_result.success:
                replication_statuses.remove_retry_worker(worker_id=worker_id)
                if replication_statuses.success_count >= quorum_count:
                    logger.info(
                        f"Successfully replicated message {message.message_id} to {quorum_count} workers after retries"
                    )
                    return ReplicationResult(
                        success=True,
                    )
        raise QuorumNotReached(
            f"Replication: Failed to reach quorum of {quorum_count} tasks"
        )

    def replicate_message_to_remaining_workers(
        self, message: Message, replication_statuses: ReplicationResult
    ) -> bool:
        """Replicate message to remaining workers in the background"""
        remaining_workers = replication_statuses.retry_workers
        # Fire background task for remaining workers
        for worker_id in remaining_workers:
            asyncio.create_task(
                self.retry_replication_using_policy(
                    worker_id=worker_id, message=message
                )
            )
        return True
