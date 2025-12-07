import asyncio
from collections import deque
import logging
import os
from typing import Dict, List

from grpc import aio

from api.generated import worker_messages_pb2, worker_messages_pb2_grpc
from master.config import get_workers_registry
from master.services.heartbeat import HeartBeatService
from master.services.replication_coordinator import (
    handle_replication_response_from_workers,
)
from master.services.retry_policy import RetryPolicy
from master.services.write_controller import manage_write_availability
from secondary_worker.domain.messages import MasterMessageReplicaResponse
from shared.domain.constants import NO_MESSAGES
from shared.domain.messages import Message, MessageStatus
from shared.domain.replication import ReplicationResult
from shared.domain.response import HealthStatus
from shared.domain.status_codes import StatusCodes
from shared.domain.worker import Worker
from shared.security.auth import get_auth_token
from shared.storage.factory import get_messages_storage
from shared.utils.concurrency import RequiredCountNotReached, wait_for_required_count


logger = logging.getLogger(__name__)


WORKERS_REGISTRY = get_workers_registry()
REPLICATION_TIMEOUT = float(os.environ.get("REPLICATION_TIMEOUT"))


class WorkersService:
    """Service to manage secondary workers and handle replication"""

    def __init__(self, replication_timeout: float = REPLICATION_TIMEOUT):
        self.replication_timeout = replication_timeout
        self.workers: Dict[str, Worker] = {}
        self.worker_clients: Dict[
            str, worker_messages_pb2_grpc.SecondaryWorkerServiceStub
        ] = {}
        self.heartbeat_service: HeartBeatService | None = None
        self._initialize_workers()
        self._background_tasks: deque = deque(maxlen=1000)

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
            recovery_callback=self.send_on_worker_recovery_request,
        )
        self.heartbeat_service.start_monitoring()

        logger.info(f"Initialized {len(self.workers)} workers")

    async def replicate_message_to_workers(
        self, message: Message, write_concern: int
    ) -> ReplicationResult | None:
        """
        Replicate message to active workers.
        Returns success only if quorum of workers acknowledge.
        """
        if not self.workers:
            return ReplicationResult(
                success=False, error_message="No workers available", total_workers=0
            )

        available_workers = self.heartbeat_service.get_all_workers()
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
            # returns also tasks still running when quorum was reached
            required_replication_count = write_concern - 1  # Subtract 1 for the master
            replication_result = await wait_for_required_count(
                tasks=replication_tasks,
                required_count=required_replication_count,
            )

            # Check if all replications succeeded
            replication_statuses = handle_replication_response_from_workers(
                results=replication_result.completed_results
            )
            if not replication_statuses.success:
                self.replicate_message_to_remaining_workers(
                    message=message, replication_statuses=replication_statuses
                )
            if replication_result.pending_tasks:
                task = asyncio.create_task(
                    self.handle_results_from_pending_workers(
                        message=message, pending_tasks=replication_result.pending_tasks
                    )
                )
                self._add_task_worker_instance(task=task)

            logger.info(
                f"Successfully replicated message {message.message_id} to {required_replication_count} workers"
            )
            return replication_statuses

        except asyncio.TimeoutError:
            logger.error(f"Replication timeout after {self.replication_timeout}s")
            return ReplicationResult(
                success=False,
                error_message=f"Replication timeout after {self.replication_timeout}s",
            )
        except RequiredCountNotReached as e:
            logger.warning(
                f"Replication: Failed to reach quorum of {required_replication_count} workers"
            )
            manage_write_availability(client_id=message.client_id, availability=False)
            replication_statuses = handle_replication_response_from_workers(
                results=e.completed_results
            )
            try:
                retry_result = (
                    await self.retry_replication_until_target_count_is_reached(
                        replication_statuses=replication_statuses,
                        replication_count=required_replication_count,
                        message=message,
                    )
                )
                if retry_result.success:
                    manage_write_availability(
                        client_id=message.client_id, availability=True
                    )
                    return ReplicationResult(success=True)
                if retry_result.success_count >= required_replication_count:
                    manage_write_availability(
                        client_id=message.client_id, availability=True
                    )
                    self.replicate_message_to_remaining_workers(
                        message=message, replication_statuses=replication_statuses
                    )
                    return ReplicationResult(success=True)
            except RequiredCountNotReached:
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
            async with asyncio.timeout(self.replication_timeout):
                result = await client.ReplicateMessage(request)
            return MasterMessageReplicaResponse(
                status=result.status,
                status_code=result.status_code,
                error_message=result.error_message,
                worker_id=worker_id,
            )
        except asyncio.TimeoutError:
            logger.error(f"Replication timeout after {self.replication_timeout}s")
            return MasterMessageReplicaResponse(
                worker_id=worker_id,
                status=MessageStatus.FAILED.value,
                status_code=StatusCodes.TIMEOUT.value,
                error_message=f"Replication timeout after {self.replication_timeout}s",
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

            return response.status == HealthStatus.HEALTHY

        except Exception as e:
            logger.warning(f"Health check failed for worker {worker_id}: {e}")
            return False

    async def retry_replication_using_policy(
        self,
        worker_id: str,
        message: Message,
    ) -> tuple[ReplicationResult, str]:
        """Retry replication to a worker based on the given retry policy"""
        state = self.heartbeat_service.get_worker_state(worker_id=worker_id)
        policy = RetryPolicy.for_health_state(state=state)
        last_result = ReplicationResult(
            success=False,
        )
        for attempt in range(policy.max_attempts):
            result = await self.replicate_to_worker(worker_id, message)
            replication_status = handle_replication_response_from_workers(
                results=[result]
            )
            if replication_status.success:
                # Task cleanup happens automatically via callback
                return replication_status, worker_id

            last_result = replication_status
            if attempt < policy.max_attempts - 1:
                delay = policy.calculate_delay(attempt=attempt)
                await asyncio.sleep(delay)

        return last_result, worker_id

    async def retry_replication_until_target_count_is_reached(
        self,
        replication_statuses: ReplicationResult,
        replication_count: int,
        message: Message,
    ):
        workers_to_retry = [
            worker_id for worker_id in replication_statuses.retry_workers
        ]
        retry_tasks = [
            asyncio.create_task(
                self.retry_replication_using_policy(
                    worker_id=worker_id, message=message
                )
            )
            for worker_id in workers_to_retry
        ]
        for task in asyncio.as_completed(retry_tasks):
            retry_result, worker_id = await task
            if retry_result.success:
                replication_statuses.remove_retry_worker(worker_id=worker_id)
                if replication_statuses.success_count >= replication_count:
                    logger.info(
                        f"Successfully replicated message {message.message_id} to {replication_count} workers after retries"
                    )
                    return ReplicationResult(
                        success=True,
                    )
        raise RequiredCountNotReached(
            f"Replication: Failed to reach quorum of {replication_count} tasks"
        )

    def replicate_message_to_remaining_workers(
        self, message: Message, replication_statuses: ReplicationResult
    ) -> bool:
        """Replicate message to remaining workers in the background"""
        remaining_workers = replication_statuses.retry_workers
        # Fire background task for remaining workers
        for worker_id in remaining_workers:
            task = asyncio.create_task(
                self.retry_replication_using_policy(
                    worker_id=worker_id, message=message
                )
            )
            self._add_task_worker_instance(task=task)
        return True

    async def handle_results_from_pending_workers(
        self, message: Message, pending_tasks: List[asyncio.Task]
    ):
        completed_results = await asyncio.gather(*pending_tasks, return_exceptions=True)
        replication_statuses = handle_replication_response_from_workers(
            results=completed_results
        )
        if not replication_statuses.success:
            self.replicate_message_to_remaining_workers(
                message=message, replication_statuses=replication_statuses
            )

    def _add_task_worker_instance(self, task: asyncio.Task):
        """
        Keep a strong reference to background tasks to prevent garbage collection.

        Python's asyncio doesn't hold strong references to tasks created with
        create_task(). Without storing a reference, tasks may be garbage collected
        before completion, causing them to be cancelled silently.

        This method stores tasks in a bounded queue (max 1000). When the queue is full,
        the oldest task reference is automatically dropped (FIFO). Tasks clean themselves
        up on completion via callback.
        """
        self._background_tasks.append(task)

        # Auto-cleanup when task completes to prevent holding references to finished tasks
        def cleanup(t):
            try:
                self._background_tasks.remove(t)
            except ValueError:
                # Task already removed (e.g., dropped due to maxlen)
                pass

        task.add_done_callback(cleanup)

    async def send_on_worker_recovery_request(self, worker_id: str):
        storage = get_messages_storage()
        last_message = storage.get_latest()
        auth_token = get_auth_token()
        master_latest_sequence = (
            last_message.sequence_number if last_message else NO_MESSAGES
        )
        request = worker_messages_pb2.RecoveryNotification(
            master_latest_sequence=master_latest_sequence, auth_token=auth_token
        )

        client = self.worker_clients[worker_id]
        result = await client.HandleRecovery(request)
        logger.info(
            f"Received recovery notification for worker {worker_id}: result={result}"
        )
        return result
