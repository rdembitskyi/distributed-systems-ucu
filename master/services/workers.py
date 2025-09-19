import logging
import asyncio
from typing import List, Dict
from shared.domain.worker import Worker
from shared.domain.replication import ReplicationResult
from grpc import aio
from shared.domain.messages import Message
from api.generated import worker_messages_pb2, worker_messages_pb2_grpc
from shared.security.auth import get_auth_token

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
        self.worker_clients: Dict[str, worker_messages_pb2_grpc.SecondaryWorkerServiceStub] = {}
        self._initialize_workers()

    def _initialize_workers(self):
        """Initialize static worker registry"""
        for worker in WORKERS_REGISTRY:
            self.workers[worker.worker_id] = worker

            # Create gRPC client for each worker
            channel = aio.insecure_channel(f"{worker.address}:{worker.port}")
            self.worker_clients[worker.worker_id] = worker_messages_pb2_grpc.SecondaryWorkerServiceStub(channel)

        logger.info(f"Initialized {len(self.workers)} workers")

    async def replicate_message_to_all(self, message: Message) -> ReplicationResult:
        """
        Replicate message to all active workers.
        Returns success only if ALL workers acknowledge.
        """
        if not self.workers:
            return ReplicationResult(success=False, error_message="No workers available")

        active_workers = {wid: worker for wid, worker in self.workers.items() if worker.is_active}

        if not active_workers:
            return ReplicationResult(success=False, error_message="No active workers available")

        logger.info(f"Replicating message {message.message_id} to {len(active_workers)} workers")

        # Create replication tasks for all workers
        replication_tasks = []
        for worker_id, worker in active_workers.items():
            task = self.replicate_to_worker(worker_id, message)
            replication_tasks.append(task)

        try:
            # Wait for all workers to respond within timeout
            results = await asyncio.wait_for(
                asyncio.gather(*replication_tasks, return_exceptions=True), timeout=self.replication_timeout
            )

            # Check if all replications succeeded
            failed_workers = []
            for i, result in enumerate(results):
                worker_id = list(active_workers.keys())[i]
                if isinstance(result, Exception) or not result:
                    failed_workers.append(worker_id)
                    logger.error(f"Replication failed for worker {worker_id}: {result}")

            if failed_workers:
                return ReplicationResult(
                    success=False,
                    failed_workers=failed_workers,
                    error_message=f"Replication failed for workers: {failed_workers}",
                )

            logger.info(f"Successfully replicated message {message.message_id} to all workers")
            return ReplicationResult(success=True)

        except asyncio.TimeoutError:
            logger.error(f"Replication timeout after {self.replication_timeout}s")
            return ReplicationResult(
                success=False, error_message=f"Replication timeout after {self.replication_timeout}s"
            )
        except Exception as e:
            logger.error(f"Unexpected error during replication: {e}")
            return ReplicationResult(success=False, error_message=f"Unexpected error: {str(e)}")

    async def replicate_to_worker(self, worker_id: str, message: Message) -> bool:
        """Replicate message to a single worker"""
        try:
            client = self.worker_clients[worker_id]

            # Create protobuf request
            pb_message = worker_messages_pb2.MessageReplicaReceived(
                message_id=message.message_id,
                content=message.content,
                sequence_number=message.sequence_number,
                parent_id=message.parent_id,
                timestamp=message.timestamp,
                signature=message.signature or "",
                status=message.status.value,
            )

            auth_token = get_auth_token()

            request = worker_messages_pb2.MasterMessageReplicaRequest(message=pb_message, auth_token=auth_token)

            # Send replication request
            response = await client.ReplicateMessage(request)

            if response.status == "success":
                logger.debug(f"Worker {worker_id} acknowledged message {message.message_id}")
                return True
            else:
                logger.error(f"Worker {worker_id} rejected message {message.message_id}: {response.error_message}")
                return False

        except Exception as e:
            logger.error(f"Failed to replicate to worker {worker_id}: {e}")
            return False

    def get_active_workers(self) -> List[Worker]:
        """Get list of active workers"""
        return [worker for worker in self.workers.values() if worker.is_active]

    def get_worker_count(self) -> int:
        """Get total number of active workers"""
        return len(self.get_active_workers())

    async def health_check_all_workers(self) -> Dict[str, bool]:
        """Check health of all workers"""
        health_results = {}

        for worker_id, worker in self.workers.items():
            try:
                client = self.worker_clients[worker_id]
                request = worker_messages_pb2.MasterHealthCheckRequest(auth_token="master_token")

                response = await asyncio.wait_for(client.ReportHealth(request), timeout=2.0)

                health_results[worker_id] = response.status == "healthy"

            except Exception as e:
                logger.error(f"Health check failed for worker {worker_id}: {e}")
                health_results[worker_id] = False

        return health_results
