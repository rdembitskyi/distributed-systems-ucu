import logging
import asyncio
from grpc import aio
from secondary_worker.transport.interface import SecondaryTransportInterface
from api.generated import worker_messages_pb2, worker_messages_pb2_grpc
from secondary_worker.domain.messages import (
    Message,
    MasterMessageReplicaResponse,
    MasterHealthCheckResponse,
)
from shared.security.auth import validate_auth_token
from shared.domain.status_codes import StatusCodes
from secondary_worker.services.replica_message_validation.replica_validation import validate_message

from shared.storage.factory import get_messages_storage


logger = logging.getLogger(__name__)


class GrpcTransport(SecondaryTransportInterface):
    """gRPC implementation of TransportInterface for Secondary Worker"""

    def __init__(self):
        self._store = get_messages_storage()

    async def get_messages(self) -> list[str]:
        """Return all messages from the in-memory list"""
        return self._store.get_messages()

    async def replicate_message(self, message: Message, master_token: str) -> MasterMessageReplicaResponse:
        """Receive a message from the master"""
        logger.info(f"Replica: Received message: {message}")

        if not validate_auth_token(token=master_token):
            logger.error("Replica: Invalid auth token from master")
            return MasterMessageReplicaResponse(
                status="error", status_code=StatusCodes.UNAUTHORIZED.value, error_message="Invalid auth token"
            )

        validation_result = validate_message(message=message)
        if not validation_result.is_valid:
            logger.error(f"Replica: Message validation failed: {message} with error: {validation_result.error}")
            return MasterMessageReplicaResponse(
                status="error", status_code=StatusCodes.BAD_REQUEST.value, error_message=validation_result.error
            )
        if validation_result.is_duplicated:
            logger.error(f"Replica: Duplicated message received: {message}")
            return MasterMessageReplicaResponse(
                status="error", status_code=StatusCodes.DUPLICATE_RECEIVED.value, error_message="Duplicated message"
            )

        await asyncio.sleep(5.0)  # Delay for testing
        result = self._store.add_message(message)

        if not result:
            logger.error("Replica: Failed to add message to the store")
            return MasterMessageReplicaResponse(
                status="error",
                status_code=StatusCodes.INTERNAL_SERVER_ERROR.value,
                error_message="Failed to add message to the store",
            )
        return MasterMessageReplicaResponse(status="success", status_code=StatusCodes.OK.value, error_message=None)

    async def report_health(self) -> MasterHealthCheckResponse:
        """Report health status to master"""
        logger.info("Replica: Reporting health status")
        return MasterHealthCheckResponse(status="healthy")

    async def start_server(self, port: int = 50052):
        """Start the async gRPC server for secondary worker"""
        self._server = aio.server()
        servicer = GrpcSecondaryServicer(self)
        worker_messages_pb2_grpc.add_SecondaryWorkerServiceServicer_to_server(servicer, self._server)
        self._server.add_insecure_port(f"[::]:{port}")
        await self._server.start()
        logger.info(f"Secondary worker gRPC server started on port {port}")
        return self._server

    async def stop_server(self):
        """Stop the async gRPC server"""
        if self._server:
            await self._server.stop(0)
            logger.info("Secondary worker gRPC server stopped")


class GrpcSecondaryServicer(worker_messages_pb2_grpc.SecondaryWorkerServiceServicer):
    """gRPC servicer implementation for Secondary Worker"""

    def __init__(self, transport: GrpcTransport):
        self.transport = transport

    async def GetMessages(self, request, context):
        """Handle GET messages requests"""
        logger.info(f"Replica: Received GET messages request")
        domain_messages = await self.transport.get_messages()

        # Convert domain messages to protobuf messages
        pb_messages = []
        for msg in domain_messages:
            pb_msg = worker_messages_pb2.Message(
                message_id=msg.message_id,
                content=msg.content,
                sequence_number=msg.sequence_number,
                parent_id=msg.parent_id,
                timestamp=msg.timestamp,
            )
            pb_messages.append(pb_msg)
        logger.info(f"Result of GET messages request: {pb_messages}")
        return worker_messages_pb2.GetMessagesResponse(messages=pb_messages)

    async def ReplicateMessage(self, request, context):
        """Handle message reception from master"""
        logger.info(f"Replica: Received replication message request: {request.message}")

        # Convert protobuf to domain object
        message = Message(
            message_id=request.message.message_id,
            content=request.message.content,
            sequence_number=request.message.sequence_number,
            parent_id=request.message.parent_id,
            timestamp=request.message.timestamp,
            signature=request.message.signature,
        )
        master_token = request.auth_token

        # Process the message
        result = await self.transport.replicate_message(message=message, master_token=master_token)

        # Return response
        return worker_messages_pb2.MasterMessageReplicaResponse(
            status=result.status, status_code=result.status_code, error_message=result.error_message
        )

    async def ReportHealth(self, request, context):
        """Handle health check requests from master"""
        logger.info(f"Replica: Received health check request: {request}")

        await self.transport.report_health()

        return worker_messages_pb2.MasterHealthCheckResponse(status="healthy")
