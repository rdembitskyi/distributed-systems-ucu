import logging
from typing import Any

from grpc import aio

from api.generated import master_messages_pb2, master_messages_pb2_grpc
from master.domain.messages import PostMessageResponse
from master.services.workers import WorkersService
from master.services.write_controller import get_write_availability
from master.transport.interface import MasterTransportInterface
from shared.domain.messages import Message
from shared.domain.response import ResponseStatus
from shared.security.auth import validate_auth_token
from shared.services.message_bulder import MessageBuilder
from shared.storage.factory import get_messages_storage


logger = logging.getLogger(__name__)


class GrpcTransport(MasterTransportInterface):
    """gRPC implementation of TransportInterface"""

    def __init__(self):
        self.store = get_messages_storage()
        self.builder = MessageBuilder(store=self.store)
        self.workers_service = WorkersService()

    async def save_message(
        self, message_content: str, client_id: str, write_concern=3
    ) -> PostMessageResponse:
        """Append a message to the in-memory list.
        By default we set write concern to 3 to ensure that the message is replicated to all workers.
        But user can override this by passing write_concern as a parameter in request
        """

        if not message_content:
            return PostMessageResponse(
                status=ResponseStatus.ERROR,
                message="Failed to create message",
                total_messages=len(self.store.get_messages()),
            )

        message = self.builder.create_message(
            content=message_content, client_id=client_id
        )

        if not message:
            return PostMessageResponse(
                status=ResponseStatus.ERROR,
                message="Failed to create message",
                total_messages=len(self.store.get_messages()),
            )

        self.store.add_message(message=message)
        await self.workers_service.replicate_message_to_workers(
            message=message, write_concern=write_concern
        )

        return PostMessageResponse(
            status=ResponseStatus.SUCCESS,
            message="Message added successfully",
            total_messages=len(self.store.get_messages()),
        )

    async def get_messages(self) -> list[Message]:
        """Return all messages from the in-memory list"""
        return self.store.get_messages()

    async def get_worker_missing_messages(self, last_sequence_number: int):
        """Get all messages after the worker's last known sequence number"""
        if last_sequence_number == 0:
            # Worker has no messages (sequence 0) - return all messages
            return self.store.get_messages()

        worker_latest = self.store.get_by_sequence(last_sequence_number)
        return self.store.get_children(worker_latest.message_id)

    async def start_server(self, port: int = 50051):
        """Start the async gRPC server"""
        self._server = aio.server()
        servicer = GrpcMessageServicer(self)
        master_messages_pb2_grpc.add_MessageServiceServicer_to_server(
            servicer, self._server
        )
        self._server.add_insecure_port(f"[::]:{port}")
        await self._server.start()
        logger.info(f"gRPC server started on port {port}")
        return self._server

    async def stop_server(self):
        """Stop the async gRPC server"""
        if self._server:
            await self._server.stop(0)
            logger.info("gRPC server stopped")


class GrpcMessageServicer(master_messages_pb2_grpc.MessageServiceServicer):
    """gRPC servicer implementation"""

    def __init__(self, transport: GrpcTransport):
        self.transport = transport

    async def PostMessage(self, request, context):
        """Handle POST message requests"""
        logger.info(f"Received POST message request: {request}")
        content = request.content
        client_id = request.client_id
        write_availability = get_write_availability(client_id=client_id)
        logger.info(f"Write availability: {write_availability}")
        if not write_availability:
            return master_messages_pb2.PostMessageResponse(
                status=ResponseStatus.ERROR,
                message="Service temporarily unavailable for writes (quorum lost)",
            )
        write_concern = request.write_concern

        result = await self.transport.save_message(
            message_content=content, write_concern=write_concern, client_id=client_id
        )
        logger.info(f"Result of POST message request: {result}")
        return master_messages_pb2.PostMessageResponse(
            status=result.status,
            message=result.message,
        )

    async def GetMessages(self, request, context):
        """Handle GET messages requests"""
        logger.info(f"Received GET messages request")
        domain_messages = await self.transport.get_messages()

        # Convert domain messages to protobuf messages
        pb_messages = []
        for msg in domain_messages:
            pb_msg = master_messages_pb2.Message(
                message_id=msg.message_id,
                content=msg.content,
                sequence_number=msg.sequence_number,
                parent_id=msg.parent_id,
                timestamp=msg.timestamp,
            )
            pb_messages.append(pb_msg)
        logger.info(f"Result of GET messages request: {pb_messages}")
        return master_messages_pb2.GetMessagesResponse(messages=pb_messages)

    async def CatchUp(self, request, context):
        """Handle Catch-up requests"""
        if not validate_auth_token(token=request.auth_token):
            logger.error("Replica: Invalid auth token from worker")
            return master_messages_pb2.CatchUpResponse(
                status=ResponseStatus.ERROR, messages=[]
            )
        missing_messages = await self.transport.get_worker_missing_messages(
            last_sequence_number=request.last_sequence_number
        )
        logger.info(f"Missing messages: {missing_messages}")
        # Convert domain messages to protobuf messages
        pb_messages = []
        for msg in missing_messages:
            pb_msg = master_messages_pb2.SignedMessage(
                message_id=msg.message_id,
                content=msg.content,
                sequence_number=msg.sequence_number,
                parent_id=msg.parent_id,
                timestamp=msg.timestamp,
                signature=msg.signature,
            )
            pb_messages.append(pb_msg)
        return master_messages_pb2.CatchUpResponse(
            status=ResponseStatus.SUCCESS, messages=pb_messages
        )
