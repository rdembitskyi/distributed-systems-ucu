import logging
from grpc import aio
from typing import Any
from master.transport.interface import MasterTransportInterface
from master.generated import messages_pb2
from master.generated import messages_pb2_grpc
from shared.domain.messages import Message
from shared.storage.factory import get_messages_storage
from shared.services.message_bulder import MessageBuilder
from master.services.workers import WorkersService


logger = logging.getLogger(__name__)


class GrpcTransport(MasterTransportInterface):
    """gRPC implementation of TransportInterface"""

    def __init__(self):
        self._store = get_messages_storage()
        self.builder = MessageBuilder(store=self._store)
        self.workers_service = WorkersService()

    async def save_message(self, message_content: str) -> dict[str, Any]:
        """Append a message to the in-memory list"""
        message = self.builder.create_message(content=message_content)

        if not message:
            return {
                "status": "error",
                "message": "Failed to create message",
                "total_messages": len(self._store.get_messages()),
            }

        replication = await self.workers_service.replicate_message_to_all(message=message)
        if not replication.success:
            logger.error(f"Failed to replicate message to all workers: {replication.error_message}")
            return {"status": "error", "message": "Failed to replicate message to all workers"}
        else:
            # add message to the store per requirement that we receive ACK from all workers
            self._store.add_message(message=message)

        return {
            "status": "success",
            "message": "Message added successfully",
            "total_messages": len(self._store.messages),
        }

    async def get_messages(self) -> list[Message]:
        """Return all messages from the in-memory list"""
        return self._store.get_messages()

    async def start_server(self, port: int = 50051):
        """Start the async gRPC server"""
        self._server = aio.server()
        servicer = GrpcMessageServicer(self)
        messages_pb2_grpc.add_MessageServiceServicer_to_server(servicer, self._server)
        self._server.add_insecure_port(f"[::]:{port}")
        await self._server.start()
        logger.info(f"gRPC server started on port {port}")
        return self._server

    async def stop_server(self):
        """Stop the async gRPC server"""
        if self._server:
            await self._server.stop(0)
            logger.info("gRPC server stopped")


class GrpcMessageServicer(messages_pb2_grpc.MessageServiceServicer):
    """gRPC servicer implementation"""

    def __init__(self, transport: GrpcTransport):
        self.transport = transport

    async def PostMessage(self, request, context):
        """Handle POST message requests"""
        logger.info(f"Received POST message request: {request}")
        content = request.content

        result = await self.transport.save_message(content)
        logger.info(f"Result of POST message request: {result}")
        return messages_pb2.PostMessageResponse(
            status=result["status"],
            message=result["message"],
        )

    async def GetMessages(self, request, context):
        """Handle GET messages requests"""
        logger.info(f"Received GET messages request")
        domain_messages = await self.transport.get_messages()

        # Convert domain messages to protobuf messages
        pb_messages = []
        for msg in domain_messages:
            pb_msg = messages_pb2.Message(
                message_id=msg.message_id,
                content=msg.content,
                sequence_number=msg.sequence_number,
                parent_id=msg.parent_id,
                timestamp=msg.timestamp,
            )
            pb_messages.append(pb_msg)
        logger.info(f"Result of GET messages request: {pb_messages}")
        return messages_pb2.GetMessagesResponse(messages=pb_messages)
