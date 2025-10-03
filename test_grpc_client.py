import logging
import asyncio
import grpc

# Import the gRPC generated stubs
from api.generated import master_messages_pb2, master_messages_pb2_grpc

logger = logging.getLogger(__name__)
SERVER_ADDRESS = "localhost:50052"


async def test_dockerized_grpc_server():
    """
    Test client for sending requests to a dockerized gRPC server.
    The server should be running via start_grpc_server.py.
    """
    try:
        # Connect to the dockerized gRPC server
        async with grpc.aio.insecure_channel(SERVER_ADDRESS) as channel:
            stub = master_messages_pb2_grpc.MessageServiceStub(channel)

            # Test 1: Send first message
            logger.info("--- Sending first message ---")
            content1 = "Hello from test client!"
            request = master_messages_pb2.PostMessageRequest(content=content1, write_concern=3)
            logger.info(f"Sending request: {request}")

            response1 = await stub.PostMessage(request)
            logger.info(
                f"Server Response: status='{response1.status}', message='{response1.message}', total={response1.total_messages}"
            )

            # Test 2: Send second message
            logger.info("\n--- Sending second message ---")
            content2 = "This is another test message."
            request = master_messages_pb2.PostMessageRequest(content=content2, write_concern=2)
            response2 = await stub.PostMessage(request)
            logger.info(
                f"Server Response: status='{response2.status}', message='{response2.message}', total={response2.total_messages}"
            )

            # Test 3: Get all messages
            logger.info("\n--- Getting all messages ---")
            get_response = await stub.GetMessages(master_messages_pb2.GetMessagesRequest())
            logger.info(f"Received {len(get_response.messages)} messages from server:")
            for i, msg in enumerate(get_response.messages, 1):
                logger.info(f"  {i}. {msg}")

            logger.info("\n--- Test completed successfully! ---")

    except grpc.aio.AioRpcError as e:
        logger.error(f"gRPC error: {e.details()} (code: {e.code()})")
        logger.error("Make sure the gRPC server is running (start_grpc_server.py)")
        raise
    except Exception as e:
        logger.error(f"Connection error: {e}")
        logger.error("Make sure the gRPC server is running on localhost:50052")
        raise


if __name__ == "__main__":
    asyncio.run(test_dockerized_grpc_server())
