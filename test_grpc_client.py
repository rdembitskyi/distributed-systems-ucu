import asyncio
import grpc

# Import the gRPC generated stubs
from api.generated import messages_pb2, messages_pb2_grpc

SERVER_ADDRESS = "localhost:50052"


async def test_dockerized_grpc_server():
    """
    Test client for sending requests to a dockerized gRPC server.
    The server should be running via start_grpc_server.py.
    """
    try:
        # Connect to the dockerized gRPC server
        async with grpc.aio.insecure_channel(SERVER_ADDRESS) as channel:
            stub = messages_pb2_grpc.MessageServiceStub(channel)

            # Test 1: Send first message
            print("--- Sending first message ---")
            content1 = "Hello from test client!"
            request = messages_pb2.PostMessageRequest(content=content1)
            print(f"Sending request: {request}")

            response1 = await stub.PostMessage(request)
            print(
                f"Server Response: status='{response1.status}', message='{response1.message}', total={response1.total_messages}"
            )

            # Test 2: Send second message
            print("\n--- Sending second message ---")
            content2 = "This is another test message."
            request = messages_pb2.PostMessageRequest(content=content2)
            response2 = await stub.PostMessage(request)
            print(
                f"Server Response: status='{response2.status}', message='{response2.message}', total={response2.total_messages}"
            )

            # Test 3: Get all messages
            print("\n--- Getting all messages ---")
            get_response = await stub.GetMessages(messages_pb2.GetMessagesRequest())
            print(f"Received {len(get_response.messages)} messages from server:")
            for i, msg in enumerate(get_response.messages, 1):
                print(f"  {i}. {msg}")

            print("\n--- Test completed successfully! ---")

    except grpc.aio.AioRpcError as e:
        print(f"gRPC error: {e.details()} (code: {e.code()})")
        print("Make sure the gRPC server is running (start_grpc_server.py)")
        raise
    except Exception as e:
        print(f"Connection error: {e}")
        print("Make sure the gRPC server is running on localhost:50052")
        raise


if __name__ == "__main__":
    asyncio.run(test_dockerized_grpc_server())
