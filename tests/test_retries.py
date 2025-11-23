"""
Integration tests for retries (Iteration 3)

Tests verify:
"""

import pytest
import asyncio
import time
from api.generated import master_messages_pb2, worker_messages_pb2


@pytest.mark.integration
async def test_message_delivery_failed_write_3(
    docker_services, master_client, worker1_client, worker2_client
):
    """
    Test that if message delivery fails, master retries and eventually succeeds.
    With w = 3 master waits until he got confirmation from all nodes about success

    Steps:
    1. Inject 2 failures into worker1 (will fail next 2 requests)
    2. Send message with w=3 (requires worker1, worker2)
    3. Worker1 fails twice, then succeeds on 3rd attempt
    4. Verify message eventually reaches worker1 after retries
    """

    test_content = "Test retry on failure"
    write_concern = 3

    # Given: Inject 2 failures into worker1
    await worker1_client.InjectFailure(
        worker_messages_pb2.InjectFailureRequest(fail_next_n_requests=2)
    )

    # When: POST message to master
    start_time = time.time()

    response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content=test_content, write_concern=write_concern, client_id="client_1"
        )
    )

    duration = time.time() - start_time

    # Then: Should eventually succeed after retries
    assert response.status == "success", f"Expected success but got: {response.status}"

    # Should take longer than normal due to retries (at least base_delay * 2)
    # Normal would be ~5s (worker delay), with retries should be ~6-7s
    assert duration > 5.0, f"Expected retries to take time, but took {duration:.2f}s"

    # Then: Master should have the message
    master_messages = await master_client.GetMessages(
        master_messages_pb2.GetMessagesRequest()
    )
    assert len(master_messages.messages) == 1
    assert master_messages.messages[0].content == test_content

    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker1_messages.messages) == 1, (
        "Worker1 should have message after retry"
    )
    assert worker1_messages.messages[0].content == test_content

    # Then: Worker2 should also have the message
    worker2_messages = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker2_messages.messages) == 1
    assert worker2_messages.messages[0].content == test_content


@pytest.mark.integration
async def test_message_delivery_failed_write_2(
    docker_services, master_client, worker1_client, worker2_client
):
    """
    Test that if message delivery fails, master retries and eventually succeeds.
    With w = 2 master waits until he got confirmation from one nodes about success

    Steps:
    1. Inject 2 failures into worker1 (will fail next 2 requests)
    2. Send message with w=2 (requires worker1)
    3. Worker1 fails twice, then succeeds on 3rd attempt
    4. Verify message eventually reaches worker1 after retries
    """

    test_content = "Test retry on failure"
    write_concern = 2

    # Given: Inject 2 failures into worker1
    await worker1_client.InjectFailure(
        worker_messages_pb2.InjectFailureRequest(fail_next_n_requests=2)
    )

    # When: POST message to master
    start_time = time.time()

    response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content=test_content, write_concern=write_concern, client_id="client_1"
        )
    )

    duration = time.time() - start_time

    # Then: Should eventually succeed after retries
    assert response.status == "success", f"Expected success but got: {response.status}"

    # Verify that we got response that master and worker 2 got the message, but for worker 1 we still retry
    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker1_messages.messages) == 0, (
        "Worker1 should have message after retry"
    )

    # Should take longer than normal due to retries (at least base_delay * 2)
    # Normal would be ~5s (worker delay), with retries should be ~6-7s
    assert duration > 5.0, f"Expected retries to take time, but took {duration:.2f}s"

    # Then: Master should have the message
    master_messages = await master_client.GetMessages(
        master_messages_pb2.GetMessagesRequest()
    )
    assert len(master_messages.messages) == 1
    assert master_messages.messages[0].content == test_content

    # Then: Worker2 should have the message straight away
    worker2_messages = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker2_messages.messages) == 1
    assert worker2_messages.messages[0].content == test_content

    await asyncio.sleep(10)
    # wait until message would be delivered to worker 1
    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker1_messages.messages) == 1, (
        "Worker1 should have message after retry"
    )
    assert worker1_messages.messages[0].content == test_content


@pytest.mark.integration
async def test_message_secondary_blocked_waiting_on_quorum(
    docker_master, worker_manager, master_client, worker1_client, worker2_client
):
    """
    Test that if there is no quorum master node would go to read-only and
    With w = 3 master waits until he got confirmation from all nodes about success

    Steps:
    1. Send message with w=3 (requires worker1, worker2)
    2. Worker1 is not started, wait on it
    3. Verify message eventually reaches worker1 after retries
    """
    start_worker, stop_worker = worker_manager

    # Start only worker1 (worker2 remains down)
    start_worker("worker1")

    test_content = "Wait for quorum"
    write_concern = 3

    # When: POST message to master
    start_time = time.time()

    async def send_message():
        return await master_client.PostMessage(
            master_messages_pb2.PostMessageRequest(
                content=test_content, write_concern=write_concern, client_id="client_1"
            )
        )

    blocking_message_task = asyncio.create_task(send_message())
    await asyncio.sleep(15.5)  # wait till node would be in read-only

    follow_up_response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="another one", write_concern=write_concern, client_id="client_1"
        )
    )

    assert follow_up_response.status == "failure"
    assert (
        follow_up_response.message
        == "Service temporarily unavailable for writes (quorum lost)"
    )

    # Now start worker2 to restore quorum
    start_worker("worker2")
    await asyncio.sleep(15.5)

    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker1_messages.messages) == 1
    assert worker1_messages.messages[0].content == test_content

    worker2_messages = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker2_messages.messages) == 1
    assert worker2_messages.messages[0].content == test_content


@pytest.mark.integration
async def test_message_secondary_blocked_waiting_on_quorum_other_client_working(
    docker_master, worker_manager, master_client, worker1_client, worker2_client
):
    """
    Test that if there is no quorum master node would go to read-only but
    other clients can send their messages

    Steps:
    1. Send message with w=3 (requires worker1, worker2) from client 1
    2. Worker1 is not started, wait on it
    3. Client 2 sends message
    """
    start_worker, stop_worker = worker_manager

    # Start only worker1 (worker2 remains down)
    start_worker("worker1")

    test_content = "Wait for quorum"
    write_concern = 3

    async def send_message():
        return await master_client.PostMessage(
            master_messages_pb2.PostMessageRequest(
                content=test_content, write_concern=write_concern, client_id="client_1"
            )
        )

    blocking_message_task = asyncio.create_task(send_message())
    await asyncio.sleep(15.5)  # wait till node would be in read-only

    follow_up_response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="another one", write_concern=write_concern, client_id="client_1"
        )
    )
    # client is blocked - no quorum for previous message
    assert follow_up_response.status == "failure"
    assert (
        follow_up_response.message
        == "Service temporarily unavailable for writes (quorum lost)"
    )

    # Second client is not blocked
    other_client_response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="another one", write_concern=write_concern - 1, client_id="client_2"
        )
    )
    assert other_client_response.status == "success"

    # Then verify messages in first worker - from both clients
    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker1_messages.messages) == 2
