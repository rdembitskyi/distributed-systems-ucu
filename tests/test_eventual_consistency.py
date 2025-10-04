"""
Integration tests for eventual consistency (Iteration 2)

Tests verify:
- w=1: Master returns immediately without waiting for workers
- Master has messages immediately
- Workers receive messages eventually (after delay)
- Eventual consistency across all nodes
"""

import pytest
import asyncio
import time
from api.generated import master_messages_pb2, worker_messages_pb2


@pytest.mark.integration
async def test_write_concern_1_immediate_response_eventual_consistency(
    docker_services, master_client, worker1_client, worker2_client
):
    """
    Test that w=1 returns immediately and workers get messages eventually.

    Expected behavior:
    - POST with w=1 returns immediately (< 1 second)
    - Master has message immediately after POST
    - Workers DON'T have message immediately (5 second delay)
    - Workers DO have message after waiting (eventual consistency)
    """

    test_content = "Test w=1 eventual consistency"
    write_concern = 1

    # When: POST message to master
    start_time = time.time()

    response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content=test_content, write_concern=write_concern
        )
    )

    end_time = time.time()
    duration = end_time - start_time

    # Then: Request should succeed immediately (< 1 second)
    assert response.status == "success"
    assert duration < 1.0, (
        f"Expected immediate response with w=1, but took {duration:.2f}s. w=1 should not wait for workers."
    )

    # Then: Master should have the message IMMEDIATELY
    master_messages = await master_client.GetMessages(
        master_messages_pb2.GetMessagesRequest()
    )
    assert len(master_messages.messages) == 1
    assert master_messages.messages[0].content == test_content

    # Then: Workers should NOT have the message yet (due to 5s delay)
    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    worker2_messages = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )

    assert len(worker1_messages.messages) == 0, "Worker1 should NOT have message yet"
    assert len(worker2_messages.messages) == 0, "Worker2 should NOT have message yet"

    # When: Wait for eventual consistency
    await asyncio.sleep(6)

    # Then: Workers should NOW have the message (eventual consistency)
    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    worker2_messages = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )

    assert len(worker1_messages.messages) == 1
    assert worker1_messages.messages[0].content == test_content

    assert len(worker2_messages.messages) == 1
    assert worker2_messages.messages[0].content == test_content


@pytest.mark.integration
async def test_write_concern_1_multiple_messages_eventual_consistency(
    docker_services, master_client, worker1_client
):
    """
    Test that multiple messages with w=1 show eventual consistency.

    Expected behavior:
    - Send 3 messages with w=1 quickly
    - Master has all 3 immediately
    - Workers don't have any initially
    - Workers have all 3 after delay
    """

    # Given: 3 messages to send with w=1
    num_messages = 3
    sent_messages = []

    # When: Send messages quickly with w=1
    start_time = time.time()
    for i in range(num_messages):
        content = f"Message {i + 1}"
        response = await master_client.PostMessage(
            master_messages_pb2.PostMessageRequest(content=content, write_concern=1)
        )
        assert response.status == "success"
        sent_messages.append(content)

    duration = time.time() - start_time

    assert duration < 2.0, f"Expected fast POSTs with w=1, took {duration:.2f}s"

    # master should have all messages immediately
    master_messages = await master_client.GetMessages(
        master_messages_pb2.GetMessagesRequest()
    )
    assert len(master_messages.messages) == num_messages
    for i, msg in enumerate(master_messages.messages):
        assert msg.content == sent_messages[i]

    # Worker should NOT have messages yet
    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker1_messages.messages) == 0

    # Wait for eventual consistency
    await asyncio.sleep(16)

    # Worker should have all messages
    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker1_messages.messages) == num_messages
    for i, msg in enumerate(worker1_messages.messages):
        assert msg.content == sent_messages[i]


@pytest.mark.integration
async def test_write_concern_1_vs_3_timing_difference(docker_services, master_client):
    """
    Compare timing between w=1 (fast) and w=3 (slow).

    Expected behavior:
    - w=1 is fast (< 1 second)
    - w=3 is slow (~5 seconds due to worker delay)
    """
    # Test w=1 (fast)
    start = time.time()
    response1 = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(content="w=1 message", write_concern=1)
    )
    w1_duration = time.time() - start

    assert response1.status == "success"
    assert w1_duration < 1.0
    print(f"✅ w=1 took {w1_duration:.2f}s (fast)")

    # Test w=3 (slow)
    start = time.time()
    response3 = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(content="w=3 message", write_concern=3)
    )
    w3_duration = time.time() - start

    assert response3.status == "success"
    assert 4.0 <= w3_duration <= 10.0
    print(f"✅ w=3 took {w3_duration:.2f}s (blocked on workers)")

    # Then: w=3 should be significantly slower than w=1
    assert w3_duration > w1_duration * 3, (
        f"w=3 ({w3_duration:.2f}s) should be much slower than w=1 ({w1_duration:.2f}s)"
    )
    print(f"✅ w=3 is {w3_duration / w1_duration:.1f}x slower than w=1")
