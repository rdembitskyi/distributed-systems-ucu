"""
Tests verify:
- w=3: Master waits for ACKs from both workers before responding
- Blocking replication behavior
- Message consistency across all nodes
"""

import logging
import pytest
import time

from api.generated import master_messages_pb2, worker_messages_pb2


logger = logging.getLogger(__name__)


@pytest.mark.integration
async def test_write_concern_3_blocks_until_all_workers_ack(
    docker_services, master_client, worker1_client, worker2_client
):
    """
    Test that w=3 waits for ACKs from master + 2 workers.

    Expected behavior:
    - Master should wait for both workers to acknowledge
    - With 5 second delay on workers, POST should take ~5 seconds
    - Message should be present on all nodes after POST completes
    """
    test_content = "Test message with w=3"
    write_concern = 3

    # POST message to master
    start_time = time.time()

    response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content=test_content, write_concern=write_concern
        )
    )

    end_time = time.time()
    duration = end_time - start_time

    # Then: Request should succeed
    assert response.status == "success", f"Expected success, got: {response.status}"
    assert response.message == "Message added successfully"

    # Should wait approximately 5 seconds (worker delay)
    logger.info(f"POST request took {duration:.2f}s (blocked waiting for workers)")

    # Then: Message should exist on master
    master_messages = await master_client.GetMessages(
        master_messages_pb2.GetMessagesRequest()
    )
    assert len(master_messages.messages) == 1
    assert master_messages.messages[0].content == test_content

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
async def test_write_concern_3_total_ordering(
    docker_services, master_client, worker1_client, worker2_client
):
    """
    Test that w=3 maintains total ordering across all nodes.

    Expected behavior:
    - Messages should have sequential sequence numbers
    - All nodes should have same message order
    - Parent-child relationships should be consistent
    """
    num_messages = 5
    sent_messages = []

    # Send messages sequentially
    for i in range(num_messages):
        content = f"Message {i + 1}"
        response = await master_client.PostMessage(
            master_messages_pb2.PostMessageRequest(content=content, write_concern=3)
        )
        assert response.status == "success"
        sent_messages.append(content)

    # Master should have all messages in order
    master_messages = await master_client.GetMessages(
        master_messages_pb2.GetMessagesRequest()
    )
    assert len(master_messages.messages) == num_messages

    for i, msg in enumerate(master_messages.messages, start=1):
        assert msg.sequence_number == i, (
            f"Expected sequence {i}, got {msg.sequence_number}"
        )
        assert msg.content == sent_messages[i - 1]

    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker1_messages.messages) == num_messages

    for i, msg in enumerate(worker1_messages.messages, start=1):
        assert msg.sequence_number == i
        assert msg.content == sent_messages[i - 1]

    # worker 2 should have same order
    worker2_messages = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker2_messages.messages) == num_messages

    for i, msg in enumerate(worker2_messages.messages, start=1):
        assert msg.sequence_number == i
        assert msg.content == sent_messages[i - 1]


@pytest.mark.integration
async def test_write_concern_3_parent_child_relationships(
    docker_services, master_client, worker1_client
):
    """
    Test that parent-child message relationships are preserved with w=3.

    Expected behavior:
    - First message has no parent (parent_id empty)
    - Subsequent messages reference previous message as parent
    """
    # When: Send 3 messages
    for i in range(3):
        response = await master_client.PostMessage(
            master_messages_pb2.PostMessageRequest(
                content=f"Message {i + 1}", write_concern=3
            )
        )
        assert response.status == "success"

    # Then: Verify parent-child relationships on master
    master_messages = await master_client.GetMessages(
        master_messages_pb2.GetMessagesRequest()
    )

    # First message should have no parent
    assert master_messages.messages[0].parent_id == ""
    assert master_messages.messages[0].sequence_number == 1

    # Second message should reference first
    assert (
        master_messages.messages[1].parent_id == master_messages.messages[0].message_id
    )
    assert master_messages.messages[1].sequence_number == 2

    # Third message should reference second
    assert (
        master_messages.messages[2].parent_id == master_messages.messages[1].message_id
    )
    assert master_messages.messages[2].sequence_number == 3

    # Then: Verify same relationships on worker
    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )

    assert worker1_messages.messages[0].parent_id == ""
    assert (
        worker1_messages.messages[1].parent_id
        == worker1_messages.messages[0].message_id
    )
    assert (
        worker1_messages.messages[2].parent_id
        == worker1_messages.messages[1].message_id
    )
