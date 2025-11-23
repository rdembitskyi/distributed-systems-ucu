"""
Integration tests for node sync (Iteration 3)

Tests verify:
"""

import asyncio

import pytest

from api.generated import master_messages_pb2, worker_messages_pb2

from .conftest import pause_worker, return_timeout, unpause_worker


@pytest.mark.integration
async def test_node_sync_after_being_turned_on(
    worker_manager, docker_master, master_client, worker1_client, worker2_client
):
    """
    Test that worker would replicate messages from master after being turned on

    """

    start_worker, stop_worker = worker_manager
    start_worker("worker1")

    async def send_message(message):
        return await master_client.PostMessage(
            master_messages_pb2.PostMessageRequest(
                content=message, write_concern=2, client_id="client_1"
            )
        )

    asyncio.create_task(send_message("test_1"))
    await asyncio.sleep(0.5)  # sleep that test_1 would be first 100%
    asyncio.create_task(send_message("test_2"))

    start_worker("worker2")
    await asyncio.sleep(return_timeout())

    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker1_messages.messages) == 2
    assert worker1_messages.messages[0].content == "test_1"
    assert worker1_messages.messages[1].content == "test_2"
    worker2_messages = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker2_messages.messages) == 2
    assert worker2_messages.messages[0].content == "test_1"
    assert worker2_messages.messages[1].content == "test_2"


@pytest.mark.integration
async def test_node_sync_after_node_sleep(
    worker_manager, docker_master, master_client, worker1_client, worker2_client
):
    """
    Test that worker would replicate messages from master after deactivation and activation again

    """

    start_worker, stop_worker = worker_manager
    start_worker("worker1")
    start_worker("worker2")

    async def send_message(message):
        return await master_client.PostMessage(
            master_messages_pb2.PostMessageRequest(
                content=message, write_concern=2, client_id="client_1"
            )
        )

    await send_message("test_1")
    await send_message("test_2")

    # Stop the second worker
    pause_worker("worker2")

    # Add more messages
    for i in range(3, 11):
        asyncio.create_task(send_message("test_" + str(i)))

    unpause_worker("worker2")
    await asyncio.sleep(return_timeout())

    worker2_messages = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker2_messages.messages) == 10
    assert worker2_messages.messages[0].content == "test_1"
    assert worker2_messages.messages[9].content == "test_10"
