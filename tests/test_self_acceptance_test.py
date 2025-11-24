import asyncio
import time

import pytest

from api.generated import master_messages_pb2, worker_messages_pb2
from shared.domain.response import ResponseStatus

from .conftest import return_timeout


@pytest.mark.integration
async def test_self_acceptance_iteration_3(
    worker_manager, docker_master, master_client, worker1_client, worker2_client
):

    # Start M + S1
    start_worker, stop_worker = worker_manager
    start_worker("worker1")

    # send (Msg1, W=1) - Ok
    response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="message 1", write_concern=1, client_id="client_1"
        )
    )
    assert response.status == ResponseStatus.SUCCESS

    # send(Msg2, W=2) - Ok
    response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="message 2", write_concern=2, client_id="client_1"
        )
    )
    assert response.status == ResponseStatus.SUCCESS

    # send(Msg3, W=3) - Wait
    async def send_blocking_message(message):
        return await master_client.PostMessage(
            master_messages_pb2.PostMessageRequest(
                content=message, write_concern=3, client_id="client_1"
            )
        )

    asyncio.create_task(send_blocking_message("message 3"))
    await asyncio.sleep(1)

    # send(Msg4, W=1) - Ok
    response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="message 4", write_concern=1, client_id="client_2"
        )
    )
    assert response.status == ResponseStatus.SUCCESS

    # Start S2
    start_worker("worker2")
    await asyncio.sleep(return_timeout())

    # verify master messages
    master_messages = await master_client.GetMessages(
        master_messages_pb2.GetMessagesRequest()
    )
    assert len(master_messages.messages) == 4
    assert master_messages.messages[3].content == "message 4"

    # verify worker 1 messages
    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker1_messages.messages) == 4
    assert worker1_messages.messages[0].content == "message 1"
    assert worker1_messages.messages[3].content == "message 4"

    # verify worker2 messages
    worker2_messages = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker2_messages.messages) == 4
    assert worker2_messages.messages[0].content == "message 1"
    assert worker2_messages.messages[3].content == "message 4"


@pytest.skip # Set 10 seconds delay for S2 in docker-compose
@pytest.mark.integration
async def test_self_acceptance_iteration_2(
    worker_manager, docker_master, master_client, worker1_client, worker2_client
):
    """Test eventual consistency with artificial delay on worker2"""
    # Start M + S1 + S2 (delay 5-10 sec)
    start_worker, stop_worker = worker_manager
    start_worker("worker1")
    start_worker("worker2")  # Set 10 seconds delay for S2 in docker-compose, otherwise this test would fail.

    # send (Msg1, W=1) - Ok
    response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="message 1", write_concern=1, client_id="client_1"
        )
    )
    assert response.status == ResponseStatus.SUCCESS

    # send (Msg2, W=2) - Ok
    response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="message 2", write_concern=2, client_id="client_1"
        )
    )
    assert response.status == ResponseStatus.SUCCESS

    # send (Msg3, W=3) - Wait
    async def send_blocking_message(message):
        return await master_client.PostMessage(
            master_messages_pb2.PostMessageRequest(
                content=message, write_concern=3, client_id="client_1"
            )
        )

    asyncio.create_task(send_blocking_message("message 3"))
    await asyncio.sleep(0.5) # send this first

    # send (Msg4, W=1) - Ok
    response = await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="message 4", write_concern=1, client_id="client_2"
        )
    )
    assert response.status == ResponseStatus.SUCCESS
    await asyncio.sleep(5.5) # wait till first worker sync up - 5 seconds delay there

    # Check messages on S2 - [Msg1, (Msg2), (Msg3)] - inconsistent due to delay
    worker2_messages = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker2_messages.messages) < 4  # S2 is behind

    # Check messages on M|S1 - [Msg1, Msg2, Msg3, Msg4]
    master_messages = await master_client.GetMessages(
        master_messages_pb2.GetMessagesRequest()
    )
    assert len(master_messages.messages) == 4

    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker1_messages.messages) == 4

    # Wait for eventual consistency
    await asyncio.sleep(return_timeout())

    worker2_messages_final = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker2_messages_final.messages) == 4
