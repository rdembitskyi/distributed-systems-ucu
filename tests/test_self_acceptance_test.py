import asyncio

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
