import asyncio

import pytest

from api.generated import master_messages_pb2, worker_messages_pb2

from .conftest import return_timeout


@pytest.mark.integration
async def test_total_order(
    docker_services, master_client, worker1_client, worker2_client
):

    # send Msg 1 - Ok
    await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="message 1", write_concern=3, client_id="client_1"
        )
    )
    # send Msg 2 - Ok
    await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="message 2", write_concern=3, client_id="client_1"
        )
    )

    # Inject failure to worker 2
    await worker2_client.InjectFailure(
        worker_messages_pb2.InjectFailureRequest(
            fail_next_n_requests=2, where="before_db_save"
        )
    )

    # SEND WITH write_concern=2, DO NOT WAIT ON WORKER 2
    await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="message 3", write_concern=2, client_id="client_1"
        )
    )

    await master_client.PostMessage(
        master_messages_pb2.PostMessageRequest(
            content="message 4", write_concern=3, client_id="client_1"
        )
    )

    # verify worker2 messages
    worker2_messages = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker2_messages.messages) == 2
    assert worker2_messages.messages[1].content == "message 2"

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

    await asyncio.sleep(return_timeout())  # Eventually synced
    worker2_messages = await worker2_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )
    assert len(worker2_messages.messages) == 4
