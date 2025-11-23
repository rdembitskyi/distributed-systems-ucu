"""
Integration tests for message deduplication (Iteration 3)

Tests verify:
- Duplicate messages are properly detected and rejected
- Message sequence numbers and ids are used to prevent duplicate processing
- System maintains idempotency when receiving same message multiple times
"""

import pytest
import asyncio
from api.generated import master_messages_pb2, worker_messages_pb2


@pytest.mark.integration
async def test_duplicate_message_rejection(
    docker_services, master_client, worker1_client, worker2_client
):
    """
    Test that duplicate messages are rejected by both master and workers
    """

    async def send_message(message):
        return await master_client.PostMessage(
            master_messages_pb2.PostMessageRequest(
                content=message, write_concern=2, client_id="client_1"
            )
        )

    await send_message(message="message_1")

    # Given: Inject 2 failures into worker1, after save to db, so we would sent retry from master
    await worker1_client.InjectFailure(
        worker_messages_pb2.InjectFailureRequest(
            fail_next_n_requests=2, where="after_db_save"
        )
    )

    # Send more messages
    asyncio.create_task(send_message(message="message_2"))
    asyncio.create_task(send_message(message="message_3"))

    await asyncio.sleep(15.5)

    # verify no duplication
    worker1_messages = await worker1_client.GetMessages(
        worker_messages_pb2.GetMessagesRequest()
    )

    assert len(worker1_messages.messages) == 3
    assert worker1_messages.messages[0].content == "message_1"
    assert worker1_messages.messages[1].content == "message_2"
