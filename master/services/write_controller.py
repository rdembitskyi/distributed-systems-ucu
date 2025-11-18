import logging
from shared.storage.factory import get_messages_storage


logger = logging.getLogger(__name__)


def manage_write_availability(client_id: str, availability: bool):
    """
    Block writes to the master node until quorum is restored for client.
    """
    storage = get_messages_storage()
    storage.set_node_write_status(client_id=client_id, status=availability)
    if availability:
        logger.info(f"Writing to master node is available")
    else:
        logger.warning(f"Node writes BLOCKED - waiting for quorum of workers")
    return


def get_write_availability(client_id: str):
    storage = get_messages_storage()
    return storage.get_node_write_status(client_id=client_id)
