import logging
from shared.utils.singleton import singleton


logger = logging.getLogger(__name__)


@singleton
class ClientStateManager:
    def __init__(self):
        self._client_states = {}

    def set_master_write_status(self, client_id: str, status: bool):
        self._client_states[client_id] = status

    def get_master_write_status(self, client_id: str) -> bool:
        # default is true, if status was not updated it means node is active for connections
        logger.info(f"here {self._client_states.get(client_id, True)}")
        return self._client_states.get(client_id, True)
