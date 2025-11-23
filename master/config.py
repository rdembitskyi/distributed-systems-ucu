import os
from typing import List

from shared.domain.worker import Worker


def get_workers_registry() -> List[Worker]:
    """Build and return the worker registry from environment config"""
    worker_1_address = os.getenv("WORKER_1_ADDRESS", "worker1")
    worker_2_address = os.getenv("WORKER_2_ADDRESS", "worker2")
    worker_1_port = int(os.getenv("WORKER_1_PORT", "50053"))
    worker_2_port = int(os.getenv("WORKER_2_PORT", "50054"))

    return [
        Worker(
            worker_id="worker1",
            address=worker_1_address,
            port=worker_1_port,
        ),
        Worker(
            worker_id="worker2",
            address=worker_2_address,
            port=worker_2_port,
        ),
    ]
