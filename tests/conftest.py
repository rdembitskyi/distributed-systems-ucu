"""
Shared pytest fixtures for integration tests
"""

import logging
import subprocess
import time

import grpc
import pytest

from api.generated import master_messages_pb2_grpc, worker_messages_pb2_grpc


logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def docker_services():
    """
    Start Docker services and ensure they're ready.
    Containers run for entire test session.
    """
    logger.info("Starting Docker containers...")

    # Start docker-compose
    subprocess.run(["docker-compose", "up", "--build", "-d"], check=True, cwd=".")

    # Wait for services to be ready
    logger.info("Waiting for master service on port 50052...")
    wait_for_service("localhost", 50052, timeout=60)
    logger.info("Master service ready")

    logger.info("Waiting for worker1 service on port 50053...")
    wait_for_service("localhost", 50053, timeout=60)
    logger.info("Worker1 service ready")

    logger.info("Waiting for worker2 service on port 50054...")
    wait_for_service("localhost", 50054, timeout=60)
    logger.info("Worker2 service ready")

    logger.info("All grpc services ready!\n")

    yield

    logger.info("Shutting down Docker containers...")
    subprocess.run(["docker-compose", "down"], check=True, cwd=".")


@pytest.fixture(scope="function")
def docker_master():
    logger.info("Starting master containers...")
    master = "master"
    subprocess.run(
        ["docker-compose", "up", "--build", "-d", master], check=True, cwd="."
    )
    time.sleep(3)
    wait_for_service("localhost", 50052, timeout=60)
    logger.info("Master service ready")
    yield

    logger.info("Shutting down master containers...")
    subprocess.run(["docker-compose", "down", master], check=True, cwd=".")


def start_worker(worker_id: str):
    """Utility function to start a worker container"""
    worker_port_dict = {
        "worker1": 50053,
        "worker2": 50054,
    }
    if worker_id not in worker_port_dict:
        raise ValueError(f"Worker {worker_id} not exist.")

    logger.info(f"Starting worker {worker_id}...")
    subprocess.run(
        ["docker-compose", "up", "--build", "-d", worker_id], check=True, cwd="."
    )
    logger.info(f"Waiting for worker service {worker_id}...")
    wait_for_service("localhost", worker_port_dict[worker_id], timeout=60)
    logger.info(f"Worker {worker_id} is ready")


def stop_worker(worker_id: str):
    """Utility function to stop a worker container"""
    logger.info(f"Shutting down worker {worker_id} container...")
    subprocess.run(["docker-compose", "stop", worker_id], check=False, cwd=".")


def pause_worker(worker_id: str):
    """Utility function to pause a worker container (keeps RAM/state)"""
    logger.info(f"Pausing worker {worker_id} container...")
    subprocess.run(
        ["docker", "pause", f"distributes-systems-{worker_id}-1"], check=True, cwd="."
    )
    logger.info(f"Worker {worker_id} paused")


def unpause_worker(worker_id: str):
    """Utility function to unpause a worker container"""
    logger.info(f"Unpausing worker {worker_id} container...")
    subprocess.run(
        ["docker", "unpause", f"distributes-systems-{worker_id}-1"], check=True, cwd="."
    )
    logger.info(f"Worker {worker_id} unpaused")


@pytest.fixture(scope="function")
def worker_manager():
    """Fixture that tracks started workers and cleans them up"""
    started_workers = []

    def _start_worker(worker_id: str):
        start_worker(worker_id)
        started_workers.append(worker_id)

    def _stop_worker(worker_id: str):
        stop_worker(worker_id)
        if worker_id in started_workers:
            started_workers.remove(worker_id)

    # Return both functions as a tuple
    yield (_start_worker, _stop_worker)

    # Cleanup: stop all workers that were started
    logger.info(f"Cleaning up started workers: {started_workers}")
    for worker_id in started_workers:
        stop_worker(worker_id)


def wait_for_service(host: str, port: int, timeout: int = 60):
    """Wait for a service to be ready"""
    start = time.time()
    while time.time() - start < timeout:
        if check_grpc_ready(host, port):
            logger.info(
                f"Service {host}:{port} accepting connections, waiting for stabilization..."
            )
            time.sleep(3)  # Extra buffer to ensure internal services are ready
            return True
        time.sleep(2)
    raise TimeoutError(f"Service {host}:{port} not ready after {timeout}s")


def check_grpc_ready(host: str, port: int) -> bool:
    """
    Check if a gRPC service is accepting connections.

    Returns:
        True if service is ready, False otherwise
    """
    try:
        channel = grpc.insecure_channel(
            f"{host}:{port}", options=[("grpc.enable_http_proxy", 0)]
        )

        # Check if channel is ready (with 2 second timeout)
        grpc.channel_ready_future(channel).result(timeout=2)

        channel.close()
        return True
    except Exception as e:
        print(f"   Service {host}:{port} not ready yet: {type(e).__name__}")
        return False


@pytest.fixture
async def master_client():
    """Async gRPC client for master node"""
    async with grpc.aio.insecure_channel("localhost:50052") as channel:
        yield master_messages_pb2_grpc.MessageServiceStub(channel)


@pytest.fixture
async def worker1_client():
    """Async gRPC client for worker1 node"""

    async with grpc.aio.insecure_channel("localhost:50053") as channel:
        yield worker_messages_pb2_grpc.SecondaryWorkerServiceStub(channel)


@pytest.fixture
async def worker2_client():
    """Async gRPC client for worker2 node"""

    async with grpc.aio.insecure_channel("localhost:50054") as channel:
        yield worker_messages_pb2_grpc.SecondaryWorkerServiceStub(channel)


def return_timeout():
    return 15.5
