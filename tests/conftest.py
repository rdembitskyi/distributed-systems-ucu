"""
Shared pytest fixtures for integration tests
"""

import logging
import pytest
import grpc
import subprocess
import time
from api.generated import master_messages_pb2_grpc
from api.generated import worker_messages_pb2_grpc


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


def wait_for_service(host: str, port: int, timeout: int = 60):
    """Wait for a service to be ready"""
    start = time.time()
    while time.time() - start < timeout:
        if check_grpc_ready(host, port):
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
