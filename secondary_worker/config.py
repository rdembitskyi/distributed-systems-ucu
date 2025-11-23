import os

from grpc import aio

from api.generated import master_messages_pb2_grpc


def get_master_address() -> str:
    """Get master address from environment or default"""
    return os.getenv("MASTER_ADDRESS", "master")


def get_master_port() -> int:
    """Get master port from environment or default"""
    return int(os.getenv("MASTER_PORT", "50052"))


def get_master_client() -> master_messages_pb2_grpc.MessageServiceStub:
    """Initialize and return gRPC client for master communication"""
    master_address = get_master_address()
    master_port = get_master_port()
    channel = aio.insecure_channel(f"{master_address}:{master_port}")
    return master_messages_pb2_grpc.MessageServiceStub(channel)
