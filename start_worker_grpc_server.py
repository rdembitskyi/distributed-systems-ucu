"""
gRPC server startup script for Secondary Worker
"""

import asyncio
import argparse
import logging
import signal
import sys
from secondary_worker.transport.grpc_transport import GrpcTransport


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("/app/logs/worker.log")],
)

logger = logging.getLogger(__name__)


class WorkerServer:
    def __init__(self, port: int):
        self.port = port
        self.transport = GrpcTransport()
        self.server = None

    async def start(self):
        """Start the gRPC server"""
        try:
            logger.info(f"Starting Secondary Worker gRPC server on port {self.port}")
            self.server = await self.transport.start_server(self.port)
            logger.info(f"Secondary Worker server started successfully on port {self.port}")

            # Keep the server running
            await self.server.wait_for_termination()

        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            raise

    async def stop(self):
        """Stop the gRPC server"""
        if self.transport:
            await self.transport.stop_server()
            logger.info("Secondary Worker server stopped")


async def main():
    parser = argparse.ArgumentParser(description="Start Secondary Worker gRPC Server")
    parser.add_argument("--port", type=str, default=50053, help="Port to run the gRPC server on (default: 50053)")

    args = parser.parse_args()

    print(1111111)
    print(int(args.port))
    worker_server = WorkerServer(int(args.port))

    # Handle graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(worker_server.stop())
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await worker_server.start()
    except KeyboardInterrupt:
        logger.info("Shutting down due to keyboard interrupt")
        await worker_server.stop()
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
