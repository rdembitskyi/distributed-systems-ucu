import asyncio
import os
import sys
import signal
import logging
from master.transport.grpc_transport import GrpcTransport

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/app/logs/master.log"),
    ],
)

logger = logging.getLogger(__name__)


async def main():
    # Get port from environment variable or use default
    port = int(os.getenv("GRPC_PORT", 50052))

    # Create and start the gRPC server
    transport = GrpcTransport()
    server = await transport.start_server(port=port)

    logger.info(f"gRPC server is running on port {port}")
    logger.info("Server ready to accept connections")

    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Keep the server running
        await server.wait_for_termination()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down server...")
        await transport.stop_server()
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
