# Distributed Systems - gRPC Replication

A distributed message replication system built with Python and gRPC, featuring a master node that coordinates message replication across multiple worker nodes.

## Architecture

- **Master Node**: Coordinates message storage and replication across workers
- **Worker Nodes**: Secondary replicas that receive and validate messages
- **gRPC Transport**: Secure communication between master and workers
- **Message Validation**: Cryptographic signatures ensure message integrity

## Components

```
├── master/                    # Master node implementation
│   ├── services/             # Core business logic for replication
│   ├── transport/            # gRPC transport layer
├── secondary_worker/         # Worker node implementation
│   ├── domain/               # Domain for Messages and Validation
│   ├── services/             # Logic for validating message integrity and order
│   └── transport/            # gRPC transport layer
├── shared/                   # Shared domain models and utilities
│   ├── domain/              # Status codes, messages, replication logic
│   ├── services/            # Utilities for creating messages/  
│   ├── security/            # Authentication and message signing
│   └── storage/             # Message storage interfaces
└── api/                     # API definitions and generated code
```

## Quick Start

### Using Docker Compose (Recommended)

1. **Start the entire system:**
   ```bash
   docker-compose up --build
   ```

   This starts:
   - Master node on port `50052`
   - Worker1 on port `50053`
   - Worker2 on port `50054`

2. **Test the system:**
   ```bash
   python test_grpc_client.py
   ```

### Manual Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start master node:**
   ```bash
   python start_grpc_server.py
   ```

3**Start worker nodes:**
   ```bash
   # Terminal 2
   PORT=50053 MACHINE_ID=1234 python start_worker_grpc_server.py

   # Terminal 3
   PORT=50054 MACHINE_ID=5678 python start_worker_grpc_server.py
   ```

## Testing

### Basic Functionality Test

```bash
# Test basic message posting and retrieval
python tests/test_grpc_client.py
```
### Advanced testing

```bash
run tests with pytest
```

It contain test cases for:
- Message Replication
- Eventual Consistency
- Testing for different write concern values
- Master Worker Synchronization


### Manual Testing Scenarios

1. **Message Replication:**
   - Send messages via `test_grpc_client.py`
   - Check logs in `./logs/` directory
   - Verify workers receive and validate messages

## Configuration

### Environment Variables

- `PORT`: Service port (default: 50052 for master, 50053+ for workers)
- `AUTH_TOKEN`: Authentication token for gRPC calls
- `MACHINE_ID`: Unique identifier for worker nodes
- `FERNET_KEY`: Encryption key for message signing

### Status Codes

The system uses standardized status codes from `shared/domain/status_codes.py`:

- `200 OK`: Successful operation
- `400 BAD_REQUEST`: Invalid request format
- `401 UNAUTHORIZED`: Authentication failed
- `409 DUPLICATE_RECEIVED`: Message already processed
- `429 RATE_LIMITED`: Too many requests
- `500 INTERNAL_SERVER_ERROR`: Server error

## Monitoring

### Logs

Check service logs:
```bash
# View all logs
docker-compose logs -f

# Specific service logs
docker-compose logs -f master
docker-compose logs -f worker1
```

## Known Limitations

- Uses hardcoded encryption keys (for development only)
- No persistent storage (messages stored in-memory)
- Limited error recovery mechanisms
