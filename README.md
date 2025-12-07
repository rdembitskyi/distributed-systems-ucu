# Distributed Systems - gRPC Replication

A distributed message replication system built with Python and gRPC, featuring a master node that coordinates message replication across multiple worker nodes with advanced fault tolerance, retry mechanisms, and consistency guarantees.

## Architecture

- **Master Node**: Coordinates message storage and replication across workers with required count blocking
- **Worker Nodes**: Secondary replicas that receive, validate, and synchronize messages
- **gRPC Transport**: Secure communication between master and workers
- **Message Validation**: Cryptographic signatures ensure message integrity
- **Health Monitoring**: Three-state heartbeat mechanism (Healthy → Suspected → Unhealthy)
- **Smart Retry Logic**: Health-based retry policies with exponential backoff and background task processing
- **Worker Recovery**: Secondaries receive recovery notifications and request missing messages from master
- **Total Order Guarantee**: Gap handling ensures consistent message ordering across all nodes

## Components

```
├── master/                    # Master node implementation
│   ├── domain/               # Master-specific domain models
│   ├── services/
│   │   ├── client_state_manager.py # Client blocking when write concern was not met
│   │   ├── heartbeat.py      # Health monitoring with 3-state machine
│   │   └── replication_coordinator.py # Replication response handling
│   │   ├── retry_policy.py   # Health-based retry policies
│   │   ├── workers.py        # Worker management, replication, retry logic
│   │   ├── write_controller.py # Quorum enforcement and read-only mode
│   ├── transport/            # gRPC transport layer
│   └── config.py             # Worker registry configuration
├── secondary_worker/         # Worker node implementation
│   ├── domain/               # Worker-specific domain models (messages, validation results)
│   ├── services/
│   │   ├── sync_service.py   # CatchUp mechanism for missed messages
│   │   └── replica_message_validation/ # Message validation and deduplication
│   ├── transport/            # gRPC transport layer with ReportHealth endpoint
│   └── config.py             # Master client configuration
├── shared/                   # Shared domain models and utilities
│   ├── domain/
│   │   ├── messages.py       # Message models with status (DELIVERED, MISSING_PARENT)
│   │   ├── response.py       # Status enums (ResponseStatus, HealthStatus, SyncStatus)
│   │   ├── status_codes.py   # Standardized status codes
│   │   └── worker.py         # Worker domain model
│   ├── services/             # Utilities for creating and signing messages
│   ├── security/
│   │   ├── auth.py           # Authentication token validation
│   │   └── message_crypto.py # Cryptographic message signing
│   ├── storage/
│   │   ├── in_memory_message_store.py # Message storage with gap handling
│   │   └── interface.py      # Storage interface
│   └── utils/
│       ├── concurrency.py    # Required count utilities and async helpers
│       └── singleton.py      # Singleton pattern for storage
├── tests/                    # Comprehensive test suite
│   ├── test_blocking_write_concern.py # Write concern w=1, w=2, w=3
│   ├── test_eventual_consistency.py   # Async replication
│   ├── test_retries.py       # Health-based retry mechanism
│   ├── test_deduplication.py # Duplicate message detection
│   ├── test_node_sync.py     # Worker recovery and sync
│   ├── test_total_order.py   # Gap handling and message ordering
│   └── test_self_acceptance_test.py # End-to-end scenario
└── api/                      # gRPC API definitions
    ├── proto/
    │   ├── master_messages.proto  # PostMessage, GetMessages, CatchUp RPCs
    │   └── worker_messages.proto  # ReplicateMessage, ReportHealth, HandleRecovery RPCs
    └── generated/            # Auto-generated protobuf code
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

## Core Features

### 1. Write Concerns & Replication

- **w=1**: Asynchronous replication - client receives immediate response after master stores the message
- **w=2**: Client blocks until message is replicated to master + 1 worker
- **w=3**: Client blocks until message is replicated to master + 2 workers (full quorum)
- **Non-blocking parallel clients**: Multiple clients can operate in parallel without blocking each other
- **Write Concern enforcement**: If count cannot be reached (w>available nodes), master switches to read-only mode for this client

**Quorum-Based Availability Check**: Before accepting any write requests, the master verifies that the system meets the configured quorum level. This prevents writes when insufficient nodes are available to maintain data consistency guarantees:

```python
if not self.workers_service.is_quorum_met():
    logger.info("Service temporarily unavailable for writes (quorum lost)")
    return PostMessageResponse(
        status=ResponseStatus.ERROR,
        message="Service temporarily unavailable for writes (quorum lost)",
        total_messages=len(self.store.get_messages()),
    )
```

**Configurable Quorum Levels** (via `QUORUM_LEVEL` environment variable):
- **1** (ONE): Master only - system accepts writes with just the master node (for testing/development)
- **2** (MAJORITY): At least 2 nodes (master + 1 worker) must be available
- **3** (ALL): All nodes (master + 2 workers) must be available for write operations

The quorum check is a system-level availability guard, while write concern (w=1, w=2, w=3) controls per-request replication acknowledgments.

**Implementation**: `master/services/workers.py:68-174`, `master/transport/controller.py:43-49`

### 2. Smart Retry Mechanism

**Retry Logic**:
- **Blocking phase**: Master retries failed replications until required count is reached
- **Background phase**: Once count is achieved, remaining failed workers are retried in background tasks
- **Health-based policies**:
  - **HEALTHY nodes**: 5 quick retry attempts (transient failures)
  - **SUSPECTED nodes**: 15 retry attempts with exponential backoff
  - **UNHEALTHY nodes**: 0 retries (delegates to sync mechanism)

**Retry delays**: Exponential backoff with jitter to prevent spamming

**Implementation**: `master/services/retry_policy.py`, `master/services/workers.py:250-319`

### 3. Heartbeat & Health Monitoring

**Three-state health machine**:
1. **HEALTHY**: Node responding normally
2. **SUSPECTED**: Consecutive failures detected, increased retry attempts
3. **UNHEALTHY**: Node confirmed down, relies on recovery mechanism

**Health checks via gRPC**: `ReportHealth` RPC endpoint on workers

**Implementation**: `master/services/heartbeat.py`

### 4. Worker Recovery & Synchronization

**Recovery flow when a worker rejoins**:
1. Master detects worker recovery via heartbeat
2. Master sends `RecoveryNotification` with its latest sequence number
3. Worker compares with its last sequence number
4. Worker requests missing messages via `CatchUp` RPC
5. Master sends all missing messages since worker's last sequence
6. Worker validates and stores messages, filling gaps

**Implementation**:
- Recovery notification: `master/services/workers.py:357-373`
- Sync service: `secondary_worker/services/sync_service.py`

### 5. Message Deduplication

- **Sequence-based validation**: Workers track received sequence numbers
- **Duplicate detection**: Messages with already-seen message_id are rejected
- **Testing**: Inject internal server errors after DB save to trigger retries and verify deduplication

**Implementation**: `secondary_worker/services/replica_message_validation/replica_validation.py`

### 6. Total Order Guarantee

**Gap handling mechanism**:
- When worker receives message with missing parent (e.g., receives msg4 before msg3):
  - Message stored with `MISSING_PARENT` status
  - Message NOT returned in `GetMessages()` until parent arrives
  - When parent arrives, orphaned children are promoted to `DELIVERED` status
  - Ensures all nodes show messages in same order

**Example**: If worker has [msg1, msg2, msg4], it only displays [msg1, msg2] until msg3 arrives

**Implementation**: `shared/storage/in_memory_message_store.py:40-129`

### 7. Timeout Configuration

- **Replication timeout**: Configurable timeout for worker responses (default: from `REPLICATION_TIMEOUT` env var)
- **Sync timeout**: Configurable timeout for catch-up operations (default: from `SYNC_TIMEOUT` constant)

**Implementation**: `master/services/workers.py:33`, `shared/domain/constants.py`

## Testing

### Comprehensive Test Suite

```bash
pytest tests/
```

**Test Coverage**:
- ✅ **Write Concerns** (`test_blocking_write_concern.py`): w=1, w=2, w=3 blocking behavior
- ✅ **Eventual Consistency** (`test_eventual_consistency.py`): Asynchronous replication with w=1
- ✅ **Retry Mechanism** (`test_retries.py`): Health-based retry policies and background tasks
- ✅ **Deduplication** (`test_deduplication.py`): Duplicate message detection with injected failures
- ✅ **Node Synchronization** (`test_node_sync.py`): Worker recovery and catch-up mechanism
- ✅ **Total Order** (`test_total_order.py`): Gap handling and message ordering across nodes
- ✅ **Self-Acceptance** (`test_self_acceptance_test.py`): End-to-end scenario with worker recovery

### Self-Check Acceptance Test

**Scenario** (from `test_self_acceptance_test.py`):
```
1. Start Master + Worker1
2. Send (Msg1, w=1) → OK (immediate response)
3. Send (Msg2, w=2) → OK (blocked until Worker1 confirms)
4. Send (Msg3, w=3) → WAITING (no count, only 2 nodes available)
5. Send (Msg4, w=1) → OK (immediate response)
6. Start Worker2 → Recovery triggered
7. Check Worker2 messages → [Msg1, Msg2, Msg3, Msg4] (all synced in order)
```


**Injected failures**:
```python
# Simulate internal server error AFTER message is saved to DB
await worker_client.InjectFailure(
    fail_next_n_requests=1,
    where="after_db_save"
)
```

### Manual Testing

```bash
# Test basic message posting and retrieval
python tests/test_grpc_client.py

# Check logs to verify replication flow
docker-compose logs -f master
docker-compose logs -f worker1
docker-compose logs -f worker2
```

## Configuration

### Environment Variables

- `PORT`: Service port (default: 50052 for master, 50053+ for workers)
- `ADDRESS`: Master for master, worker1 and worker2 for replica nodes
- `AUTH_TOKEN`: Authentication token for gRPC calls
- `MACHINE_ID`: Unique identifier for worker nodes
- `FERNET_KEY`: Encryption key for message signing
- `QUORUM_LEVEL`: Minimum nodes required for system availability (1=ONE, 2=MAJORITY, 3=ALL; default: 1)
- `REPLICATION_TIMEOUT`: Timeout in seconds for replication operations (default: 5.5)

### Status Codes

The system uses standardized status codes from `shared/domain/status_codes.py`:

- `200 OK`: Successful operation
- `400 BAD_REQUEST`: Invalid request format
- `401 UNAUTHORIZED`: Authentication failed
- `409 DUPLICATE_RECEIVED`: Message already processed
- `429 RATE_LIMITED`: Too many requests
- `500 INTERNAL_SERVER_ERROR`: Server error


### Logs

Check service logs:
```bash
# View all logs
docker-compose logs -f

# Specific service logs
docker-compose logs -f master
docker-compose logs -f worker1
```

## Implementation Highlights

### Async Task-Based Retry Architecture

The retry mechanism uses a two-phase approach:

1. **Blocking Phase**: When required write concern count is not reached, master blocks the client and retries failed workers until threshold is met
2. **Background Phase**: Once write concern is achieved, client receives success response while remaining failed workers are retried in background async tasks

This ensures:
- ✅ Clients get fast responses once write concern is reached
- ✅ Eventual consistency for all workers without blocking clients
- ✅ Parallel clients never block each other
- ✅ Strong references to background tasks prevent garbage collection (bounded deque with max 1000 tasks)

**Code**: `master/services/workers.py:306-331`

### Worker Recovery Protocol

When a worker comes back online after being down:

1. **Heartbeat Detection**: Master heartbeat service detects worker is healthy again
2. **Recovery Notification**: Master sends `RecoveryNotification` with current sequence number
3. **Gap Detection**: Worker compares master's sequence with its own last sequence
4. **Catch-Up Request**: Worker requests all missing messages via `CatchUp` RPC
5. **Batch Sync**: Master sends all missing messages in single response
6. **Validation & Storage**: Worker validates each message and fills gaps

This ensures workers automatically catch up on missed messages without manual intervention.

**Code**: `master/services/workers.py:357-373`, `secondary_worker/services/sync_service.py:27-115`

## Known Limitations

- Uses hardcoded encryption keys (for development only)
- No persistent storage (messages stored in-memory)
