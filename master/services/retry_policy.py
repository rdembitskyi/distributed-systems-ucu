from dataclasses import dataclass
import random

from shared.domain.worker import WorkerHealthState


@dataclass
class RetryPolicy:
    """
    Defines retry behavior based on worker health.

    Retry Strategy:
    - HEALTHY: 5 quick retries for transient failures (network blips, brief overload)
    - SUSPECTED: 15 retries with backoff for degraded but potentially alive nodes
    - UNHEALTHY: 0 retries - delegate to sync mechanism for efficiency

    Why not infinite retries on UNHEALTHY nodes?
    Retrying dead nodes wastes resources. Instead, we use the sync mechanism:
    1. Heartbeat detects recovery (heartbeat.py:121-123)
    2. Triggers recovery callback (workers.py:349-365)
    3. Worker gets ALL missed messages via sync (sync_service.py:37-111)

    This provides eventual consistency guarantees without wasting resources.
    The system never gives up on message delivery - it just switches mechanisms.
    """

    max_attempts: int
    base_delay: float  # seconds
    max_delay: float
    backoff_multiplier: float = 2.0

    @staticmethod
    def for_health_state(state: WorkerHealthState) -> "RetryPolicy":
        """Get retry policy based on worker health state"""
        policies = {
            WorkerHealthState.HEALTHY: RetryPolicy(
                max_attempts=5, base_delay=0.5, max_delay=15.0
            ),
            WorkerHealthState.SUSPECTED: RetryPolicy(
                max_attempts=10, base_delay=2.0, max_delay=30.0
            ),
            WorkerHealthState.UNHEALTHY: RetryPolicy(
                max_attempts=0,  # Sync mechanism handles recovery (see class docstring)
                base_delay=30.0,
                max_delay=180.0,
            ),
        }
        return policies[state]

    def calculate_delay(self, attempt: int) -> float:
        # Calculate exponential backoff
        exponential_delay = self.base_delay * (self.backoff_multiplier**attempt)

        # Cap at max_delay
        capped_delay = min(exponential_delay, self.max_delay)

        # Apply equal jitter: 50% base + 50% random
        half_delay = capped_delay / 2
        jittered_delay = half_delay + random.uniform(0, half_delay)

        return jittered_delay
