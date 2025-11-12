import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from shared.domain.worker import Worker, WorkerHealthState
from typing import Dict, List, Callable, Awaitable


logger = logging.getLogger(__name__)


class Thresholds(Enum):
    HEALTH_THRESHOLD = 0.9
    SUSPECTED_THRESHOLD = 0.6
    UNHEALTHY_THRESHOLD = 0.3


@dataclass
class HealthCheckResult:
    """Result of a health check"""

    worker_id: str
    is_healthy: bool
    timestamp: datetime = field(default_factory=datetime.now)
    details: str | None = None


@dataclass
class WorkerHealthInfo:
    """Health information for a worker"""

    worker_id: str
    state: WorkerHealthState = WorkerHealthState.HEALTHY
    health_history: deque = field(default_factory=lambda: deque(maxlen=20))
    last_heartbeat: datetime = field(default_factory=datetime.now)
    last_state_change: datetime = field(default_factory=datetime.now)

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.health_history if r.is_healthy)

    @property
    def success_rate(self) -> float:
        total_checks = len(self.health_history)
        if total_checks == 0:
            return 1.0
        return self.success_count / total_checks


class HeartBeatService:
    """Service to monitor worker health via heartbeats"""

    def __init__(
        self,
        workers: List[Worker],
        health_check_func: Callable[[str], Awaitable[bool]],
        check_interval: int = 10,
    ):
        self.workers: list[Worker] = workers
        self.worker_health: Dict[str, WorkerHealthInfo] = {
            worker.worker_id: WorkerHealthInfo(worker_id=worker.worker_id)
            for worker in self.workers
        }
        self.health_check_func = health_check_func
        self.check_interval = check_interval
        self._task = None
        self._running = False

    def start_monitoring(self):
        """Start monitoring workers"""
        if self._running:
            logger.warning("HeartbeatService already running")
            return

        self._running = True
        self._task = asyncio.create_task(self.monitor_workers())

    async def stop(self):
        """Stop background heartbeat monitoring"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("HeartbeatService stopped")

    async def monitor_workers(self):
        """Monitor all workers' health in a loop"""
        while self._running:
            try:
                # Check all workers concurrently
                tasks = [self._check_worker(worker) for worker in self.workers]
                await asyncio.gather(*tasks, return_exceptions=False)

                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(self.check_interval)

    async def _check_worker(self, worker: Worker):
        try:
            is_alive: bool = await self.health_check_func(worker.worker_id)
            check_result = HealthCheckResult(
                worker_id=worker.worker_id, is_healthy=is_alive
            )
            logger.info(
                f"HealthCheckResult for worker {worker.worker_id}: is_alive = {is_alive}"
            )
        except Exception as e:
            logger.error(f"Error in heartbeat loop: {e}")
            check_result = HealthCheckResult(
                worker_id=worker.worker_id, is_healthy=False, details=str(e)
            )
            logger.info(
                f"HealthCheckResult for worker {worker.worker_id} failed: {check_result.details}"
            )
        self._process_health_check_result(result=check_result)

    def _process_health_check_result(self, result: HealthCheckResult):
        worker_health = self.worker_health.get(result.worker_id)
        if not worker_health:
            logger.error(f"Received health check for unknown worker {result.worker_id}")
            return
        worker_health.health_history.append(result)

        worker_health.last_heartbeat = datetime.now()
        old_state = worker_health.state
        new_state = old_state

        success_rate = worker_health.success_rate

        # State transitions based on success rate
        if worker_health.state == WorkerHealthState.HEALTHY:
            if worker_health.success_rate < Thresholds.HEALTH_THRESHOLD.value:
                new_state = WorkerHealthState.SUSPECTED

        elif worker_health.state == WorkerHealthState.SUSPECTED:
            if worker_health.success_rate < Thresholds.SUSPECTED_THRESHOLD.value:
                new_state = WorkerHealthState.UNHEALTHY
            elif worker_health.success_rate >= Thresholds.HEALTH_THRESHOLD.value:
                new_state = WorkerHealthState.HEALTHY

        elif worker_health.state == WorkerHealthState.UNHEALTHY:
            if worker_health.success_rate >= Thresholds.SUSPECTED_THRESHOLD.value:
                new_state = WorkerHealthState.SUSPECTED
            if worker_health.success_rate >= Thresholds.HEALTH_THRESHOLD.value:
                new_state = WorkerHealthState.HEALTHY

        # Apply state change
        if new_state != old_state:
            logger.warning(
                f"Worker {worker_health.worker_id} state transition: "
                f"{old_state.value} -> {new_state.value} "
                f"(success_rate: {success_rate:.1%}"
            )
            worker_health.state = new_state
            worker_health.last_state_change = datetime.now()

    def get_worker_state(self, worker_id: str) -> WorkerHealthState:
        """Get current health state of a worker"""
        worker_health = self.worker_health.get(worker_id)
        return worker_health.state

    def get_worker_health(self, worker_id: str) -> WorkerHealthInfo | None:
        """Get current health info of a worker"""
        return self.worker_health.get(worker_id)

    def get_healthy_workers(self) -> List[str]:
        """Get list of HEALTHY worker IDs"""
        return [
            wid
            for wid, info in self.worker_health.items()
            if info.state == WorkerHealthState.HEALTHY
        ]

    def get_available_workers(self) -> List[str]:
        """Get HEALTHY + SUSPECTED worker IDs"""
        return [
            wid
            for wid, info in self.worker_health.items()
            if info.state in (WorkerHealthState.HEALTHY, WorkerHealthState.SUSPECTED)
        ]

    def get_unhealthy_workers(self) -> List[str]:
        """Get UNHEALTHY worker IDs"""
        return [
            wid
            for wid, info in self.worker_health.items()
            if info.state == WorkerHealthState.UNHEALTHY
        ]
