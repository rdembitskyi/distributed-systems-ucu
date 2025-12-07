import asyncio
from dataclasses import dataclass
import logging


logger = logging.getLogger(__name__)


class RequiredCountNotReached(Exception):
    def __init__(self, message: str, completed_results: list = None):
        super().__init__(message)
        self.completed_results = completed_results or []


@dataclass
class CompletionResult:
    completed_results: list  # Coroutine task results including exceptions
    pending_tasks: list


async def wait_for_required_count(tasks, required_count: int):
    """Wait until required_count tasks complete successfully"""
    logger.info(f"Replication: Waiting for count of {required_count} tasks")
    tasks = [asyncio.create_task(coro) for coro in tasks]

    success_tasks = []
    failure_tasks = []

    if required_count == 0:
        logger.info("Replication: No required count, returning immediately")
        return CompletionResult(completed_results=[], pending_tasks=tasks)

    for coro in asyncio.as_completed(tasks):
        result = await coro
        if bool(result):
            success_tasks.append(result)
        else:
            failure_tasks.append(result)
        logger.info(f"Replication: Success count: {len(success_tasks)}")

        if len(success_tasks) >= required_count:
            pending_tasks = [task for task in tasks if not task.done()]
            return CompletionResult(
                completed_results=success_tasks + failure_tasks,
                pending_tasks=pending_tasks,
            )
    raise RequiredCountNotReached(
        message=f"Replication: Failed to reach count of {required_count} tasks",
        completed_results=success_tasks + failure_tasks,
    )
