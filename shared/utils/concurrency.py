import asyncio
import logging


logger = logging.getLogger(__name__)


class QuorumNotReached(Exception):
    pass


async def wait_for_quorum(tasks, required_count: int, timeout: float):
    """Wait until required_count tasks complete successfully"""
    logger.info(f"Replication: Waiting for quorum of {required_count} tasks")
    success_tasks = []
    failure_tasks = []

    for coro in asyncio.as_completed(tasks):
        async with asyncio.timeout(timeout):
            try:
                if required_count == 0:
                    return success_tasks
                result = await coro
                success_tasks.append(result)
                logger.info(f"Replication: Success count: {len(success_tasks)}")

                if len(success_tasks) >= required_count:
                    return success_tasks
            except asyncio.TimeoutError as e:
                logger.error(f"Replication: Task timed out: {e} waiting for replica response")
                failure_tasks.append(e)
            except Exception as e:
                logger.error(f"Replication: Error: {e} received from worker")
                failure_tasks.append(e)
    raise QuorumNotReached(f"Replication: Failed to reach quorum of {required_count} tasks")
