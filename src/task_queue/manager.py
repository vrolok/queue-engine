# src/task_queue/manager.py

import asyncio
import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Optional, List, Dict

from .models import Task, TaskStatus, FailureReason, DeadLetterEntry
from .exceptions import QueueError, QueueFullError, QueueEmptyError, TaskNotFoundError

logger = logging.getLogger(__name__)


class AsyncTaskQueueManager:
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.queue = asyncio.Queue(maxsize=max_size)
        self.dlq = asyncio.Queue(maxsize=max_size)
        self.task_map: Dict[str, Task] = {}
        self.dlq_map: Dict[str, DeadLetterEntry] = {}
        self._lock = asyncio.Lock()
        logger.info(f"Initialized AsyncTaskQueue with max size: {max_size}")

    async def enqueue(self, task: Task) -> Task:
        async with self._lock:
            try:
                # First verify we can add to task_map
                if task.task_id in self.task_map:
                    raise QueueError(f"Task {task.task_id} already exists")

                # Then try to put in queue
                try:
                    # await self.queue.put(task.task_id)
                    self.queue.put_nowait(task.task_id)
                except asyncio.QueueFull:
                    raise QueueFullError("Queue is at maximum capacity")

                # If successful, add to task map
                self.task_map[task.task_id] = task
                logger.info(f"Task {task.task_id} enqueued successfully")
                return task

            except Exception as e:
                logger.error(f"Error enqueueing task: {str(e)}")
                # Cleanup if needed
                if task.task_id in self.task_map:
                    del self.task_map[task.task_id]
                raise

    async def dequeue(self) -> Optional[Task]:
        async with self._lock:
            try:
                # First verify queue is not empty
                if self.queue.empty():
                    logger.debug("Queue is empty")
                    return None

                task_id = await self.queue.get()
                task = self.task_map.get(task_id)

                if not task:
                    # Clean up invalid queue entry instead of re-queuing
                    logger.error(f"Task {task_id} not found in task map - cleaning up")
                    return None

                # Update task status
                task.status = TaskStatus.PROCESSING
                task.started_at = datetime.now(timezone.utc)
                task.updated_at = datetime.now(timezone.utc)

                logger.info(f"Task {task_id} dequeued successfully")
                return task

            except QueueEmptyError:
                logger.info("Attempted to dequeue from empty queue")
                raise
            except Exception as e:
                logger.error(f"Error dequeuing task: {str(e)}")
                raise QueueError(f"Failed to dequeue task: {str(e)}")

    async def get_task(self, task_id: str) -> Optional[Task]:
        """Get task by ID"""
        async with self._lock:
            task = self.task_map.get(task_id)
            if not task:
                raise TaskNotFoundError(f"Task {task_id} not found")
            return task

    async def update_task_status(
        self, task_id: str, status: TaskStatus, error_message: str = None
    ) -> Task:
        """Update task status atomically"""
        async with self._lock:
            task = self.task_map.get(task_id)
            if not task:
                raise TaskNotFoundError(f"Task {task_id} not found")

            task.status = status
            task.updated_at = datetime.now(timezone.utc)

            if status == TaskStatus.COMPLETED:
                task.completed_at = datetime.now(timezone.utc)

            if error_message:
                task.error_message = error_message

            return task

    async def get_queue_size(self) -> int:
        """Get current queue size"""
        try:
            return self.queue.qsize()
        except Exception as e:
            logger.error(f"Error getting queue size: {str(e)}")
            return 0  # Return 0 as a safe fallback

    async def get_all_tasks(self) -> List[Task]:
        """Get all tasks"""
        async with self._lock:
            return deepcopy(list(self.task_map.values()))

    async def move_to_dlq(
        self,
        task: Task,
        failure_reason: FailureReason,
        error_message: str,
        error_stack: Optional[str] = None,
    ) -> DeadLetterEntry:
        """Move a failed task to the Dead Letter Queue."""
        async with self._lock:
            try:
                dlq_entry = DeadLetterEntry(
                    task_id=task.task_id,
                    original_task=task.dict(),
                    failure_reason=failure_reason,
                    error_message=error_message,
                    retry_count=task.retry_count,
                    last_error_stack=error_stack,
                )

                await self.dlq.put(task.task_id)
                self.dlq_map[task.task_id] = dlq_entry

                # Remove from original queue if present
                if task.task_id in self.task_map:
                    del self.task_map[task.task_id]

                logger.warning(
                    f"Task {task.task_id} moved to DLQ. "
                    f"Reason: {failure_reason}, Error: {error_message}"
                )
                return dlq_entry

            except Exception as e:
                logger.error(f"Error moving task to DLQ: {str(e)}")
                raise

    async def get_dlq_tasks(self) -> List[DeadLetterEntry]:
        """Get all tasks in the Dead Letter Queue."""
        async with self._lock:
            return list(self.dlq_map.values())

    async def retry_dlq_task(self, task_id: str) -> Optional[Task]:
        """Retry a task from the Dead Letter Queue."""
        async with self._lock:
            if task_id not in self.dlq_map:
                raise TaskNotFoundError(f"Task {task_id} not found in DLQ")

            dlq_entry = self.dlq_map[task_id]
            original_task = Task(**dlq_entry.original_task)
            original_task.retry_count = 0
            original_task.status = TaskStatus.PENDING

            # Remove from DLQ
            del self.dlq_map[task_id]

            # Re-queue the task
            await self.enqueue(original_task)

            logger.info(f"Task {task_id} retried from DLQ")
            return original_task
