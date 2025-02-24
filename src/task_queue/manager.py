# src/task_queue/manager.py

import asyncio
import logging
from datetime import datetime
from typing import Optional, List, Dict
from collections import deque

from .models import Task, TaskStatus
from .exceptions import QueueError, QueueFullError, QueueEmptyError, TaskNotFoundError

logger = logging.getLogger(__name__)


class AsyncTaskQueueManager:
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.queue = asyncio.Queue(maxsize=max_size)
        self.task_map: Dict[str, Task] = {}
        self._lock = asyncio.Lock()
        logger.info(f"Initialized AsyncTaskQueue with max size: {max_size}")

    async def enqueue(self, task: Task) -> Task:
        """Atomically enqueue a task"""
        async with self._lock:
            try:
                # First verify we can add to task_map
                if task.task_id in self.task_map:
                    raise QueueError(f"Task {task.task_id} already exists")

                # Then try to put in queue
                try:
                    await self.queue.put(task.task_id)
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
        """Atomically dequeue a task"""
        async with self._lock:
            try:
                # First verify queue is not empty
                if self.queue.empty():
                    raise QueueEmptyError("Queue is empty")

                # Get task from map first to verify existence
                task_id = await self.queue.get()
                task = self.task_map.get(task_id)

                if not task:
                    logger.error(f"Task {task_id} not found in task map")
                    # Put task_id back in queue if task not found
                    await self.queue.put(task_id)
                    raise TaskNotFoundError(f"Task {task_id} not found")

                # Update task status
                task.status = TaskStatus.PROCESSING
                task.started_at = datetime.utcnow()
                task.updated_at = datetime.utcnow()

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
            task.updated_at = datetime.utcnow()

            if status == TaskStatus.COMPLETED:
                task.completed_at = datetime.utcnow()

            if error_message:
                task.error_message = error_message

            return task

    async def get_queue_size(self) -> int:
        """Get current queue size"""
        return self.queue.qsize()

    async def get_all_tasks(self) -> List[Task]:
        """Get all tasks"""
        async with self._lock:
            return list(self.task_map.values())
