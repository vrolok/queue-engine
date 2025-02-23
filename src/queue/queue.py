# src/queue/queue.py
import logging
from datetime import datetime
from typing import Optional, List
from collections import deque
from threading import Lock

from .models import Task, TaskStatus
from .exceptions import QueueFullError, QueueEmptyError, TaskNotFoundError

logger = logging.getLogger(__name__)


class TaskQueue:
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.queue = deque(maxlen=max_size)
        self.task_map = {}  # For O(1) task lookup
        self.lock = Lock()  # Thread-safe operations
        logger.info(f"Initialized TaskQueue with max size: {max_size}")

    def enqueue(self, task: Task) -> Task:
        """
        Add a task to the queue
        """
        with self.lock:
            if len(self.queue) >= self.max_size:
                logger.error("Queue is full")
                raise QueueFullError("Queue is at maximum capacity")

            try:
                self.queue.append(task.task_id)
                self.task_map[task.task_id] = task
                logger.info(f"Task {task.task_id} enqueued successfully")
                return task
            except Exception as e:
                logger.error(f"Error enqueueing task: {str(e)}")
                raise QueueError(f"Failed to enqueue task: {str(e)}")

    def dequeue(self) -> Optional[Task]:
        """
        Remove and return the next task from the queue
        """
        with self.lock:
            try:
                if not self.queue:
                    raise QueueEmptyError("Queue is empty")

                task_id = self.queue.popleft()
                task = self.task_map.get(task_id)

                if task:
                    task.status = TaskStatus.PROCESSING
                    task.started_at = datetime.utcnow()
                    task.updated_at = datetime.utcnow()
                    logger.info(f"Task {task_id} dequeued successfully")
                    return task
                else:
                    logger.error(f"Task {task_id} not found in task map")
                    raise TaskNotFoundError(f"Task {task_id} not found")

            except QueueEmptyError:
                logger.info("Attempted to dequeue from empty queue")
                raise
            except Exception as e:
                logger.error(f"Error dequeuing task: {str(e)}")
                raise QueueError(f"Failed to dequeue task: {str(e)}")

    def get_task(self, task_id: str) -> Optional[Task]:
        """
        Get task by ID without removing it from the queue
        """
        with self.lock:
            task = self.task_map.get(task_id)
            if not task:
                raise TaskNotFoundError(f"Task {task_id} not found")
            return task

    def update_task_status(
        self, task_id: str, status: TaskStatus, error_message: str = None
    ) -> Task:
        """
        Update the status of a task
        """
        with self.lock:
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

    def get_queue_size(self) -> int:
        """
        Get the current size of the queue
        """
        return len(self.queue)

    def get_all_tasks(self) -> List[Task]:
        """
        Get all tasks in the queue
        """
        return list(self.task_map.values())
