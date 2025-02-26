# src/task_queue/service.py

import logging
from typing import Optional, List
from .manager import AsyncTaskQueueManager
from .models import Task, TaskStatus, FailureReason, DeadLetterEntry


logger = logging.getLogger(__name__)

class QueueService:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(QueueService, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # Initialize only once
        if not self._initialized:
            self.queue = AsyncTaskQueueManager()
            self._initialized = True

    async def enqueue_task(self, task: Task) -> Task:
        return await self.queue.enqueue(task)

    async def dequeue_task(self) -> Optional[Task]:
        return await self.queue.dequeue()

    async def get_task(self, task_id: str) -> Optional[Task]:
        return await self.queue.get_task(task_id)

    async def update_task_status(
        self, task_id: str, status: TaskStatus, error_message: str = None
    ) -> Task:
        return await self.queue.update_task_status(task_id, status, error_message)

    async def get_queue_size(self) -> int:
        return await self.queue.get_queue_size()

    async def get_all_tasks(self) -> List[Task]:
        return await self.queue.get_all_tasks()

    async def move_to_dlq(
        self,
        task: Task,
        failure_reason: FailureReason,
        error_message: str,
        stack_trace: Optional[str] = None
    ) -> DeadLetterEntry:
        try:
            return await self.queue.move_to_dlq(
                task,
                failure_reason,
                error_message,
                stack_trace
            )
        except Exception as e:
            logger.error(f"Failed to move task {task.task_id} to DLQ: {str(e)}")
            raise

    async def get_dlq_tasks(self) -> List[DeadLetterEntry]:
        return await self.queue.get_dlq_tasks()

    async def retry_dlq_task(self, task_id: str) -> Optional[Task]:
        return await self.queue.retry_dlq_task(task_id)