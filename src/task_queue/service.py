# src/task_queue/service.py

from typing import Optional, List
from .manager import AsyncTaskQueueManager
from .models import Task, TaskStatus


class QueueService:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(QueueService, cls).__new__(cls)
        return cls._instance

    async def __init__(self):
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
