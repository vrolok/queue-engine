# src/queue/service.py
from fastapi.concurrency import run_in_threadpool
from typing import Optional
from .manager import TaskQueueManager
from .models import Task, TaskStatus


class QueueService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(QueueService, cls).__new__(cls)
            cls._instance.queue = TaskQueueManager()
        return cls._instance

    async def enqueue_task(self, task: Task) -> Task:
        return await run_in_threadpool(self.queue.enqueue, task)

    async def dequeue_task(self) -> Optional[Task]:
        return await run_in_threadpool(self.queue.dequeue)

    async def get_task(self, task_id: str) -> Optional[Task]:
        return await run_in_threadpool(self.queue.get_task, task_id)

    async def update_task_status(
        self, task_id: str, status: TaskStatus, error_message: str = None
    ) -> Task:
        return await run_in_threadpool(
            self.queue.update_task_status, task_id, status, error_message
        )