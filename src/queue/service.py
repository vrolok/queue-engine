# src/queue/service.py
from typing import Optional
from .queue import TaskQueue
from .models import Task, TaskStatus


class QueueService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(QueueService, cls).__new__(cls)
            cls._instance.queue = TaskQueue()
        return cls._instance

    def enqueue_task(self, task: Task) -> Task:
        return self.queue.enqueue(task)

    def dequeue_task(self) -> Optional[Task]:
        return self.queue.dequeue()

    def get_task(self, task_id: str) -> Optional[Task]:
        return self.queue.get_task(task_id)

    def update_task_status(
        self, task_id: str, status: TaskStatus, error_message: str = None
    ) -> Task:
        return self.queue.update_task_status(task_id, status, error_message)
