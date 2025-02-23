# src/worker/worker.py
import asyncio
import logging
from typing import Optional

from src.task_queue.service import QueueService
from src.task_queue.models import Task, TaskStatus
from src.task_queue.exceptions import QueueEmptyError
from .dispatcher import TaskDispatcher

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, worker_id: str, poll_interval: float = 1.0):
        self.worker_id = worker_id
        self.poll_interval = poll_interval
        self.queue_service = QueueService()
        self.dispatcher = TaskDispatcher()
        self.running = False

    async def process_task(self, task: Task) -> None:
        try:
            logger.info(f"Worker {self.worker_id} processing task {task.task_id}")

            handler = self.dispatcher.get_handler(task.task_type)
            await handler.handle(task)

            self.queue_service.update_task_status(task.task_id, TaskStatus.COMPLETED)

            logger.info(f"Task {task.task_id} completed successfully")

        except Exception as e:
            logger.error(f"Error processing task {task.task_id}: {str(e)}")

            if task.retry_count < task.max_retries:
                task.retry_count += 1
                self.queue_service.update_task_status(
                    task.task_id, TaskStatus.RETRYING, error_message=str(e)
                )
            else:
                self.queue_service.update_task_status(
                    task.task_id, TaskStatus.FAILED, error_message=str(e)
                )

    async def run(self) -> None:
        self.running = True
        logger.info(f"Worker {self.worker_id} started")

        while self.running:
            try:
                task = self.queue_service.dequeue_task()
                if task:
                    await self.process_task(task)
                else:
                    await asyncio.sleep(self.poll_interval)

            except QueueEmptyError:
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Worker {self.worker_id} encountered error: {str(e)}")
                await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        self.running = False
        logger.info(f"Worker {self.worker_id} stopped")
