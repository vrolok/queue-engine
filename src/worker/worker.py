import asyncio
import logging
from typing import Optional
from fastapi.concurrency import run_in_threadpool

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
        self._current_task: Optional[Task] = None

    async def poll_queue(self) -> Optional[Task]:
        """Poll the queue for new tasks."""
        try:
            task = await self.queue_service.dequeue_task()
            return task
        except QueueEmptyError:
            return None
        except Exception as e:
            logger.error(f"Error polling queue: {str(e)}")
            return None

    async def process_task(self, task: Task) -> None:
        """Process a single task with proper error handling."""
        self._current_task = task
        try:
            logger.info(f"Worker {self.worker_id} processing task {task.task_id}")

            handler = self.dispatcher.get_handler(task.task_type)
            await handler.handle(task)

            await self.queue_service.update_task_status(
                task.task_id, TaskStatus.COMPLETED
            )
            logger.info(f"Task {task.task_id} completed successfully")

        except Exception as e:
            logger.error(f"Error processing task {task.task_id}: {str(e)}")
            await self._handle_task_error(task, str(e))
        finally:
            self._current_task = None

    async def _handle_task_error(self, task: Task, error_message: str) -> None:
        """Handle task errors and retry logic."""
        if task.retry_count < task.max_retries:
            task.retry_count += 1
            await self.queue_service.update_task_status(
                task.task_id, TaskStatus.RETRYING, error_message=error_message
            )
        else:
            await self.queue_service.update_task_status(
                task.task_id, TaskStatus.FAILED, error_message=error_message
            )

    async def run(self) -> None:
        """Main worker run loop with improved error handling."""
        self.running = True
        logger.info(f"Worker {self.worker_id} started")

        while self.running:
            try:
                task = await self.poll_queue()
                if task:
                    await self.process_task(task)
                else:
                    await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Worker {self.worker_id} encountered error: {str(e)}")
                await asyncio.sleep(self.poll_interval)

    async def stop(self) -> None:
        """Gracefully stop the worker."""
        self.running = False
        if self._current_task:
            logger.info(f"Worker {self.worker_id} waiting for current task to complete")
            # Wait for current task to complete with timeout
            try:
                await asyncio.wait_for(
                    self.process_task(self._current_task), timeout=30.0
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"Worker {self.worker_id} forced to stop while processing task"
                )
        logger.info(f"Worker {self.worker_id} stopped")
