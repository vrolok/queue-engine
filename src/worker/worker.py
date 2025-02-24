import asyncio
import logging
from typing import Optional, Dict, Any
from fastapi.concurrency import run_in_threadpool

from src.task_queue.service import QueueService
from src.task_queue.models import Task, TaskStatus
from src.task_queue.exceptions import QueueEmptyError
from src.api.models import RetryPolicy
from .handlers import RetryHandler
from .dispatcher import TaskDispatcher

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, worker_id: str, poll_interval: float = 1.0):
        self.worker_id = worker_id
        self.poll_interval = poll_interval
        self.queue_service = QueueService()
        self.dispatcher = TaskDispatcher()
        self.running = False
        self.is_busy = False
        self.current_task = None
        self.tasks_processed = 0
        self._shutdown_event = asyncio.Event()

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
        """Process a task with retry logic."""
        self.is_busy = True
        self.current_task = task

        try:
            handler = self.dispatcher.get_handler(task.task_type)
            retry_policy = RetryPolicy()  # Use default or from task config
            retry_handler = RetryHandler(retry_policy)

            success = await retry_handler.execute_with_retry(task, handler.handle)

            if success:
                await self.queue_service.update_task_status(
                    task.task_id, TaskStatus.COMPLETED
                )
                logger.info(f"Task {task.task_id} completed successfully")
            else:
                await self._move_to_dead_letter_queue(task)

        except Exception as e:
            logger.error(
                f"Worker {self.worker_id} encountered unhandled error "
                f"processing task {task.task_id}: {str(e)}"
            )
            await self._move_to_dead_letter_queue(task)

        finally:
            self.is_busy = False
            self.current_task = None

    async def _move_to_dead_letter_queue(self, task: Task) -> None:
        """Move failed task to dead letter queue."""
        try:
            await self.queue_service.update_task_status(
                task.task_id,
                TaskStatus.FAILED,
                error_message=f"Task failed after {task.retry_count} retries",
            )
            logger.warning(f"Task {task.task_id} moved to dead letter queue")
        except Exception as e:
            logger.error(
                f"Error moving task {task.task_id} to dead letter queue: {str(e)}"
            )

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

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Gracefully shutdown the worker"""
        logger.info(f"Worker {self.worker_id} initiating shutdown...")
        self.running = False
        self._shutdown_event.set()

        if self.current_task:
            try:
                # Wait for current task to complete with timeout
                await asyncio.wait_for(self._wait_for_current_task(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    f"Worker {self.worker_id} shutdown timed out while processing task"
                )

    async def _wait_for_current_task(self) -> None:
        """Wait for the current task to complete"""
        while self.is_busy:
            await asyncio.sleep(0.1)

    async def run(self) -> None:
        """Main worker loop with graceful shutdown support"""
        self.running = True
        logger.info(f"Worker {self.worker_id} started")

        while self.running:
            try:
                # Check for shutdown signal
                if self._shutdown_event.is_set():
                    break

                task = await self.poll_queue()
                if task:
                    await self.process_task(task)
                else:
                    # Use wait_for to make shutdown more responsive
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(), timeout=self.poll_interval
                        )
                    except asyncio.TimeoutError:
                        continue

            except Exception as e:
                logger.error(f"Worker {self.worker_id} encountered error: {str(e)}")
                await asyncio.sleep(self.poll_interval)

        logger.info(f"Worker {self.worker_id} stopped")

    @property
    def stats(self) -> Dict[str, Any]:
        """Return worker statistics"""
        return {
            "worker_id": self.worker_id,
            "is_busy": self.is_busy,
            "tasks_processed": self.tasks_processed,
            "current_task_id": self.current_task.task_id if self.current_task else None,
        }
