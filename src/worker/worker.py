# src/worker/worker.py
import asyncio
import logging
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Callable

from src.task_queue.service import QueueService
from src.task_queue.models import Task, TaskStatus, FailureReason
from src.api.models import RetryPolicy
from .dispatcher import TaskDispatcher
from src.log_handler.dlq_logger import DLQLogger

logger = logging.getLogger(__name__)


class Worker:
    """Worker class responsible for processing tasks with retry logic and DLQ support."""

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.dispatcher = TaskDispatcher()
        self.queue_service = QueueService()
        self.dlq_logger = DLQLogger()
        self.is_busy = False
        self.current_task: Optional[Task] = None
        self.stats = {"tasks_processed": 0, "tasks_failed": 0, "tasks_moved_to_dlq": 0}

    async def process_task(self, task: Task) -> None:
        """
        Process a task with retry logic and DLQ integration.
        Includes exponential backoff and comprehensive error handling.
        """
        self.is_busy = True
        self.current_task = task

        try:
            logger.info(
                f"Worker {self.worker_id} starting to process task {task.task_id}"
            )

            # Get appropriate handler for task type
            handler = self.dispatcher.get_handler(task.task_type)
            retry_policy = RetryPolicy()

            # Execute task with retry logic
            success = await self._execute_with_retry(task, handler.handle, retry_policy)

            if success:
                await self._handle_success(task)
            else:
                await self._handle_failure(task, "Max retries exceeded")

        except Exception as e:
            error_details = self._capture_error_details(e)
            await self._handle_failure(task, str(e), error_details)

        finally:
            self.is_busy = False
            self.current_task = None

    async def _execute_with_retry(
        self, task: Task, handler_func: Callable, retry_policy: RetryPolicy
    ) -> bool:
        """Execute task with exponential backoff retry logic."""
        attempt = 0

        while attempt < retry_policy.max_attempts:
            try:
                attempt += 1
                await handler_func(task)
                return True

            except Exception as e:
                if attempt >= retry_policy.max_attempts:
                    logger.error(
                        f"Task {task.task_id} failed permanently after "
                        f"{attempt} attempts. Error: {str(e)}"
                    )
                    return False

                delay = retry_policy.calculate_delay(attempt)
                logger.warning(
                    f"Task {task.task_id} failed (attempt {attempt}/{retry_policy.max_attempts}). "
                    f"Retrying in {delay:.2f} seconds. Error: {str(e)}"
                )

                # Update task status before retry
                await self.queue_service.update_task_status(
                    task.task_id, TaskStatus.RETRYING, error_message=str(e)
                )

                await asyncio.sleep(delay)

        return False

    async def _handle_success(self, task: Task) -> None:
        """Handle successful task completion."""
        await self.queue_service.update_task_status(task.task_id, TaskStatus.COMPLETED)
        self.stats["tasks_processed"] += 1
        logger.info(f"Task {task.task_id} completed successfully")

    async def _handle_failure(
        self, task: Task, error_message: str, error_details: Optional[Dict] = None
    ) -> None:
        """Handle task failure and move to DLQ if necessary."""
        try:
            self.stats["tasks_failed"] += 1

            # Move to DLQ
            dlq_entry = await self.queue_service.move_to_dlq(
                task,
                FailureReason.RETRY_EXHAUSTED,
                error_message,
                error_details.get("stack_trace") if error_details else None,
            )

            self.stats["tasks_moved_to_dlq"] += 1

            # Log DLQ entry
            self.dlq_logger.log_task_moved_to_dlq(
                task.task_id,
                FailureReason.RETRY_EXHAUSTED,
                task.retry_count,
                error_details or {"error_message": error_message},
            )

        except Exception as e:
            logger.error(f"Error moving task {task.task_id} to DLQ: {str(e)}")

    def _capture_error_details(self, error: Exception) -> Dict[str, Any]:
        """Capture detailed error information."""
        return {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "stack_trace": traceback.format_exc(),
            "timestamp": datetime.now(timezone.utc),
        }

    @property
    def worker_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        return {
            "worker_id": self.worker_id,
            "is_busy": self.is_busy,
            "current_task_id": self.current_task.task_id if self.current_task else None,
            **self.stats,
        }
