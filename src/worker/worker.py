# src/worker/worker.py
import asyncio
import traceback
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Callable, List

import ray
from ray.exceptions import RayTaskError, GetTimeoutError
from ray.util.queue import Queue as RayQueue

from src.task_queue.models import Task, TaskStatus, FailureReason, DeadLetterEntry
from src.api.models import RetryPolicy
from .dispatcher import TaskDispatcher
from src.log_handler.logging_config import get_logger

logger = get_logger(__name__)


@ray.remote
class RayWorker:
    """Ray-based worker actor for processing tasks."""

    def __init__(self, worker_id: str, dlq_actor_handle=None):
        self.worker_id = worker_id
        self.dispatcher = TaskDispatcher()
        self.dlq_actor = dlq_actor_handle
        self.is_busy = False
        self.current_task_id: Optional[str] = None
        self.stats = {"tasks_processed": 0, "tasks_failed": 0, "tasks_moved_to_dlq": 0}
        logger.info(f"Ray worker {worker_id} initialized")

    def get_status(self) -> Dict[str, Any]:
        """Get worker status."""
        return {
            "worker_id": self.worker_id,
            "is_busy": self.is_busy,
            "current_task_id": self.current_task_id,
            **self.stats
        }

    def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a task with Ray's retry capabilities.
        Returns the processed task with updated status.
        """
        task_obj = Task.from_dict(task)
        self.is_busy = True
        self.current_task_id = task_obj.task_id
        
        start_time = time.time()
        result = {"success": False, "task": task_obj.to_dict()}
        
        try:
            logger.info(f"Worker {self.worker_id} processing task {task_obj.task_id}")
            
            # Update task status
            task_obj.status = TaskStatus.PROCESSING
            task_obj.started_at = datetime.now(timezone.utc)
            task_obj.updated_at = datetime.now(timezone.utc)
            
            # Get appropriate handler for task type
            handler = self.dispatcher.get_handler(task_obj.task_type)
            
            # Execute task (Ray will handle retries via the retry decorator)
            handler.handle(task_obj)
            
            # Update task on success
            task_obj.status = TaskStatus.COMPLETED
            task_obj.completed_at = datetime.now(timezone.utc)
            task_obj.updated_at = datetime.now(timezone.utc)
            
            self.stats["tasks_processed"] += 1
            logger.info(f"Task {task_obj.task_id} completed successfully")
            
            result = {"success": True, "task": task_obj.to_dict()}
            
        except Exception as e:
            error_details = self._capture_error_details(e)
            
            # Update task with error information
            task_obj.status = TaskStatus.FAILED
            task_obj.error_message = str(e)
            task_obj.updated_at = datetime.now(timezone.utc)
            task_obj.retry_count += 1
            
            self.stats["tasks_failed"] += 1
            logger.error(
                f"Task {task_obj.task_id} failed: {str(e)}\n"
                f"Stack trace: {error_details.get('stack_trace')}"
            )
            
            # Move to DLQ if retry exhausted
            if task_obj.retry_count >= task_obj.max_retries:
                self._move_to_dlq(task_obj, error_details)
            
            result = {
                "success": False, 
                "task": task_obj.to_dict(),
                "error": error_details
            }
            
        finally:
            self.is_busy = False
            self.current_task_id = None
            processing_time = time.time() - start_time
            logger.debug(f"Task {task_obj.task_id} processing took {processing_time:.2f}s")
            
        return result

    def _move_to_dlq(self, task: Task, error_details: Dict[str, Any]) -> None:
        """Move failed task to DLQ using the DLQ actor."""
        try:
            if self.dlq_actor:
                dlq_entry = DeadLetterEntry(
                    task_id=task.task_id,
                    original_task=task.to_dict(),
                    failure_reason=FailureReason.RETRY_EXHAUSTED,
                    error_message=error_details.get("error_message", "Unknown error"),
                    retry_count=task.retry_count,
                    last_error_stack=error_details.get("stack_trace")
                )
                
                # Add to DLQ via Ray actor
                ray.get(self.dlq_actor.add_entry.remote(dlq_entry.to_dict()))
                
                self.stats["tasks_moved_to_dlq"] += 1
                logger.warning(
                    f"Task {task.task_id} moved to DLQ after {task.retry_count} attempts"
                )
        except Exception as e:
            logger.error(f"Error moving task {task.task_id} to DLQ: {str(e)}")

    def _capture_error_details(self, error: Exception) -> Dict[str, Any]:
        """Capture detailed error information."""
        return {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "stack_trace": traceback.format_exc(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


@ray.remote
class DeadLetterQueueActor:
    """Ray actor for managing the Dead Letter Queue."""
    
    def __init__(self):
        self.entries: Dict[str, Dict[str, Any]] = {}
        logger.info("DLQ Actor initialized")
        
    def add_entry(self, entry: Dict[str, Any]) -> bool:
        """Add entry to the DLQ."""
        try:
            dlq_entry = DeadLetterEntry.from_dict(entry)
            self.entries[dlq_entry.task_id] = entry
            
            # Log DLQ entry
            logger.warning(
                f"Task {dlq_entry.task_id} moved to DLQ:\n"
                f"Reason: {dlq_entry.failure_reason}\n"
                f"Retry Count: {dlq_entry.retry_count}\n"
                f"Error: {dlq_entry.error_message}\n"
                f"Timestamp: {datetime.now(timezone.utc)}"
            )
            
            logger.info(f"Added task {dlq_entry.task_id} to DLQ")
            return True
        except Exception as e:
            logger.error(f"Error adding entry to DLQ: {str(e)}")
            return False
            
    def get_all_entries(self) -> List[Dict[str, Any]]:
        """Get all entries in the DLQ."""
        return list(self.entries.values())
        
    def get_entry(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get entry by task ID."""
        return self.entries.get(task_id)
        
    def retry_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Remove task from DLQ and prepare it for reprocessing."""
        if task_id in self.entries:
            entry = self.entries.pop(task_id)
            dlq_entry = DeadLetterEntry.from_dict(entry)
            
            # Reset task for retry
            task_dict = dlq_entry.original_task
            task = Task.from_dict(task_dict)
            task.retry_count = 0
            task.status = TaskStatus.PENDING
            task.error_message = None
            
            logger.info(f"Task {task_id} removed from DLQ for retry")
            return task.to_dict()
        return None


@ray.remote
class RateLimiterActor:
    """Ray actor for rate limiting task execution."""
    
    def __init__(self, requests_per_second: float, burst_size: int = 1):
        self.rate = requests_per_second
        self.burst_size = burst_size
        self.tokens = burst_size
        self.last_refill = time.time()
        self.requests_served = 0
        logger.info(f"RateLimiter initialized with {requests_per_second} req/s and burst size {burst_size}")
        
    def acquire(self) -> bool:
        """Try to acquire a token for task execution."""
        self._refill_tokens()
        
        if self.tokens >= 1:
            self.tokens -= 1
            self.requests_served += 1
            return True
        return False
        
    def _refill_tokens(self) -> None:
        """Refill tokens based on time elapsed."""
        now = time.time()
        time_passed = now - self.last_refill
        new_tokens = time_passed * self.rate
        
        self.tokens = min(self.tokens + new_tokens, self.burst_size)
        self.last_refill = now
        
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        return {
            "current_tokens": self.tokens,
            "rate": self.rate,
            "burst_size": self.burst_size,
            "requests_served": self.requests_served
        }


# Note: Legacy Worker class has been removed to reduce code duplication
# All functionality is now handled by RayWorker
