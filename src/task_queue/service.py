# src/task_queue/service.py
import asyncio
import logging
import time
import uuid
from typing import Dict, List, Optional, Any

import ray
from ray.actor import ActorHandle

from .models import Task, TaskStatus, FailureReason, DeadLetterEntry, RayTaskReference
from src.worker.pool import get_ray_worker_pool
from .exceptions import QueueEmptyError, TaskNotFoundError

logger = logging.getLogger(__name__)


class RayTaskService:
    """Service for managing task submission and execution with Ray."""
    
    def __init__(self):
        self.task_refs: Dict[str, RayTaskReference] = {}
        self.worker_pool = get_ray_worker_pool()
        self._lock = asyncio.Lock()
        logger.info("RayTaskService initialized")
        
    async def initialize(self) -> None:
        """Initialize the Ray worker pool if needed."""
        if not ray.is_initialized():
            await self.worker_pool.initialize_ray()
        
        # Make sure worker pool is started
        if not self.worker_pool._is_initialized:
            await self.worker_pool.start()
            
    async def submit_task(self, task: Task) -> Task:
        """Submit a task for processing with Ray."""
        await self.initialize()
        
        async with self._lock:
            try:
                # Submit task to Ray worker pool
                result = await self.worker_pool.submit_task(task.to_dict())
                
                if not result["success"]:
                    logger.error(f"Failed to submit task {task.task_id}: {result.get('error')}")
                    task.status = TaskStatus.FAILED
                    task.error_message = result.get("error", "Unknown error during submission")
                    return task
                
                # Store reference to submitted task
                self.task_refs[task.task_id] = RayTaskReference(
                    task_id=task.task_id,
                    object_ref=result["task_ref"]
                )
                
                logger.info(f"Task {task.task_id} submitted successfully")
                return task
                
            except Exception as e:
                logger.error(f"Error submitting task {task.task_id}: {str(e)}")
                task.status = TaskStatus.FAILED
                task.error_message = f"Submission error: {str(e)}"
                return task
    
    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get the status of a task."""
        if task_id not in self.task_refs:
            return None
            
        try:
            # Get task reference
            task_ref = self.task_refs[task_id]
            
            # Check if task has completed
            if ray.wait([task_ref.object_ref], timeout=0)[0]:
                # Task completed, get result
                result = ray.get(task_ref.object_ref)
                
                # Clean up reference
                del self.task_refs[task_id]
                
                return result
            else:
                # Task still running
                return {
                    "task_id": task_id,
                    "status": "processing", 
                    "in_progress": True
                }
                
        except Exception as e:
            logger.error(f"Error getting task status for {task_id}: {str(e)}")
            return {
                "task_id": task_id,
                "status": "error", 
                "error_message": str(e)
            }
    
    async def get_all_task_statuses(self) -> List[Dict[str, Any]]:
        """Get status of all tracked tasks."""
        results = []
        
        for task_id in list(self.task_refs.keys()):
            status = await self.get_task_status(task_id)
            if status:
                results.append(status)
                
        return results
    
    async def get_dlq_entries(self) -> List[Dict[str, Any]]:
        """Get all entries from the DLQ."""
        await self.initialize()
        
        try:
            if self.worker_pool.dlq_actor:
                return ray.get(self.worker_pool.dlq_actor.get_all_entries.remote())
            return []
        except Exception as e:
            logger.error(f"Error getting DLQ entries: {str(e)}")
            return []
    
    async def retry_dlq_task(self, task_id: str) -> Optional[Task]:
        """Retry a task from the DLQ."""
        await self.initialize()
        
        try:
            if self.worker_pool.dlq_actor:
                task_dict = ray.get(self.worker_pool.dlq_actor.retry_task.remote(task_id))
                if task_dict:
                    task = Task.from_dict(task_dict)
                    # Submit for processing
                    await self.submit_task(task)
                    return task
            return None
        except Exception as e:
            logger.error(f"Error retrying task {task_id} from DLQ: {str(e)}")
            return None


class QueueService:
    """Service for task queue operations with Ray integration."""
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(QueueService, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # Initialize only once
        if not self._initialized:
            self._ray_service = RayTaskService()
            QueueService._initialized = True
        
    async def enqueue_task(self, task: Task) -> Task:
        """Submit a task for processing."""
        return await self._ray_service.submit_task(task)
        
    async def dequeue_task(self) -> Optional[Task]:
        """Legacy method - now a no-op as Ray manages task execution."""
        # This method is deprecated with Ray - tasks are submitted directly to workers
        logger.warning("dequeue_task is deprecated with Ray - use submit_task instead")
        return None
        
    async def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task by ID."""
        status = await self._ray_service.get_task_status(task_id)
        if not status:
            raise TaskNotFoundError(f"Task {task_id} not found")
            
        # Convert status to Task object
        if "task" in status:
            return Task.from_dict(status["task"])
            
        # Create basic task info
        return Task(
            task_id=task_id,
            task_type="unknown",
            payload={},
            status=TaskStatus.PROCESSING if status.get("in_progress") else TaskStatus.FAILED
        )
        
    async def update_task_status(
        self, task_id: str, status: TaskStatus, error_message: str = None
    ) -> Task:
        """Update task status (legacy method - now a no-op as Ray handles status)."""
        logger.warning("update_task_status is deprecated with Ray - statuses are managed automatically")
        task = await self.get_task(task_id)
        if task:
            # These updates are local only - Ray manages the actual task state
            task.status = status
            if error_message:
                task.error_message = error_message
        return task
        
    async def get_queue_size(self) -> int:
        """Get approximate number of pending tasks."""
        try:
            return len(self._ray_service.task_refs)
        except Exception as e:
            logger.error(f"Error getting queue size: {str(e)}")
            return 0
        
    async def get_all_tasks(self) -> List[Task]:
        """Get all tasks."""
        statuses = await self._ray_service.get_all_task_statuses()
        tasks = []
        
        for status in statuses:
            if "task" in status:
                tasks.append(Task.from_dict(status["task"]))
            else:
                # Basic placeholder for in-progress tasks
                tasks.append(Task(
                    task_id=status.get("task_id", "unknown"),
                    task_type="unknown",
                    payload={},
                    status=TaskStatus.PROCESSING
                ))
                
        return tasks
        
    async def move_to_dlq(
        self,
        task: Task,
        failure_reason: FailureReason,
        error_message: str,
        stack_trace: Optional[str] = None
    ) -> DeadLetterEntry:
        """Move a task to the DLQ (handled by Ray workers now)."""
        logger.warning("move_to_dlq is deprecated with Ray - handled automatically by workers")
        
        # Create a DLQ entry for compatibility
        return DeadLetterEntry(
            task_id=task.task_id,
            original_task=task.to_dict(),
            failure_reason=failure_reason,
            error_message=error_message,
            retry_count=task.retry_count,
            last_error_stack=stack_trace
        )
        
    async def get_dlq_tasks(self) -> List[DeadLetterEntry]:
        """Get all tasks in the DLQ."""
        entries = await self._ray_service.get_dlq_entries()
        return [DeadLetterEntry.from_dict(entry) for entry in entries]
        
    async def retry_dlq_task(self, task_id: str) -> Optional[Task]:
        """Retry a task from the DLQ."""
        return await self._ray_service.retry_dlq_task(task_id)