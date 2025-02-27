# src/api/router.py
import logging
import uuid
import ray
from fastapi import APIRouter, BackgroundTasks, Query, Depends
from typing import List, Dict, Any, Optional, TYPE_CHECKING

from .models import (
    TaskType,
    TaskSubmission,
    TaskResponse,
    TaskDetailResponse,
    SystemStatusResponse,
)
from .exceptions import TaskValidationError, TaskQueueError
from src.task_queue.exceptions import QueueError
from src.task_queue.models import Task

# Import for type checking only
if TYPE_CHECKING:
    from src.task_queue.service import QueueService
    from src.task_scheduler import TaskScheduler

# Initialize logger
logger = logging.getLogger(__name__)

# Create router instance
router = APIRouter()


def get_task_service():
    from src.task_queue.service import RayTaskService

    return RayTaskService()


def get_queue_service():
    from src.task_queue.service import QueueService

    return QueueService()


def get_scheduler():
    from src.main import app_state

    return app_state.scheduler


async def validate_task_payload(task: TaskSubmission) -> None:
    """Validate task payload based on task type"""
    if task.task_type == TaskType.HTTP_REQUEST:
        if "url" not in task.payload:
            raise TaskValidationError(
                "HTTP request tasks must include 'url' in payload"
            )
    elif task.task_type == TaskType.BACKGROUND_PROCESSING:
        if "process_type" not in task.payload:
            raise TaskValidationError(
                "Background processing tasks must include 'process_type' in payload"
            )


@router.post("/tasks", response_model=TaskResponse)
async def create_task(
    task_submission: TaskSubmission,
    background_tasks: BackgroundTasks,
    queue_service: "QueueService" = Depends(get_queue_service),
):
    """
    Create a new task and submit it to Ray for processing
    """
    try:
        # Generate unique task ID
        task_id = str(uuid.uuid4())

        # Validate task payload
        await validate_task_payload(task_submission)

        # Create task instance
        task = Task(
            task_id=task_id,
            task_type=task_submission.task_type,
            payload=task_submission.payload,
            max_retries=(task_submission.retry_policy.max_attempts - 1),
        )

        # Submit task to Ray (don't wait for result)
        background_tasks.add_task(queue_service.enqueue_task, task)

        logger.info(f"Task {task_id} submitted to Ray")

        return TaskResponse(
            task_id=task.task_id,
            status=task.status,
            message="Task submitted for processing",
        )

    except Exception as e:
        logger.exception("Unexpected error creating task")
        raise TaskQueueError(f"Failed to create task: {str(e)}")


@router.get("/tasks/{task_id}", response_model=TaskDetailResponse)
async def get_task(
    task_id: str, queue_service: "QueueService" = Depends(get_queue_service)
):
    """
    Get detailed task status by ID from Ray
    """
    try:
        task = await queue_service.get_task(task_id)

        # Get Ray-specific information
        ray_service = get_task_service()
        ray_status = await ray_service.get_task_status(task_id)

        # Merge information
        worker_id = None
        processing_time = None
        error_details = None

        if ray_status:
            if "task" in ray_status and "worker_id" in ray_status:
                worker_id = ray_status.get("worker_id")
            if "error" in ray_status:
                error_details = ray_status.get("error")

        return TaskDetailResponse(
            task_id=task.task_id,
            status=task.status,
            message=f"Task status: {task.status}",
            task_type=task.task_type,
            created_at=task.created_at if hasattr(task, "created_at") else None,
            started_at=task.started_at,
            completed_at=task.completed_at,
            retry_count=task.retry_count,
            worker_id=worker_id,
            error_details=error_details,
        )
    except QueueError as e:
        logger.error(f"Error retrieving task {task_id}: {str(e)}")
        raise TaskQueueError(f"Failed to retrieve task: {str(e)}")


@router.get("/tasks", response_model=List[TaskResponse])
async def list_tasks(
    status: Optional[str] = Query(None, description="Filter by task status"),
    queue_service: "QueueService" = Depends(get_queue_service),
):
    """
    List all tasks in the Ray system
    """
    try:
        tasks = await queue_service.get_all_tasks()

        # Filter by status if requested
        if status:
            tasks = [t for t in tasks if t.status == status]

        return [
            TaskResponse(
                task_id=task.task_id,
                status=task.status,
                message=f"Task status: {task.status}",
            )
            for task in tasks
        ]
    except Exception as e:
        logger.error(f"Error listing tasks: {str(e)}")
        raise TaskQueueError(f"Failed to list tasks: {str(e)}")


@router.get("/system/status", response_model=SystemStatusResponse)
async def get_system_status(scheduler: "TaskScheduler" = Depends(get_scheduler)):
    """
    Get Ray system status and metrics
    """
    try:
        # Get scheduler stats with Ray metrics
        stats = await scheduler.get_stats()

        # Get Ray dashboard URL
        dashboard_url = await scheduler.get_ray_dashboard_url()

        # Get Ray cluster info
        ray_nodes = len(ray.nodes()) if ray.is_initialized() else 0

        return SystemStatusResponse(
            worker_count=stats["worker_count"],
            active_workers=stats["active_workers"],
            queue_length=stats["queue_length"],
            processing_rate=stats["processing_rate"],
            error_rate=stats["error_rate"],
            ray_dashboard_url=dashboard_url,
            ray_nodes=ray_nodes,
            ray_resources=stats.get("ray_resources", {}),
            worker_details=stats.get("worker_details", []),
        )
    except Exception as e:
        logger.error(f"Error getting system status: {str(e)}")
        raise TaskQueueError(f"Failed to get system status: {str(e)}")


@router.post("/tasks/{task_id}/retry", response_model=TaskResponse)
async def retry_task(
    task_id: str, queue_service: "QueueService" = Depends(get_queue_service)
):
    """
    Retry a failed task from the DLQ
    """
    try:
        task = await queue_service.retry_dlq_task(task_id)
        if not task:
            raise TaskQueueError(f"Task {task_id} not found in DLQ")

        return TaskResponse(
            task_id=task.task_id, status=task.status, message=f"Task retried from DLQ"
        )
    except Exception as e:
        logger.error(f"Error retrying task {task_id}: {str(e)}")
        raise TaskQueueError(f"Failed to retry task: {str(e)}")


@router.get("/dlq", response_model=List[Dict[str, Any]])
async def list_dlq_tasks(queue_service: "QueueService" = Depends(get_queue_service)):
    """
    List all tasks in the Dead Letter Queue
    """
    try:
        dlq_tasks = await queue_service.get_dlq_tasks()
        return [task.dict() for task in dlq_tasks]
    except Exception as e:
        logger.error(f"Error listing DLQ tasks: {str(e)}")
        raise TaskQueueError(f"Failed to list DLQ tasks: {str(e)}")
