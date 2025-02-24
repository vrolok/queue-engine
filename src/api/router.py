# src/api/router.py
import logging
import uuid
from fastapi import APIRouter
from typing import List

from .models import TaskType, TaskSubmission, TaskResponse
from .exceptions import TaskValidationError, TaskQueueError
from src.task_queue.models import Task
from src.task_queue.service import QueueService
from src.task_queue.exceptions import QueueError, QueueFullError

# Initialize logger
logger = logging.getLogger(__name__)

# Create router instance
router = APIRouter()


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
async def create_task(task_submission: TaskSubmission):
    """
    Create a new task and add it to the queue
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

        # Enqueue task
        queue_service = QueueService()
        queued_task = await queue_service.enqueue_task(task)

        logger.info(f"Task {task_id} successfully queued")

        return TaskResponse(
            task_id=queued_task.task_id,
            status=queued_task.status,
            message="Task successfully queued",
        )

    except QueueFullError as e:
        logger.error(f"Queue is full: {str(e)}")
        raise TaskQueueError("Queue is full, please try again later")
    except Exception as e:
        logger.exception("Unexpected error creating task")
        raise TaskQueueError(f"Failed to create task: {str(e)}")


@router.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str):
    """
    Get task status by ID
    """
    try:
        queue_service = QueueService()
        task = queue_service.get_task(task_id)

        return TaskResponse(
            task_id=task.task_id,
            status=task.status,
            message=f"Task status: {task.status}",
        )
    except QueueError as e:
        logger.error(f"Error retrieving task {task_id}: {str(e)}")
        raise TaskQueueError(f"Failed to retrieve task: {str(e)}")


@router.get("/tasks", response_model=List[TaskResponse])
async def list_tasks():
    """
    List all tasks in the queue
    """
    try:
        queue_service = QueueService()
        tasks = queue_service.queue.get_all_tasks()

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
