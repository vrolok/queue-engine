# src/api/router.py
import logging
import uuid
from fastapi import APIRouter, Depends
from typing import Optional

from .models import TaskSubmission, TaskResponse
from .exceptions import TaskValidationError, TaskQueueError

# Initialize logger
logger = logging.getLogger(__name__)

# Create router instance
router = APIRouter()


async def validate_task_payload(task: TaskSubmission) -> None:
    """Validate task payload based on task type"""
    if task.task_type == "http_request":
        if "url" not in task.payload:
            raise TaskValidationError(
                "HTTP request tasks must include 'url' in payload"
            )
    elif task.task_type == "background_processing":
        if "process_type" not in task.payload:
            raise TaskValidationError(
                "Background processing tasks must include 'process_type' in payload"
            )


@router.post("/tasks", response_model=TaskResponse)
async def create_task(task: TaskSubmission):
    """
    Create a new task with the following parameters:
    - task_type: Type of task to execute
    - payload: Task-specific details
    - retry_policy: Optional retry configuration
    """
    try:
        # Generate unique task ID
        task_id = str(uuid.uuid4())

        # Validate task payload
        await validate_task_payload(task)

        # Log task creation
        logger.info(f"Creating task {task_id} of type {task.task_type}")

        # TODO: Here we'll add the task to the queue once the queue module is implemented
        # For now, we'll just log it
        logger.info(f"Task {task_id} payload: {task.payload}")

        return TaskResponse(
            task_id=task_id, status="accepted", message="Task successfully queued"
        )

    except TaskValidationError as e:
        logger.error(f"Task validation error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating task: {str(e)}")
        raise TaskQueueError(f"Failed to create task: {str(e)}")
