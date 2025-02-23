"""API module for the Async Task Processing Service.

This module provides the FastAPI application and endpoints for task management.
"""

from .app import create_app
from .models import TaskType, TaskSubmission, TaskResponse, RetryPolicy
from .router import router
from .exceptions import TaskValidationError, TaskQueueError

__all__ = [
    'create_app',
    'TaskType',
    'TaskSubmission',
    'TaskResponse',
    'RetryPolicy',
    'router',
    'TaskValidationError',
    'TaskQueueError'
]

__version__ = '1.0.0'