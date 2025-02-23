"""Worker module for the Async Task Processing Service.

This module provides task processing and handling functionality.
"""

from .worker import Worker, WorkerManager
from .handlers import (
    TaskHandler,
    HttpRequestHandler,
    BackgroundProcessingHandler,
    TextProcessingHandler,
)
from .dispatcher import TaskDispatcher

__all__ = [
    "Worker",
    "WorkerManager",
    "TaskHandler",
    "HttpRequestHandler",
    "BackgroundProcessingHandler",
    "TextProcessingHandler",
    "TaskDispatcher",
]

__version__ = "1.0.0"
