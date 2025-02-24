"""Worker module for the Async Task Processing Service.

This module provides task processing and handling functionality.
"""

from .worker import Worker
from .pool import WorkerPool
from .handlers import (
    HttpRequestHandler,
    BackgroundProcessingHandler,
    TextProcessingHandler,
)
from .dispatcher import TaskDispatcher

__all__ = [
    "Worker",
    "WorkerPool",
    "HttpRequestHandler",
    "BackgroundProcessingHandler",
    "TextProcessingHandler",
    "TaskDispatcher",
]

__version__ = "1.0.0"
