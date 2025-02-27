"""Worker module for the Async Task Processing Service.

This module provides task processing and handling functionality using Ray actors.
"""

from .worker import RayWorker, DeadLetterQueueActor, RateLimiterActor
from .pool import RayWorkerPool, get_ray_worker_pool
from .handlers import (
    HttpRequestHandler,
    BackgroundProcessingHandler,
    TextProcessingHandler,
)
from .dispatcher import TaskDispatcher

__all__ = [
    "RayWorker",
    "RayWorkerPool",
    "get_ray_worker_pool",
    "DeadLetterQueueActor",
    "RateLimiterActor",
    "HttpRequestHandler",
    "BackgroundProcessingHandler",
    "TextProcessingHandler",
    "TaskDispatcher",
]

__version__ = "2.0.0"
