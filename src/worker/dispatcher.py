# src/worker/dispatcher.py
from typing import Dict, Type
from .handlers import (
    BaseTaskHandler,
    HttpRequestHandler,
    BackgroundProcessingHandler,
    TextProcessingHandler,
)
from src.api.models import TaskType


class TaskDispatcher:
    def __init__(self):
        self.handlers: Dict[str, Type[BaseTaskHandler]] = {
            TaskType.HTTP_REQUEST: HttpRequestHandler,
            TaskType.BACKGROUND_PROCESSING: BackgroundProcessingHandler,
            TaskType.TEXT_PROCESSING: TextProcessingHandler,
        }

    def get_handler(self, task_type: str) -> BaseTaskHandler:
        handler_class = self.handlers.get(task_type)
        if not handler_class:
            raise ValueError(f"No handler found for task type: {task_type}")
        return handler_class()
