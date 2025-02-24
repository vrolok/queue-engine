# src/log_handler/__init__.py
from .dlq_logger import DLQLogger

__all__ = [
    "DLQLogger",
]

__version__ = "1.0.0"