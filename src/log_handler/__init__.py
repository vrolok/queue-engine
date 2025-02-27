# src/log_handler/__init__.py
from .logging_config import setup_logging, get_logger, shutdown_logging

__all__ = [
    "setup_logging",
    "get_logger",
    "shutdown_logging"
]

__version__ = "1.0.0"