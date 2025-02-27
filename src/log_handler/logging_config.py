# src/log_handler/logging_config.py
import logging
import queue
import os
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from typing import Optional

# Global variable to ensure we only configure logging once
_logging_configured = False
_log_listener = None


def setup_logging(
    log_level: int = logging.INFO,
    log_file: Optional[str] = None,
    module_levels: Optional[dict] = None
) -> QueueListener:
    """
    Central logging configuration for the entire application.
    
    Args:
        log_level: Base logging level for the application
        log_file: Optional file path to write logs to
        module_levels: Dictionary mapping module names to specific log levels
                      e.g. {"src.worker": logging.DEBUG}
    
    Returns:
        QueueListener instance that should be started/stopped with the application
    """
    global _logging_configured, _log_listener
    
    # Only configure once
    if _logging_configured and _log_listener is not None:
        return _log_listener
    
    # Create log directory if needed
    if log_file and os.path.dirname(log_file):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Create queue for non-blocking logging
    log_queue = queue.Queue()
    
    # Configure formatters
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Create handlers
    handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)
    
    # File handler if requested
    if log_file:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5  # 10MB
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    
    # Setup queue handler and listener
    queue_handler = QueueHandler(log_queue)
    listener = QueueListener(
        log_queue, *handlers, respect_handler_level=True
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    
    # Remove any existing handlers to avoid duplicates
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    
    root_logger.addHandler(queue_handler)
    root_logger.setLevel(log_level)
    
    # Configure module-specific log levels
    if module_levels:
        for module_name, level in module_levels.items():
            logging.getLogger(module_name).setLevel(level)
    
    # Start the listener
    listener.start()
    
    _logging_configured = True
    _log_listener = listener
    
    return listener


def get_logger(name: str) -> logging.Logger:
    """
    Get a properly configured logger for the given module.
    
    Args:
        name: The module name, typically __name__
        
    Returns:
        A configured logger instance
    """
    return logging.getLogger(name)


def shutdown_logging():
    """
    Properly shutdown logging resources.
    Should be called during application shutdown.
    """
    global _log_listener, _logging_configured
    
    if _log_listener is not None:
        _log_listener.stop()
        _log_listener = None
        _logging_configured = False