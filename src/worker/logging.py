# src/worker/logging.py
import logging
import queue
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from typing import Optional


def setup_worker_logging(log_file: str = "worker.log") -> QueueListener:
    """Setup async-compatible logging for workers"""

    # Create queue for non-blocking logging
    log_queue = queue.Queue()

    # Create handlers
    file_handler = RotatingFileHandler(
        log_file, maxBytes=1024 * 1024, backupCount=3  # 1MB
    )
    console_handler = logging.StreamHandler()

    # Create formatters
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Setup queue handler and listener
    queue_handler = QueueHandler(log_queue)
    listener = QueueListener(
        log_queue, file_handler, console_handler, respect_handler_level=True
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(queue_handler)
    root_logger.setLevel(logging.INFO)

    return listener
