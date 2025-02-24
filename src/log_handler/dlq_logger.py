import logging
from datetime import datetime
from typing import Dict, Any

class DLQLogger:
    def __init__(self):
        self.logger = logging.getLogger("dlq")
        self.logger.setLevel(logging.INFO)
        
        # Add handlers if needed
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def log_task_moved_to_dlq(
        self,
        task_id: str,
        reason: str,
        retry_count: int,
        error_details: Dict[str, Any]
    ):
        """Log when a task is moved to DLQ."""
        self.logger.warning(
            f"Task {task_id} moved to DLQ:\n"
            f"Reason: {reason}\n"
            f"Retry Count: {retry_count}\n"
            f"Error Details: {error_details}\n"
            f"Timestamp: {datetime.now(datetime.timezone.utc).isoformat()}"
        )

    def log_dlq_task_processed(self, task_id: str, action: str):
        """Log when a DLQ task is processed."""
        self.logger.info(
            f"DLQ Task {task_id} processed:\n"
            f"Action: {action}\n"
            f"Timestamp: {datetime.now(datetime.timezone.utc).isoformat()}"
        )