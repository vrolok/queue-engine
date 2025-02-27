# src/worker/actors.py
import ray
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from src.log_handler.logging_config import get_logger

logger = get_logger(__name__)


@ray.remote
class DeadLetterQueueActor:
    def __init__(self):
        self.entries: Dict[str, Dict[str, Any]] = {}
        logger.info("DLQ Actor initialized")

    def add_entry(self, entry: Dict[str, Any]) -> bool:
        try:
            # We'll use a dictionary approach to avoid importing DeadLetterEntry
            task_id = entry.get("task_id")
            self.entries[task_id] = entry

            logger.warning(
                f"Task {task_id} moved to DLQ:\n"
                f"Reason: {entry.get('failure_reason')}\n"
                f"Retry Count: {entry.get('retry_count')}\n"
                f"Error: {entry.get('error_message')}\n"
                f"Timestamp: {datetime.now(timezone.utc)}"
            )

            logger.info(f"Added task {task_id} to DLQ")
            return True
        except Exception as e:
            logger.error(f"Error adding entry to DLQ: {str(e)}")
            return False

    def get_all_entries(self) -> List[Dict[str, Any]]:
        return list(self.entries.values())

    def get_entry(self, task_id: str) -> Optional[Dict[str, Any]]:
        return self.entries.get(task_id)

    def retry_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        if task_id in self.entries:
            entry = self.entries.pop(task_id)

            # Extract the original task
            original_task = entry.get("original_task", {})

            # Reset task state for retry
            original_task["retry_count"] = 0
            original_task["status"] = "pending"
            original_task["error_message"] = None

            logger.info(f"Task {task_id} removed from DLQ for retry")
            return original_task
        return None


@ray.remote
class RateLimiterActor:
    def __init__(self, requests_per_second: float, burst_size: int = 1):
        self.rate = requests_per_second
        self.burst_size = burst_size
        self.tokens = burst_size
        self.last_refill = time.time()
        self.requests_served = 0
        logger.info(
            f"RateLimiter initialized with {requests_per_second} req/s and burst size {burst_size}"
        )

    def acquire(self) -> bool:
        self._refill_tokens()

        if self.tokens >= 1:
            self.tokens -= 1
            self.requests_served += 1
            return True
        return False

    def _refill_tokens(self) -> None:
        now = time.time()
        time_passed = now - self.last_refill
        new_tokens = time_passed * self.rate

        self.tokens = min(self.tokens + new_tokens, self.burst_size)
        self.last_refill = now

    def get_stats(self) -> Dict[str, Any]:
        return {
            "current_tokens": self.tokens,
            "rate": self.rate,
            "burst_size": self.burst_size,
            "requests_served": self.requests_served,
        }
