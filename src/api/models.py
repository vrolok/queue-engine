# src/api/models.py

import random
from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class TaskType(str, Enum):
    HTTP_REQUEST = "http_request"
    BACKGROUND_PROCESSING = "background_processing"
    TEXT_PROCESSING = "text_processing"


class RetryPolicy(BaseModel):
    max_attempts: int = Field(default=3, ge=1, le=10)
    initial_delay: float = Field(default=1.0, ge=0.1, le=60.0)
    max_delay: float = Field(default=300.0, ge=1.0, le=3600.0)
    exponential_base: float = Field(default=2.0, ge=1.1, le=10.0)
    jitter: bool = Field(default=True)

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff and optional jitter."""
        # Calculate base delay
        delay = min(
            self.initial_delay * (self.exponential_base ** (attempt - 1)),
            self.max_delay,
        )

        # Add jitter if enabled
        if self.jitter:
            delay *= 0.5 + random.random()

        return delay


class TaskSubmission(BaseModel):
    task_type: TaskType
    payload: Dict[str, Any]
    retry_policy: Optional[RetryPolicy] = Field(default_factory=lambda: RetryPolicy())


class TaskResponse(BaseModel):
    task_id: str
    status: str = "pending"
    message: str
