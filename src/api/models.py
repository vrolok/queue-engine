# src/api/models.py

import random
from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, validator


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
            delay = min(delay, self.max_delay)

        return delay


class TaskSubmission(BaseModel):
    task_type: TaskType
    payload: Dict[str, Any]
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)


class TaskResponse(BaseModel):
    task_id: str
    status: str = "pending"
    message: str


class TaskDetailResponse(BaseModel):
    task_id: str
    status: str = "pending"
    message: str
    task_type: Optional[str] = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retry_count: Optional[int] = None
    worker_id: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None


class SystemStatusResponse(BaseModel):
    worker_count: int
    active_workers: int
    queue_length: int
    processing_rate: float
    error_rate: float
    ray_dashboard_url: Optional[str] = None
    ray_nodes: int = 0
    ray_resources: Dict[str, Any] = Field(default_factory=dict)
    worker_details: List[Dict[str, Any]] = Field(default_factory=list)
