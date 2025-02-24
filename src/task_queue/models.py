# src/queue/models.py
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class Task(BaseModel):
    task_id: str
    task_type: str
    payload: Dict[str, Any]
    retry_count: int = 0
    max_retries: int = 3
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.now(datetime.timezone.utc))
    updated_at: datetime = Field(default_factory=datetime.now(datetime.timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class FailureReason(str, Enum):
    RETRY_EXHAUSTED = "retry_exhausted"
    PERMANENT_FAILURE = "permanent_failure"
    INVALID_TASK = "invalid_task"


class DeadLetterEntry(BaseModel):
    task_id: str
    original_task: Dict[str, Any]
    failure_reason: FailureReason
    error_message: str
    failed_at: datetime = Field(default_factory=datetime.utcnow)
    retry_count: int
    last_error_stack: Optional[str] = None
