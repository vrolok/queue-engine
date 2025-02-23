# src/api/models.py
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


class TaskSubmission(BaseModel):
    task_type: TaskType
    payload: Dict[str, Any]
    retry_policy: Optional[RetryPolicy] = None


class TaskResponse(BaseModel):
    task_id: str
    status: str = "pending"
    message: str
