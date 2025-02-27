# src/task_queue/models.py
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
from ray.actor import ActorHandle


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
    started_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    
    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert task to dictionary for Ray serialization"""
        return self.dict(exclude_none=True)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        """Create task from dictionary for Ray deserialization"""
        return cls(**data)


class FailureReason(str, Enum):
    RETRY_EXHAUSTED = "retry_exhausted"
    PERMANENT_FAILURE = "permanent_failure"
    INVALID_TASK = "invalid_task"


class DeadLetterEntry(BaseModel):
    task_id: str
    original_task: Dict[str, Any]
    failure_reason: FailureReason
    error_message: str
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count: int
    last_error_stack: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert DLQ entry to dictionary for Ray serialization"""
        return self.dict(exclude_none=True)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DeadLetterEntry":
        """Create DLQ entry from dictionary for Ray deserialization"""
        return cls(**data)
        

class RayTaskReference:
    """Reference to a task in the Ray system"""
    def __init__(
        self, 
        task_id: str,
        object_ref: Any,  # Ray ObjectRef
        worker_actor: Optional[ActorHandle] = None
    ):
        self.task_id = task_id
        self.object_ref = object_ref
        self.worker_actor = worker_actor
        self.submission_time = datetime.now(timezone.utc)
