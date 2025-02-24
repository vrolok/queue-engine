# src/scheduler/models.py
from pydantic import BaseModel, Field

class SchedulerConfig(BaseModel):
    max_concurrent_tasks: int = Field(default=10, ge=1)
    tasks_per_second: float = Field(default=5.0, ge=0.1)
    scaling_factor: float = Field(default=1.5, ge=1.0)
    min_workers: int = Field(default=1, ge=1)
    max_workers: int = Field(default=10, ge=1)

class QueueMetrics(BaseModel):
    queue_length: int = 0
    processing_rate: float = 0.0
    error_rate: float = 0.0
    average_latency: float = 0.0