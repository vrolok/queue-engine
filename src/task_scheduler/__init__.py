# src/task_scheduler/__init__.py
from .scheduler import TaskScheduler
from .models import SchedulerConfig, QueueMetrics
from .rate_limiter import TokenBucketRateLimiter, RateLimitConfig
from .auto_scaler import AutoScaler, ScalingConfig

__all__ = [
    'TaskScheduler',
    'SchedulerConfig',
    'QueueMetrics',
    'TokenBucketRateLimiter',
    'RateLimitConfig',
    'AutoScaler',
    'ScalingConfig'
]

__version__ = '1.0.0'