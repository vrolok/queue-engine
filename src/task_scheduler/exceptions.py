# src/scheduler/exceptions.py
class SchedulerError(Exception):
    """Base exception for scheduler errors"""
    pass

class RateLimitExceededError(SchedulerError):
    """Raised when rate limit is exceeded"""
    pass