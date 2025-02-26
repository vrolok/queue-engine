# src/worker/exceptions.py

class WorkerError(Exception):
    """Base exception for worker operations"""
    pass


class TaskHandlerNotFoundError(WorkerError):
    """Raised when no handler is found for a task type"""
    pass


class WorkerBusyError(WorkerError):
    """Raised when trying to assign a task to a busy worker"""
    pass


class WorkerPoolError(WorkerError):
    """Raised when an error occurs in the worker pool"""
    pass