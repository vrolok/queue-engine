# src/queue/exceptions.py
class QueueError(Exception):
    """Base exception for queue operations"""

    pass


class QueueFullError(QueueError):
    """Raised when the queue is full"""

    pass


class QueueEmptyError(QueueError):
    """Raised when trying to dequeue from an empty queue"""

    pass


class TaskNotFoundError(QueueError):
    """Raised when a task is not found"""

    pass
