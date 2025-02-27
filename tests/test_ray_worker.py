import asyncio
import pytest
import ray
import uuid
from unittest.mock import MagicMock, patch

from src.worker.worker import RayWorker, DeadLetterQueueActor, RateLimiterActor
from src.task_queue.models import Task, TaskStatus


@pytest.fixture(scope="module")
def ray_setup():
    """Initialize Ray for testing"""
    if not ray.is_initialized():
        ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    yield
    # Don't shut down Ray between tests to save time


@pytest.fixture
def sample_task():
    """Create a sample task for testing"""
    return Task(
        task_id=str(uuid.uuid4()),
        task_type="test_task",
        payload={"test_key": "test_value"},
        status=TaskStatus.PENDING
    )


@pytest.mark.asyncio
async def test_ray_worker_initialization(ray_setup):
    """Test RayWorker initialization"""
    # Create a worker
    worker_id = f"test-worker-{uuid.uuid4()}"
    worker = RayWorker.remote(worker_id)
    
    # Get worker status
    status = ray.get(worker.get_status.remote())
    
    # Verify initialization
    assert status["worker_id"] == worker_id
    assert status["is_busy"] is False
    assert status["tasks_processed"] == 0
    assert status["tasks_failed"] == 0
    assert status["tasks_moved_to_dlq"] == 0


@pytest.mark.asyncio
async def test_worker_process_task_success(ray_setup, sample_task):
    """Test successful task processing by a Ray worker"""
    # Create the worker
    worker = RayWorker.remote("test-worker")
    
    # Mock the task dispatcher to return a handler that succeeds
    with patch('src.worker.dispatcher.TaskDispatcher.get_handler') as mock_get_handler:
        # Create a mock handler that succeeds
        mock_handler = MagicMock()
        mock_handler.handle.return_value = None
        mock_get_handler.return_value = mock_handler
        
        # Process the task
        result = ray.get(worker.process_task.remote(sample_task.to_dict()))
        
        # Verify the result
        assert result["success"] is True
        assert result["task"]["status"] == TaskStatus.COMPLETED
        assert result["task"]["task_id"] == sample_task.task_id
        
        # Verify worker state has been updated
        status = ray.get(worker.get_status.remote())
        assert status["tasks_processed"] == 1
        assert status["tasks_failed"] == 0


@pytest.mark.asyncio
async def test_worker_process_task_failure(ray_setup, sample_task):
    """Test failed task processing by a Ray worker"""
    # Create the worker
    worker = RayWorker.remote("test-worker")
    
    # Mock the task dispatcher to return a handler that fails
    with patch('src.worker.dispatcher.TaskDispatcher.get_handler') as mock_get_handler:
        # Create a mock handler that fails
        mock_handler = MagicMock()
        mock_handler.handle.side_effect = Exception("Test failure")
        mock_get_handler.return_value = mock_handler
        
        # Process the task
        result = ray.get(worker.process_task.remote(sample_task.to_dict()))
        
        # Verify the result
        assert result["success"] is False
        assert result["task"]["status"] == TaskStatus.FAILED
        assert result["task"]["task_id"] == sample_task.task_id
        assert "error" in result
        assert result["error"]["error_message"] == "Test failure"
        
        # Verify worker state has been updated
        status = ray.get(worker.get_status.remote())
        assert status["tasks_processed"] == 0
        assert status["tasks_failed"] == 1


@pytest.mark.asyncio
async def test_dead_letter_queue_actor(ray_setup, sample_task):
    """Test the DeadLetterQueueActor functionality"""
    # Create the DLQ actor
    dlq_actor = DeadLetterQueueActor.remote()
    
    # Create a DLQ entry
    from src.task_queue.models import DeadLetterEntry, FailureReason
    dlq_entry = DeadLetterEntry(
        task_id=sample_task.task_id,
        original_task=sample_task.to_dict(),
        failure_reason=FailureReason.RETRY_EXHAUSTED,
        error_message="Test DLQ entry",
        retry_count=3,
        last_error_stack="Test stack trace"
    )
    
    # Add entry to DLQ
    success = ray.get(dlq_actor.add_entry.remote(dlq_entry.to_dict()))
    assert success is True
    
    # Retrieve all entries
    entries = ray.get(dlq_actor.get_all_entries.remote())
    assert len(entries) == 1
    assert entries[0]["task_id"] == sample_task.task_id
    
    # Retrieve specific entry
    entry = ray.get(dlq_actor.get_entry.remote(sample_task.task_id))
    assert entry["task_id"] == sample_task.task_id
    
    # Retry task
    task_dict = ray.get(dlq_actor.retry_task.remote(sample_task.task_id))
    assert task_dict["task_id"] == sample_task.task_id
    assert task_dict["status"] == TaskStatus.PENDING
    assert task_dict["retry_count"] == 0
    
    # Verify the entry has been removed
    entries = ray.get(dlq_actor.get_all_entries.remote())
    assert len(entries) == 0


@pytest.mark.asyncio
async def test_rate_limiter_actor(ray_setup):
    """Test the RateLimiterActor functionality"""
    # Create rate limiter with 5 req/sec and burst of 3
    rate_limiter = RateLimiterActor.remote(5.0, 3)
    
    # Should be able to acquire 3 tokens immediately (burst size)
    assert ray.get(rate_limiter.acquire.remote()) is True
    assert ray.get(rate_limiter.acquire.remote()) is True
    assert ray.get(rate_limiter.acquire.remote()) is True
    
    # 4th request should be denied as burst is exhausted
    assert ray.get(rate_limiter.acquire.remote()) is False
    
    # Wait for token replenishment (1 token = 0.2 seconds)
    await asyncio.sleep(0.3)
    
    # Should be able to acquire 1 token now
    assert ray.get(rate_limiter.acquire.remote()) is True
    
    # Stats should reflect usage
    stats = ray.get(rate_limiter.get_stats.remote())
    assert stats["requests_served"] == 4  # 3 initial + 1 after waiting
    assert stats["rate"] == 5.0
    assert stats["burst_size"] == 3