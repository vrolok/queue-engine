import asyncio
import pytest
import ray
from unittest.mock import patch, MagicMock

from src.worker.pool import RayWorkerPool
from src.worker.worker import RayWorker, DeadLetterQueueActor


@pytest.fixture(scope="module")
def ray_setup():
    """Initialize Ray for testing"""
    if not ray.is_initialized():
        ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    yield
    # Don't shut down Ray between tests to save time


@pytest.mark.asyncio
async def test_ray_worker_pool_initialization(ray_setup):
    """Test worker pool initialization"""
    # Create worker pool
    pool = RayWorkerPool(min_workers=2, max_workers=5)
    
    # Initialize pool
    await pool.initialize_ray()
    await pool.start()
    
    # Verify initial state
    assert len(pool.workers) == 2  # Should start with min_workers
    assert pool._is_initialized is True
    
    # Clean up
    await pool.shutdown()
    assert pool._is_initialized is False
    assert len(pool.workers) == 0


@pytest.mark.asyncio
async def test_worker_pool_scaling(ray_setup):
    """Test worker pool scaling"""
    # Create worker pool
    pool = RayWorkerPool(min_workers=1, max_workers=5)
    await pool.start()
    
    # Initial state
    assert len(pool.workers) == 1
    
    # Scale up
    await pool.scale_to(3)
    assert len(pool.workers) == 3
    
    # Scale down
    await pool.scale_to(2)
    await asyncio.sleep(0.1)  # Allow time for worker pool to update
    assert len(pool.workers) == 2
    
    # Scale beyond max should limit to max
    await pool.scale_to(10)
    assert len(pool.workers) == 5  # max_workers is 5
    
    # Scale below min should maintain min
    await pool.scale_to(0)
    assert len(pool.workers) == 1  # min_workers is 1
    
    # Clean up
    await pool.shutdown()


@pytest.mark.asyncio
async def test_worker_pool_get_available_worker(ray_setup):
    """Test getting an available worker from pool"""
    # Create worker pool
    pool = RayWorkerPool(min_workers=2, max_workers=5)
    await pool.start()
    
    # Should be able to get a worker
    worker = await pool.get_available_worker()
    assert worker is not None
    
    # Clean up
    await pool.shutdown()


@pytest.mark.asyncio
async def test_worker_pool_submit_task(ray_setup):
    """Test submitting a task to the worker pool"""
    # Create worker pool
    pool = RayWorkerPool(min_workers=1, max_workers=2)
    await pool.start()
    
    # Mock rate limiter to always allow tasks
    pool.can_process_task = MagicMock(return_value=True)
    
    # Create sample task
    sample_task = {
        "task_id": "test-task-1",
        "task_type": "test_task",
        "payload": {"test_key": "test_value"},
        "status": "pending"
    }
    
    # Submit task
    result = await pool.submit_task(sample_task)
    
    # Verify submission was successful
    assert result["success"] is True
    assert "task_ref" in result
    
    # Submit another task with rate limiter failing
    pool.can_process_task = MagicMock(return_value=False)
    result = await pool.submit_task(sample_task)
    
    # Should fail due to rate limiting
    assert result["success"] is False
    assert "Rate limit exceeded" in result["error"]
    
    # Clean up
    await pool.shutdown()


@pytest.mark.asyncio
async def test_worker_pool_statistics(ray_setup):
    """Test worker pool statistics collection"""
    # Create worker pool
    pool = RayWorkerPool(min_workers=2, max_workers=5)
    await pool.start()
    
    # Get worker stats
    stats = await pool.get_worker_stats()
    
    # Verify stats
    assert len(stats) == 2  # Should have stats for both workers
    assert all("worker_id" in stat for stat in stats)
    assert all("is_busy" in stat for stat in stats)
    assert all("tasks_processed" in stat for stat in stats)
    
    # Clean up
    await pool.shutdown()


@pytest.mark.asyncio
async def test_worker_pool_rate_limiting(ray_setup):
    """Test worker pool rate limiting"""
    # Create worker pool
    pool = RayWorkerPool(min_workers=1, max_workers=2)
    await pool.start()
    
    # Initially should be able to process tasks
    can_process = await pool.can_process_task()
    assert isinstance(can_process, bool)  # May be True or False depending on rate limiter
    
    # Test with rate limiter raising an exception
    with patch.object(ray, 'get', side_effect=Exception("Test error")):
        # Should handle exceptions gracefully
        can_process = await pool.can_process_task()
        assert can_process is False
    
    # Clean up
    await pool.shutdown()