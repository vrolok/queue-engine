import asyncio
import pytest
import ray
import uuid
from unittest.mock import patch, MagicMock, AsyncMock

from src.task_queue.service import RayTaskService, QueueService
from src.task_queue.models import Task, TaskStatus, DeadLetterEntry, FailureReason


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
async def test_ray_task_service_initialization(ray_setup):
    """Test RayTaskService initialization"""
    service = RayTaskService()
    
    # Initialize service
    await service.initialize()
    
    # Verify state
    assert service.worker_pool is not None
    assert service.worker_pool._is_initialized is True
    
    # Worker pool should have been started
    assert len(service.worker_pool.workers) > 0


@pytest.mark.asyncio
async def test_submit_task(ray_setup, sample_task):
    """Test submitting a task through the Ray task service"""
    service = RayTaskService()
    await service.initialize()
    
    # Mock successful submission
    mock_submit_result = {"success": True, "task_ref": MagicMock()}
    with patch.object(service.worker_pool, 'submit_task', AsyncMock(return_value=mock_submit_result)):
        # Submit task
        result = await service.submit_task(sample_task)
        
        # Verify task was submitted
        assert result.task_id == sample_task.task_id
        assert result.status == TaskStatus.PENDING
        assert service.worker_pool.submit_task.called
        assert sample_task.task_id in service.task_refs
    
    # Mock failed submission
    mock_error_result = {"success": False, "error": "Test error"}
    with patch.object(service.worker_pool, 'submit_task', AsyncMock(return_value=mock_error_result)):
        # Submit task
        result = await service.submit_task(sample_task)
        
        # Verify error handling
        assert result.task_id == sample_task.task_id
        assert result.status == TaskStatus.FAILED
        assert "Test error" in result.error_message
        assert service.worker_pool.submit_task.called


@pytest.mark.asyncio
async def test_get_task_status(ray_setup, sample_task):
    """Test getting task status"""
    service = RayTaskService()
    
    # Mock task reference
    mock_ref = MagicMock()
    service.task_refs[sample_task.task_id] = MagicMock(object_ref=mock_ref)
    
    # Mock Ray wait with completed task
    with patch.object(ray, 'wait', return_value=([mock_ref], [])):
        # Mock Ray get with success result
        mock_result = {"success": True, "task": sample_task.to_dict()}
        with patch.object(ray, 'get', return_value=mock_result):
            status = await service.get_task_status(sample_task.task_id)
            assert status == mock_result
            # Task ref should be removed after completion
            assert sample_task.task_id not in service.task_refs
    
    # Test non-existent task
    status = await service.get_task_status("non-existent")
    assert status is None
    
    # Test in-progress task
    service.task_refs[sample_task.task_id] = MagicMock(object_ref=mock_ref)
    with patch.object(ray, 'wait', return_value=([], [mock_ref])):
        status = await service.get_task_status(sample_task.task_id)
        assert status["task_id"] == sample_task.task_id
        assert status["in_progress"] is True


@pytest.mark.asyncio
async def test_get_all_task_statuses(ray_setup, sample_task):
    """Test getting all task statuses"""
    service = RayTaskService()
    
    # Create multiple task references
    task1_id = f"task-{uuid.uuid4()}"
    task2_id = f"task-{uuid.uuid4()}"
    service.task_refs[task1_id] = MagicMock(object_ref=MagicMock())
    service.task_refs[task2_id] = MagicMock(object_ref=MagicMock())
    
    # Mock get_task_status to return predetermined results
    mock_status1 = {"task_id": task1_id, "status": "processing", "in_progress": True}
    mock_status2 = {"task_id": task2_id, "status": "completed", "in_progress": False}
    
    with patch.object(service, 'get_task_status', AsyncMock(side_effect=[mock_status1, mock_status2])):
        statuses = await service.get_all_task_statuses()
        
        # Should have status for both tasks
        assert len(statuses) == 2
        assert any(s["task_id"] == task1_id for s in statuses)
        assert any(s["task_id"] == task2_id for s in statuses)


@pytest.mark.asyncio
async def test_queue_service_integration(ray_setup, sample_task):
    """Test QueueService integration with RayTaskService"""
    # Initialize queue service
    queue_service = QueueService()
    
    # Mock RayTaskService methods
    with patch.object(queue_service._ray_service, 'submit_task', AsyncMock(return_value=sample_task)):
        result = await queue_service.enqueue_task(sample_task)
        assert result.task_id == sample_task.task_id
        assert queue_service._ray_service.submit_task.called
    
    # Test dequeue_task (should be no-op)
    result = await queue_service.dequeue_task()
    assert result is None
    
    # Test get_task with mock task status
    mock_status = {"task": sample_task.to_dict()}
    with patch.object(queue_service._ray_service, 'get_task_status', AsyncMock(return_value=mock_status)):
        result = await queue_service.get_task(sample_task.task_id)
        assert result.task_id == sample_task.task_id
        assert queue_service._ray_service.get_task_status.called


@pytest.mark.asyncio
async def test_dlq_operations(ray_setup):
    """Test DLQ operations through the service layer"""
    service = RayTaskService()
    await service.initialize()
    
    # Create DLQ entry
    task_id = f"test-task-{uuid.uuid4()}"
    dlq_entry = DeadLetterEntry(
        task_id=task_id,
        original_task={"task_id": task_id, "task_type": "test", "payload": {}},
        failure_reason=FailureReason.RETRY_EXHAUSTED,
        error_message="Test error",
        retry_count=3
    )
    
    # Mock DLQ actor methods
    mock_entries = [dlq_entry.to_dict()]
    mock_task = {"task_id": task_id, "status": "pending", "task_type": "test", "payload": {}}
    
    with patch.object(ray, 'get', side_effect=[mock_entries, mock_task]):
        # Get DLQ entries
        entries = await service.get_dlq_entries()
        assert len(entries) == 1
        assert entries[0]["task_id"] == task_id
        
        # Retry DLQ task
        with patch.object(service, 'submit_task', AsyncMock(return_value=None)):
            result = await service.retry_dlq_task(task_id)
            assert result is not None
            assert result.task_id == task_id
            assert service.submit_task.called