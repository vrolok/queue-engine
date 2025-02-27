import pytest
import ray
import asyncio
import uuid
from http import HTTPStatus
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock, MagicMock

from src.main import create_app
from src.task_queue.models import Task, TaskStatus
from src.api.models import TaskType


@pytest.fixture(scope="module")
def ray_setup():
    """Initialize Ray for testing"""
    if not ray.is_initialized():
        ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    yield
    # Don't shut down Ray between tests to save time


@pytest.fixture
def test_client(ray_setup):
    """Create a test client for the application"""
    # Create app but patch the initialize_ray method to avoid double initialization
    with patch('src.worker.pool.RayWorkerPool.initialize_ray', AsyncMock(return_value=None)):
        with patch('src.worker.pool.RayWorkerPool.start', AsyncMock(return_value=None)):
            app = create_app()
            # Mock app state to avoid actual worker pool and scheduler interactions
            app.state.worker_pool = MagicMock()
            app.state.scheduler = MagicMock(get_stats=AsyncMock(return_value={
                "worker_count": 2,
                "active_workers": 1,
                "queue_length": 0,
                "processing_rate": 1.0,
                "error_rate": 0.0
            }))
            yield TestClient(app)


def test_submit_task(test_client):
    """Test task submission endpoint"""
    # Mock QueueService.enqueue_task to avoid actual task submission
    with patch('src.task_queue.service.QueueService.enqueue_task', AsyncMock(return_value=Task(
        task_id="test-task-id",
        task_type=TaskType.HTTP_REQUEST,
        payload={"url": "https://example.com"},
        status=TaskStatus.PENDING
    ))):
        # Make request
        response = test_client.post("/api/v1/tasks", json={
            "task_type": "http_request",
            "payload": {"url": "https://example.com"},
            "retry_policy": {"max_attempts": 3}
        })
        
        # Verify response
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["status"] == "pending"
        assert "task_id" in data


def test_task_validation(test_client):
    """Test task validation"""
    # HTTP request without URL should fail validation
    response = test_client.post("/api/v1/tasks", json={
        "task_type": "http_request",
        "payload": {"method": "GET"}  # Missing url
    })
    
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


def test_get_task(test_client):
    """Test getting task status endpoint"""
    # Mock QueueService.get_task to return a test task
    task_id = "test-task-id"
    with patch('src.task_queue.service.QueueService.get_task', AsyncMock(return_value=Task(
        task_id=task_id,
        task_type=TaskType.HTTP_REQUEST,
        payload={"url": "https://example.com"},
        status=TaskStatus.PROCESSING
    ))):
        # Make request
        response = test_client.get(f"/api/v1/tasks/{task_id}")
        
        # Verify response
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["task_id"] == task_id
        assert data["status"] == "processing"


def test_list_tasks(test_client):
    """Test listing tasks endpoint"""
    # Mock QueueService.get_all_tasks to return test tasks
    with patch('src.task_queue.service.QueueService.get_all_tasks', AsyncMock(return_value=[
        Task(
            task_id="task-1",
            task_type=TaskType.HTTP_REQUEST,
            payload={"url": "https://example.com"},
            status=TaskStatus.COMPLETED
        ),
        Task(
            task_id="task-2",
            task_type=TaskType.HTTP_REQUEST,
            payload={"url": "https://example.org"},
            status=TaskStatus.PROCESSING
        )
    ])):
        # Make request
        response = test_client.get("/api/v1/tasks")
        
        # Verify response
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert len(data) == 2
        assert any(task["task_id"] == "task-1" for task in data)
        assert any(task["task_id"] == "task-2" for task in data)


def test_retry_task(test_client):
    """Test task retry endpoint"""
    # Mock QueueService.retry_dlq_task to succeed
    task_id = "test-task-id"
    with patch('src.task_queue.service.QueueService.retry_dlq_task', AsyncMock(return_value=Task(
        task_id=task_id,
        task_type=TaskType.HTTP_REQUEST,
        payload={"url": "https://example.com"},
        status=TaskStatus.PENDING
    ))):
        # Make request
        response = test_client.post(f"/api/v1/tasks/{task_id}/retry")
        
        # Verify response
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["task_id"] == task_id
        assert data["status"] == "pending"


def test_get_system_status(test_client):
    """Test system status endpoint"""
    # Scheduler.get_stats is already mocked in the fixture
    
    # Make request
    response = test_client.get("/api/v1/system/status")
    
    # Verify response
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data["worker_count"] == 2
    assert data["active_workers"] == 1
    assert data["queue_length"] == 0
    assert data["processing_rate"] == 1.0
    assert data["error_rate"] == 0.0