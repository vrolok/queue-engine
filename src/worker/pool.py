# src/worker/pool.py
import asyncio
from typing import Dict, Optional, List, Any

import ray
from ray.actor import ActorHandle
from ray.exceptions import GetTimeoutError

from src.log_handler.logging_config import get_logger

logger = get_logger(__name__)


_WORKER_POOL_INSTANCE = None


def get_ray_worker_pool():
    """Get or create the RayWorkerPool singleton instance."""
    global _WORKER_POOL_INSTANCE
    if _WORKER_POOL_INSTANCE is None:
        _WORKER_POOL_INSTANCE = RayWorkerPool()
    return _WORKER_POOL_INSTANCE


class RayWorkerPool:
    """Ray-based worker pool that manages Ray worker actors"""

    def __init__(self, min_workers: int = 1, max_workers: int = 10):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.workers: Dict[str, ActorHandle] = {}
        self.worker_status: Dict[str, Dict[str, Any]] = {}
        self.dlq_actor = None
        self.rate_limiter = None
        self._lock = asyncio.Lock()
        self._is_initialized = False
        self._is_shutting_down = False

    async def initialize_ray(self) -> None:
        from src.ray_init import initialize_ray as init_ray
        init_ray()

    async def start(self) -> None:
        """Start Ray and initialize the worker pool."""
        if self._is_initialized:
            logger.warning("Worker pool already initialized")
            return

        await self.initialize_ray()

        from src.worker.actors import DeadLetterQueueActor, RateLimiterActor

        # Create DLQ actor
        self.dlq_actor = DeadLetterQueueActor.remote()
        logger.info("DLQ actor created")

        # Create rate limiter actor
        self.rate_limiter = RateLimiterActor.remote(
            10.0, 20
        )  # Default 10 tasks/sec with burst of 20
        logger.info("Rate limiter actor created")

        # Create initial workers
        await self.scale_to(self.min_workers)

        self._is_initialized = True
        self._is_shutting_down = False
        logger.info(f"Ray worker pool started with {self.min_workers} workers")

    async def shutdown(self) -> None:
        """Gracefully shutdown Ray workers."""
        if not self._is_initialized or self._is_shutting_down:
            return

        logger.info(f"Shutting down Ray worker pool with {len(self.workers)} workers")
        self._is_shutting_down = True

        # Clear worker references
        self.workers.clear()
        self.worker_status.clear()

        # Don't actually shut down Ray - it's a shared resource
        # and will be shut down by the application

        self._is_initialized = False
        logger.info("Ray worker pool shutdown complete")

    async def scale_to(self, count: int) -> None:
        """Scale the worker pool to the specified count."""
        async with self._lock:
            target = max(self.min_workers, min(count, self.max_workers))
            current_count = len(self.workers)

            if target == current_count:
                return

            try:
                if target > current_count:
                    # Import worker class at runtime to avoid circular imports
                    from src.worker.worker import RayWorker

                    # Scale up
                    for i in range(current_count + 1, target + 1):
                        worker_id = f"worker-{i}"
                        worker = RayWorker.remote(worker_id, self.dlq_actor)
                        self.workers[worker_id] = worker

                        # Get initial status
                        status = ray.get(worker.get_status.remote())
                        self.worker_status[worker_id] = status

                        logger.info(f"Added Ray worker {worker_id}")
                else:
                    # Scale down - remove idle workers
                    workers_to_remove = []
                    for worker_id, worker in self.workers.items():
                        try:
                            status = ray.get(worker.get_status.remote(), timeout=0.5)
                            self.worker_status[worker_id] = status

                            if not status["is_busy"]:
                                workers_to_remove.append(worker_id)
                                if len(self.workers) - len(workers_to_remove) <= target:
                                    break
                        except GetTimeoutError:
                            # If worker doesn't respond, mark for removal
                            workers_to_remove.append(worker_id)

                    # Remove workers
                    for worker_id in workers_to_remove:
                        if worker_id in self.workers:
                            del self.workers[worker_id]
                        if worker_id in self.worker_status:
                            del self.worker_status[worker_id]
                        logger.info(f"Removed Ray worker {worker_id}")

                        if len(self.workers) <= target:
                            break

                logger.info(f"Scaled Ray worker pool to {len(self.workers)} workers")

            except Exception as e:
                logger.error(f"Error during worker scaling operations: {str(e)}")
                raise

    async def get_available_worker(self) -> Optional[ActorHandle]:
        """Get an available worker from the pool."""
        if self._is_shutting_down:
            return None

        try:
            async with self._lock:
                # Update worker status
                for worker_id, worker in list(self.workers.items()):
                    try:
                        status = ray.get(worker.get_status.remote(), timeout=0.5)
                        self.worker_status[worker_id] = status
                    except GetTimeoutError:
                        logger.warning(f"Worker {worker_id} not responding")
                        continue

                # Find available worker
                for worker_id, status in self.worker_status.items():
                    if not status["is_busy"] and worker_id in self.workers:
                        worker = self.workers[worker_id]
                        # Don't mark as busy here - Ray will do that
                        return worker

                return None

        except Exception as e:
            logger.error(f"Error getting available worker: {str(e)}")
            return None

    async def can_process_task(self) -> bool:
        """Check if a task can be processed based on rate limits."""
        if self._is_shutting_down or not self.rate_limiter:
            return False

        try:
            return ray.get(self.rate_limiter.acquire.remote())
        except Exception as e:
            logger.error(f"Error checking rate limiter: {str(e)}")
            return False

    async def get_worker_stats(self) -> List[Dict]:
        """Get statistics for all workers."""
        try:
            if not self.workers:
                return []

            # Update worker status
            for worker_id, worker in list(self.workers.items()):
                try:
                    status = ray.get(worker.get_status.remote(), timeout=0.5)
                    self.worker_status[worker_id] = status
                except GetTimeoutError:
                    logger.warning(f"Worker {worker_id} not responding")
                    continue

            return list(self.worker_status.values())

        except Exception as e:
            logger.error(f"Error getting worker stats: {str(e)}")
            return []

    async def submit_task(self, task_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a task for processing and return a future."""
        if self._is_shutting_down:
            return {"success": False, "error": "Worker pool is shutting down"}

        # Get available worker
        worker = await self.get_available_worker()
        if not worker:
            return {"success": False, "error": "No available workers"}

        # Check rate limiter
        if not await self.can_process_task():
            return {"success": False, "error": "Rate limit exceeded"}

        try:
            # Submit task to Ray worker (non-blocking)
            ref = worker.process_task.remote(task_dict)
            return {"success": True, "task_ref": ref}
        except Exception as e:
            logger.error(f"Error submitting task: {str(e)}")
            return {"success": False, "error": str(e)}