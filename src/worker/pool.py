# src/worker/pool.py
import asyncio
import logging
from typing import Dict, Optional, List
from .worker import Worker

logger = logging.getLogger(__name__)


class WorkerPool:
    def __init__(self, min_workers: int = 1, max_workers: int = 10):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.workers: Dict[str, Worker] = {}
        self._shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Start the initial worker pool"""
        await self.scale_to(self.min_workers)

    async def shutdown(self) -> None:
        """Gracefully shutdown all workers"""
        logger.info(f"Shutting down worker pool with {len(self.workers)} workers")
        self._shutdown_event.set()

        shutdown_tasks = [worker.shutdown() for worker in self.workers.values()]

        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks)

        self.workers.clear()
        logger.info("Worker pool shutdown complete")

    async def scale_to(self, count: int) -> None:
        """Scale the worker pool to the specified count"""
        try:
            async with self._lock:
                target = max(self.min_workers, min(count, self.max_workers))

            try:
                while len(self.workers) < target:
                    worker_id = f"worker-{len(self.workers) + 1}"
                    self.workers[worker_id] = Worker(worker_id)
                    logger.info(f"Added worker {worker_id}")

                idle_workers = [(id, w) for id, w in self.workers.items() if not w.is_busy]

                while len(self.workers) > target and idle_workers:
                    worker_id, worker = idle_workers.pop(0)
                    del self.workers[worker_id]
                    logger.info(f"Removed worker {worker_id}")
            except Exception as e:
                logger.error(f"Error during worker scaling operations: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Error acquiring lock for scaling operation: {str(e)}")
            raise

    async def get_available_worker(self) -> Optional[Worker]:
        try:
            async with self._lock:
                available_worker = next(
                    (worker for worker in self.workers.values() if not worker.is_busy), None
                )
                if available_worker:
                    available_worker.is_busy = True  # Mark as busy immediately
                return available_worker
        except Exception as e:
            logger.error(f"Error while acquiring worker from pool: {str(e)}")
            return None  # Return None on failure to avoid cascading errors

    def get_worker_stats(self) -> List[Dict]:
        """Get statistics for all workers"""
        return [worker.stats for worker in self.workers.values()]
