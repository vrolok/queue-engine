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
        target = max(self.min_workers, min(count, self.max_workers))

        # Add workers if needed
        while len(self.workers) < target:
            worker_id = f"worker-{len(self.workers) + 1}"
            self.workers[worker_id] = Worker(worker_id)
            logger.info(f"Added worker {worker_id}")

        # Remove workers if needed
        while len(self.workers) > target:
            worker_id, worker = next(
                ((id, w) for id, w in self.workers.items() if not w.is_busy),
                (None, None),
            )
            if worker_id:
                del self.workers[worker_id]
                logger.info(f"Removed worker {worker_id}")

    def get_available_worker(self) -> Optional[Worker]:
        """Get the first available worker"""
        return next(
            (worker for worker in self.workers.values() if not worker.is_busy), None
        )

    def get_worker_stats(self) -> List[Dict]:
        """Get statistics for all workers"""
        return [worker.stats for worker in self.workers.values()]
