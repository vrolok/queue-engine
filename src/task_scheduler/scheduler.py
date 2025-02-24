# src/task_scheduler/scheduler.py
import asyncio
import logging
from src.task_queue.service import QueueService
from src.worker.pool import WorkerPool
from .models import SchedulerConfig, QueueMetrics
from .rate_limiter import TokenBucketRateLimiter, RateLimitConfig

logger = logging.getLogger(__name__)


class TaskScheduler:
    def __init__(self, config: SchedulerConfig):
        self.config = config
        self.queue_service = QueueService()
        self.worker_pool = WorkerPool(
            min_workers=config.min_workers, max_workers=config.max_workers
        )
        self.rate_limiter = TokenBucketRateLimiter(
            RateLimitConfig(
                max_requests=config.tasks_per_second,
                time_window=1.0,
                burst_size=config.max_concurrent_tasks,
            )
        )
        self.metrics = QueueMetrics()
        self.running = False

    async def start(self):
        """Start the scheduler"""
        self.running = True
        logger.info("Starting task scheduler")

        # Initialize worker pool
        await self.worker_pool.scale_to(self.config.min_workers)

        # Start main scheduling loop
        await self._scheduling_loop()

    async def _scheduling_loop(self):
        """Main scheduling loop"""
        while self.running:
            try:
                # Update metrics
                await self._update_metrics()

                # Check if we can process more tasks
                if await self.rate_limiter.acquire():
                    # Get an available worker
                    worker = self.worker_pool.get_available_worker()
                    if worker:
                        # Try to get a task
                        task = await self.queue_service.dequeue_task()
                        if task:
                            # Process the task
                            await worker.process_task(task)

                # Auto-scale workers based on metrics
                await self._auto_scale()

                # Small delay to prevent tight loop
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in scheduling loop: {str(e)}")
                await asyncio.sleep(1.0)

    async def _auto_scale(self):
        """Auto-scale workers based on queue metrics"""
        if self.metrics.queue_length > self.config.tasks_per_second * 2:
            # Scale up if queue is growing
            current_workers = len(self.worker_pool.workers)
            await self.worker_pool.scale_to(current_workers + 1)
        elif self.metrics.queue_length < self.config.tasks_per_second / 2:
            # Scale down if queue is small
            current_workers = len(self.worker_pool.workers)
            await self.worker_pool.scale_to(current_workers - 1)

    async def stop(self):
        """Stop the scheduler"""
        self.running = False
        logger.info("Stopping task scheduler")
        # Scale down to minimum workers
        await self.worker_pool.scale_to(0)
