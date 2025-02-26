import asyncio
import logging
import time
from typing import Optional

from src.task_queue.service import QueueService
from src.worker.pool import WorkerPool
from .models import SchedulerConfig, QueueMetrics
from .rate_limiter import TokenBucketRateLimiter, RateLimitConfig
from .auto_scaler import AutoScaler, ScalingConfig

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
        self.auto_scaler = AutoScaler(
            ScalingConfig(
                min_workers=config.min_workers,
                max_workers=config.max_workers,
                scale_up_threshold=1.5,  # Scale up when queue length > 1.5x workers
                scale_down_threshold=0.5,  # Scale down when queue length < 0.5x workers
                cooldown_period=30.0,  # 30 seconds between scaling operations
            )
        )
        self.metrics = QueueMetrics()
        self.last_metrics_update = 0
        self.metrics_update_interval = 5.0
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.processing_times = []
        self.max_processing_times_history = 100
        self.last_scaling_time = 0
        self.scaling_cooldown = 5.0
        self.running = False

    async def start(self):
        """Start the task scheduler and worker pool."""
        self.running = True
        logger.info("Starting task scheduler")

        # Initialize worker pool
        await self.worker_pool.start()

        # Start scheduling loop in a background task
        asyncio.create_task(self._scheduling_loop())
        logger.info("Task scheduler started successfully")

    async def _update_metrics(self) -> None:
        """Update queue and processing metrics."""
        now = time.monotonic()

        # Only update metrics at the specified interval
        if now - self.last_metrics_update < self.metrics_update_interval:
            return

        try:
            # Get current queue size
            queue_length = await self.queue_service.get_queue_size()

            # Calculate processing rate (tasks per second)
            processing_rate = self.tasks_processed / self.metrics_update_interval

            # Calculate error rate
            error_rate = 0
            if self.tasks_processed > 0:
                error_rate = self.tasks_failed / self.tasks_processed

            # Calculate average latency
            avg_latency = 0
            if self.processing_times:
                avg_latency = sum(self.processing_times) / len(self.processing_times)

            # Update metrics object
            self.metrics = QueueMetrics(
                queue_length=queue_length,
                processing_rate=processing_rate,
                error_rate=error_rate,
                average_latency=avg_latency,
            )

            # Reset counters
            self.tasks_processed = 0
            self.tasks_failed = 0
            self.last_metrics_update = now

            # Log metrics
            logger.debug(f"Queue metrics: {self.metrics}")

        except Exception as e:
            logger.error(f"Error updating metrics: {str(e)}")

    async def _scheduling_loop(self):
        """Main scheduling loop for processing tasks."""
        while self.running:
            try:
                await self._update_metrics()

                if await self.rate_limiter.acquire():
                    worker = self.worker_pool.get_available_worker()
                    if worker:
                        task = None
                        try:
                            task = await self.queue_service.dequeue_task()
                            if task:
                                start_time = time.monotonic()
                                await worker.process_task(task)
                                self.tasks_processed += 1
                                processing_time = time.monotonic() - start_time
                                self._record_processing_time(processing_time)
                        except Exception as e:
                            if task:
                                logger.error(
                                    f"Error processing task {task.task_id} by worker {worker.worker_id}: {str(e)}"
                                )
                                self.tasks_failed += 1
                        finally:
                            # Always reset worker state
                            worker.is_busy = False
                            worker.current_task = None

                await self._auto_scale()
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in scheduling loop: {str(e)}")
                await asyncio.sleep(1.0)

    def _record_processing_time(self, processing_time: float) -> None:
        """Record task processing time for metrics calculation."""
        self.processing_times.append(processing_time)
        # Keep processing times history limited
        if len(self.processing_times) > self.max_processing_times_history:
            self.processing_times = self.processing_times[
                -self.max_processing_times_history :
            ]

    async def _auto_scale(self) -> None:
        """Auto-scale the worker pool based on queue metrics."""
        try:
            current_time = time.monotonic()
            # Cooldown check
            if current_time - self.last_scaling_time < self.scaling_cooldown:
                return

            # Get desired worker count from auto-scaler
            desired_workers = await self.auto_scaler.calculate_desired_workers(
                self.metrics.queue_length
            )

            # Scale worker pool if needed
            if desired_workers is not None:
                current_workers = len(self.worker_pool.workers)
                if desired_workers != current_workers:
                    logger.info(
                        f"Auto-scaling worker pool from {current_workers} to {desired_workers} workers"
                    )
                    await self.worker_pool.scale_to(desired_workers)
                    self.last_scaling_time = current_time

        except Exception as e:
            logger.error(f"Error in auto-scaling: {str(e)}")

    async def stop(self) -> None:
        """Stop the task scheduler and worker pool."""
        logger.info("Stopping task scheduler")
        self.running = False

        # Wait a moment for the scheduling loop to notice we're stopping
        await asyncio.sleep(0.5)

        # Scale down to zero workers
        await self.worker_pool.scale_to(0)
        logger.info("Task scheduler stopped")

    async def get_stats(self) -> dict:
        """Get scheduler statistics."""
        return {
            "queue_length": self.metrics.queue_length,
            "processing_rate": self.metrics.processing_rate,
            "error_rate": self.metrics.error_rate,
            "average_latency": self.metrics.average_latency,
            "worker_count": len(self.worker_pool.workers),
            "active_workers": sum(
                1 for w in self.worker_pool.workers.values() if w.is_busy
            ),
        }
