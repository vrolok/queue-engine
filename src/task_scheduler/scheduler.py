import asyncio
import logging
import time
import ray
from typing import Optional, Dict, Any, List

from src.task_queue.service import QueueService, RayTaskService
from src.worker.pool import RayWorkerPool, WorkerPool
from .models import SchedulerConfig, QueueMetrics
from .rate_limiter import TokenBucketRateLimiter, RateLimitConfig
from .auto_scaler import AutoScaler, ScalingConfig

logger = logging.getLogger(__name__)


class TaskScheduler:
    def __init__(self, config: SchedulerConfig):
        self.config = config
        self.queue_service = QueueService()
        self.ray_task_service = RayTaskService()
        
        # Use Ray worker pool directly
        self.worker_pool = RayWorkerPool(
            min_workers=config.min_workers, max_workers=config.max_workers
        )
        
        # Note: Rate limiting is now handled by the RateLimiterActor in Ray
        # The legacy rate limiter is kept for backward compatibility
        self.rate_limiter = TokenBucketRateLimiter(
            RateLimitConfig(
                max_requests=config.tasks_per_second,
                time_window=1.0,
                burst_size=config.max_concurrent_tasks,
            )
        )
        
        # Auto-scaling is handled by Ray but we keep the interface for metrics
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
        self.processing_times = []
        self.max_processing_times_history = 100
        self.last_scaling_time = 0
        self.scaling_cooldown = 5.0
        self.running = False
        logger.info("Ray-powered TaskScheduler initialized")

    async def start(self):
        """Start the task scheduler with Ray integration."""
        self.running = True
        logger.info("Starting Ray-powered task scheduler")

        # Initialize ray worker pool
        await self.worker_pool.start()

        # Start metrics collection and auto-scaling loop
        asyncio.create_task(self._metrics_loop())
        logger.info("Ray-powered task scheduler started successfully")

    async def _metrics_loop(self):
        """Loop for collecting metrics and auto-scaling."""
        while self.running:
            try:
                await self._update_metrics()
                await self._auto_scale()
                await asyncio.sleep(self.metrics_update_interval)
            except Exception as e:
                logger.error(f"Error in metrics loop: {str(e)}")
                await asyncio.sleep(1.0)

    async def _update_metrics(self) -> None:
        """Update queue and processing metrics using Ray data."""
        now = time.monotonic()

        # Only update metrics at the specified interval
        if now - self.last_metrics_update < self.metrics_update_interval:
            return

        try:
            # Get Ray worker stats
            worker_stats = await self.worker_pool.get_worker_stats()
            
            # Get current queue size from Ray task service
            queue_length = await self.queue_service.get_queue_size()
            
            # Calculate metrics from worker stats
            tasks_processed = sum(stats.get("tasks_processed", 0) for stats in worker_stats)
            tasks_failed = sum(stats.get("tasks_failed", 0) for stats in worker_stats)
            active_workers = sum(1 for stats in worker_stats if stats.get("is_busy", False))
            
            # Calculate processing rate using Ray metrics
            processing_rate = tasks_processed / self.metrics_update_interval if tasks_processed > 0 else 0
            
            # Calculate error rate
            error_rate = tasks_failed / tasks_processed if tasks_processed > 0 else 0
            
            # For latency, we use the Ray worker processing times if available
            # Otherwise, we estimate from our tracking
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

            self.last_metrics_update = now

            # Log metrics
            logger.debug(f"Ray queue metrics: {self.metrics}")

        except Exception as e:
            logger.error(f"Error updating Ray metrics: {str(e)}")

    async def _auto_scale(self) -> None:
        """Auto-scale the Ray worker pool based on queue metrics."""
        try:
            current_time = time.monotonic()
            # Cooldown check
            if current_time - self.last_scaling_time < self.scaling_cooldown:
                return

            # Get desired worker count from auto-scaler
            desired_workers = await self.auto_scaler.calculate_desired_workers(
                self.metrics.queue_length
            )

            # Scale Ray worker pool if needed
            if desired_workers is not None:
                current_workers = len(self.worker_pool.workers)
                if desired_workers != current_workers:
                    logger.info(
                        f"Auto-scaling Ray worker pool from {current_workers} to {desired_workers} workers"
                    )
                    await self.worker_pool.scale_to(desired_workers)
                    self.last_scaling_time = current_time

        except Exception as e:
            logger.error(f"Error in Ray auto-scaling: {str(e)}")

    async def stop(self) -> None:
        """Stop the Ray task scheduler and worker pool."""
        logger.info("Stopping Ray task scheduler")
        self.running = False

        # Wait a moment for loops to notice we're stopping
        await asyncio.sleep(0.5)

        # Scale down Ray workers
        await self.worker_pool.scale_to(0)
        logger.info("Ray task scheduler stopped")

    async def get_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics including Ray metrics."""
        # Get Ray cluster resources
        try:
            ray_resources = None
            if ray.is_initialized():
                ray_resources = ray.available_resources()
        except Exception as e:
            logger.error(f"Error getting Ray resources: {str(e)}")
            ray_resources = {"error": str(e)}

        # Get worker stats
        worker_stats = await self.worker_pool.get_worker_stats()
        
        return {
            "queue_length": self.metrics.queue_length,
            "processing_rate": self.metrics.processing_rate,
            "error_rate": self.metrics.error_rate,
            "average_latency": self.metrics.average_latency,
            "worker_count": len(self.worker_pool.workers),
            "active_workers": sum(1 for w in worker_stats if w.get("is_busy", False)),
            "ray_resources": ray_resources,
            "worker_details": worker_stats[:5]  # Include first 5 worker details
        }
    
    async def get_ray_dashboard_url(self) -> Optional[str]:
        """Get the URL for the Ray dashboard if available."""
        if ray.is_initialized():
            try:
                return ray.get_dashboard_url()
            except Exception as e:
                logger.error(f"Error getting Ray dashboard URL: {str(e)}")
        return None
