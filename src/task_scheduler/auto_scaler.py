# src/scheduler/auto_scaler.py
import time
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ScalingConfig:
    min_workers: int
    max_workers: int
    scale_up_threshold: float  # queue length / worker ratio to trigger scale up
    scale_down_threshold: float  # queue length / worker ratio to trigger scale down
    cooldown_period: float  # seconds to wait between scaling operations


class AutoScaler:
    def __init__(self, config: ScalingConfig):
        self.config = config
        self.current_workers = config.min_workers
        self.last_scale_time = 0
        self.lock = asyncio.Lock()

    async def calculate_desired_workers(self, queue_length: int) -> Optional[int]:
        """Calculate the desired number of workers based on current metrics."""
        async with self.lock:
            now = time.monotonic()
            if now - self.last_scale_time < self.config.cooldown_period:
                return None

            workers_ratio = queue_length / max(1, self.current_workers)

            if workers_ratio > self.config.scale_up_threshold:
                desired = min(
                    self.config.max_workers,
                    self.current_workers + max(1, queue_length // 10),
                )
                if desired > self.current_workers:
                    self.last_scale_time = now
                    return desired

            elif workers_ratio < self.config.scale_down_threshold:
                desired = max(self.config.min_workers, self.current_workers - 1)
                if desired < self.current_workers:
                    self.last_scale_time = now
                    return desired

            return None
