# src/scheduler/rate_limiter.py
import asyncio
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class RateLimitConfig:
    max_requests: int
    time_window: float  # in seconds
    burst_size: Optional[int] = None


class TokenBucketRateLimiter:
    def __init__(self, config: RateLimitConfig):
        self.max_tokens = config.burst_size or config.max_requests
        self.tokens = self.max_tokens
        self.rate = config.max_requests / config.time_window
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self.lock:
            now = time.monotonic()
            # Add tokens based on time passed
            time_passed = now - self.last_update
            new_tokens = time_passed * self.rate
            self.tokens = min(self.max_tokens, self.tokens + new_tokens)
            self.last_update = now

            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_for_token(self) -> None:
        while not await self.acquire():
            await asyncio.sleep(1.0 / self.rate)
