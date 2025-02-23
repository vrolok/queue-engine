# src/worker/handlers.py
import asyncio
import logging
import random
from typing import Dict, Any
from datetime import datetime
import aiohttp
from src.queue.models import Task, TaskStatus

logger = logging.getLogger(__name__)


class BaseTaskHandler:
    """Base class for all task handlers with common functionality"""

    async def simulate_processing_time(
        self, min_seconds: float = 0.1, max_seconds: float = 2.0
    ) -> None:
        """Simulate processing time to mimic real-world operations"""
        processing_time = random.uniform(min_seconds, max_seconds)
        await asyncio.sleep(processing_time)

    async def validate_payload(self, task: Task, required_fields: list) -> None:
        """Validate that all required fields are present in the payload"""
        missing_fields = [
            field for field in required_fields if field not in task.payload
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


class HttpRequestHandler(BaseTaskHandler):
    """Handler for HTTP request tasks"""

    async def handle(self, task: Task) -> None:
        logger.info(f"Processing HTTP request task: {task.task_id}")

        # Validate payload
        await self.validate_payload(task, ["url", "method"])

        url = task.payload["url"]
        method = task.payload.get("method", "GET")
        headers = task.payload.get("headers", {})
        body = task.payload.get("body")

        # Simulate network latency
        await self.simulate_processing_time(0.5, 3.0)

        try:
            # Simulate HTTP request
            if random.random() < 0.1:  # 10% chance of failure
                raise Exception("Simulated network error")

            logger.info(f"Simulated {method} request to {url}")

            # Simulate successful response
            response_status = random.choice([200, 201, 204])
            logger.info(f"Request completed with status {response_status}")

        except Exception as e:
            logger.error(f"HTTP request failed: {str(e)}")
            raise


class BackgroundProcessingHandler(BaseTaskHandler):
    """Handler for background processing tasks"""

    async def handle(self, task: Task) -> None:
        logger.info(f"Processing background task: {task.task_id}")

        # Validate payload
        await self.validate_payload(task, ["process_type", "data"])

        process_type = task.payload["process_type"]
        data = task.payload["data"]

        try:
            # Simulate processing time based on data size
            data_size = len(str(data))
            processing_time = min(data_size / 1000, 5.0)  # Max 5 seconds
            await self.simulate_processing_time(processing_time, processing_time * 1.5)

            # Simulate processing steps
            steps = ["initializing", "processing", "finalizing"]
            for step in steps:
                logger.info(f"Background processing step: {step}")
                await asyncio.sleep(0.5)

            # Simulate processing result
            if random.random() < 0.05:  # 5% chance of failure
                raise Exception("Simulated processing error")

            logger.info(f"Background processing completed for type: {process_type}")

        except Exception as e:
            logger.error(f"Background processing failed: {str(e)}")
            raise


class TextProcessingHandler(BaseTaskHandler):
    """Handler for text processing tasks"""

    async def handle(self, task: Task) -> None:
        logger.info(f"Processing text task: {task.task_id}")

        # Validate payload
        await self.validate_payload(task, ["text", "operation"])

        text = task.payload["text"]
        operation = task.payload["operation"]

        try:
            # Simulate text processing
            await self.simulate_processing_time(0.2, 1.0)

            # Simulate different text operations
            if operation == "analyze":
                logger.info(f"Analyzing text of length: {len(text)}")
                # Simulate text analysis
                await asyncio.sleep(0.5)

            elif operation == "transform":
                logger.info("Transforming text")
                # Simulate text transformation
                await asyncio.sleep(0.3)

            else:
                raise ValueError(f"Unsupported text operation: {operation}")

            # Simulate processing error
            if random.random() < 0.08:  # 8% chance of failure
                raise Exception("Simulated text processing error")

            logger.info(f"Text processing completed for operation: {operation}")

        except Exception as e:
            logger.error(f"Text processing failed: {str(e)}")
            raise


class TaskHandlerFactory:
    """Factory class for creating task handlers"""

    _handlers = {
        "http_request": HttpRequestHandler,
        "background_processing": BackgroundProcessingHandler,
        "text_processing": TextProcessingHandler,
    }

    @classmethod
    def get_handler(cls, task_type: str) -> BaseTaskHandler:
        handler_class = cls._handlers.get(task_type)
        if not handler_class:
            raise ValueError(f"No handler found for task type: {task_type}")
        return handler_class()
