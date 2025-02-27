# src/worker/handlers.py
import asyncio
import logging
import aiohttp
import random
from typing import Callable, Dict, Any
from src.task_queue.models import Task
from src.api.models import RetryPolicy


logger = logging.getLogger(__name__)


class HttpRequestHandler:
    """
    Handler for making HTTP requests to external services.
    Supports various HTTP methods, headers, and request bodies.
    """

    SUPPORTED_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
    DEFAULT_TIMEOUT = 30  # seconds

    async def validate_payload(self, task: Task) -> None:
        """Validate the task payload has required fields for HTTP requests."""
        required_fields = ["url", "method"]
        missing_fields = [
            field for field in required_fields if field not in task.payload
        ]

        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

        method = task.payload["method"].upper()
        if method not in self.SUPPORTED_METHODS:
            raise ValueError(
                f"Unsupported HTTP method: {method}. Supported methods: {', '.join(self.SUPPORTED_METHODS)}"
            )

        # Validate URL format
        url = task.payload["url"]
        if not url.startswith(("http://", "https://")):
            raise ValueError(
                f"Invalid URL format: {url}. URL must start with http:// or https://"
            )

    async def handle(self, task: Task) -> Dict[str, Any]:
        """
        Handle HTTP request tasks by making actual HTTP requests to the specified URL.

        Args:
            task: Task object containing the request details

        Returns:
            Dict containing response information

        Raises:
            Exception: If the request fails or validation fails
        """
        logger.info(f"Processing HTTP request task: {task.task_id}")

        # Validate the payload
        await self.validate_payload(task)

        # Extract request parameters
        url = task.payload["url"]
        method = task.payload["method"].upper()
        headers = task.payload.get("headers", {})
        body = task.payload.get("body")
        timeout = task.payload.get("timeout", self.DEFAULT_TIMEOUT)

        # Log request details (excluding sensitive headers)
        safe_headers = self._get_safe_headers_for_logging(headers)
        logger.info(f"Making {method} request to {url} with headers: {safe_headers}")

        # Prepare request parameters
        request_kwargs = {
            "url": url,
            "headers": headers,
            "timeout": aiohttp.ClientTimeout(total=timeout),
        }

        # Add body if present, handling different content types
        if body is not None:
            content_type = headers.get("Content-Type", "").lower()

            if "application/json" in content_type:
                request_kwargs["json"] = body
            elif "application/x-www-form-urlencoded" in content_type:
                request_kwargs["data"] = body
            else:
                # Default to sending as JSON if content type not specified
                request_kwargs["data"] = body

        # Make the actual HTTP request
        try:
            async with aiohttp.ClientSession() as session:
                request_method = getattr(session, method.lower())

                async with request_method(**request_kwargs) as response:
                    response_body = await self._get_response_body(response)

                    result = {
                        "status_code": response.status,
                        "headers": dict(response.headers),
                        "body": response_body,
                        "url": str(response.url),
                    }

                    if 200 <= response.status < 300:
                        logger.info(f"HTTP request successful: {response.status}")
                    else:
                        logger.warning(
                            f"HTTP request returned non-success status: {response.status}"
                        )

                    return result

        except aiohttp.ClientError as e:
            logger.error(f"HTTP request failed with client error: {str(e)}")
            raise Exception(f"HTTP request failed: {str(e)}")
        except asyncio.TimeoutError:
            logger.error(f"HTTP request timed out after {timeout} seconds")
            raise Exception(f"HTTP request timed out after {timeout} seconds")
        except Exception as e:
            logger.error(f"Unexpected error during HTTP request: {str(e)}")
            raise

    async def _get_response_body(self, response: aiohttp.ClientResponse) -> Any:
        """Extract and parse response body based on content type."""
        content_type = response.headers.get("Content-Type", "").lower()

        try:
            if "application/json" in content_type:
                return await response.json()
            else:
                text = await response.text()
                return text
        except Exception as e:
            logger.warning(f"Failed to parse response body: {str(e)}")
            return await response.read()

    def _get_safe_headers_for_logging(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Create a copy of headers with sensitive information masked for logging."""
        safe_headers = headers.copy()
        sensitive_headers = ["authorization", "x-api-key", "api-key", "token"]

        for header in sensitive_headers:
            if header.lower() in map(str.lower, safe_headers.keys()):
                for key in list(safe_headers.keys()):
                    if key.lower() == header.lower():
                        safe_headers[key] = "********"

        return safe_headers


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


class RetryHandler:
    def __init__(self, retry_policy: RetryPolicy):
        self.retry_policy = retry_policy
        self.attempt = 0
        self.last_error = None

    async def execute_with_retry(
        self, task: Task, handler_func: Callable[[Task], Any]
    ) -> bool:
        """Execute a task with retry logic."""
        while self.attempt < self.retry_policy.max_attempts:
            try:
                self.attempt += 1
                await handler_func(task)
                return True

            except Exception as e:
                self.last_error = e
                delay = self.retry_policy.calculate_delay(self.attempt)

                if self.attempt >= self.retry_policy.max_attempts:
                    logger.error(
                        f"Task {task.task_id} failed permanently after "
                        f"{self.attempt} attempts. Error: {str(e)}"
                    )
                    return False

                logger.warning(
                    f"Task {task.task_id} failed (attempt {self.attempt}/"
                    f"{self.retry_policy.max_attempts}). "
                    f"Retrying in {delay:.2f} seconds. Error: {str(e)}"
                )

                await asyncio.sleep(delay)

        return False
