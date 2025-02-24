import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api import router
from src.task_scheduler import TaskScheduler, SchedulerConfig
from src.worker import WorkerPool

logger = logging.getLogger(__name__)


# Global state management
class AppState:
    def __init__(self):
        self.scheduler: TaskScheduler = None
        self.worker_pool: WorkerPool = None
        self.is_shutting_down: bool = False


app_state = AppState()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager for the FastAPI application.
    Handles startup and shutdown events properly.
    """
    try:
        # Startup
        logger.info("Starting application services...")

        # Initialize worker pool and scheduler
        config = SchedulerConfig()  # Load from config.yaml if needed
        app_state.worker_pool = WorkerPool(
            min_workers=config.min_workers, max_workers=config.max_workers
        )
        app_state.scheduler = TaskScheduler(config)

        # Start services
        await app_state.worker_pool.start()
        await app_state.scheduler.start()

        logger.info("Application startup complete")
        yield

        # Shutdown
        logger.info("Initiating graceful shutdown...")
        app_state.is_shutting_down = True

        # Allow time for current tasks to complete (configurable timeout)
        shutdown_timeout = 30  # seconds
        try:
            await asyncio.wait_for(
                app_state.worker_pool.shutdown(), timeout=shutdown_timeout
            )
        except asyncio.TimeoutError:
            logger.warning(f"Worker pool shutdown timed out after {shutdown_timeout}s")

        # Stop scheduler
        await app_state.scheduler.stop()
        logger.info("Application shutdown complete")

    except Exception as e:
        logger.error(f"Error during application lifecycle: {str(e)}")
        raise


def create_app() -> FastAPI:
    """Creates and configures the FastAPI application"""
    app = FastAPI(
        title="Async Task Processing Service",
        description="API for submitting and managing asynchronous tasks",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router, prefix="/api/v1")
    return app


def run_app():
    """Runs the application with Uvicorn"""
    try:
        app = create_app()
        config = uvicorn.Config(
            app=app,
            host="0.0.0.0",
            port=8000,
            log_level="info",
            timeout_graceful_shutdown=30,
        )
        server = uvicorn.Server(config)
        server.run()
    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}")
        raise


if __name__ == "__main__":
    run_app()
