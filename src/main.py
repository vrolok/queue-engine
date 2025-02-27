import asyncio
import os
from contextlib import asynccontextmanager

import uvicorn
import ray
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api import router
from src.task_scheduler import TaskScheduler, SchedulerConfig
from src.worker.pool import RayWorkerPool
from src.log_handler.logging_config import setup_logging, get_logger, shutdown_logging


# Initialize centralized logging
log_listener = setup_logging(
    log_file="logs/queue_engine.log",
    module_levels={
        "src.worker": os.environ.get("WORKER_LOG_LEVEL", "INFO"),
        "ray": os.environ.get("RAY_LOG_LEVEL", "WARNING"),
    }
)
logger = get_logger(__name__)

# Global state management
class AppState:
    def __init__(self):
        self.scheduler: TaskScheduler = None
        self.worker_pool: RayWorkerPool = None
        self.is_shutting_down: bool = False


app_state = AppState()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager for the FastAPI application.
    Handles startup and shutdown events properly with Ray integration.
    """
    try:
        # Startup
        logger.info("Starting application services with Ray...")

        # Initialize Ray
        if not ray.is_initialized():
            ray_address = os.environ.get("RAY_ADDRESS", None)
            if ray_address:
                # Connect to existing Ray cluster
                ray.init(address=ray_address)
                logger.info(f"Connected to Ray cluster at {ray_address}")
            else:
                # Start local Ray instance
                ray.init(ignore_reinit_error=True)
                logger.info("Started local Ray instance")

        # Initialize worker pool and scheduler
        config = SchedulerConfig()  # Load from config.yaml if needed
        app_state.worker_pool = RayWorkerPool(
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

        # Don't shutdown Ray here - it may be being used by other services
        # Ray lifecycle is managed separately
        
        # Shutdown logging system
        shutdown_logging()

    except Exception as e:
        logger.error(f"Error during application lifecycle: {str(e)}")
        raise


def create_app() -> FastAPI:
    """Creates and configures the FastAPI application"""
    app = FastAPI(
        title="Ray-powered Task Processing Service",
        description="API for submitting and managing distributed tasks with Ray",
        version="2.0.0",
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
    
    # Add Ray dashboard URL to application info
    @app.get("/ray-dashboard")
    async def ray_dashboard():
        """Get Ray dashboard URL"""
        try:
            dashboard_url = ray.get_dashboard_url()
            return {"dashboard_url": dashboard_url}
        except Exception as e:
            return {"error": f"Ray dashboard not available: {str(e)}"}

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
