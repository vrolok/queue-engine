# src/ray_init.py
import ray
from typing import Optional, Dict, Any
from src.log_handler.logging_config import get_logger


logger = get_logger(__name__)
_ray_initialized = False


def initialize_ray(
    address: Optional[str] = None, ignore_reinit: bool = True, **kwargs
) -> bool:
    """
    Initialize Ray with the given configuration.

    Args:
        address: Optional Ray cluster address to connect to
        ignore_reinit: Whether to ignore reinitialization
        **kwargs: Additional parameters to pass to ray.init()

    Returns:
        bool: True if Ray was initialized, False if it was already initialized
    """
    global _ray_initialized

    if ray.is_initialized():
        if not _ray_initialized:
            logger.info("Ray was already initialized externally")
            _ray_initialized = True
        return False

    try:
        init_kwargs = {"ignore_reinit_error": ignore_reinit}

        if address:
            init_kwargs["address"] = address
            logger.info(f"Connecting to Ray cluster at {address}")
        else:
            logger.info("Starting local Ray instance")

        # Add any additional kwargs
        init_kwargs.update(kwargs)

        # Initialize Ray
        ray.init(**init_kwargs)
        _ray_initialized = True
        logger.info("Ray initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize Ray: {str(e)}")
        raise


def get_ray_resources() -> Dict[str, Any]:
    """Get available Ray resources."""
    if not ray.is_initialized():
        return {}
    return ray.available_resources()


def get_ray_dashboard_url() -> Optional[str]:
    """Get the URL for the Ray dashboard."""
    if not ray.is_initialized():
        return None
    try:
        return ray.get_dashboard_url()
    except Exception as e:
        logger.error(f"Error getting Ray dashboard URL: {str(e)}")
        return None


def shutdown_ray() -> None:
    """Shutdown Ray if it was initialized by this module."""
    global _ray_initialized
    if _ray_initialized and ray.is_initialized():
        try:
            ray.shutdown()
            _ray_initialized = False
            logger.info("Ray shutdown complete")
        except Exception as e:
            logger.error(f"Error shutting down Ray: {str(e)}")
