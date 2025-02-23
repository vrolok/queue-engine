# src/api/app.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .router import router


def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    app = FastAPI(
        title="Async Task Processing Service",
        description="API for submitting and managing asynchronous tasks",
        version="1.0.0",
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # In production, replace with specific origins
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include the router
    app.include_router(router, prefix="/api/v1")

    return app
