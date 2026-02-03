"""FastAPI application factory for Crypto Lake API server."""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from api.event_bus import EventBus
from api.routes_rest import router as rest_router
from api.routes_ws import router as ws_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app startup and shutdown."""
    logger.info("API server starting up")
    yield
    logger.info("API server shutting down")


def create_app(
    config: dict,
    event_bus: Optional[EventBus] = None,
    health_data: Optional[dict] = None,
) -> FastAPI:
    """
    Create and configure the FastAPI application.

    Args:
        config: Application config dict (from config.yml)
        event_bus: EventBus instance for real-time streaming (optional)
        health_data: Shared health data dict from orchestrator (optional)

    Returns:
        Configured FastAPI application
    """
    api_config = config.get("api", {})

    app = FastAPI(
        title="Crypto Lake API",
        description="Real-time and historical crypto market data",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Store shared state
    app.state.config = config
    app.state.event_bus = event_bus
    app.state.health_data = health_data or {}

    # CORS middleware
    cors_origins = api_config.get("cors_origins", ["http://localhost:3000"])
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register routers
    app.include_router(rest_router)
    app.include_router(ws_router)

    return app
