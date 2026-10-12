"""FastAPI main application entrypoint."""

from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from volmigrate.core.config import get_settings
from volmigrate.core.database import init_db
from volmigrate.core.logging import setup_logging
from volmigrate.api.v1.router import api_v1_router

settings = get_settings()
setup_logging(log_level="DEBUG" if settings.DEBUG else "INFO")
logger = logging.getLogger("volmigrate.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for database initialization."""
    logger.info("Initializing Volmigrate API...")
    await init_db()
    yield
    logger.info("Shutting down Volmigrate API...")


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="Streaming migration tool from Docker Volumes to Kubernetes PVCs",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API v1 router
app.include_router(api_v1_router, prefix=settings.API_V1_PREFIX)


import os
from fastapi.responses import FileResponse

STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "static")
INDEX_PATH = os.path.join(STATIC_DIR, "index.html")


@app.get("/healthz", tags=["Health"])
async def health_check():
    """Service health check."""
    return {
        "status": "healthy",
        "service": settings.PROJECT_NAME,
        "version": settings.VERSION,
    }


@app.get("/", tags=["UI"])
@app.get("/ui", tags=["UI"])
async def serve_ui():
    """Serve Volmigrate Web Dashboard."""
    if os.path.exists(INDEX_PATH):
        return FileResponse(INDEX_PATH)
    return {
        "name": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "docs": "/docs",
        "health": "/healthz",
    }
