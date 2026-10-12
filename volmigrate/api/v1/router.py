"""API v1 root router combining connections, discovery, migrations, and events."""

from fastapi import APIRouter
from volmigrate.api.v1.connections import router as connections_router
from volmigrate.api.v1.discovery import router as discovery_router
from volmigrate.api.v1.migrations import router as migrations_router
from volmigrate.api.v1.events import router as events_router

api_v1_router = APIRouter()
api_v1_router.include_router(connections_router)
api_v1_router.include_router(discovery_router)
api_v1_router.include_router(migrations_router)
api_v1_router.include_router(events_router)
