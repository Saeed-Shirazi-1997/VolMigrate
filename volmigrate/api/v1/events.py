"""Server-Sent Events (SSE) live progress and events stream."""

import asyncio
from typing import AsyncGenerator
from fastapi import APIRouter, Request, HTTPException, Depends
from sse_starlette.sse import EventSourceResponse
from sqlalchemy.ext.asyncio import AsyncSession

from volmigrate.api.deps import get_db
from volmigrate.services.migration_service import MigrationService
from volmigrate.services.event_broadcaster import broadcaster

router = APIRouter(tags=["Live Events"])


@router.get("/migrations/{migration_id}/events")
async def stream_migration_events(
    migration_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Subscribe to real-time Server-Sent Events (SSE) for progress, throughput, and status."""
    mig = await MigrationService.get_migration(db, migration_id)
    if not mig:
        raise HTTPException(status_code=404, detail="Migration not found")

    async def event_generator() -> AsyncGenerator[dict, None]:
        queue = await broadcaster.subscribe(migration_id)

        # Emit initial state snapshot
        yield {
            "event": "snapshot",
            "data": mig.model_dump_json() if hasattr(mig, "model_dump_json") else str(mig.status),
        }

        try:
            while True:
                if await request.is_disconnected():
                    break

                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield {
                        "event": msg.event_type,
                        "data": msg.model_dump_json(),
                    }
                except asyncio.TimeoutError:
                    # Keep-alive ping
                    yield {"event": "ping", "data": "keep-alive"}
        finally:
            await broadcaster.unsubscribe(migration_id, queue)

    return EventSourceResponse(event_generator())
