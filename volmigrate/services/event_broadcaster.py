"""In-memory event broadcaster for real-time Server-Sent Events (SSE)."""

import asyncio
from typing import Dict, Set
from volmigrate.schemas.event import SSEMessage


class EventBroadcaster:
    """Pub/Sub bus for multiplexing live migration progress to SSE listeners."""

    def __init__(self):
        self._subscribers: Dict[str, Set[asyncio.Queue[SSEMessage]]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, migration_id: str) -> asyncio.Queue[SSEMessage]:
        """Register a new listener queue for a specific migration ID."""
        async with self._lock:
            q: asyncio.Queue[SSEMessage] = asyncio.Queue(maxsize=100)
            if migration_id not in self._subscribers:
                self._subscribers[migration_id] = set()
            self._subscribers[migration_id].add(q)
            return q

    async def unsubscribe(self, migration_id: str, queue: asyncio.Queue[SSEMessage]) -> None:
        """Unregister a listener queue."""
        async with self._lock:
            if migration_id in self._subscribers:
                self._subscribers[migration_id].discard(queue)
                if not self._subscribers[migration_id]:
                    del self._subscribers[migration_id]

    async def publish(self, migration_id: str, message: SSEMessage) -> None:
        """Publish event message to all active listeners."""
        async with self._lock:
            queues = list(self._subscribers.get(migration_id, set()))

        for q in queues:
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                pass  # Avoid blocking if slow consumer


# Global singleton broadcaster
broadcaster = EventBroadcaster()
