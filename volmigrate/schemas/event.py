"""SSE event message schema."""

from typing import Optional, Dict, Any
from pydantic import BaseModel


class SSEMessage(BaseModel):
    event_type: str  # "status", "progress", "log", "completed", "failed"
    migration_id: str
    status: str
    step: str
    transferred_bytes: int = 0
    total_bytes: Optional[int] = None
    throughput_bytes_per_sec: float = 0.0
    percent: Optional[float] = None
    message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
