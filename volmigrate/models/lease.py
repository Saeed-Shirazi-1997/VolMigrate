"""Worker lease and heartbeat model for resilient job coordination."""

from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime
from volmigrate.core.database import Base


class WorkerLease(Base):
    __tablename__ = "worker_leases"

    migration_id = Column(String(36), primary_key=True)
    worker_id = Column(String(100), nullable=False, index=True)
    lease_until = Column(DateTime(timezone=True), nullable=False, index=True)
    last_heartbeat = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    acquired_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
