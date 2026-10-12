"""Migration audit events and diagnostic stream."""

from datetime import datetime, timezone
import uuid
from sqlalchemy import Column, String, DateTime, Text, Enum as SAEnum
from volmigrate.core.database import Base
from volmigrate.models.enums import EventLevel


class MigrationEvent(Base):
    __tablename__ = "migration_events"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    migration_id = Column(String(36), nullable=False, index=True)
    step = Column(String(100), nullable=False)
    level = Column(SAEnum(EventLevel), default=EventLevel.INFO, nullable=False)
    message = Column(Text, nullable=False)
    payload = Column(Text, nullable=True)  # JSON metadata or details
    
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
