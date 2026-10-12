"""Migration entity and configuration snapshot."""

from datetime import datetime, timezone
import uuid
from sqlalchemy import Column, String, BigInteger, Float, Boolean, DateTime, Text, Enum as SAEnum
from volmigrate.core.database import Base
from volmigrate.models.enums import MigrationStatus, ValidationPolicy, OverwritePolicy


class Migration(Base):
    __tablename__ = "migrations"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(150), nullable=False)

    # Connections
    source_connection_id = Column(String(36), nullable=False, index=True)
    dest_connection_id = Column(String(36), nullable=False, index=True)

    # Source parameters
    source_volume_name = Column(String(255), nullable=False)

    # Destination parameters
    dest_namespace = Column(String(100), nullable=False, default="default")
    dest_pvc_name = Column(String(255), nullable=False)
    create_pvc_if_missing = Column(Boolean, default=True, nullable=False)
    storage_class_name = Column(String(100), nullable=True)
    capacity_bytes = Column(BigInteger, nullable=True)
    access_modes = Column(String(50), default="ReadWriteOnce", nullable=False)

    # Execution State
    status = Column(
        SAEnum(MigrationStatus),
        default=MigrationStatus.DRAFT,
        nullable=False,
        index=True,
    )
    current_step = Column(String(100), default="Initialized", nullable=False)

    # Progress & Metrics
    transferred_bytes = Column(BigInteger, default=0, nullable=False)
    total_bytes_estimated = Column(BigInteger, nullable=True)
    throughput_bytes_per_sec = Column(Float, default=0.0, nullable=False)
    checksum_sha256 = Column(String(64), nullable=True)

    # Policies
    validation_policy = Column(
        SAEnum(ValidationPolicy),
        default=ValidationPolicy.FAST_MANIFEST,
        nullable=False,
    )
    overwrite_policy = Column(
        SAEnum(OverwritePolicy),
        default=OverwritePolicy.FAIL_IF_EXISTS,
        nullable=False,
    )

    # Diagnostics & Snapshots
    config_snapshot = Column(Text, nullable=True)  # JSON snapshot at migration start
    error_message = Column(Text, nullable=True)
    worker_id = Column(String(100), nullable=True, index=True)

    # Timestamps
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
