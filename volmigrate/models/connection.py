"""Connection entity for Docker and Kubernetes configurations."""

from datetime import datetime, timezone
import uuid
from sqlalchemy import Column, String, DateTime, Text, Enum as SAEnum
from volmigrate.core.database import Base
from volmigrate.models.enums import ConnectionType, ConnectionStatus


class Connection(Base):
    __tablename__ = "connections"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(100), nullable=False, unique=True)
    type = Column(SAEnum(ConnectionType), nullable=False)
    status = Column(SAEnum(ConnectionStatus), default=ConnectionStatus.UNTESTED, nullable=False)
    
    # Target endpoint / socket (e.g. unix:///var/run/docker.sock or https://k8s-cluster:6443)
    endpoint = Column(String(255), nullable=True)
    
    # Encrypted JSON containing config (kubeconfig payload, client certs, docker tls, etc.)
    config_encrypted = Column(Text, nullable=True)
    
    last_tested_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
