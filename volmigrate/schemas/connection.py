"""Pydantic schemas for Connection management."""

from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, ConfigDict, Field
from volmigrate.models.enums import ConnectionType, ConnectionStatus


class ConnectionBase(BaseModel):
    name: str = Field(..., min_length=2, max_length=100)
    type: ConnectionType
    endpoint: Optional[str] = Field(None, description="Docker socket or K8s API endpoint")


class ConnectionCreate(ConnectionBase):
    # Optional raw config payload (e.g. kubeconfig yaml or docker TLS certificates/host)
    config_payload: Optional[Dict[str, Any]] = None


class ConnectionUpdate(BaseModel):
    name: Optional[str] = None
    endpoint: Optional[str] = None
    config_payload: Optional[Dict[str, Any]] = None


class ConnectionRead(ConnectionBase):
    model_config = ConfigDict(from_attributes=True)

    id: str
    status: ConnectionStatus
    last_tested_at: Optional[datetime] = None
    last_error: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class ConnectionTestResult(BaseModel):
    success: bool
    version_info: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
