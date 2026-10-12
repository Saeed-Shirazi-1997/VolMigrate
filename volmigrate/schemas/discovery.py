"""Schemas for Docker and Kubernetes resource discovery."""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class VolumeConsumer(BaseModel):
    container_id: str
    container_name: str
    status: str
    is_running: bool
    mode: str = "rw"  # "rw" or "ro"


class VolumeInfo(BaseModel):
    name: str
    driver: str
    mountpoint: Optional[str] = None
    created_at: Optional[str] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    active_consumers_count: int = 0
    estimated_size_bytes: Optional[int] = None


class VolumeInspectResult(BaseModel):
    name: str
    driver: str
    mountpoint: Optional[str] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    consumers: List[VolumeConsumer] = Field(default_factory=list)
    estimated_size_bytes: Optional[int] = None
    readable: bool = True
    warning: Optional[str] = None


class NamespaceInfo(BaseModel):
    name: str
    status: str = "Active"


class StorageClassInfo(BaseModel):
    name: str
    provisioner: str
    reclaim_policy: Optional[str] = None
    volume_binding_mode: Optional[str] = None
    is_default: bool = False
    allow_volume_expansion: bool = True


class PVCInfo(BaseModel):
    name: str
    namespace: str
    status: str  # Bound, Pending, etc.
    capacity_bytes: Optional[int] = None
    storage_class_name: Optional[str] = None
    access_modes: List[str] = Field(default_factory=list)


class PreflightCheckItem(BaseModel):
    name: str
    passed: bool
    severity: str = "error"  # "error", "warning", "info"
    message: str


class PreflightCheckResult(BaseModel):
    passed: bool
    can_proceed: bool
    checks: List[PreflightCheckItem] = Field(default_factory=list)
