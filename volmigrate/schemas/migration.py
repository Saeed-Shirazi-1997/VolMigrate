"""Schemas for Migration planning, execution, and monitoring."""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, ConfigDict, Field
from volmigrate.models.enums import MigrationStatus, ValidationPolicy, OverwritePolicy
from volmigrate.schemas.discovery import PreflightCheckResult


class MigrationPlanRequest(BaseModel):
    source_connection_id: str
    dest_connection_id: str
    source_volume_name: str
    dest_namespace: str = "default"
    dest_pvc_name: str
    create_pvc_if_missing: bool = True
    storage_class_name: Optional[str] = None
    capacity_bytes: Optional[int] = None
    access_modes: str = "ReadWriteOnce"
    validation_policy: ValidationPolicy = ValidationPolicy.FAST_MANIFEST
    overwrite_policy: OverwritePolicy = OverwritePolicy.FAIL_IF_EXISTS


class MigrationPlanResponse(BaseModel):
    plan_valid: bool
    source_volume_name: str
    estimated_size_bytes: Optional[int] = None
    active_consumers_count: int = 0
    dest_pvc_exists: bool = False
    dest_pvc_bound: bool = False
    preflight: PreflightCheckResult
    warnings: List[str] = Field(default_factory=list)


class MigrationCreate(BaseModel):
    name: Optional[str] = None
    source_connection_id: str
    dest_connection_id: str
    source_volume_name: str
    dest_namespace: str = "default"
    dest_pvc_name: str
    create_pvc_if_missing: bool = True
    storage_class_name: Optional[str] = None
    capacity_bytes: Optional[int] = None
    access_modes: str = "ReadWriteOnce"
    validation_policy: ValidationPolicy = ValidationPolicy.FAST_MANIFEST
    overwrite_policy: OverwritePolicy = OverwritePolicy.FAIL_IF_EXISTS
    force_offline_copy: bool = False  # Explicit operator acknowledgment if consumers are running


class MigrationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    source_connection_id: str
    dest_connection_id: str
    source_volume_name: str
    dest_namespace: str
    dest_pvc_name: str
    storage_class_name: Optional[str] = None
    capacity_bytes: Optional[int] = None
    access_modes: str
    status: MigrationStatus
    current_step: str
    transferred_bytes: int
    total_bytes_estimated: Optional[int] = None
    throughput_bytes_per_sec: float
    validation_policy: ValidationPolicy
    overwrite_policy: OverwritePolicy
    error_message: Optional[str] = None
    worker_id: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class MigrationEventRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    migration_id: str
    step: str
    level: str
    message: str
    payload: Optional[str] = None
    created_at: datetime


class MigrationDetail(MigrationRead):
    events: List[MigrationEventRead] = Field(default_factory=list)


class MigrationCancelRequest(BaseModel):
    reason: Optional[str] = "Operator cancelled"


class MigrationRetryRequest(BaseModel):
    force: bool = False
