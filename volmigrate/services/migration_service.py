"""Migration Service for lifecycle management, planning, and event emission."""

from datetime import datetime, timezone
import json
import logging
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from volmigrate.models import Migration, MigrationEvent, MigrationStatus, EventLevel
from volmigrate.schemas.migration import (
    MigrationPlanRequest,
    MigrationPlanResponse,
    MigrationCreate,
    MigrationDetail,
)
from volmigrate.schemas.event import SSEMessage
from volmigrate.services.connection_service import ConnectionService
from volmigrate.services.discovery_service import DiscoveryService
from volmigrate.services.event_broadcaster import broadcaster

logger = logging.getLogger("volmigrate.services.migration")


class MigrationService:

    @staticmethod
    async def plan_migration(
        db: AsyncSession, plan_req: MigrationPlanRequest
    ) -> MigrationPlanResponse:
        """Evaluate preflight, examine source consumers, and return a plan report."""
        preflight = await DiscoveryService.run_preflight(
            db=db,
            source_conn_id=plan_req.source_connection_id,
            dest_conn_id=plan_req.dest_connection_id,
            source_volume_name=plan_req.source_volume_name,
            dest_namespace=plan_req.dest_namespace,
            dest_pvc_name=plan_req.dest_pvc_name,
            storage_class_name=plan_req.storage_class_name,
            create_pvc_if_missing=plan_req.create_pvc_if_missing,
        )

        # Inspect source details
        est_size = None
        active_consumers_count = 0
        warnings = []

        try:
            source_conn = await ConnectionService.get_connection(db, plan_req.source_connection_id)
            if source_conn:
                adapter = ConnectionService.get_adapter(source_conn)
                vol_inspect = await adapter.inspect_volume(plan_req.source_volume_name)
                est_size = vol_inspect.estimated_size_bytes
                running_c = [c for c in vol_inspect.consumers if c.is_running]
                active_consumers_count = len(running_c)
                if active_consumers_count > 0:
                    warnings.append(
                        f"There are {active_consumers_count} active container(s) attached to this volume. "
                        "Safe offline migration requires stopping writes before transferring."
                    )
        except Exception as e:
            logger.debug("Plan inspect error: %s", e)

        # Inspect dest PVC
        pvc_exists = False
        pvc_bound = False
        try:
            dest_conn = await ConnectionService.get_connection(db, plan_req.dest_connection_id)
            if dest_conn:
                adapter = ConnectionService.get_adapter(dest_conn)
                pvc_info = await adapter.inspect_pvc(plan_req.dest_namespace, plan_req.dest_pvc_name)
                if pvc_info:
                    pvc_exists = True
                    pvc_bound = pvc_info.status == "Bound"
        except Exception as e:
            logger.debug("Plan pvc error: %s", e)

        return MigrationPlanResponse(
            plan_valid=preflight.can_proceed,
            source_volume_name=plan_req.source_volume_name,
            estimated_size_bytes=est_size,
            active_consumers_count=active_consumers_count,
            dest_pvc_exists=pvc_exists,
            dest_pvc_bound=pvc_bound,
            preflight=preflight,
            warnings=warnings,
        )

    @staticmethod
    async def create_migration(db: AsyncSession, data: MigrationCreate) -> Migration:
        """Create and queue a migration job."""
        name = data.name or f"migrate-{data.source_volume_name}-to-{data.dest_pvc_name}"

        # Snapshot of parameters
        snapshot = {
            "source_volume_name": data.source_volume_name,
            "dest_namespace": data.dest_namespace,
            "dest_pvc_name": data.dest_pvc_name,
            "storage_class_name": data.storage_class_name,
            "capacity_bytes": data.capacity_bytes,
            "access_modes": data.access_modes,
            "validation_policy": data.validation_policy.value,
            "overwrite_policy": data.overwrite_policy.value,
            "force_offline_copy": data.force_offline_copy,
        }

        migration = Migration(
            name=name,
            source_connection_id=data.source_connection_id,
            dest_connection_id=data.dest_connection_id,
            source_volume_name=data.source_volume_name,
            dest_namespace=data.dest_namespace,
            dest_pvc_name=data.dest_pvc_name,
            create_pvc_if_missing=data.create_pvc_if_missing,
            storage_class_name=data.storage_class_name,
            capacity_bytes=data.capacity_bytes,
            access_modes=data.access_modes,
            validation_policy=data.validation_policy,
            overwrite_policy=data.overwrite_policy,
            config_snapshot=json.dumps(snapshot),
            status=MigrationStatus.QUEUED,
            current_step="Queued for execution",
        )
        db.add(migration)
        await db.commit()
        await db.refresh(migration)

        # Log initial event
        await MigrationService.emit_event(
            db=db,
            migration_id=migration.id,
            step="Created",
            level=EventLevel.INFO,
            message=f"Migration '{name}' queued for execution",
        )

        return migration

    @staticmethod
    async def get_migration(db: AsyncSession, migration_id: str) -> Optional[Migration]:
        result = await db.execute(select(Migration).where(Migration.id == migration_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_migration_detail(
        db: AsyncSession, migration_id: str
    ) -> Optional[MigrationDetail]:
        migration = await MigrationService.get_migration(db, migration_id)
        if not migration:
            return None

        events_res = await db.execute(
            select(MigrationEvent)
            .where(MigrationEvent.migration_id == migration_id)
            .order_by(MigrationEvent.created_at.asc())
        )
        events = list(events_res.scalars().all())

        detail_dict = {c.name: getattr(migration, c.name) for c in migration.__table__.columns}
        detail_dict["events"] = events
        return MigrationDetail.model_validate(detail_dict)

    @staticmethod
    async def list_migrations(db: AsyncSession) -> List[Migration]:
        result = await db.execute(select(Migration).order_by(Migration.created_at.desc()))
        return list(result.scalars().all())

    @staticmethod
    async def cancel_migration(
        db: AsyncSession, migration_id: str, reason: str = "Operator requested cancellation"
    ) -> bool:
        migration = await MigrationService.get_migration(db, migration_id)
        if not migration:
            return False

        if migration.status in (
            MigrationStatus.COMPLETED,
            MigrationStatus.FAILED,
            MigrationStatus.CANCELLED,
        ):
            return False

        migration.status = MigrationStatus.CANCELLED
        migration.error_message = reason
        migration.current_step = "Cancelled"
        migration.completed_at = datetime.now(timezone.utc)
        await db.commit()

        await MigrationService.emit_event(
            db=db,
            migration_id=migration_id,
            step="Cancelled",
            level=EventLevel.WARNING,
            message=reason,
        )
        return True

    @staticmethod
    async def retry_migration(db: AsyncSession, migration_id: str, force: bool = False) -> bool:
        migration = await MigrationService.get_migration(db, migration_id)
        if not migration:
            return False

        if migration.status not in (MigrationStatus.FAILED, MigrationStatus.CANCELLED) and not force:
            return False

        migration.status = MigrationStatus.QUEUED
        migration.current_step = "Re-queued for execution"
        migration.error_message = None
        migration.transferred_bytes = 0
        migration.throughput_bytes_per_sec = 0.0
        migration.started_at = None
        migration.completed_at = None
        migration.worker_id = None
        await db.commit()

        await MigrationService.emit_event(
            db=db,
            migration_id=migration_id,
            step="Retrying",
            level=EventLevel.INFO,
            message="Migration re-queued for retry by operator",
        )
        return True

    @staticmethod
    async def emit_event(
        db: AsyncSession,
        migration_id: str,
        step: str,
        level: EventLevel,
        message: str,
        payload: Optional[dict] = None,
    ) -> MigrationEvent:
        event = MigrationEvent(
            migration_id=migration_id,
            step=step,
            level=level,
            message=message,
            payload=json.dumps(payload) if payload else None,
        )
        db.add(event)
        await db.commit()
        await db.refresh(event)

        # Broadcast via SSE
        await broadcaster.publish(
            migration_id=migration_id,
            message=SSEMessage(
                event_type="log",
                migration_id=migration_id,
                status="event",
                step=step,
                message=message,
                data=payload,
            ),
        )
        return event
