"""Atomic Job Claim and Lease Management for distributed workers."""

from datetime import datetime, timezone, timedelta
import logging
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from volmigrate.core.config import get_settings
from volmigrate.models import Migration, WorkerLease, MigrationStatus

logger = logging.getLogger("volmigrate.worker.lease")


class LeaseManager:
    """Manages worker leases, atomic job claiming, and heartbeats."""

    def __init__(self, worker_id: Optional[str] = None):
        self.settings = get_settings()
        self.worker_id = worker_id or self.settings.WORKER_ID
        self.lease_seconds = self.settings.LEASE_DURATION_SECONDS

    async def claim_next_job(self, db: AsyncSession) -> Optional[Migration]:
        """Atomically claim a queued migration job or recover an expired lease."""
        now = datetime.now(timezone.utc)
        lease_expiry = now + timedelta(seconds=self.lease_seconds)

        # 1. Try to claim a newly queued migration
        # In PostgreSQL: SELECT ... FOR UPDATE SKIP LOCKED
        # In SQLite / general: atomic status update
        stmt = (
            select(Migration)
            .where(Migration.status == MigrationStatus.QUEUED)
            .order_by(Migration.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        result = await db.execute(stmt)
        migration = result.scalar_one_or_none()

        if migration:
            migration.status = MigrationStatus.PREPARING
            migration.worker_id = self.worker_id
            migration.started_at = now
            migration.current_step = "Claimed by worker"

            # Create or update lease
            lease = await db.get(WorkerLease, migration.id)
            if not lease:
                lease = WorkerLease(
                    migration_id=migration.id,
                    worker_id=self.worker_id,
                    lease_until=lease_expiry,
                    last_heartbeat=now,
                    acquired_at=now,
                )
                db.add(lease)
            else:
                lease.worker_id = self.worker_id
                lease.lease_until = lease_expiry
                lease.last_heartbeat = now

            await db.commit()
            await db.refresh(migration)
            logger.info("Worker %s successfully claimed job %s", self.worker_id, migration.id)
            return migration

        # 2. Check for stale leases on in-progress migrations (Recoverability - Section 11)
        stale_stmt = (
            select(WorkerLease)
            .join(Migration, Migration.id == WorkerLease.migration_id)
            .where(
                and_(
                    WorkerLease.lease_until < now,
                    Migration.status.in_([MigrationStatus.PREPARING, MigrationStatus.TRANSFERRING]),
                )
            )
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        stale_res = await db.execute(stale_stmt)
        stale_lease = stale_res.scalar_one_or_none()

        if stale_lease:
            stale_mig = await db.get(Migration, stale_lease.migration_id)
            if stale_mig:
                logger.warning(
                    "Worker %s adopting stale job %s (previous owner: %s expired at %s)",
                    self.worker_id,
                    stale_mig.id,
                    stale_lease.worker_id,
                    stale_lease.lease_until,
                )
                stale_mig.worker_id = self.worker_id
                stale_mig.current_step = "Adopted by worker after previous lease expiration"
                stale_lease.worker_id = self.worker_id
                stale_lease.lease_until = lease_expiry
                stale_lease.last_heartbeat = now
                await db.commit()
                await db.refresh(stale_mig)
                return stale_mig

        return None

    async def heartbeat(self, db: AsyncSession, migration_id: str) -> bool:
        """Renew the worker lease with an updated heartbeat timestamp."""
        now = datetime.now(timezone.utc)
        lease_expiry = now + timedelta(seconds=self.lease_seconds)

        lease = await db.get(WorkerLease, migration_id)
        if not lease or lease.worker_id != self.worker_id:
            logger.warning(
                "Cannot heartbeat lease for %s: lease missing or owner mismatch", migration_id
            )
            return False

        lease.lease_until = lease_expiry
        lease.last_heartbeat = now
        await db.commit()
        return True

    async def release_lease(self, db: AsyncSession, migration_id: str) -> None:
        """Remove worker lease upon completion or failure."""
        lease = await db.get(WorkerLease, migration_id)
        if lease:
            await db.delete(lease)
            await db.commit()
