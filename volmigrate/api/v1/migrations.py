"""Migration planning, management, and control endpoints."""

from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from volmigrate.api.deps import get_db
from volmigrate.schemas.migration import (
    MigrationPlanRequest,
    MigrationPlanResponse,
    MigrationCreate,
    MigrationRead,
    MigrationDetail,
    MigrationCancelRequest,
    MigrationRetryRequest,
)
from volmigrate.services.migration_service import MigrationService

router = APIRouter(prefix="/migrations", tags=["Migrations"])


@router.post("/plan", response_model=MigrationPlanResponse)
async def plan_migration(
    plan_req: MigrationPlanRequest, db: AsyncSession = Depends(get_db)
):
    """Run preflight checks and produce a validation migration plan report."""
    try:
        return await MigrationService.plan_migration(db, plan_req)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("", response_model=List[MigrationRead])
async def list_migrations(db: AsyncSession = Depends(get_db)):
    """List all migrations."""
    return await MigrationService.list_migrations(db)


@router.post("", response_model=MigrationRead, status_code=status.HTTP_201_CREATED)
async def create_migration(
    data: MigrationCreate, db: AsyncSession = Depends(get_db)
):
    """Create and queue a migration job."""
    try:
        return await MigrationService.create_migration(db, data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{migration_id}", response_model=MigrationDetail)
async def get_migration(migration_id: str, db: AsyncSession = Depends(get_db)):
    """Get migration status, metrics, and audit event logs."""
    mig = await MigrationService.get_migration_detail(db, migration_id)
    if not mig:
        raise HTTPException(status_code=404, detail="Migration not found")
    return mig


@router.post("/{migration_id}/cancel")
async def cancel_migration(
    migration_id: str,
    req: MigrationCancelRequest = MigrationCancelRequest(),
    db: AsyncSession = Depends(get_db),
):
    """Request graceful cancellation of an active or queued migration."""
    cancelled = await MigrationService.cancel_migration(
        db, migration_id, reason=req.reason or "Operator cancelled"
    )
    if not cancelled:
        raise HTTPException(
            status_code=400, detail="Cannot cancel migration (either not found or already finished)"
        )
    return {"message": "Migration cancellation requested successfully"}


@router.post("/{migration_id}/retry")
async def retry_migration(
    migration_id: str,
    req: MigrationRetryRequest = MigrationRetryRequest(),
    db: AsyncSession = Depends(get_db),
):
    """Retry a failed or cancelled migration."""
    retried = await MigrationService.retry_migration(db, migration_id, force=req.force)
    if not retried:
        raise HTTPException(
            status_code=400,
            detail="Cannot retry migration (only failed or cancelled migrations can be retried)",
        )
    return {"message": "Migration re-queued for execution"}
