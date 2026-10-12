"""Discovery endpoints for Docker volumes and Kubernetes destinations."""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from volmigrate.api.deps import get_db
from volmigrate.schemas.discovery import (
    VolumeInfo,
    VolumeInspectResult,
    NamespaceInfo,
    StorageClassInfo,
    PVCInfo,
)
from volmigrate.services.discovery_service import DiscoveryService

router = APIRouter(prefix="/discovery", tags=["Discovery"])


# Docker Discovery
@router.get("/sources/{connection_id}/volumes", response_model=List[VolumeInfo])
async def list_source_volumes(connection_id: str, db: AsyncSession = Depends(get_db)):
    """List Docker volumes available on the source connection."""
    try:
        return await DiscoveryService.list_volumes(db, connection_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get(
    "/sources/{connection_id}/volumes/{volume_name}", response_model=VolumeInspectResult
)
async def inspect_source_volume(
    connection_id: str, volume_name: str, db: AsyncSession = Depends(get_db)
):
    """Inspect Docker volume metadata, attached container consumers, and size estimate."""
    try:
        return await DiscoveryService.inspect_volume(db, connection_id, volume_name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Volume not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Kubernetes Discovery
@router.get(
    "/destinations/{connection_id}/namespaces", response_model=List[NamespaceInfo]
)
async def list_destination_namespaces(
    connection_id: str, db: AsyncSession = Depends(get_db)
):
    """List namespaces in the destination Kubernetes cluster."""
    try:
        return await DiscoveryService.list_namespaces(db, connection_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get(
    "/destinations/{connection_id}/storage-classes",
    response_model=List[StorageClassInfo],
)
async def list_destination_storage_classes(
    connection_id: str, db: AsyncSession = Depends(get_db)
):
    """List StorageClasses in the destination Kubernetes cluster."""
    try:
        return await DiscoveryService.list_storage_classes(db, connection_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get(
    "/destinations/{connection_id}/namespaces/{namespace}/pvcs/{pvc_name}",
    response_model=Optional[PVCInfo],
)
async def inspect_destination_pvc(
    connection_id: str,
    namespace: str,
    pvc_name: str,
    db: AsyncSession = Depends(get_db),
):
    """Inspect PVC in destination namespace."""
    try:
        res = await DiscoveryService.inspect_pvc(db, connection_id, namespace, pvc_name)
        if not res:
            raise HTTPException(status_code=404, detail="PVC not found")
        return res
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
