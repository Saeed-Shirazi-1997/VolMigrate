"""Connection management endpoints."""

from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from volmigrate.api.deps import get_db
from volmigrate.schemas.connection import (
    ConnectionCreate,
    ConnectionRead,
    ConnectionUpdate,
    ConnectionTestResult,
)
from volmigrate.services.connection_service import ConnectionService

router = APIRouter(prefix="/connections", tags=["Connections"])


@router.get("", response_model=List[ConnectionRead])
async def list_connections(db: AsyncSession = Depends(get_db)):
    """List all configured Docker and Kubernetes connections."""
    return await ConnectionService.list_connections(db)


@router.post("", response_model=ConnectionRead, status_code=status.HTTP_201_CREATED)
async def create_connection(data: ConnectionCreate, db: AsyncSession = Depends(get_db)):
    """Register a new connection."""
    return await ConnectionService.create_connection(db, data)


@router.get("/{connection_id}", response_model=ConnectionRead)
async def get_connection(connection_id: str, db: AsyncSession = Depends(get_db)):
    """Get connection details."""
    conn = await ConnectionService.get_connection(db, connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    return conn


@router.put("/{connection_id}", response_model=ConnectionRead)
async def update_connection(
    connection_id: str, data: ConnectionUpdate, db: AsyncSession = Depends(get_db)
):
    """Update connection settings."""
    conn = await ConnectionService.update_connection(db, connection_id, data)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    return conn


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(connection_id: str, db: AsyncSession = Depends(get_db)):
    """Delete a connection."""
    deleted = await ConnectionService.delete_connection(db, connection_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Connection not found")
    return None


@router.post("/{connection_id}/test", response_model=ConnectionTestResult)
async def test_connection(connection_id: str, db: AsyncSession = Depends(get_db)):
    """Test connectivity to Docker Engine or Kubernetes API."""
    return await ConnectionService.test_connection(db, connection_id)
