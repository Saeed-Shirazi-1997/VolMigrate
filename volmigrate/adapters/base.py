"""Abstract base contracts for Source and Destination adapters."""

from abc import ABC, abstractmethod
from typing import AsyncIterator, List, Optional, Dict, Any
from volmigrate.schemas.discovery import (
    VolumeInfo,
    VolumeInspectResult,
    VolumeConsumer,
    NamespaceInfo,
    StorageClassInfo,
    PVCInfo,
)


class BaseSourceAdapter(ABC):
    """Abstract contract for Source Adapters (Section 7)."""

    @abstractmethod
    async def test_connection(self) -> Dict[str, Any]:
        """Test connection and return health & version metadata."""
        pass

    @abstractmethod
    async def list_volumes(self) -> List[VolumeInfo]:
        """List all discovered volumes."""
        pass

    @abstractmethod
    async def inspect_volume(self, volume_id: str) -> VolumeInspectResult:
        """Inspect volume metadata, labels, and estimation."""
        pass

    @abstractmethod
    async def list_consumers(self, volume_id: str) -> List[VolumeConsumer]:
        """List containers actively using or mounting this volume."""
        pass

    @abstractmethod
    async def check_read_access(self, volume_id: str) -> bool:
        """Verify that worker or source engine can read volume content."""
        pass

    @abstractmethod
    async def open_read_stream(
        self, volume_id: str, options: Optional[Dict[str, Any]] = None
    ) -> AsyncIterator[bytes]:
        """Open raw tar archive byte stream directly from the source volume."""
        pass


class BaseDestinationAdapter(ABC):
    """Abstract contract for Destination Adapters (Section 8)."""

    @abstractmethod
    async def test_connection(self) -> Dict[str, Any]:
        """Test connection and return health & cluster version metadata."""
        pass

    @abstractmethod
    async def list_namespaces(self) -> List[NamespaceInfo]:
        """List available Kubernetes namespaces."""
        pass

    @abstractmethod
    async def list_storage_classes(self) -> List[StorageClassInfo]:
        """List available StorageClasses."""
        pass

    @abstractmethod
    async def inspect_pvc(self, namespace: str, pvc_name: str) -> Optional[PVCInfo]:
        """Inspect an existing PVC in the destination namespace."""
        pass

    @abstractmethod
    async def prepare_destination(
        self,
        namespace: str,
        pvc_name: str,
        create_if_missing: bool = True,
        storage_class_name: Optional[str] = None,
        capacity_bytes: Optional[int] = None,
        access_modes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Prepare or verify PVC existence and bound readiness."""
        pass

    @abstractmethod
    async def wait_until_writable(
        self, destination_info: Dict[str, Any], timeout_seconds: int = 120
    ) -> Dict[str, Any]:
        """Prepare temporary transfer helper pod and wait until PVC is writable."""
        pass

    @abstractmethod
    async def cleanup_temporary_resources(self, resources: Dict[str, Any]) -> None:
        """Safely clean up temporary helper pods without modifying target PVC data."""
        pass
