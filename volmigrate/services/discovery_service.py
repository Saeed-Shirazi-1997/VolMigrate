"""Discovery and Preflight service for Docker and Kubernetes resources."""

import logging
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession

from volmigrate.services.connection_service import ConnectionService
from volmigrate.schemas.discovery import (
    VolumeInfo,
    VolumeInspectResult,
    NamespaceInfo,
    StorageClassInfo,
    PVCInfo,
    PreflightCheckResult,
    PreflightCheckItem,
)

logger = logging.getLogger("volmigrate.services.discovery")


class DiscoveryService:

    @staticmethod
    async def list_volumes(db: AsyncSession, connection_id: str) -> List[VolumeInfo]:
        conn = await ConnectionService.get_connection(db, connection_id)
        if not conn:
            raise ValueError("Docker connection not found")
        adapter = ConnectionService.get_adapter(conn)
        return await adapter.list_volumes()

    @staticmethod
    async def inspect_volume(
        db: AsyncSession, connection_id: str, volume_name: str
    ) -> VolumeInspectResult:
        conn = await ConnectionService.get_connection(db, connection_id)
        if not conn:
            raise ValueError("Docker connection not found")
        adapter = ConnectionService.get_adapter(conn)
        return await adapter.inspect_volume(volume_name)

    @staticmethod
    async def list_namespaces(db: AsyncSession, connection_id: str) -> List[NamespaceInfo]:
        conn = await ConnectionService.get_connection(db, connection_id)
        if not conn:
            raise ValueError("Kubernetes connection not found")
        adapter = ConnectionService.get_adapter(conn)
        return await adapter.list_namespaces()

    @staticmethod
    async def list_storage_classes(
        db: AsyncSession, connection_id: str
    ) -> List[StorageClassInfo]:
        conn = await ConnectionService.get_connection(db, connection_id)
        if not conn:
            raise ValueError("Kubernetes connection not found")
        adapter = ConnectionService.get_adapter(conn)
        return await adapter.list_storage_classes()

    @staticmethod
    async def inspect_pvc(
        db: AsyncSession, connection_id: str, namespace: str, pvc_name: str
    ) -> Optional[PVCInfo]:
        conn = await ConnectionService.get_connection(db, connection_id)
        if not conn:
            raise ValueError("Kubernetes connection not found")
        adapter = ConnectionService.get_adapter(conn)
        return await adapter.inspect_pvc(namespace, pvc_name)

    @staticmethod
    async def run_preflight(
        db: AsyncSession,
        source_conn_id: str,
        dest_conn_id: str,
        source_volume_name: str,
        dest_namespace: str,
        dest_pvc_name: str,
        storage_class_name: Optional[str] = None,
        create_pvc_if_missing: bool = True,
    ) -> PreflightCheckResult:
        """Execute preflight checks across source Docker and destination Kubernetes."""
        checks: List[PreflightCheckItem] = []
        can_proceed = True

        # 1. Source Connection Check
        source_conn = await ConnectionService.get_connection(db, source_conn_id)
        if not source_conn:
            return PreflightCheckResult(
                passed=False,
                can_proceed=False,
                checks=[
                    PreflightCheckItem(
                        name="source_connection",
                        passed=False,
                        severity="error",
                        message=f"Source connection {source_conn_id} does not exist",
                    )
                ],
            )

        source_adapter = ConnectionService.get_adapter(source_conn)
        try:
            await source_adapter.test_connection()
            checks.append(
                PreflightCheckItem(
                    name="source_connection",
                    passed=True,
                    severity="info",
                    message="Docker source connection verified",
                )
            )
        except Exception as e:
            checks.append(
                PreflightCheckItem(
                    name="source_connection",
                    passed=False,
                    severity="error",
                    message=f"Docker connection failed: {e}",
                )
            )
            can_proceed = False

        # 2. Source Volume Existence & Consumers Check
        volume_inspect = None
        if can_proceed:
            try:
                volume_inspect = await source_adapter.inspect_volume(source_volume_name)
                checks.append(
                    PreflightCheckItem(
                        name="source_volume",
                        passed=True,
                        severity="info",
                        message=f"Volume '{source_volume_name}' found (driver: {volume_inspect.driver})",
                    )
                )

                # Check active consumers (Offline copy safety policy - Section 10)
                running_consumers = [c for c in volume_inspect.consumers if c.is_running]
                if running_consumers:
                    consumer_names = ", ".join(c.container_name for c in running_consumers)
                    checks.append(
                        PreflightCheckItem(
                            name="volume_consumers",
                            passed=False,
                            severity="warning",
                            message=f"Volume has {len(running_consumers)} running container(s): {consumer_names}. Offline copy policy recommends stopping writes.",
                        )
                    )
                else:
                    checks.append(
                        PreflightCheckItem(
                            name="volume_consumers",
                            passed=True,
                            severity="info",
                            message="No running containers attached. Safe for offline transfer.",
                        )
                    )
            except Exception as e:
                checks.append(
                    PreflightCheckItem(
                        name="source_volume",
                        passed=False,
                        severity="error",
                        message=f"Cannot inspect volume '{source_volume_name}': {e}",
                    )
                )
                can_proceed = False

        # 3. Destination Connection Check
        dest_conn = await ConnectionService.get_connection(db, dest_conn_id)
        if not dest_conn:
            checks.append(
                PreflightCheckItem(
                    name="dest_connection",
                    passed=False,
                    severity="error",
                    message=f"Destination connection {dest_conn_id} does not exist",
                )
            )
            return PreflightCheckResult(passed=False, can_proceed=False, checks=checks)

        dest_adapter = ConnectionService.get_adapter(dest_conn)
        try:
            await dest_adapter.test_connection()
            checks.append(
                PreflightCheckItem(
                    name="dest_connection",
                    passed=True,
                    severity="info",
                    message="Kubernetes cluster connection verified",
                )
            )
        except Exception as e:
            checks.append(
                PreflightCheckItem(
                    name="dest_connection",
                    passed=False,
                    severity="error",
                    message=f"Kubernetes cluster connection failed: {e}",
                )
            )
            can_proceed = False

        # 4. Destination Namespace Check
        if can_proceed:
            try:
                namespaces = await dest_adapter.list_namespaces()
                ns_names = {ns.name for ns in namespaces}
                if dest_namespace in ns_names:
                    checks.append(
                        PreflightCheckItem(
                            name="dest_namespace",
                            passed=True,
                            severity="info",
                            message=f"Namespace '{dest_namespace}' exists",
                        )
                    )
                else:
                    checks.append(
                        PreflightCheckItem(
                            name="dest_namespace",
                            passed=False,
                            severity="error",
                            message=f"Namespace '{dest_namespace}' not found in cluster",
                        )
                    )
                    can_proceed = False
            except Exception as e:
                checks.append(
                    PreflightCheckItem(
                        name="dest_namespace",
                        passed=False,
                        severity="error",
                        message=f"Failed checking namespace: {e}",
                    )
                )
                can_proceed = False

        # 5. Destination StorageClass Check
        if can_proceed and storage_class_name:
            try:
                storage_classes = await dest_adapter.list_storage_classes()
                sc_names = {sc.name for sc in storage_classes}
                if storage_class_name in sc_names:
                    checks.append(
                        PreflightCheckItem(
                            name="storage_class",
                            passed=True,
                            severity="info",
                            message=f"StorageClass '{storage_class_name}' is available",
                        )
                    )
                else:
                    checks.append(
                        PreflightCheckItem(
                            name="storage_class",
                            passed=False,
                            severity="error",
                            message=f"StorageClass '{storage_class_name}' not found",
                        )
                    )
                    can_proceed = False
            except Exception as e:
                checks.append(
                    PreflightCheckItem(
                        name="storage_class",
                        passed=False,
                        severity="warning",
                        message=f"Could not verify storage class: {e}",
                    )
                )

        # 6. Destination PVC Check
        if can_proceed:
            try:
                pvc_info = await dest_adapter.inspect_pvc(dest_namespace, dest_pvc_name)
                if pvc_info:
                    checks.append(
                        PreflightCheckItem(
                            name="dest_pvc",
                            passed=True,
                            severity="info",
                            message=f"Existing PVC '{dest_pvc_name}' found (Status: {pvc_info.status})",
                        )
                    )
                elif create_pvc_if_missing:
                    checks.append(
                        PreflightCheckItem(
                            name="dest_pvc",
                            passed=True,
                            severity="info",
                            message=f"PVC '{dest_pvc_name}' will be automatically provisioned",
                        )
                    )
                else:
                    checks.append(
                        PreflightCheckItem(
                            name="dest_pvc",
                            passed=False,
                            severity="error",
                            message=f"PVC '{dest_pvc_name}' does not exist and create_pvc_if_missing is False",
                        )
                    )
                    can_proceed = False
            except Exception as e:
                checks.append(
                    PreflightCheckItem(
                        name="dest_pvc",
                        passed=False,
                        severity="error",
                        message=f"Failed checking destination PVC: {e}",
                    )
                )
                can_proceed = False

        all_passed = all(c.passed for c in checks if c.severity == "error")
        return PreflightCheckResult(
            passed=all_passed,
            can_proceed=can_proceed,
            checks=checks,
        )
