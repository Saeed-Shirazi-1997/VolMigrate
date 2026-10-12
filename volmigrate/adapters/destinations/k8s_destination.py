"""Kubernetes Destination Adapter implementation."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import time
import uuid
from typing import List, Optional, Dict, Any
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

from volmigrate.adapters.base import BaseDestinationAdapter
from volmigrate.schemas.discovery import NamespaceInfo, StorageClassInfo, PVCInfo

logger = logging.getLogger("volmigrate.adapters.k8s")
_thread_pool = ThreadPoolExecutor(max_workers=8)


class KubernetesDestinationAdapter(BaseDestinationAdapter):
    """Adapter for provisioning PVCs and preparing streaming write targets in Kubernetes."""

    def __init__(
        self,
        kubeconfig_dict: Optional[Dict[str, Any]] = None,
        context: Optional[str] = None,
        in_cluster: bool = False,
    ):
        self.kubeconfig_dict = kubeconfig_dict
        self.context = context
        self.in_cluster = in_cluster
        self._api_client: Optional[client.ApiClient] = None

    def _get_api_client(self) -> client.ApiClient:
        if self._api_client is None:
            if self.in_cluster:
                config.load_incluster_config()
                self._api_client = client.ApiClient()
            elif self.kubeconfig_dict:
                self._api_client = config.new_client_from_config_dict(
                    self.kubeconfig_dict, context=self.context
                )
            else:
                # Load default local kubeconfig (~/.kube/config)
                try:
                    config.load_kube_config(context=self.context)
                    self._api_client = client.ApiClient()
                except Exception:
                    config.load_incluster_config()
                    self._api_client = client.ApiClient()
        return self._api_client

    async def test_connection(self) -> Dict[str, Any]:
        """Test connection and return cluster version information."""
        def _test():
            api = self._get_api_client()
            version_api = client.VersionApi(api)
            info = version_api.get_code()
            core_v1 = client.CoreV1Api(api)
            namespaces = core_v1.list_namespace(limit=1)
            return {
                "success": True,
                "git_version": info.git_version,
                "platform": info.platform,
                "major": info.major,
                "minor": info.minor,
                "accessible_namespaces_count": len(namespaces.items),
            }

        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(_thread_pool, _test)
        except Exception as e:
            logger.error("Kubernetes connection test failed: %s", e)
            raise RuntimeError(f"Failed to connect to Kubernetes cluster: {e}") from e

    async def list_namespaces(self) -> List[NamespaceInfo]:
        """List cluster namespaces."""
        def _list():
            api = self._get_api_client()
            core_v1 = client.CoreV1Api(api)
            ns_list = core_v1.list_namespace()
            return [
                NamespaceInfo(
                    name=ns.metadata.name,
                    status=ns.status.phase if ns.status else "Active",
                )
                for ns in ns_list.items
            ]

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(_thread_pool, _list)

    async def list_storage_classes(self) -> List[StorageClassInfo]:
        """List cluster storage classes."""
        def _list():
            api = self._get_api_client()
            storage_v1 = client.StorageV1Api(api)
            sc_list = storage_v1.list_storage_class()
            results = []
            for sc in sc_list.items:
                is_default = False
                annotations = sc.metadata.annotations or {}
                if (
                    annotations.get("storageclass.kubernetes.io/is-default-class") == "true"
                    or annotations.get("storageclass.beta.kubernetes.io/is-default-class") == "true"
                ):
                    is_default = True

                results.append(
                    StorageClassInfo(
                        name=sc.metadata.name,
                        provisioner=sc.provisioner,
                        reclaim_policy=sc.reclaim_policy,
                        volume_binding_mode=sc.volume_binding_mode,
                        is_default=is_default,
                        allow_volume_expansion=bool(sc.allow_volume_expansion),
                    )
                )
            return results

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(_thread_pool, _list)

    async def inspect_pvc(self, namespace: str, pvc_name: str) -> Optional[PVCInfo]:
        """Inspect PVC if it exists."""
        def _inspect():
            api = self._get_api_client()
            core_v1 = client.CoreV1Api(api)
            try:
                pvc = core_v1.read_namespaced_persistent_volume_claim(pvc_name, namespace)
            except ApiException as e:
                if e.status == 404:
                    return None
                raise

            cap_bytes = None
            if pvc.status and pvc.status.capacity and "storage" in pvc.status.capacity:
                storage_str = pvc.status.capacity["storage"]
                cap_bytes = self._parse_k8s_quantity(storage_str)
            elif pvc.spec and pvc.spec.resources and pvc.spec.resources.requests:
                storage_str = pvc.spec.resources.requests.get("storage")
                if storage_str:
                    cap_bytes = self._parse_k8s_quantity(storage_str)

            return PVCInfo(
                name=pvc.metadata.name,
                namespace=namespace,
                status=pvc.status.phase if pvc.status else "Unknown",
                capacity_bytes=cap_bytes,
                storage_class_name=pvc.spec.storage_class_name,
                access_modes=pvc.spec.access_modes or [],
            )

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(_thread_pool, _inspect)

    @staticmethod
    def _parse_k8s_quantity(quantity: str) -> int:
        """Parse k8s storage quantities (e.g., 10Gi, 500M) into bytes."""
        quantity = quantity.strip()
        multipliers = {
            "Ki": 1024,
            "Mi": 1024**2,
            "Gi": 1024**3,
            "Ti": 1024**4,
            "Pi": 1024**5,
            "K": 1000,
            "M": 1000**2,
            "G": 1000**3,
            "T": 1000**4,
        }
        for suffix, mult in multipliers.items():
            if quantity.endswith(suffix):
                val = quantity[: -len(suffix)]
                try:
                    return int(float(val) * mult)
                except ValueError:
                    return 0
        try:
            return int(quantity)
        except ValueError:
            return 0

    async def prepare_destination(
        self,
        namespace: str,
        pvc_name: str,
        create_if_missing: bool = True,
        storage_class_name: Optional[str] = None,
        capacity_bytes: Optional[int] = None,
        access_modes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Verify PVC or create it if missing."""
        loop = asyncio.get_running_loop()

        def _prepare():
            api = self._get_api_client()
            core_v1 = client.CoreV1Api(api)

            existing = None
            try:
                existing = core_v1.read_namespaced_persistent_volume_claim(pvc_name, namespace)
            except ApiException as e:
                if e.status != 404:
                    raise

            if existing:
                return {
                    "created": False,
                    "pvc_name": pvc_name,
                    "namespace": namespace,
                    "phase": existing.status.phase if existing.status else "Unknown",
                }

            if not create_if_missing:
                raise RuntimeError(
                    f"Destination PVC '{pvc_name}' does not exist in namespace '{namespace}', and create_if_missing is False"
                )

            # Create PVC
            storage_str = "10Gi"
            if capacity_bytes:
                # Round to nearest MiB or GiB
                gib = capacity_bytes / (1024**3)
                if gib >= 1:
                    storage_str = f"{max(1, int(gib + 0.99))}Gi"
                else:
                    mib = max(10, int(capacity_bytes / (1024**2) + 1))
                    storage_str = f"{mib}Mi"

            modes = access_modes or ["ReadWriteOnce"]

            pvc_spec = client.V1PersistentVolumeClaimSpec(
                access_modes=modes,
                resources=client.V1VolumeResourceRequirements(
                    requests={"storage": storage_str}
                ),
            )
            if storage_class_name:
                pvc_spec.storage_class_name = storage_class_name

            pvc_body = client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(
                    name=pvc_name,
                    namespace=namespace,
                    labels={"app.kubernetes.io/managed-by": "volmigrate"},
                ),
                spec=pvc_spec,
            )

            created_pvc = core_v1.create_namespaced_persistent_volume_claim(
                namespace=namespace, body=pvc_body
            )
            return {
                "created": True,
                "pvc_name": pvc_name,
                "namespace": namespace,
                "phase": created_pvc.status.phase if created_pvc.status else "Pending",
            }

        return await loop.run_in_executor(_thread_pool, _prepare)

    async def wait_until_writable(
        self, destination_info: Dict[str, Any], timeout_seconds: int = 120
    ) -> Dict[str, Any]:
        """Spawn a temporary non-privileged transfer helper pod mounting the target PVC."""
        namespace = destination_info["namespace"]
        pvc_name = destination_info["pvc_name"]
        pod_name = f"volmigrate-transfer-{uuid.uuid4().hex[:8]}"

        loop = asyncio.get_running_loop()

        def _spawn_and_wait():
            api = self._get_api_client()
            core_v1 = client.CoreV1Api(api)

            # Create helper pod
            pod_manifest = client.V1Pod(
                metadata=client.V1ObjectMeta(
                    name=pod_name,
                    namespace=namespace,
                    labels={
                        "app.kubernetes.io/name": "volmigrate-worker",
                        "app.kubernetes.io/component": "transfer-helper",
                    },
                ),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    containers=[
                        client.V1Container(
                            name="helper",
                            image="alpine:3.20",
                            command=["sh", "-c", "trap 'exit 0' TERM INT; sleep 3600 & wait"],
                            volume_mounts=[
                                client.V1VolumeMount(
                                    name="vol",
                                    mount_path="/destination",
                                )
                            ],
                            resources=client.V1ResourceRequirements(
                                requests={"cpu": "100m", "memory": "64Mi"},
                                limits={"cpu": "1000m", "memory": "512Mi"},
                            ),
                        )
                    ],
                    volumes=[
                        client.V1Volume(
                            name="vol",
                            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                claim_name=pvc_name
                            ),
                        )
                    ],
                ),
            )

            core_v1.create_namespaced_pod(namespace=namespace, body=pod_manifest)

            # Wait for pod to enter Running state
            start_time = time.time()
            while time.time() - start_time < timeout_seconds:
                p = core_v1.read_namespaced_pod(pod_name, namespace)
                if p.status and p.status.phase == "Running":
                    return {
                        "namespace": namespace,
                        "pvc_name": pvc_name,
                        "helper_pod_name": pod_name,
                        "mount_path": "/destination",
                    }
                if p.status and p.status.phase in ("Failed", "Unknown"):
                    raise RuntimeError(f"Helper pod entered invalid phase: {p.status.phase}")
                time.sleep(2)

            raise TimeoutError(f"Helper pod '{pod_name}' did not enter Running state within {timeout_seconds}s")

        return await loop.run_in_executor(_thread_pool, _spawn_and_wait)

    async def cleanup_temporary_resources(self, resources: Dict[str, Any]) -> None:
        """Safely clean up helper pod (never touches destination PVC data)."""
        pod_name = resources.get("helper_pod_name")
        namespace = resources.get("namespace")
        if not pod_name or not namespace:
            return

        def _cleanup():
            api = self._get_api_client()
            core_v1 = client.CoreV1Api(api)
            try:
                core_v1.delete_namespaced_pod(
                    name=pod_name,
                    namespace=namespace,
                    grace_period_seconds=1,
                )
                logger.info("Successfully cleaned up helper pod %s in %s", pod_name, namespace)
            except ApiException as e:
                if e.status != 404:
                    logger.warning("Failed to delete helper pod %s: %s", pod_name, e)

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(_thread_pool, _cleanup)
