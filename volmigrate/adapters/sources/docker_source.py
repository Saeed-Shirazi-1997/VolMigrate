"""Docker Source Adapter implementation."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import os
from typing import AsyncIterator, List, Optional, Dict, Any
import docker
from docker.errors import DockerException, NotFound

from volmigrate.adapters.base import BaseSourceAdapter
from volmigrate.schemas.discovery import VolumeInfo, VolumeInspectResult, VolumeConsumer

logger = logging.getLogger("volmigrate.adapters.docker")
_thread_pool = ThreadPoolExecutor(max_workers=8)


class DockerSourceAdapter(BaseSourceAdapter):
    """Adapter for inspecting and streaming from Docker volumes."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        tls_config: Optional[Any] = None,
    ):
        self.base_url = base_url
        self.tls_config = tls_config
        self._client: Optional[docker.DockerClient] = None

    def _get_client(self) -> docker.DockerClient:
        if self._client is None:
            if self.base_url:
                self._client = docker.DockerClient(
                    base_url=self.base_url,
                    tls=self.tls_config,
                    timeout=30,
                )
            else:
                self._client = docker.from_env(timeout=30)
        return self._client

    async def test_connection(self) -> Dict[str, Any]:
        """Test Docker connection and return server info."""
        def _test():
            client = self._get_client()
            client.ping()
            version = client.version()
            info = client.info()
            return {
                "success": True,
                "version": version.get("Version", "unknown"),
                "api_version": version.get("ApiVersion", "unknown"),
                "os": info.get("OperatingSystem", "unknown"),
                "containers": info.get("Containers", 0),
                "server_time": info.get("SystemTime"),
            }

        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(_thread_pool, _test)
        except DockerException as e:
            logger.error("Docker connection test failed: %s", e)
            raise RuntimeError(f"Failed to connect to Docker: {e}") from e

    async def list_volumes(self) -> List[VolumeInfo]:
        """List all Docker volumes with basic metadata."""
        def _list():
            client = self._get_client()
            vols = client.volumes.list()
            # Find consumers for all volumes in one pass
            containers = client.containers.list(all=True)
            volume_consumer_counts: Dict[str, int] = {}
            for c in containers:
                for mount in c.attrs.get("Mounts", []):
                    vol_name = mount.get("Name")
                    if vol_name:
                        volume_consumer_counts[vol_name] = volume_consumer_counts.get(vol_name, 0) + 1

            result = []
            for v in vols:
                attrs = v.attrs
                name = attrs.get("Name")
                driver = attrs.get("Driver", "local")
                mountpoint = attrs.get("Mountpoint")
                created_at = attrs.get("CreatedAt")
                labels = attrs.get("Labels") or {}
                result.append(
                    VolumeInfo(
                        name=name,
                        driver=driver,
                        mountpoint=mountpoint,
                        created_at=created_at,
                        labels=labels,
                        active_consumers_count=volume_consumer_counts.get(name, 0),
                    )
                )
            return result

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(_thread_pool, _list)

    async def list_consumers(self, volume_id: str) -> List[VolumeConsumer]:
        """List containers that mount the given volume."""
        def _consumers():
            client = self._get_client()
            containers = client.containers.list(all=True)
            consumers = []
            for c in containers:
                for mount in c.attrs.get("Mounts", []):
                    if mount.get("Name") == volume_id or mount.get("Source") == volume_id:
                        status = c.status
                        is_running = status == "running"
                        consumers.append(
                            VolumeConsumer(
                                container_id=c.id[:12],
                                container_name=c.name,
                                status=status,
                                is_running=is_running,
                                mode=mount.get("Mode", "rw"),
                            )
                        )
            return consumers

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(_thread_pool, _consumers)

    async def inspect_volume(self, volume_id: str) -> VolumeInspectResult:
        """Inspect detailed volume metadata, consumers, and estimated size."""
        def _inspect():
            client = self._get_client()
            try:
                v = client.volumes.get(volume_id)
            except NotFound:
                raise FileNotFoundError(f"Docker volume '{volume_id}' not found")

            attrs = v.attrs
            name = attrs.get("Name")
            driver = attrs.get("Driver", "local")
            mountpoint = attrs.get("Mountpoint")
            labels = attrs.get("Labels") or {}

            # Check consumers
            containers = client.containers.list(all=True)
            consumers = []
            for c in containers:
                for mount in c.attrs.get("Mounts", []):
                    if mount.get("Name") == volume_id or mount.get("Source") == volume_id:
                        consumers.append(
                            VolumeConsumer(
                                container_id=c.id[:12],
                                container_name=c.name,
                                status=c.status,
                                is_running=c.status == "running",
                                mode=mount.get("Mode", "rw"),
                            )
                        )

            # Size estimation: check local filesystem first if accessible, else try local image
            size_bytes = None
            warning = None
            if mountpoint and os.path.isdir(mountpoint):
                try:
                    total_size = 0
                    for dirpath, dirnames, filenames in os.walk(mountpoint):
                        for f in filenames:
                            fp = os.path.join(dirpath, f)
                            if not os.path.islink(fp):
                                total_size += os.path.getsize(fp)
                    size_bytes = total_size
                except Exception:
                    pass

            return VolumeInspectResult(
                name=name,
                driver=driver,
                mountpoint=mountpoint,
                labels=labels,
                consumers=consumers,
                estimated_size_bytes=size_bytes,
                readable=True,
                warning=warning,
            )

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(_thread_pool, _inspect)

    async def check_read_access(self, volume_id: str) -> bool:
        """Verify reading from volume."""
        try:
            inspect_result = await self.inspect_volume(volume_id)
            return inspect_result.readable
        except Exception:
            return False

    async def open_read_stream(
        self, volume_id: str, options: Optional[Dict[str, Any]] = None
    ) -> AsyncIterator[bytes]:
        """Stream tar bytes directly from source Docker volume without temp disk archive."""
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[Optional[bytes]] = asyncio.Queue(maxsize=32)
        exception_holder = []

        def _producer():
            client = self._get_client()
            container = None
            try:
                # Select available local image with tar
                image = "alpine:3.20"
                try:
                    local_tags = {tag for img in client.images.list() for tag in (img.tags or [])}
                    for candidate in ["alpine:3.20", "alpine:latest", "valkey/valkey:9-alpine", "postgres:18-alpine", "busybox:latest"]:
                        if candidate in local_tags:
                            image = candidate
                            break
                except Exception:
                    pass

                # Launch temporary read-only container
                container = client.containers.create(
                    image=image,
                    command=["tar", "-c", "-C", "/data", "."],
                    volumes={volume_id: {"bind": "/data", "mode": "ro"}},
                    network_disabled=True,
                )
                container.start()

                # Stream stdout from container as tar stream
                stream = container.logs(stream=True, stdout=True, stderr=False)
                for chunk in stream:
                    if chunk:
                        asyncio.run_coroutine_threadsafe(queue.put(chunk), loop).result()

                asyncio.run_coroutine_threadsafe(queue.put(None), loop).result()
            except Exception as e:
                logger.error("Error reading tar stream from Docker volume: %s", e)
                exception_holder.append(e)
                asyncio.run_coroutine_threadsafe(queue.put(None), loop).result()
            finally:
                if container:
                    try:
                        container.remove(force=True)
                    except Exception:
                        pass

        # Run producer in thread
        loop.run_in_executor(_thread_pool, _producer)

        while True:
            chunk = await queue.get()
            if chunk is None:
                if exception_holder:
                    raise exception_holder[0]
                break
            yield chunk
