"""Streaming Tar Transfer Engine with bounded buffer, backpressure, and metrics."""

import asyncio
import hashlib
import time
from typing import AsyncIterator, Callable, Optional, Awaitable
from kubernetes.stream import stream as k8s_stream
from kubernetes import client
from volmigrate.core.config import get_settings


class StreamingTransferEngine:
    """Orchestrates streaming tar byte transfer from source stream to destination pod."""

    def __init__(
        self,
        progress_callback: Optional[Callable[[int, float, Optional[str]], Awaitable[None]]] = None,
        cancel_event: Optional[asyncio.Event] = None,
    ):
        self.progress_callback = progress_callback
        self.cancel_event = cancel_event
        self.transferred_bytes = 0
        self.start_time = 0.0
        self.hasher = hashlib.sha256()
        self.settings = get_settings()

    async def transfer(
        self,
        source_stream: AsyncIterator[bytes],
        dest_api_client: client.ApiClient,
        namespace: str,
        helper_pod_name: str,
        mount_path: str = "/destination",
    ) -> dict:
        """Stream tar bytes directly into the Kubernetes helper pod's tar extraction process."""
        self.start_time = time.time()
        last_progress_time = self.start_time
        last_progress_bytes = 0

        core_v1 = client.CoreV1Api(dest_api_client)

        # Exec command inside helper pod to untar from stdin:
        # tar -x -p -f - -C /destination
        # -p preserves permissions
        exec_command = [
            "tar",
            "-x",
            "-p",
            "-f",
            "-",
            "-C",
            mount_path,
        ]

        # Use WebSocket client from kubernetes.stream
        ws_client = k8s_stream(
            core_v1.connect_get_namespaced_pod_exec,
            helper_pod_name,
            namespace,
            command=exec_command,
            stderr=True,
            stdin=True,
            stdout=True,
            tty=False,
            _preload_content=False,
        )

        try:
            async for chunk in source_stream:
                if self.cancel_event and self.cancel_event.is_set():
                    raise asyncio.CancelledError("Transfer was cancelled by operator")

                if not chunk:
                    continue

                chunk_len = len(chunk)
                self.transferred_bytes += chunk_len
                self.hasher.update(chunk)

                # Send bytes to stdin of k8s exec process
                # Run ws_client.write_stdin in executor to prevent blocking the event loop
                await asyncio.to_thread(ws_client.write_stdin, chunk)

                # Periodic progress reporting (every ~0.5s or 1MB)
                now = time.time()
                elapsed_since_report = now - last_progress_time
                if elapsed_since_report >= 0.5 or (self.transferred_bytes - last_progress_bytes) >= 1024 * 1024:
                    bps = (self.transferred_bytes - last_progress_bytes) / max(0.001, elapsed_since_report)
                    last_progress_time = now
                    last_progress_bytes = self.transferred_bytes

                    if self.progress_callback:
                        await self.progress_callback(self.transferred_bytes, bps, None)

            # Close stdin to signal EOF to tar
            await asyncio.to_thread(ws_client.write_stdin, "")

            # Wait for tar process to complete
            ws_client.run_forever(timeout=30)

            # Check if there were errors in stderr
            error_msg = ws_client.read_stderr()
            if error_msg and "error" in error_msg.lower():
                raise RuntimeError(f"Error during remote tar extraction: {error_msg}")

            total_elapsed = max(0.001, time.time() - self.start_time)
            avg_bps = self.transferred_bytes / total_elapsed
            checksum = self.hasher.hexdigest()

            if self.progress_callback:
                await self.progress_callback(self.transferred_bytes, avg_bps, checksum)

            return {
                "transferred_bytes": self.transferred_bytes,
                "duration_seconds": total_elapsed,
                "avg_throughput_bps": avg_bps,
                "checksum_sha256": checksum,
            }

        finally:
            try:
                ws_client.close()
            except Exception:
                pass
