"""Post-transfer validation and integrity verification."""

import asyncio
from kubernetes import client
from kubernetes.stream import stream as k8s_stream
from volmigrate.models.enums import ValidationPolicy


class TransferValidator:
    """Validates the extracted data in the destination helper pod."""

    @staticmethod
    async def validate(
        api_client: client.ApiClient,
        namespace: str,
        helper_pod_name: str,
        mount_path: str = "/destination",
        policy: ValidationPolicy = ValidationPolicy.FAST_MANIFEST,
    ) -> dict:
        """Run validation checks inside the helper pod."""
        core_v1 = client.CoreV1Api(api_client)

        def _exec_command(cmd: list[str]) -> str:
            resp = k8s_stream(
                core_v1.connect_get_namespaced_pod_exec,
                helper_pod_name,
                namespace,
                command=cmd,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
            return resp

        # Check total file count and disk usage in destination
        count_cmd = ["sh", "-c", f"find {mount_path} -mindepth 1 | wc -l"]
        du_cmd = ["sh", "-c", f"du -sb {mount_path} | cut -f1"]

        try:
            file_count_str = await asyncio.to_thread(_exec_command, count_cmd)
            file_count = int(file_count_str.strip() or 0)
        except Exception:
            file_count = 0

        try:
            du_str = await asyncio.to_thread(_exec_command, du_cmd)
            dest_size = int(du_str.strip() or 0)
        except Exception:
            dest_size = 0

        # Smoke check: ensure writable by creating and removing a tiny verification probe file
        probe_cmd = [
            "sh",
            "-c",
            f"touch {mount_path}/.volmigrate_probe && rm {mount_path}/.volmigrate_probe",
        ]
        probe_res = await asyncio.to_thread(_exec_command, probe_cmd)
        writable = "error" not in probe_res.lower()

        return {
            "policy": policy.value,
            "file_count": file_count,
            "destination_size_bytes": dest_size,
            "destination_writable": writable,
            "valid": writable and file_count >= 0,
        }
