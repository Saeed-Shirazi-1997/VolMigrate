"""Migration Executor for running the end-to-end migration pipeline."""

import asyncio
from datetime import datetime, timezone
import logging
from typing import Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession

from volmigrate.core.database import get_db_context
from volmigrate.core.config import get_settings
from volmigrate.models import Migration, MigrationStatus, EventLevel
from volmigrate.schemas.event import SSEMessage
from volmigrate.services.connection_service import ConnectionService
from volmigrate.services.migration_service import MigrationService
from volmigrate.services.event_broadcaster import broadcaster
from volmigrate.engine.streaming import StreamingTransferEngine
from volmigrate.engine.validator import TransferValidator
from volmigrate.worker.lease_manager import LeaseManager

logger = logging.getLogger("volmigrate.worker.executor")


class MigrationExecutor:
    """Executes a single claimed migration through the state machine."""

    def __init__(self, lease_manager: LeaseManager):
        self.lease_manager = lease_manager
        self.settings = get_settings()

    async def execute(self, migration_id: str) -> bool:
        """Run the migration lifecycle for the given migration ID."""
        heartbeat_task: Optional[asyncio.Task] = None
        cancel_event = asyncio.Event()
        temporary_resources: Dict[str, Any] = {}
        dest_adapter = None

        async def _heartbeat_loop():
            while True:
                try:
                    await asyncio.sleep(self.settings.HEARTBEAT_INTERVAL_SECONDS)
                    async with get_db_context() as session:
                        # Also check if cancellation was requested in DB
                        mig = await session.get(Migration, migration_id)
                        if mig and mig.status == MigrationStatus.CANCELLED:
                            logger.info("Cancellation detected for job %s", migration_id)
                            cancel_event.set()
                            break

                        renewed = await self.lease_manager.heartbeat(session, migration_id)
                        if not renewed:
                            logger.warning("Heartbeat failed for %s", migration_id)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Error in heartbeat loop: %s", e)

        heartbeat_task = asyncio.create_task(_heartbeat_loop())

        try:
            # 1. PREPARING PHASE
            async with get_db_context() as session:
                mig = await session.get(Migration, migration_id)
                if not mig:
                    return False
                mig.status = MigrationStatus.PREPARING
                mig.current_step = "Preparing destination PVC and helper resources"
                await session.commit()

                await MigrationService.emit_event(
                    session,
                    migration_id,
                    "Preparing",
                    EventLevel.INFO,
                    f"Acquiring connections and preparing PVC '{mig.dest_pvc_name}' in '{mig.dest_namespace}'",
                )

                source_conn = await ConnectionService.get_connection(session, mig.source_connection_id)
                dest_conn = await ConnectionService.get_connection(session, mig.dest_conn_id if hasattr(mig, 'dest_conn_id') else mig.dest_connection_id)

                if not source_conn or not dest_conn:
                    raise RuntimeError("Source or destination connection not found")

                source_adapter = ConnectionService.get_adapter(source_conn)
                dest_adapter = ConnectionService.get_adapter(dest_conn)

                # Prepare PVC
                dest_prep = await dest_adapter.prepare_destination(
                    namespace=mig.dest_namespace,
                    pvc_name=mig.dest_pvc_name,
                    create_if_missing=mig.create_pvc_if_missing,
                    storage_class_name=mig.storage_class_name,
                    capacity_bytes=mig.capacity_bytes,
                )
                logger.info("Destination PVC preparation result: %s", dest_prep)

                # Wait for PVC to be writable by launching non-privileged helper pod
                await MigrationService.emit_event(
                    session,
                    migration_id,
                    "Preparing",
                    EventLevel.INFO,
                    "Spawning temporary non-privileged transfer helper pod",
                )

                temporary_resources = await dest_adapter.wait_until_writable(dest_prep)
                logger.info("Temporary transfer helper pod ready: %s", temporary_resources)

            # 2. TRANSFERRING PHASE
            async with get_db_context() as session:
                mig = await session.get(Migration, migration_id)
                mig.status = MigrationStatus.TRANSFERRING
                mig.current_step = "Streaming tar transfer in progress"
                await session.commit()

                await MigrationService.emit_event(
                    session,
                    migration_id,
                    "Transferring",
                    EventLevel.INFO,
                    f"Beginning streaming tar transfer from Docker volume '{mig.source_volume_name}'",
                )

            # Define progress callback
            async def _on_progress(transferred_bytes: int, bps: float, checksum: Optional[str]):
                async with get_db_context() as p_session:
                    m = await p_session.get(Migration, migration_id)
                    if m:
                        m.transferred_bytes = transferred_bytes
                        m.throughput_bytes_per_sec = bps
                        if checksum:
                            m.checksum_sha256 = checksum
                        await p_session.commit()

                        pct = None
                        if m.total_bytes_estimated and m.total_bytes_estimated > 0:
                            pct = min(100.0, (transferred_bytes / m.total_bytes_estimated) * 100.0)

                        await broadcaster.publish(
                            migration_id,
                            SSEMessage(
                                event_type="progress",
                                migration_id=migration_id,
                                status=m.status.value,
                                step=m.current_step,
                                transferred_bytes=transferred_bytes,
                                total_bytes=m.total_bytes_estimated,
                                throughput_bytes_per_sec=bps,
                                percent=pct,
                            ),
                        )

            # Open source read stream and pipe to destination helper pod
            source_stream = source_adapter.open_read_stream(mig.source_volume_name)
            engine = StreamingTransferEngine(
                progress_callback=_on_progress,
                cancel_event=cancel_event,
            )

            transfer_result = await engine.transfer(
                source_stream=source_stream,
                dest_api_client=dest_adapter._get_api_client(),
                namespace=temporary_resources["namespace"],
                helper_pod_name=temporary_resources["helper_pod_name"],
                mount_path=temporary_resources["mount_path"],
            )

            # 3. VALIDATING PHASE
            async with get_db_context() as session:
                mig = await session.get(Migration, migration_id)
                mig.status = MigrationStatus.VALIDATING
                mig.current_step = "Validating extracted data integrity"
                await session.commit()

                await MigrationService.emit_event(
                    session,
                    migration_id,
                    "Validating",
                    EventLevel.INFO,
                    "Validating destination file tree and write probe",
                )

                val_result = await TransferValidator.validate(
                    api_client=dest_adapter._get_api_client(),
                    namespace=temporary_resources["namespace"],
                    helper_pod_name=temporary_resources["helper_pod_name"],
                    mount_path=temporary_resources["mount_path"],
                    policy=mig.validation_policy,
                )

                if not val_result.get("valid"):
                    raise RuntimeError(f"Post-transfer validation failed: {val_result}")

                # 4. COMPLETED PHASE
                mig.status = MigrationStatus.COMPLETED
                mig.current_step = "Migration successfully completed"
                mig.completed_at = datetime.now(timezone.utc)
                mig.transferred_bytes = transfer_result["transferred_bytes"]
                mig.checksum_sha256 = transfer_result["checksum_sha256"]
                mig.throughput_bytes_per_sec = transfer_result["avg_throughput_bps"]
                await session.commit()

                await MigrationService.emit_event(
                    session,
                    migration_id,
                    "Completed",
                    EventLevel.INFO,
                    f"Successfully transferred {transfer_result['transferred_bytes']} bytes. Checksum: {transfer_result['checksum_sha256']}",
                    payload=val_result,
                )

                await broadcaster.publish(
                    migration_id,
                    SSEMessage(
                        event_type="completed",
                        migration_id=migration_id,
                        status="completed",
                        step="Completed",
                        transferred_bytes=transfer_result["transferred_bytes"],
                        throughput_bytes_per_sec=transfer_result["avg_throughput_bps"],
                        message="Migration finished successfully",
                    ),
                )

            return True

        except asyncio.CancelledError:
            logger.warning("Migration %s was cancelled during execution", migration_id)
            async with get_db_context() as session:
                mig = await session.get(Migration, migration_id)
                if mig:
                    mig.status = MigrationStatus.CANCELLED
                    mig.current_step = "Cancelled"
                    mig.completed_at = datetime.now(timezone.utc)
                    await session.commit()
            return False

        except Exception as e:
            logger.error("Migration %s failed: %s", migration_id, e, exc_info=True)
            async with get_db_context() as session:
                mig = await session.get(Migration, migration_id)
                if mig:
                    mig.status = MigrationStatus.FAILED
                    mig.error_message = str(e)
                    mig.current_step = "Failed"
                    mig.completed_at = datetime.now(timezone.utc)
                    await session.commit()

                    await MigrationService.emit_event(
                        session,
                        migration_id,
                        "Failed",
                        EventLevel.ERROR,
                        f"Migration failed with error: {e}",
                    )

                    await broadcaster.publish(
                        migration_id,
                        SSEMessage(
                            event_type="failed",
                            migration_id=migration_id,
                            status="failed",
                            step="Failed",
                            message=str(e),
                        ),
                    )
            return False

        finally:
            if heartbeat_task and not heartbeat_task.done():
                heartbeat_task.cancel()

            # Clean up temporary helper pod (Safe by default: destination PVC is preserved)
            if dest_adapter and temporary_resources:
                try:
                    await dest_adapter.cleanup_temporary_resources(temporary_resources)
                except Exception as e:
                    logger.warning("Error cleaning up helper resources: %s", e)

            # Release worker lease
            async with get_db_context() as session:
                await self.lease_manager.release_lease(session, migration_id)
