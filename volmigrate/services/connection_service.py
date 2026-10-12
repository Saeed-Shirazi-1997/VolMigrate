"""Connection service for managing and instantiating source/dest adapters."""

from datetime import datetime, timezone
import json
import logging
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from volmigrate.core.security import encrypt_secret, decrypt_secret
from volmigrate.models import Connection, ConnectionType, ConnectionStatus
from volmigrate.schemas.connection import ConnectionCreate, ConnectionUpdate, ConnectionTestResult
from volmigrate.adapters.sources.docker_source import DockerSourceAdapter
from volmigrate.adapters.destinations.k8s_destination import KubernetesDestinationAdapter

logger = logging.getLogger("volmigrate.services.connection")


class ConnectionService:

    @staticmethod
    async def create_connection(db: AsyncSession, data: ConnectionCreate) -> Connection:
        config_enc = None
        if data.config_payload:
            config_json = json.dumps(data.config_payload)
            config_enc = encrypt_secret(config_json)

        conn = Connection(
            name=data.name,
            type=data.type,
            endpoint=data.endpoint,
            config_encrypted=config_enc,
            status=ConnectionStatus.UNTESTED,
        )
        db.add(conn)
        await db.commit()
        await db.refresh(conn)
        return conn

    @staticmethod
    async def get_connection(db: AsyncSession, connection_id: str) -> Optional[Connection]:
        result = await db.execute(select(Connection).where(Connection.id == connection_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def list_connections(db: AsyncSession) -> List[Connection]:
        result = await db.execute(select(Connection).order_by(Connection.created_at.desc()))
        return list(result.scalars().all())

    @staticmethod
    async def update_connection(
        db: AsyncSession, connection_id: str, data: ConnectionUpdate
    ) -> Optional[Connection]:
        conn = await ConnectionService.get_connection(db, connection_id)
        if not conn:
            return None

        if data.name is not None:
            conn.name = data.name
        if data.endpoint is not None:
            conn.endpoint = data.endpoint
        if data.config_payload is not None:
            config_json = json.dumps(data.config_payload)
            conn.config_encrypted = encrypt_secret(config_json)

        conn.status = ConnectionStatus.UNTESTED
        await db.commit()
        await db.refresh(conn)
        return conn

    @staticmethod
    async def delete_connection(db: AsyncSession, connection_id: str) -> bool:
        conn = await ConnectionService.get_connection(db, connection_id)
        if not conn:
            return False
        await db.delete(conn)
        await db.commit()
        return True

    @staticmethod
    def get_adapter(connection: Connection):
        """Build configured adapter instance from connection model."""
        config_payload = {}
        if connection.config_encrypted:
            try:
                decrypted_json = decrypt_secret(connection.config_encrypted)
                config_payload = json.loads(decrypted_json)
            except Exception as e:
                logger.error("Failed to decrypt connection config: %s", e)

        if connection.type == ConnectionType.DOCKER:
            return DockerSourceAdapter(
                base_url=connection.endpoint,
                tls_config=config_payload.get("tls"),
            )
        elif connection.type == ConnectionType.KUBERNETES:
            kubeconfig_dict = config_payload.get("kubeconfig")
            if not kubeconfig_dict and config_payload.get("kubeconfig_raw"):
                try:
                    import yaml
                    kubeconfig_dict = yaml.safe_load(config_payload["kubeconfig_raw"])
                except Exception as e:
                    logger.error("Failed to parse kubeconfig YAML: %s", e)

            return KubernetesDestinationAdapter(
                kubeconfig_dict=kubeconfig_dict,
                context=config_payload.get("context"),
                in_cluster=config_payload.get("in_cluster", False),
            )
        raise ValueError(f"Unknown connection type: {connection.type}")

    @staticmethod
    async def test_connection(db: AsyncSession, connection_id: str) -> ConnectionTestResult:
        conn = await ConnectionService.get_connection(db, connection_id)
        if not conn:
            return ConnectionTestResult(success=False, error="Connection not found")

        adapter = ConnectionService.get_adapter(conn)
        try:
            test_res = await adapter.test_connection()
            conn.status = ConnectionStatus.ACTIVE
            conn.last_tested_at = datetime.now(timezone.utc)
            conn.last_error = None
            await db.commit()
            return ConnectionTestResult(
                success=True,
                version_info=str(test_res.get("version") or test_res.get("git_version")),
                details=test_res,
            )
        except Exception as e:
            conn.status = ConnectionStatus.ERROR
            conn.last_tested_at = datetime.now(timezone.utc)
            conn.last_error = str(e)
            await db.commit()
            return ConnectionTestResult(success=False, error=str(e))
