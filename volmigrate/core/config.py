"""Application configuration and settings."""

from functools import lru_cache
import os
import socket
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # General
    PROJECT_NAME: str = "Volmigrate"
    VERSION: str = "0.1.0"
    API_V1_PREFIX: str = "/api"
    DEBUG: bool = False

    # Database
    # Defaults to SQLite for immediate local dev/testing, supports PostgreSQL in production
    DATABASE_URL: str = "sqlite+aiosqlite:///./volmigrate.db"
    SYNC_DATABASE_URL: str = "sqlite:///./volmigrate.db"

    # Worker Settings
    WORKER_ID: str = os.getenv("WORKER_ID", f"worker-{socket.gethostname()}")
    LEASE_DURATION_SECONDS: int = 30
    HEARTBEAT_INTERVAL_SECONDS: int = 10
    WORKER_POLL_INTERVAL_SECONDS: float = 2.0

    # Transfer Settings
    TRANSFER_BUFFER_SIZE_BYTES: int = 4 * 1024 * 1024  # 4 MB bounded buffer
    DEFAULT_TRANSFER_IMAGE: str = "alpine:3.20"

    # Security
    SECRET_KEY: str = "volmigrate-secret-key-change-in-production-32b!"
    
    # Defaults
    DEFAULT_DOCKER_URL: str = "unix:///var/run/docker.sock"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
