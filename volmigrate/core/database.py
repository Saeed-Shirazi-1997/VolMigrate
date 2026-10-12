"""Database setup, session management, and Base model.

Supports both native PostgreSQL (asyncpg) and zero-dependency SQLite.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import asyncio
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from volmigrate.core.config import get_settings

logger = logging.getLogger("volmigrate.database")
Base = declarative_base()

settings = get_settings()
db_url = settings.DATABASE_URL

# Check if using PostgreSQL with asyncpg
if db_url.startswith("postgresql://") or "asyncpg" in db_url:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
    if db_url.startswith("postgresql://"):
        async_db_url = db_url.replace("postgresql://", "postgresql+asyncpg://")
    else:
        async_db_url = db_url

    engine = create_async_engine(async_db_url, echo=settings.DEBUG, future=True)
    async_session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    is_native_async = True

else:
    # Try aiosqlite if available, else use sync engine with async wrapper
    try:
        import aiosqlite
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

        if db_url.startswith("sqlite://") and not db_url.startswith("sqlite+"):
            async_db_url = db_url.replace("sqlite://", "sqlite+aiosqlite://")
        else:
            async_db_url = db_url

        engine = create_async_engine(async_db_url, echo=settings.DEBUG, future=True)
        async_session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
        is_native_async = True
    except ImportError:
        # Transparent Async wrapper over synchronous SQLite session
        clean_url = db_url.replace("+aiosqlite", "")
        sync_engine = create_engine(clean_url, echo=settings.DEBUG, future=True)
        sync_session_factory = sessionmaker(bind=sync_engine, expire_on_commit=False)
        is_native_async = False


class AsyncSessionWrapper:
    """Wrapper that provides async methods over a synchronous SQLAlchemy Session."""

    def __init__(self, sync_session: Session):
        self._s = sync_session

    async def execute(self, statement, *args, **kwargs):
        return await asyncio.to_thread(self._s.execute, statement, *args, **kwargs)

    async def get(self, entity, ident, *args, **kwargs):
        return await asyncio.to_thread(self._s.get, entity, ident, *args, **kwargs)

    async def commit(self):
        await asyncio.to_thread(self._s.commit)

    async def rollback(self):
        await asyncio.to_thread(self._s.rollback)

    async def refresh(self, instance, *args, **kwargs):
        await asyncio.to_thread(self._s.refresh, instance, *args, **kwargs)

    def add(self, instance):
        self._s.add(instance)

    async def delete(self, instance):
        await asyncio.to_thread(self._s.delete, instance)

    async def close(self):
        await asyncio.to_thread(self._s.close)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            await self.rollback()
        await self.close()


async def get_db() -> AsyncGenerator[any, None]:
    """Dependency for FastAPI route handlers."""
    if is_native_async:
        async with async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        sync_sess = sync_session_factory()
        wrapper = AsyncSessionWrapper(sync_sess)
        try:
            yield wrapper
            await wrapper.commit()
        except Exception:
            await wrapper.rollback()
            raise
        finally:
            await wrapper.close()


@asynccontextmanager
async def get_db_context():
    """Context manager for background tasks and workers."""
    if is_native_async:
        async with async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    else:
        sync_sess = sync_session_factory()
        wrapper = AsyncSessionWrapper(sync_sess)
        try:
            yield wrapper
            await wrapper.commit()
        except Exception:
            await wrapper.rollback()
            raise
        finally:
            await wrapper.close()


async def init_db() -> None:
    """Initialize database tables."""
    if is_native_async:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    else:
        await asyncio.to_thread(Base.metadata.create_all, sync_engine)
