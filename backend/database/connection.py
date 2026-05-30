"""
backend/database/connection.py
──────────────────────────────────────────────────────────────────────────────
asyncpg connection pool and SQLAlchemy 2.0 async session management for the
Real-Time Anomaly Detection System.

Implements:
  • create_engine()      — asyncpg-backed engine with min/max pool sizes from
                           backend_requirements.docx §3 (min=10, max=50).
  • DatabaseSessionManager — lifecycle manager that owns the engine and session
                             factory; used in FastAPI lifespan context.
  • get_db_session()     — FastAPI dependency (async generator).
  • get_raw_connection() — low-level asyncpg connection for COPY / batch ops.

Pool configuration (backend_requirements.docx §4):
  pool_size    = 10   (min_size — always-warm connections)
  max_overflow = 40   (max_size 50 = pool_size 10 + overflow 40)
  pool_timeout = 30 s — raise TimeoutError instead of queuing indefinitely
  pool_recycle = 3600 s — recycle connections every hour (avoids stale TCP)
  pool_pre_ping= True — validate connections before use (detects DB restarts)
──────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import contextlib
import logging
from collections.abc import AsyncGenerator, AsyncIterator

import asyncpg
from app.core.database import Base
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Pool constants
# Source: backend_requirements.docx §3 and §4
# ─────────────────────────────────────────────────────────────────────────────
_POOL_MIN_SIZE: int = 10   # always-warm connections (pool_size)
_POOL_MAX_SIZE: int = 50   # hard cap (pool_size + max_overflow)
_POOL_OVERFLOW: int = _POOL_MAX_SIZE - _POOL_MIN_SIZE   # = 40
_POOL_TIMEOUT: int  = 30   # seconds — max wait for a pooled connection
_POOL_RECYCLE: int  = 3600 # seconds — recycle stale connections hourly


# ─────────────────────────────────────────────────────────────────────────────
# DatabaseSessionManager
# Central owner of the async engine + session factory.
# Call .init(url) once at application startup, .close() at shutdown.
# ─────────────────────────────────────────────────────────────────────────────
class DatabaseSessionManager:
    """
    Manages the asyncpg-backed SQLAlchemy engine and session factory lifecycle.

    Usage (FastAPI lifespan)::

        db_manager = DatabaseSessionManager()

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            db_manager.init(settings.DATABASE_URL)
            yield
            await db_manager.close()

    All pool parameters honour backend_requirements.docx §3 / §4 targets.
    """

    def __init__(self) -> None:
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker[AsyncSession] | None = None

    # ── Initialisation ────────────────────────────────────────────────────
    def init(
        self,
        database_url: str,
        *,
        echo: bool = False,
        pool_size: int = _POOL_MIN_SIZE,
        max_overflow: int = _POOL_OVERFLOW,
        pool_timeout: int = _POOL_TIMEOUT,
        pool_recycle: int = _POOL_RECYCLE,
    ) -> None:
        """
        Create the async engine and session factory.

        Parameters
        ----------
        database_url:
            asyncpg DSN — ``postgresql+asyncpg://user:pass@host/db``
        echo:
            Log all emitted SQL (set True only in development).
        pool_size:
            Minimum always-warm connections (default 10 per §3).
        max_overflow:
            Extra connections allowed above pool_size (default 40; total=50).
        pool_timeout:
            Seconds to wait before raising TimeoutError when pool is exhausted.
        pool_recycle:
            Seconds before a connection is recycled to avoid stale TCP sessions.
        """
        if self._engine is not None:
            logger.warning(
                "DatabaseSessionManager.init() called more than once — ignoring."
            )
            return

        logger.info(
            "Initialising asyncpg engine pool_size=%d max_total=%d url=%s",
            pool_size,
            pool_size + max_overflow,
            _redact_url(database_url),
        )

        self._engine = create_async_engine(
            database_url,
            echo=echo,
            # ── Pool settings (backend_requirements.docx §3 / §4) ────────
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=True,         # detect stale connections proactively
            # ── asyncpg connect_args ──────────────────────────────────────
            connect_args={
                "server_settings": {
                    "application_name": "anomaly_detection_api",
                    "jit":             "off",  # disable JIT — faster for OLTP
                },
                "command_timeout": 60,          # statement timeout (seconds)
            },
        )

        self._session_factory = async_sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=False,  # avoid implicit lazy-loads after commit
            autocommit=False,
            autoflush=False,
        )

        logger.info("Engine ready — asyncpg pool initialised.")

    # ── Shutdown ──────────────────────────────────────────────────────────
    async def close(self) -> None:
        """Dispose the engine (drains the pool gracefully)."""
        if self._engine is None:
            logger.warning(
                "DatabaseSessionManager.close() called before .init() — no-op."
            )
            return
        await self._engine.dispose()
        self._engine = None
        self._session_factory = None
        logger.info("Async engine disposed — all pool connections closed.")

    # ── Session context manager ───────────────────────────────────────────
    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """
        Async context manager that yields a transactional ``AsyncSession``.

        Commits on clean exit; rolls back and re-raises on any exception.

        Usage::

            async with db_manager.session() as session:
                session.add(event)

        """
        if self._session_factory is None:
            raise RuntimeError(
                "DatabaseSessionManager not initialised. "
                "Call .init(database_url) first."
            )

        async with self._session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    # ── Raw connection context manager ────────────────────────────────────
    @contextlib.asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        """
        Yield a raw ``AsyncConnection`` for DDL, COPY, or bulk operations
        that bypass the ORM session layer.

        Usage::

            async with db_manager.connect() as conn:
                await conn.run_sync(Base.metadata.create_all)
        """
        if self._engine is None:
            raise RuntimeError(
                "DatabaseSessionManager not initialised. "
                "Call .init(database_url) first."
            )
        async with self._engine.begin() as conn:
            yield conn

    # ── DDL helpers ───────────────────────────────────────────────────────
    async def create_all_tables(self) -> None:
        """
        Emit CREATE TABLE statements for all models registered on ``Base``.
        Intended for testing / local dev; use Alembic for production migrations.
        """
        async with self.connect() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("create_all_tables: all mapped tables created (if not exists).")

    async def drop_all_tables(self) -> None:
        """
        Drop all tables — DESTRUCTIVE.  Only for test teardown.
        """
        async with self.connect() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        logger.warning("drop_all_tables: all mapped tables dropped.")

    # ── Properties ────────────────────────────────────────────────────────
    @property
    def engine(self) -> AsyncEngine:
        """The underlying ``AsyncEngine`` (for advanced inspection)."""
        if self._engine is None:
            raise RuntimeError("Engine not initialised.")
        return self._engine


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton — shared by FastAPI app and background workers
# ─────────────────────────────────────────────────────────────────────────────
db_manager = DatabaseSessionManager()


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI dependency — yields an AsyncSession per request
# ─────────────────────────────────────────────────────────────────────────────
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency that provides a managed ``AsyncSession``.

    Commits on success, rolls back on exception, closes always.

    Usage::

        @router.get("/events")
        async def list_events(db: AsyncSession = Depends(get_db_session)):
            ...
    """
    async with db_manager.session() as session:
        yield session


# ─────────────────────────────────────────────────────────────────────────────
# Low-level asyncpg pool (bypass SQLAlchemy for COPY / TimescaleDB-specific ops)
# ─────────────────────────────────────────────────────────────────────────────
_raw_pool: asyncpg.Pool | None = None  # type: ignore[type-arg]


async def init_raw_pool(database_dsn: str) -> None:
    """
    Initialise a bare asyncpg pool for COPY-based bulk inserts.

    Parameters
    ----------
    database_dsn:
        Plain asyncpg DSN — ``postgresql://user:pass@host/db``
        (no ``+asyncpg`` driver prefix, unlike the SQLAlchemy URL).
    """
    global _raw_pool
    if _raw_pool is not None:
        return
    _raw_pool = await asyncpg.create_pool(
        dsn=database_dsn,
        min_size=_POOL_MIN_SIZE,
        max_size=_POOL_MAX_SIZE,
        timeout=_POOL_TIMEOUT,
        command_timeout=60,
        server_settings={
            "application_name": "anomaly_detection_bulk_writer",
            "jit":             "off",
        },
    )
    logger.info(
        "Raw asyncpg pool ready — min=%d max=%d dsn=%s",
        _POOL_MIN_SIZE,
        _POOL_MAX_SIZE,
        _redact_url(database_dsn),
    )


async def close_raw_pool() -> None:
    """Drain and close the raw asyncpg pool."""
    global _raw_pool
    if _raw_pool is not None:
        await _raw_pool.close()
        _raw_pool = None
        logger.info("Raw asyncpg pool closed.")


@contextlib.asynccontextmanager
async def get_raw_connection() -> AsyncIterator[asyncpg.Connection]:  # type: ignore[type-arg]
    """
    Async context manager that borrows a connection from the raw asyncpg pool.

    Intended for:
      • COPY FROM STDIN (bulk batch inserts)
      • TimescaleDB-specific SQL not easily expressed via ORM

    Usage::

        async with get_raw_connection() as conn:
            await conn.copy_records_to_table(
                "anomaly_events",
                schema_name="anomaly",
                records=batch,
                columns=["event_id", "event_time", ...],
            )
    """
    if _raw_pool is None:
        raise RuntimeError(
            "Raw asyncpg pool not initialised. Call init_raw_pool() first."
        )
    async with _raw_pool.acquire() as conn:
        yield conn


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────
def _redact_url(url: str) -> str:
    """Return the DSN with the password replaced by ***."""
    try:
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(url)
        if parsed.password:
            netloc = parsed.hostname or ""
            if parsed.username:
                netloc = f"{parsed.username}:***@{netloc}"
            if parsed.port:
                netloc = f"{netloc}:{parsed.port}"
            return urlunparse(parsed._replace(netloc=netloc))
    except Exception:
        pass
    return "<redacted>"
