"""
Application Lifespan — startup/shutdown event handlers
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.core.config import settings
from app.core.kafka import kafka_producer_manager
from app.core.redis_client import redis_pool

try:
    from database.connection import close_raw_pool, db_manager, init_raw_pool
except ModuleNotFoundError:
    from backend.database.connection import close_raw_pool, db_manager, init_raw_pool

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Manage application startup and shutdown.
    - Initialize database engine and connection pool
    - Create database tables (if they do not exist)
    - Start Kafka producer
    - Connect Redis pool
    """
    logger.info(
        "Starting Anomaly Detection API",
        env=settings.APP_ENV,
        threshold=settings.ANOMALY_SCORE_THRESHOLD,
    )

    # ── Startup ────────────────────────────────────────────────
    # Initialize Redis connection pool
    await redis_pool.connect()
    logger.info("Redis connection pool established", url=settings.REDIS_URL)

    # Initialize low-level raw asyncpg pool for bulk writes
    try:
        # Convert postgresql+asyncpg:// to postgresql://
        dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
        await init_raw_pool(dsn)
    except Exception as raw_exc:
        logger.error("Failed to initialize raw asyncpg pool", error=str(raw_exc))

    # Initialize SQLAlchemy database engine and auto-create new tables
    try:
        db_manager.init(settings.DATABASE_URL, echo=settings.APP_ENV == "development")
        await db_manager.create_all_tables()
        logger.info("TimescaleDB tables checked and initialised successfully")
    except Exception as db_exc:
        logger.error("Failed to initialize database session manager", error=str(db_exc))

    # Start Kafka Producer
    await kafka_producer_manager.start()
    logger.info(
        "Kafka producer started",
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    )

    yield  # Application runs here

    # ── Shutdown ───────────────────────────────────────────────
    logger.info("Shutting down Anomaly Detection API...")
    await kafka_producer_manager.stop()
    try:
        await close_raw_pool()
    except Exception:
        pass
    await db_manager.close()
    await redis_pool.disconnect()
    logger.info("All connections closed. Goodbye.")
