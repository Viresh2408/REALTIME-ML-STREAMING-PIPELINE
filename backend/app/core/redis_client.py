"""
Redis connection pool — rate limiting + caching (Redis 7.2)
"""
from __future__ import annotations

import structlog
from redis.asyncio import ConnectionPool, Redis

from app.core.config import settings

logger = structlog.get_logger(__name__)


class RedisPool:
    """Async Redis connection pool manager for FastAPI lifespan."""

    def __init__(self) -> None:
        self._pool: ConnectionPool | None = None
        self._client: Redis | None = None

    async def connect(self) -> None:
        self._pool = ConnectionPool.from_url(
            settings.REDIS_URL,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
            decode_responses=True,
        )
        self._client = Redis(connection_pool=self._pool)
        await self._client.ping()
        logger.info("Redis connected", url=settings.REDIS_URL)

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
        if self._pool:
            await self._pool.aclose()
        logger.info("Redis disconnected")

    @property
    def client(self) -> Redis:
        if self._client is None:
            raise RuntimeError("Redis client not initialised. Call connect() first.")
        return self._client


redis_pool = RedisPool()


async def get_redis() -> Redis:
    """FastAPI dependency — yields a connected Redis client."""
    return redis_pool.client
