"""
Redis sliding window rate-limiting middleware
Restricts traffic to 100 requests/second per identifier (API key, Bearer token subject, or IP).
Uses Redis sorted sets (zset) with transaction pipelines for atomic operations.
"""
from __future__ import annotations

import time
import structlog
from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from app.core.redis_client import redis_pool

logger = structlog.get_logger(__name__)

class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    FastAPI Middleware implementing sliding window rate limiting via Redis.
    Limits clients to 100 requests per second.
    """

    def __init__(
        self,
        app,
        limit: int = 100,
        window_seconds: float = 1.0,
    ) -> None:
        super().__init__(app)
        self.limit = limit
        self.window_seconds = window_seconds

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # Bypass rate limiting for health check and metrics endpoints
        path = request.url.path
        if path in ("/health", "/metrics", "/api/v1/health") or path.startswith("/ws"):
            return await call_next(request)

        # Identify client (API key, JWT sub, or fallback to client host IP)
        identifier = None

        # 1. Check API Key header
        api_key = request.headers.get("X-API-Key")
        if api_key:
            identifier = f"apikey:{api_key}"

        # 2. Check Auth token
        if not identifier:
            auth_header = request.headers.get("Authorization")
            if auth_header and auth_header.startswith("Bearer "):
                # We won't fully decode JWT here, just use the token string or partition
                token = auth_header.split(" ")[1]
                identifier = f"token:{token[-16:]}"  # Use last 16 chars of token as unique id

        # 3. Fallback to client IP
        if not identifier:
            client_ip = request.client.host if request.client else "unknown"
            identifier = f"ip:{client_ip}"

        redis_key = f"rate_limit:{identifier}"
        now = time.time()
        clear_before = now - self.window_seconds

        try:
            client = redis_pool.client
            # Redis Pipeline for atomic Zset operations
            pipe = client.pipeline()
            # Remove expired elements
            pipe.zremrangebyscore(redis_key, 0, clear_before)
            # Count elements in window
            pipe.zcard(redis_key)
            # Execute first batch
            _, current_count = await pipe.execute()

            if current_count >= self.limit:
                logger.warning("Rate limit exceeded", identifier=identifier, count=current_count)
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={"detail": "Too Many Requests. Rate limit exceeded (100 req/s)."},
                    headers={"Retry-After": "1"},
                )

            # Add current request timestamp and set TTL
            pipe = client.pipeline()
            pipe.zadd(redis_key, {str(now): now})
            pipe.expire(redis_key, int(self.window_seconds * 2) or 2)
            await pipe.execute()

        except Exception as exc:
            # Resilient fallback: log error and allow request if Redis is down
            logger.error("Rate limiter Redis failure, bypassing check", error=str(exc))

        return await call_next(request)
