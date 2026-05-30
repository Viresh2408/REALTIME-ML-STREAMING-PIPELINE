"""
FastAPI Application Entry Point
Real-Time Anomaly Detection & Recommendation System
"""
from __future__ import annotations

import time
from typing import Dict, Any

import structlog
from fastapi import FastAPI, APIRouter, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app
from sqlalchemy import text

from app.api.v1.router import api_router
from app.api.v1.websocket import router as ws_router
from app.core.config import settings
from app.core.lifespan import lifespan
from app.middleware.rate_limit import RateLimitMiddleware
try:
    from database.connection import db_manager
except ModuleNotFoundError:
    from backend.database.connection import db_manager
from app.core.redis_client import redis_pool

logger = structlog.get_logger(__name__)

# ─────────────────────────────────────────────────────────────
# Application factory
# ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="Anomaly Detection API",
    description=(
        "Real-Time Anomaly Detection & Recommendation System. "
        "REST + WebSocket API powered by FastAPI, LangGraph agents, "
        "Apache Kafka, and TimescaleDB."
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# ── Rate Limiting Middleware ─────────────────────────────────
app.add_middleware(RateLimitMiddleware, limit=100)

# ── CORS ──────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Prometheus metrics endpoint ───────────────────────────────
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# ── WebSocket Direct Mount ────────────────────────────────────
# Allows matching the exact /ws/events, /ws/alerts, /ws/metrics endpoints
app.include_router(ws_router, prefix="/ws", tags=["WebSocket Direct"])

# ── API routes ────────────────────────────────────────────────
app.include_router(api_router, prefix=settings.API_V1_PREFIX)


# ── Health checks ─────────────────────────────────────────────
@app.get("/health", tags=["Health"])
async def health() -> dict[str, str]:
    """Liveness probe endpoint."""
    return {"status": "ok", "service": "fastapi-backend"}


@app.get("/api/v1/health", tags=["Health"])
@app.get("/api/v1/health/detailed", tags=["Health"])
async def health_detailed() -> dict[str, Any]:
    """
    Detailed system health probe checking database, caching, and stream infrastructures.
    Reports connectivity latency and operational status.
    """
    results: Dict[str, Any] = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": {},
    }

    # 1. TimescaleDB check
    t_start = time.time()
    try:
        async with db_manager.session() as session:
            await session.execute(text("SELECT 1"))
        latency = round((time.time() - t_start) * 1000, 2)
        results["services"]["timescaledb"] = {"status": "healthy", "latency_ms": latency}
    except Exception as exc:
        results["status"] = "degraded"
        results["services"]["timescaledb"] = {"status": "unhealthy", "error": str(exc)}

    # 2. Redis check
    t_start = time.time()
    try:
        client = redis_pool.client
        await client.ping()
        latency = round((time.time() - t_start) * 1000, 2)
        results["services"]["redis"] = {"status": "healthy", "latency_ms": latency}
    except Exception as exc:
        results["status"] = "degraded"
        results["services"]["redis"] = {"status": "unhealthy", "error": str(exc)}

    # 3. Kafka check
    t_start = time.time()
    try:
        from app.core.kafka import kafka_producer_manager
        producer = kafka_producer_manager.producer
        # Check metadata to verify connectivity
        producer.list_topics(timeout=1.0)
        latency = round((time.time() - t_start) * 1000, 2)
        results["services"]["kafka"] = {"status": "healthy", "latency_ms": latency}
    except Exception as exc:
        results["status"] = "degraded"
        results["services"]["kafka"] = {"status": "unhealthy", "error": str(exc)}

    if results["status"] == "degraded":
        # Check if all critical services are down, make it unhealthy
        unhealthy_count = sum(
            1 for s in results["services"].values() if s["status"] == "unhealthy"
        )
        if unhealthy_count >= 2:
            results["status"] = "unhealthy"

    return results


from datetime import datetime, timezone
