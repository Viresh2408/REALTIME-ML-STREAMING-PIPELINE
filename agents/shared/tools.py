"""
Shared agent tools — LangChain-compatible tool definitions
Used by Orchestrator Agent and LLM reasoning nodes.
"""
from __future__ import annotations

import os
from typing import Any

import httpx
import structlog

logger = structlog.get_logger(__name__)

BACKEND_URL = os.environ.get("BACKEND_INTERNAL_URL", "http://fastapi-backend:8000")


async def get_system_health() -> dict[str, Any]:
    """
    Fetch health status from FastAPI backend and downstream services.
    Used by Orchestrator Agent on each heartbeat tick.
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{BACKEND_URL}/health")
            resp.raise_for_status()
            return resp.json()
    except Exception as exc:
        logger.warning("Health check failed", error=str(exc))
        return {"status": "degraded", "error": str(exc)}


async def get_recent_anomaly_stats(window_minutes: int = 60) -> dict[str, Any]:
    """
    Fetch recent anomaly stats from backend API.
    Used by Orchestrator Agent for routing decisions.
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{BACKEND_URL}/api/v1/stats",
                params={"window_minutes": window_minutes},
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as exc:
        logger.warning("Failed to fetch anomaly stats", error=str(exc))
        return {}


async def trigger_model_reload(model_version: str) -> bool:
    """
    Signal the ML inference worker to hot-reload its model artifact.
    Returns True if the signal was accepted.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{BACKEND_URL}/api/v1/internal/model-reload",
                json={"model_version": model_version},
            )
            return resp.status_code == 200
    except Exception as exc:
        logger.error("Model reload signal failed", error=str(exc))
        return False
