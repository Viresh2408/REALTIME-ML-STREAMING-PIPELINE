import os
from typing import Any

import httpx
from fastapi import FastAPI
from mcp.server import Server
from mcp_fastapi import create_mcp_server

app = FastAPI(title="model-management-mcp")

server = Server("model-management-mcp")

BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://fastapi-backend:8000/api/v1")


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe for docker-compose healthcheck."""
    return {"status": "ok", "service": "mcp-model-server"}


@server.tool()
async def get_model_status() -> dict[str, Any]:
    """Get current model version, load time, and inference latency."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BACKEND_API_URL}/model/status")
        response.raise_for_status()
        return response.json()


@server.tool()
async def trigger_retraining(reason: str, force: bool = False) -> dict[str, Any]:
    """Kick off model retraining job immediately."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BACKEND_API_URL}/model/retrain", json={"reason": reason, "force": force}
        )
        response.raise_for_status()
        return response.json()


@server.tool()
async def get_retraining_status(job_id: str) -> dict[str, Any]:
    """Check status of a running retraining job."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BACKEND_API_URL}/model/retrain/{job_id}")
        response.raise_for_status()
        return response.json()


@server.tool()
async def rollback_model(version: str, reason: str) -> dict[str, Any]:
    """Revert to a previous model version."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BACKEND_API_URL}/model/rollback", json={"version": version, "reason": reason}
        )
        response.raise_for_status()
        return response.json()


@server.tool()
async def get_model_metrics(version: str | None = None) -> dict[str, Any]:
    """Get Precision, recall, F1 for a model version."""
    async with httpx.AsyncClient() as client:
        params = {"version": version} if version else {}
        response = await client.get(f"{BACKEND_API_URL}/model/metrics", params=params)
        response.raise_for_status()
        return response.json()


@server.tool()
async def list_model_versions(limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    """All available model versions with their metrics."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{BACKEND_API_URL}/model/versions", params={"limit": limit, "offset": offset}
        )
        response.raise_for_status()
        return response.json()


# Create ASGI app from MCP server
mcp_app = create_mcp_server(server)
app.mount("/sse", mcp_app)
