import os
import uuid
import httpx
from typing import Optional, List, Dict, Any

from fastapi import FastAPI
from mcp_fastapi import create_mcp_server
from mcp.server import Server

app = FastAPI(title="model-management-mcp")

server = Server("model-management-mcp")

BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://fastapi-backend:8000/api/v1")

@server.tool()
async def get_model_status() -> Dict[str, Any]:
    """Get current model version, load time, and inference latency."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BACKEND_API_URL}/model/status")
        response.raise_for_status()
        return response.json()

@server.tool()
async def trigger_retraining(reason: str, force: bool = False) -> Dict[str, Any]:
    """Kick off model retraining job immediately."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BACKEND_API_URL}/model/retrain",
            json={"reason": reason, "force": force}
        )
        response.raise_for_status()
        return response.json()

@server.tool()
async def get_retraining_status(job_id: str) -> Dict[str, Any]:
    """Check status of a running retraining job."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BACKEND_API_URL}/model/retrain/{job_id}")
        response.raise_for_status()
        return response.json()

@server.tool()
async def rollback_model(version: str, reason: str) -> Dict[str, Any]:
    """Revert to a previous model version."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BACKEND_API_URL}/model/rollback",
            json={"version": version, "reason": reason}
        )
        response.raise_for_status()
        return response.json()

@server.tool()
async def get_model_metrics(version: Optional[str] = None) -> Dict[str, Any]:
    """Get Precision, recall, F1 for a model version."""
    async with httpx.AsyncClient() as client:
        params = {"version": version} if version else {}
        response = await client.get(f"{BACKEND_API_URL}/model/metrics", params=params)
        response.raise_for_status()
        return response.json()

@server.tool()
async def list_model_versions(limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    """All available model versions with their metrics."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{BACKEND_API_URL}/model/versions",
            params={"limit": limit, "offset": offset}
        )
        response.raise_for_status()
        return response.json()

# Create ASGI app from MCP server
mcp_app = create_mcp_server(server)
app.mount("/sse", mcp_app)
