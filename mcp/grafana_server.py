import os
import httpx
from typing import Optional, Dict, Any
from datetime import datetime

from fastapi import FastAPI
from mcp_fastapi import create_mcp_server
from mcp.server import Server

app = FastAPI(title="grafana-mcp")
server = Server("grafana-mcp")

GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://grafana:3000")
GRAFANA_API_KEY = os.environ.get("GRAFANA_API_KEY", "")

def get_grafana_headers():
    headers = {"Content-Type": "application/json"}
    if GRAFANA_API_KEY:
        headers["Authorization"] = f"Bearer {GRAFANA_API_KEY}"
    return headers

@server.tool()
async def create_annotation(text: str, tags: list[str]) -> Dict[str, Any]:
    """Creates a global annotation in Grafana."""
    async with httpx.AsyncClient() as client:
        payload = {
            "text": text,
            "tags": tags,
            "time": int(datetime.now().timestamp() * 1000)
        }
        response = await client.post(
            f"{GRAFANA_URL}/api/annotations",
            json=payload,
            headers=get_grafana_headers()
        )
        response.raise_for_status()
        return response.json()

@server.tool()
async def get_dashboard_snapshot(dashboard_uid: str) -> Dict[str, Any]:
    """Generates a snapshot link for a Grafana dashboard."""
    # Grafana snapshot API
    async with httpx.AsyncClient() as client:
        payload = {
            "dashboard": {"uid": dashboard_uid},
            "expires": 3600
        }
        response = await client.post(
            f"{GRAFANA_URL}/api/snapshots",
            json=payload,
            headers=get_grafana_headers()
        )
        response.raise_for_status()
        return response.json()

@server.tool()
async def silence_alert(alert_id: str, duration_hours: int = 1) -> Dict[str, Any]:
    """Silences an alert in Grafana Alerting."""
    async with httpx.AsyncClient() as client:
        # Simple mock payload for Alertmanager silence
        payload = {
            "matchers": [
                {"name": "alertname", "value": alert_id, "isRegex": False, "isEqual": True}
            ],
            "startsAt": datetime.utcnow().isoformat() + "Z",
            "endsAt": "2099-12-31T23:59:59Z",  # In real impl, compute endsAt based on duration_hours
            "createdBy": "mcp-agent",
            "comment": "Silenced via MCP"
        }
        response = await client.post(
            f"{GRAFANA_URL}/api/alertmanager/grafana/api/v2/silences",
            json=payload,
            headers=get_grafana_headers()
        )
        if response.status_code == 202:
            return {"status": "silenced", "silence_id": response.json().get("silenceID")}
        response.raise_for_status()
        return response.json()

# Create ASGI app from MCP server
# Note: Grafana-mcp could use standard REST or SSE. Using SSE for standard MCP protocol compliance.
mcp_app = create_mcp_server(server)
app.mount("/sse", mcp_app)
