from typing import Any

from fastapi import FastAPI

app = FastAPI(title="MCP Server Registry")


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe for docker-compose healthcheck."""
    return {"status": "ok", "service": "mcp-registry"}


@app.get("/mcp/servers")
async def list_servers() -> dict[str, list[dict[str, Any]]]:
    """Lists all available MCP servers in this cluster."""
    servers = [
        {
            "name": "anomaly-detection-mcp",
            "url": "http://mcp-anomaly-server:8001/sse",
            "transport": "sse",
            "description": "Core anomaly operations: query, label, trigger",
        },
        {
            "name": "model-management-mcp",
            "url": "http://mcp-model-server:8002/sse",
            "transport": "sse",
            "description": "Model status, retrain, rollback",
        },
        {
            "name": "kafka-ops-mcp",
            "url": "http://mcp-kafka-server:8003/sse",
            "transport": "sse",
            "description": "Topic inspection, consumer lag, produce test events",
        },
        {
            "name": "grafana-mcp",
            "url": "http://mcp-grafana-server:8004/sse",
            "transport": "sse",
            "description": "Annotation creation, dashboard snapshot, alert silence",
        },
    ]
    return {"servers": servers}
