from fastapi import FastAPI
from typing import Dict, Any, List

app = FastAPI(title="MCP Server Registry")

@app.get("/mcp/servers")
async def list_servers() -> Dict[str, List[Dict[str, Any]]]:
    """Lists all available MCP servers in this cluster."""
    servers = [
        {
            "name": "anomaly-detection-mcp",
            "url": "http://mcp-anomaly-server:8001/sse",
            "transport": "sse",
            "description": "Core anomaly operations: query, label, trigger"
        },
        {
            "name": "model-management-mcp",
            "url": "http://mcp-model-server:8002/sse",
            "transport": "sse",
            "description": "Model status, retrain, rollback"
        },
        {
            "name": "kafka-ops-mcp",
            "url": "http://mcp-kafka-server:8003/sse",
            "transport": "sse",
            "description": "Topic inspection, consumer lag, produce test events"
        },
        {
            "name": "grafana-mcp",
            "url": "http://mcp-grafana-server:8004/sse",
            "transport": "sse",  # As per Python MCP SDK stdio/sse standard bindings
            "description": "Annotation creation, dashboard snapshot, alert silence"
        }
    ]
    return {"servers": servers}
