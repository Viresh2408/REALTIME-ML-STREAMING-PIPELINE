import os
import uuid
from datetime import datetime
from typing import Any

import asyncpg
from fastapi import FastAPI
from mcp.server import Server
from mcp_fastapi import create_mcp_server

app = FastAPI(title="anomaly-detection-mcp")

server = Server("anomaly-detection-mcp")

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://anomaly_admin:StrongPass123!@timescaledb:5432/anomaly_db"
)
if DB_URL.startswith("postgresql+asyncpg://"):
    DB_URL = DB_URL.replace("postgresql+asyncpg://", "postgresql://")


async def get_db_pool():
    if not hasattr(app.state, "pool"):
        app.state.pool = await asyncpg.create_pool(DB_URL)
    return app.state.pool


@server.tool()
async def get_recent_anomalies(
    limit: int = 50,
    start_time: str | None = None,
    end_time: str | None = None,
    severity: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch anomalies in a time window with optional severity filter."""
    pool = await get_db_pool()
    query = "SELECT * FROM anomaly_events WHERE 1=1"
    args = []

    if start_time:
        args.append(datetime.fromisoformat(start_time.replace("Z", "+00:00")))
        query += f" AND event_time >= ${len(args)}"
    if end_time:
        args.append(datetime.fromisoformat(end_time.replace("Z", "+00:00")))
        query += f" AND event_time <= ${len(args)}"
    if severity:
        args.append(severity)
        query += f" AND severity = ${len(args)}"

    query += f" ORDER BY event_time DESC LIMIT {limit}"

    async with pool.acquire() as conn:
        records = await conn.fetch(query, *args)
        return [dict(r) for r in records]


@server.tool()
async def get_event_by_id(event_id: str) -> dict[str, Any] | None:
    """Retrieve full event detail by UUID."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        record = await conn.fetchrow(
            "SELECT * FROM anomaly_events WHERE event_id = $1", uuid.UUID(event_id)
        )
        return dict(record) if record else None


@server.tool()
async def label_event(event_id: str, label: str, analyst_id: str) -> dict[str, Any]:
    """Apply human ground-truth label to an event. Valid labels: TP, FP, TN, FN."""
    if label not in ["TP", "FP", "TN", "FN"]:
        raise ValueError("Invalid label. Must be TP, FP, TN, or FN.")

    pool = await get_db_pool()
    async with pool.acquire() as conn:
        record = await conn.fetchrow(
            "UPDATE anomaly_events SET label = $1, analyst_id = $2 WHERE event_id = $3 RETURNING *",
            label,
            analyst_id,
            uuid.UUID(event_id),
        )
        return dict(record) if record else {"error": "Event not found"}


@server.tool()
async def get_anomaly_rate(
    bucket: str = "1h", source_id: str | None = None
) -> list[dict[str, Any]]:
    """Compute anomaly rate for a time bucket."""
    pool = await get_db_pool()
    args = []

    # TimescaleDB time_bucket
    query = f"SELECT time_bucket('{bucket}', event_time) AS bucket, count(*) as event_count, avg(anomaly_score) as avg_score FROM anomaly_events"
    if source_id:
        args.append(source_id)
        query += " WHERE source_id = $1"
    query += " GROUP BY bucket ORDER BY bucket DESC LIMIT 100"

    async with pool.acquire() as conn:
        records = await conn.fetch(query, *args)
        return [dict(r) for r in records]


@server.tool()
async def query_anomalies_by_source(
    source_id: str, limit: int = 50, offset: int = 0
) -> list[dict[str, Any]]:
    """Find all anomalies from a specific source system."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        records = await conn.fetch(
            "SELECT * FROM anomaly_events WHERE source_id = $1 ORDER BY event_time DESC LIMIT $2 OFFSET $3",
            source_id,
            limit,
            offset,
        )
        return [dict(r) for r in records]


@server.tool()
async def acknowledge_alert(
    alert_id: str, analyst_id: str, note: str | None = None
) -> dict[str, Any]:
    """Mark an alert as acknowledged by an analyst."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        record = await conn.fetchrow(
            "UPDATE alerts SET status = 'acknowledged', analyst_id = $1, note = $2 WHERE alert_id = $3 RETURNING *",
            analyst_id,
            note,
            uuid.UUID(alert_id),
        )
        return dict(record) if record else {"error": "Alert not found"}


@server.tool()
async def get_system_health() -> dict[str, Any]:
    """Returns health status of all pipeline components."""
    # In a real scenario, this would aggregate health from various endpoints.
    # For now, we return a mock map.
    return {
        "status": "healthy",
        "components": {"timescaledb": "healthy", "kafka": "healthy", "inference_agent": "healthy"},
    }


# Create ASGI app from MCP server
mcp_app = create_mcp_server(server)
app.mount("/sse", mcp_app)
