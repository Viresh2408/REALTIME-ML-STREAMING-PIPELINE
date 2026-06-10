"""
Integration tests — FastAPI backend + TimescaleDB
Uses testcontainers-python 0.12 to spin up a real TimescaleDB instance.
pytest 8.x + pytest-asyncio 0.23.7
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from testcontainers.postgres import PostgresContainer

TIMESCALE_IMAGE = "timescale/timescaledb:2.15.3-pg16"


# Redundant timescaledb_container fixture removed to use the central session-scoped one from conftest.py


@pytest.mark.integration
class TestHealthEndpoint:
    """Tests against the live FastAPI application."""

    @pytest.mark.asyncio
    async def test_health_returns_ok(self) -> None:
        """GET /health should return 200 with status=ok."""
        from backend.app.main import app

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/health")

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "ok"
        assert body["service"] == "fastapi-backend"

    @pytest.mark.asyncio
    async def test_openapi_schema_is_accessible(self) -> None:
        """GET /openapi.json should return a valid OpenAPI schema."""
        from backend.app.main import app

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/openapi.json")

        assert response.status_code == 200
        schema = response.json()
        assert "openapi" in schema
        assert schema["info"]["title"] == "Anomaly Detection API"


@pytest.mark.integration
class TestAnomalyEventsAPI:
    """Integration tests for anomaly events endpoints."""

    @pytest.mark.asyncio
    async def test_list_events_requires_auth(self) -> None:
        """GET /api/v1/events should return 401 without JWT."""
        from backend.app.main import app

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/v1/events")

        assert response.status_code in {401, 403, 422}
