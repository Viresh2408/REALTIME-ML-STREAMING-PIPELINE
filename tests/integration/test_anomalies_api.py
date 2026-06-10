"""
tests/integration/test_anomalies_api.py
─────────────────────────────────────────────────────────────────────────────
Integration tests for GET /api/v1/anomalies endpoints.

Coverage targets (anomalies.py):
  Lines 56-272  – list_anomalies, get_anomaly_stats, get_heatmap, get_anomaly

All tests run WITHOUT a live database/Kafka/Redis:
  • AsyncSession   → AsyncMock with configured return values
  • Redis          → fakeredis
  • Kafka          → MagicMock
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from tests.integration.conftest_mocks import auth_headers, build_db_mock, make_anomaly_event


# ── Shared async test-client helper ──────────────────────────────────────────


@asynccontextmanager
async def anomalies_client(
    db_session_mock: AsyncMock | None = None,
) -> AsyncGenerator[AsyncClient, None]:
    """Yield an HTTPX AsyncClient wired to the FastAPI app with mocked infra."""
    try:
        import fakeredis.aioredis as fake_aio

        _redis = fake_aio.FakeRedis(decode_responses=True)
    except ImportError:
        _redis = AsyncMock()
        _redis.get = AsyncMock(return_value=None)
        _redis.ping = AsyncMock(return_value=True)

    from app.main import app  # type: ignore[import]
    from app.core.database import get_db_session  # type: ignore[import]
    from app.core.redis_client import redis_pool  # type: ignore[import]
    from app.services.event_service import KafkaProducerSingleton  # type: ignore[import]

    redis_pool._client = _redis
    mock_producer = MagicMock()
    KafkaProducerSingleton._producer = mock_producer

    _session = db_session_mock or build_db_mock()

    async def _session_override():
        yield _session

    app.dependency_overrides[get_db_session] = _session_override

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client
    finally:
        app.dependency_overrides.clear()
        KafkaProducerSingleton._producer = None
        redis_pool._client = None


# ─────────────────────────────────────────────────────────────────────────────
# 1. test_list_anomalies_returns_paginated_results
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_anomalies_returns_paginated_results():
    """
    GET /api/v1/anomalies returns a list of AnomalyEventOut items.
    Covers lines 56-119 of anomalies.py (full list path).
    """
    rows = [
        make_anomaly_event(anomaly_score=0.91, source_id=f"src-{i}", is_anomaly=True)
        for i in range(3)
    ]

    # anomalies.py uses raw SQL → result.fetchall()
    # We simulate fetchall() returning our mock rows
    session = AsyncMock()
    exec_result = MagicMock()
    exec_result.fetchall.return_value = rows
    session.execute = AsyncMock(return_value=exec_result)

    async with anomalies_client(db_session_mock=session) as client:
        response = await client.get(
            "/api/v1/anomalies",
            params={"limit": 3, "offset": 0},
            headers=auth_headers("viewer"),
        )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 3
    # Each item must satisfy AnomalyEventOut schema
    for item in data:
        assert "event_id" in item
        assert "anomaly_score" in item
        assert item["is_anomaly"] is True


# ─────────────────────────────────────────────────────────────────────────────
# 2. test_list_anomalies_filters_by_min_score
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_anomalies_filters_by_min_score():
    """
    GET /api/v1/anomalies?min_score=0.9 should only return events with
    anomaly_score >= 0.9.  The WHERE clause is assembled in anomalies.py.
    """
    high_score_event = make_anomaly_event(anomaly_score=0.95)
    session = AsyncMock()
    exec_result = MagicMock()
    exec_result.fetchall.return_value = [high_score_event]
    session.execute = AsyncMock(return_value=exec_result)

    async with anomalies_client(db_session_mock=session) as client:
        response = await client.get(
            "/api/v1/anomalies",
            params={"min_score": 0.9},
            headers=auth_headers("viewer"),
        )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["anomaly_score"] >= 0.9


# ─────────────────────────────────────────────────────────────────────────────
# 3. test_list_anomalies_filters_by_source_id
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_anomalies_filters_by_source_id():
    """
    GET /api/v1/anomalies?source_id=sensor-42 filters by origin.
    """
    target_event = make_anomaly_event(source_id="sensor-42")
    session = AsyncMock()
    exec_result = MagicMock()
    exec_result.fetchall.return_value = [target_event]
    session.execute = AsyncMock(return_value=exec_result)

    async with anomalies_client(db_session_mock=session) as client:
        response = await client.get(
            "/api/v1/anomalies",
            params={"source_id": "sensor-42"},
            headers=auth_headers("viewer"),
        )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["source_id"] == "sensor-42"


# ─────────────────────────────────────────────────────────────────────────────
# 4. test_get_anomaly_stats_returns_rate_and_count
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_anomaly_stats_returns_rate_and_count():
    """
    GET /api/v1/anomalies/stats should return AnomalyStatsOut with
    window_minutes, total_events, anomaly_count, anomaly_rate_pct.
    Covers lines 122-169 of anomalies.py.
    """
    stats_row = {
        "total_events": 500,
        "anomaly_count": 25,
        "avg_score": 0.81,
        "max_score": 0.99,
        "anomaly_rate_pct": 5.0,
    }
    session = AsyncMock()
    exec_result = MagicMock()
    mappings_mock = MagicMock()
    mappings_mock.one.return_value = stats_row
    exec_result.mappings.return_value = mappings_mock
    session.execute = AsyncMock(return_value=exec_result)

    async with anomalies_client(db_session_mock=session) as client:
        response = await client.get(
            "/api/v1/anomalies/stats",
            headers=auth_headers("viewer"),
        )

    assert response.status_code == 200
    data = response.json()
    assert "total_events" in data
    assert "anomaly_count" in data
    assert "anomaly_rate_pct" in data
    assert "window_minutes" in data
    assert data["total_events"] == 500
    assert data["anomaly_count"] == 25
    assert data["anomaly_rate_pct"] == pytest.approx(5.0, abs=0.01)


# ─────────────────────────────────────────────────────────────────────────────
# 5. test_get_anomaly_heatmap_returns_matrix
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_anomaly_heatmap_returns_matrix():
    """
    GET /api/v1/anomalies/heatmap should return HeatmapOut with data list.
    Covers lines 172-237 of anomalies.py.
    """
    now = datetime.now(UTC)
    heatmap_row = MagicMock()
    heatmap_row.time_bucket = now - timedelta(hours=1)
    heatmap_row.source_id = "sensor-1"
    heatmap_row.event_count = 100
    heatmap_row.anomaly_count = 8
    heatmap_row.avg_score = 0.82
    heatmap_row.max_score = 0.97

    session = AsyncMock()
    exec_result = MagicMock()
    exec_result.fetchall.return_value = [heatmap_row]
    session.execute = AsyncMock(return_value=exec_result)

    async with anomalies_client(db_session_mock=session) as client:
        response = await client.get(
            "/api/v1/anomalies/heatmap",
            params={"resolution": "1h"},
            headers=auth_headers("viewer"),
        )

    assert response.status_code == 200
    data = response.json()
    assert "resolution" in data
    assert "data" in data
    assert isinstance(data["data"], list)
    assert len(data["data"]) == 1
    item = data["data"][0]
    assert item["source_id"] == "sensor-1"
    assert item["event_count"] == 100
    assert item["anomaly_count"] == 8


# ─────────────────────────────────────────────────────────────────────────────
# 6. test_anomalies_requires_auth
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_anomalies_requires_auth():
    """
    GET /api/v1/anomalies without token must return 401.
    """
    session = AsyncMock()
    exec_result = MagicMock()
    exec_result.fetchall.return_value = []
    session.execute = AsyncMock(return_value=exec_result)

    async with anomalies_client(db_session_mock=session) as client:
        response = await client.get("/api/v1/anomalies")

    assert response.status_code in {401, 403}


@pytest.mark.asyncio
async def test_anomalies_stats_requires_auth():
    """
    GET /api/v1/anomalies/stats without token must return 401.
    """
    async with anomalies_client() as client:
        response = await client.get("/api/v1/anomalies/stats")

    assert response.status_code in {401, 403}


@pytest.mark.asyncio
async def test_list_anomalies_invalid_severity_returns_400():
    """
    GET /api/v1/anomalies?severity=INVALID should return 400.
    Covers lines 70-74 of anomalies.py (invalid severity branch).
    """
    session = AsyncMock()
    exec_result = MagicMock()
    exec_result.fetchall.return_value = []
    session.execute = AsyncMock(return_value=exec_result)

    async with anomalies_client(db_session_mock=session) as client:
        response = await client.get(
            "/api/v1/anomalies",
            params={"severity": "INVALID"},
            headers=auth_headers("viewer"),
        )

    assert response.status_code == 400
    assert "invalid severity" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_get_single_anomaly_returns_404_when_missing():
    """
    GET /api/v1/anomalies/{id} with no matching row should return 404.
    Covers lines 240-265 of anomalies.py.
    """
    missing_id = uuid.uuid4()
    session = AsyncMock()
    exec_result = MagicMock()
    exec_result.fetchone.return_value = None
    session.execute = AsyncMock(return_value=exec_result)

    async with anomalies_client(db_session_mock=session) as client:
        response = await client.get(
            f"/api/v1/anomalies/{missing_id}",
            headers=auth_headers("viewer"),
        )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_single_anomaly_returns_full_event():
    """
    GET /api/v1/anomalies/{id} returns a full AnomalyEventOut including
    feature_vector. Covers lines 266-281 of anomalies.py.
    """
    event_id = uuid.uuid4()
    event_row = make_anomaly_event(
        event_id=event_id,
        source_id="gateway-east",
        anomaly_score=0.88,
        feature_vector=[0.1, 0.2, 0.3],
    )

    session = AsyncMock()
    exec_result = MagicMock()
    exec_result.fetchone.return_value = event_row
    session.execute = AsyncMock(return_value=exec_result)

    async with anomalies_client(db_session_mock=session) as client:
        response = await client.get(
            f"/api/v1/anomalies/{event_id}",
            headers=auth_headers("viewer"),
        )

    assert response.status_code == 200
    data = response.json()
    assert data["event_id"] == str(event_id)
    assert "feature_vector" in data
    assert isinstance(data["feature_vector"], list)
    assert data["anomaly_score"] == pytest.approx(0.88, abs=0.001)
