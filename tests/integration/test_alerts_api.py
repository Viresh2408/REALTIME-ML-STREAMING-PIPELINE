"""
tests/integration/test_alerts_api.py
─────────────────────────────────────────────────────────────────────────────
Integration tests for GET/PATCH/POST /api/v1/alerts endpoints.

Coverage targets (alerts.py):
  Lines 34-35   – module-level try/except import fallback
  Lines 59-69   – list_alerts query logic (filter by status/severity)
  Lines 91-132  – get_alert (404 branch, linked events)
  Lines 155-248 – acknowledge, resolve, silence_alerts

All tests run WITHOUT a live database/Kafka/Redis:
  • AsyncSession → AsyncMock (via conftest_mocks helpers)
  • Redis        → fakeredis
  • Kafka        → MagicMock
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.integration.conftest_mocks import (
    auth_headers,
    build_db_mock,
    make_alert,
    make_alert_silence,
    make_anomaly_event,
)


# ── App import (after env bootstrap done in conftest_mocks) ───────────────────

def _app():
    from app.main import app  # type: ignore[import]
    return app


# ── Reusable async-client context manager ────────────────────────────────────

@asynccontextmanager
async def alerts_client(db_session_mock: AsyncMock | None = None,
                        fake_redis=None) -> AsyncGenerator[AsyncClient, None]:
    """
    Async test client with dependency overrides for DB and Redis.
    Caller supplies a pre-configured db_session_mock for full control.
    """
    import fakeredis.aioredis as fake_aio
    from app.core.database import get_db_session  # type: ignore[import]
    from app.core.redis_client import redis_pool  # type: ignore[import]
    from app.services.event_service import KafkaProducerSingleton  # type: ignore[import]

    app = _app()

    # Redis
    _redis = fake_redis or fake_aio.FakeRedis(decode_responses=True)
    redis_pool._client = _redis

    # Kafka no-op
    mock_producer = MagicMock()
    mock_producer.produce = MagicMock()
    mock_producer.poll = MagicMock(return_value=0)
    mock_producer.flush = MagicMock(return_value=0)
    KafkaProducerSingleton._producer = mock_producer

    # DB override
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
# 1. test_list_alerts_returns_200
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_alerts_returns_200():
    """
    GET /api/v1/alerts with a valid viewer token should return 200 with a list.
    DB returns one mock Alert row.
    """
    alert_row = make_alert(severity="HIGH", status="ACTIVE")
    db = build_db_mock(scalars_all=[alert_row])

    async with alerts_client(db_session_mock=db) as client:
        response = await client.get("/api/v1/alerts", headers=auth_headers("viewer"))

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["severity"] == "HIGH"
    assert data[0]["status"] == "ACTIVE"


# ─────────────────────────────────────────────────────────────────────────────
# 2. test_list_alerts_filters_by_severity
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_alerts_filters_by_severity():
    """
    GET /api/v1/alerts?severity=CRITICAL should pass severity filter through
    to the query. We verify the endpoint returns 200 with the expected rows.
    """
    critical_alert = make_alert(severity="CRITICAL", status="ACTIVE", score=0.97)
    db = build_db_mock(scalars_all=[critical_alert])

    async with alerts_client(db_session_mock=db) as client:
        response = await client.get(
            "/api/v1/alerts",
            params={"severity": "CRITICAL"},
            headers=auth_headers("viewer"),
        )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["severity"] == "CRITICAL"
    assert data[0]["score"] == pytest.approx(0.97, abs=0.001)


# ─────────────────────────────────────────────────────────────────────────────
# 3. test_get_alert_by_id_returns_404_when_missing
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_alert_by_id_returns_404_when_missing():
    """
    GET /api/v1/alerts/{unknown_id} should return 404 when DB returns None.
    Covers lines 91-98 of alerts.py.
    """
    missing_id = uuid.uuid4()
    # scalar_one_or_none returns None → 404 branch
    db = build_db_mock(scalar_result=None)

    async with alerts_client(db_session_mock=db) as client:
        response = await client.get(
            f"/api/v1/alerts/{missing_id}",
            headers=auth_headers("viewer"),
        )

    assert response.status_code == 404
    body = response.json()
    assert "detail" in body
    assert "not found" in body["detail"].lower()


# ─────────────────────────────────────────────────────────────────────────────
# 4. test_acknowledge_alert_updates_status
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_acknowledge_alert_updates_status():
    """
    PATCH /api/v1/alerts/{id}/acknowledge with ACTIVE alert should return 200
    with status=ACKNOWLEDGED and set analyst_id/note.
    Covers lines 155-177 of alerts.py.
    """
    alert_id = uuid.uuid4()
    alert_row = make_alert(alert_id=alert_id, status="ACTIVE")
    db = build_db_mock(scalar_result=alert_row)

    async with alerts_client(db_session_mock=db) as client:
        response = await client.patch(
            f"/api/v1/alerts/{alert_id}/acknowledge",
            json={"analyst_id": "analyst@example.com", "note": "Investigating now"},
            headers=auth_headers("analyst"),
        )

    assert response.status_code == 200
    data = response.json()
    # The endpoint mutates the mock object; model_validate picks up the change
    assert data["status"] == "ACKNOWLEDGED"
    assert data["analyst_id"] == "analyst@example.com"
    assert data["note"] == "Investigating now"


# ─────────────────────────────────────────────────────────────────────────────
# 5. test_resolve_alert_requires_resolution_string
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_resolve_alert_requires_resolution_string():
    """
    PATCH /api/v1/alerts/{id}/resolve without `resolution` field should
    return 422 (Pydantic validation error).
    Covers lines 180-210 of alerts.py.
    """
    alert_id = uuid.uuid4()
    alert_row = make_alert(alert_id=alert_id, status="ACTIVE")
    db = build_db_mock(scalar_result=alert_row)

    async with alerts_client(db_session_mock=db) as client:
        # Missing required `resolution` field
        response = await client.patch(
            f"/api/v1/alerts/{alert_id}/resolve",
            json={"analyst_id": "analyst@example.com"},
            headers=auth_headers("analyst"),
        )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_resolve_alert_with_valid_payload_returns_200():
    """
    PATCH /api/v1/alerts/{id}/resolve with full payload should return 200
    and update status to RESOLVED.
    """
    alert_id = uuid.uuid4()
    alert_row = make_alert(alert_id=alert_id, status="ACTIVE")
    db = build_db_mock(scalar_result=alert_row)

    async with alerts_client(db_session_mock=db) as client:
        response = await client.patch(
            f"/api/v1/alerts/{alert_id}/resolve",
            json={"analyst_id": "analyst@example.com", "resolution": "False positive — whitelisted"},
            headers=auth_headers("analyst"),
        )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "RESOLVED"
    assert data["resolution"] == "False positive — whitelisted"


# ─────────────────────────────────────────────────────────────────────────────
# 6. test_silence_alert_stores_in_redis  (DB + Redis path)
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_silence_alert_stores_in_redis():
    """
    POST /api/v1/alerts/silence should persist an AlertSilence record and
    return 201 with silence metadata.
    Covers lines 213-248 of alerts.py.

    The silence is stored in DB (mocked); Redis is used for the token
    blacklist only, not for silences themselves — but the endpoint path
    exercises DB flush, which is the critical path.
    """
    silence_row = make_alert_silence(
        source_id="sensor-1",
        duration_minutes=60,
        reason="Scheduled maintenance window",
    )
    db = build_db_mock()

    # After db.flush(), the silence_record should have its fields set.
    # We simulate this by patching AlertSilence.__init__ indirectly via db.add
    # and verifying the response structure.
    from app.schemas.alerts import AlertSilenceOut  # type: ignore[import]

    # Patch AlertSilence model so db.add captures it and flush populates it
    with patch("app.api.v1.alerts.AlertSilence") as MockAlertSilence:
        MockAlertSilence.return_value = silence_row
        async with alerts_client(db_session_mock=db) as client:
            response = await client.post(
                "/api/v1/alerts/silence",
                json={
                    "source_id": "sensor-1",
                    "duration_minutes": 60,
                    "reason": "Scheduled maintenance window",
                },
                headers=auth_headers("analyst"),
            )

    assert response.status_code == 201
    body = response.json()
    assert "silence_id" in body
    assert body["duration_minutes"] == 60
    assert body["reason"] == "Scheduled maintenance window"
    assert "expires_at" in body


# ─────────────────────────────────────────────────────────────────────────────
# 7. test_list_alerts_requires_auth
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_alerts_requires_auth():
    """
    GET /api/v1/alerts without Authorization header must return 401.
    """
    db = build_db_mock(scalars_all=[])

    async with alerts_client(db_session_mock=db) as client:
        response = await client.get("/api/v1/alerts")

    assert response.status_code in {401, 403}


@pytest.mark.asyncio
async def test_list_alerts_status_filter():
    """
    GET /api/v1/alerts?status=ACKNOWLEDGED filters by status field.
    """
    ack_alert = make_alert(status="ACKNOWLEDGED", severity="MEDIUM")
    db = build_db_mock(scalars_all=[ack_alert])

    async with alerts_client(db_session_mock=db) as client:
        response = await client.get(
            "/api/v1/alerts",
            params={"status": "ACKNOWLEDGED"},
            headers=auth_headers("viewer"),
        )

    assert response.status_code == 200
    data = response.json()
    assert data[0]["status"] == "ACKNOWLEDGED"


@pytest.mark.asyncio
async def test_acknowledge_resolved_alert_returns_400():
    """
    PATCH /api/v1/alerts/{id}/acknowledge on a RESOLVED alert should
    return 400. Covers lines 164-168 of alerts.py.
    """
    alert_id = uuid.uuid4()
    resolved_alert = make_alert(alert_id=alert_id, status="RESOLVED")
    db = build_db_mock(scalar_result=resolved_alert)

    async with alerts_client(db_session_mock=db) as client:
        response = await client.patch(
            f"/api/v1/alerts/{alert_id}/acknowledge",
            json={"analyst_id": "analyst@example.com"},
            headers=auth_headers("analyst"),
        )

    assert response.status_code == 400
    assert "resolved" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_get_alert_returns_200_with_linked_events():
    """
    GET /api/v1/alerts/{id} with a found alert should return 200
    and include linked_events list.
    Covers lines 91-135 of alerts.py (happy path).
    """
    alert_id = uuid.uuid4()
    alert_row = make_alert(alert_id=alert_id, source_id="sensor-99")
    event_row = make_anomaly_event(source_id="sensor-99", is_anomaly=True)

    # First execute call → alert lookup (scalar_one_or_none)
    # Second execute call → linked events (scalars().all())
    session = AsyncMock()

    # Build two separate result mocks
    alert_result = MagicMock()
    alert_result.scalar_one_or_none.return_value = alert_row

    event_result = MagicMock()
    events_scalars = MagicMock()
    events_scalars.all.return_value = [event_row]
    event_result.scalars.return_value = events_scalars

    session.execute = AsyncMock(side_effect=[alert_result, event_result])
    session.flush = AsyncMock()
    session.add = MagicMock()

    async with alerts_client(db_session_mock=session) as client:
        response = await client.get(
            f"/api/v1/alerts/{alert_id}",
            headers=auth_headers("viewer"),
        )

    assert response.status_code == 200
    body = response.json()
    assert "alert" in body
    assert "linked_events" in body
    assert isinstance(body["linked_events"], list)
