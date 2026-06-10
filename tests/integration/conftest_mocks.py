"""
tests/integration/conftest_mocks.py
────────────────────────────────────────────────────────────────────────
Central fixtures shared by the new API integration test files:
  • test_alerts_api.py
  • test_anomalies_api.py
  • test_auth_api.py
  • test_events_api.py

Design goals:
  - No live infrastructure (no DB, Kafka, or Redis required).
  - fakeredis is used for the Redis blacklist/token store.
  - SQLAlchemy AsyncSession is fully mocked (AsyncMock).
  - Kafka Producer is replaced with a MagicMock.
  - Auth helpers (get_current_user, is_token_revoked) can be overridden
    via FastAPI dependency_overrides for fine-grained per-test control.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

# ── sys.path setup so bare `app.*` imports resolve ────────────────────────────
_BACKEND_DIR = Path(__file__).parent.parent.parent / "backend"
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

# ── Minimal env before any app import ─────────────────────────────────────────
_CI_ENV: dict[str, str] = {
    "DATABASE_URL": "postgresql+asyncpg://test_user:test_pw@localhost:5432/test_db",
    "TIMESCALE_PASSWORD": "test_pw",
    "JWT_SECRET_KEY": os.environ.get(
        "JWT_SECRET_KEY", "ci-test-secret-key-minimum-32-chars-here"
    ),
    "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", "sk-ant-test-key"),
    "CORS_ORIGINS": '[\"http://localhost:3000\"]',
    "TESTING": "true",
    "REDIS_URL": "redis://localhost:6379/0",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
}
for _k, _v in _CI_ENV.items():
    os.environ.setdefault(_k, _v)


# ── Lazy import of the FastAPI app (after env vars are set) ───────────────────

def _import_app():
    from app.main import app  # type: ignore[import]
    return app


# ── Helpers to build canonical ORM-like mock rows ─────────────────────────────

def make_alert(
    *,
    alert_id: uuid.UUID | None = None,
    source_id: str = "sensor-1",
    severity: str = "HIGH",
    status: str = "ACTIVE",
    score: float = 0.92,
    analyst_id: str | None = None,
    note: str | None = None,
    resolution: str | None = None,
    created_at: datetime | None = None,
    acknowledged_at: datetime | None = None,
    resolved_at: datetime | None = None,
) -> MagicMock:
    """Return a MagicMock that satisfies AlertOut.model_validate()."""
    row = MagicMock()
    row.alert_id = alert_id or uuid.uuid4()
    row.source_id = source_id
    row.severity = severity
    row.status = status
    row.score = score
    row.analyst_id = analyst_id
    row.note = note
    row.resolution = resolution
    row.created_at = created_at or datetime.now(UTC)
    row.acknowledged_at = acknowledged_at
    row.resolved_at = resolved_at
    # Pydantic model_validate uses __dict__ access or attribute access
    return row


def make_alert_silence(
    *,
    silence_id: uuid.UUID | None = None,
    source_id: str | None = "sensor-1",
    duration_minutes: int = 30,
    reason: str = "maintenance",
    created_at: datetime | None = None,
    expires_at: datetime | None = None,
) -> MagicMock:
    row = MagicMock()
    row.silence_id = silence_id or uuid.uuid4()
    row.source_id = source_id
    row.duration_minutes = duration_minutes
    row.reason = reason
    row.created_at = created_at or datetime.now(UTC)
    from datetime import timedelta
    row.expires_at = expires_at or (row.created_at + timedelta(minutes=duration_minutes))
    return row


def make_anomaly_event(
    *,
    event_id: uuid.UUID | None = None,
    source_id: str = "sensor-42",
    anomaly_score: float = 0.85,
    is_anomaly: bool = True,
    feature_vector: list[float] | None = None,
    model_version: str = "v1.0",
    event_time: datetime | None = None,
    processed_at: datetime | None = None,
) -> MagicMock:
    row = MagicMock()
    row.event_id = event_id or uuid.uuid4()
    row.source_id = source_id
    row.anomaly_score = anomaly_score
    row.is_anomaly = is_anomaly
    row.feature_vector = feature_vector or [0.1, 0.2, 0.3, 0.4, 0.5]
    row.model_version = model_version
    row.event_time = event_time or datetime.now(UTC)
    row.processed_at = processed_at or datetime.now(UTC)
    return row


def make_event_label(
    *,
    label_id: uuid.UUID | None = None,
    event_id: uuid.UUID | None = None,
    label: str = "TP",
    analyst_id: str = "analyst-1",
    note: str | None = None,
    labeled_at: datetime | None = None,
) -> MagicMock:
    row = MagicMock()
    row.label_id = label_id or uuid.uuid4()
    row.event_id = event_id or uuid.uuid4()
    row.label = label
    row.analyst_id = analyst_id
    row.note = note
    row.labeled_at = labeled_at or datetime.now(UTC)
    return row


# ── Generic async DB session mock ─────────────────────────────────────────────

def build_db_mock(scalar_result=None, scalars_all=None, fetchall=None, fetchone=None,
                  mappings_one=None) -> AsyncMock:
    """
    Build an AsyncMock for an SQLAlchemy AsyncSession.

    Parameters control what execute() returns:
      scalar_result  – value from result.scalar_one_or_none()
      scalars_all    – list from result.scalars().all()
      fetchall       – list from result.fetchall()
      fetchone       – value from result.fetchone()
      mappings_one   – value from result.mappings().one()
    """
    session = AsyncMock()

    exec_result = MagicMock()
    exec_result.scalar_one_or_none.return_value = scalar_result
    scalars_mock = MagicMock()
    scalars_mock.all.return_value = scalars_all or []
    exec_result.scalars.return_value = scalars_mock
    exec_result.fetchall.return_value = fetchall or []
    exec_result.fetchone.return_value = fetchone
    mappings_mock = MagicMock()
    mappings_mock.one.return_value = mappings_one
    exec_result.mappings.return_value = mappings_mock

    session.execute = AsyncMock(return_value=exec_result)
    session.flush = AsyncMock(return_value=None)
    session.commit = AsyncMock(return_value=None)
    session.rollback = AsyncMock(return_value=None)
    session.close = AsyncMock(return_value=None)
    session.add = MagicMock()
    return session


# ── Token helpers ─────────────────────────────────────────────────────────────

def get_access_token(role: str = "admin") -> str:
    """Create a real JWT for test usage, signed with the CI secret."""
    from datetime import timedelta
    from app.api.v1.auth import create_token  # type: ignore[import]
    email_map = {
        "admin": "admin@example.com",
        "analyst": "analyst@example.com",
        "viewer": "viewer@example.com",
    }
    return create_token(email_map[role], role, "access", timedelta(hours=1))


def auth_headers(role: str = "admin") -> dict[str, str]:
    return {"Authorization": f"Bearer {get_access_token(role)}"}


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def fake_redis():
    """In-memory fakeredis async client (no real Redis needed)."""
    try:
        import fakeredis.aioredis as fake_aio
        return fake_aio.FakeRedis(decode_responses=True)
    except ImportError:
        # fakeredis not installed — use an AsyncMock that behaves like redis
        r = AsyncMock()
        r.get = AsyncMock(return_value=None)
        r.set = AsyncMock(return_value=True)
        r.setex = AsyncMock(return_value=True)
        r.ping = AsyncMock(return_value=True)
        return r


@pytest_asyncio.fixture()
async def api_client(fake_redis):
    """
    HTTPX AsyncClient wired to the FastAPI app.

    Overrides:
      • get_db_session   → yields a permissive AsyncMock session
      • redis_pool.client → fakeredis instance
      • KafkaProducerSingleton.get → MagicMock (no real broker)
    """
    app = _import_app()

    from app.core.database import get_db_session  # type: ignore[import]
    from app.core.redis_client import redis_pool  # type: ignore[import]
    from app.services.event_service import KafkaProducerSingleton  # type: ignore[import]

    # Patch redis client
    redis_pool._client = fake_redis  # type: ignore[attr-defined]

    # Patch Kafka producer
    mock_producer = MagicMock()
    mock_producer.produce = MagicMock()
    mock_producer.poll = MagicMock(return_value=0)
    mock_producer.flush = MagicMock(return_value=0)
    KafkaProducerSingleton._producer = mock_producer

    # Provide a default (empty) session via override — tests can re-override per-test
    async def _default_session_override():
        yield build_db_mock()

    app.dependency_overrides[get_db_session] = _default_session_override

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client, app, mock_producer

    # Cleanup
    app.dependency_overrides.clear()
    KafkaProducerSingleton._producer = None
    redis_pool._client = None
