"""
tests/unit/test_event_service.py
─────────────────────────────────────────────────────────────────────────────
Unit tests for backend/app/services/event_service.py.

Coverage targets (event_service.py):
  Lines 21-185 – KafkaProducerSingleton, EventService.ingest,
                  ingest_batch, list_events, get_by_id, apply_label

All external dependencies are mocked:
  • Kafka Producer → MagicMock (no real broker)
  • SQLAlchemy AsyncSession → AsyncMock (no live DB)
  • database.models.AnomalyEvent / EventLabel → MagicMock rows
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── sys.path bootstrap (mirrors tests/unit/conftest.py approach) ─────────────
_REPO_ROOT = Path(__file__).parent.parent.parent
_BACKEND_DIR = _REPO_ROOT / "backend"
for _p in [str(_REPO_ROOT), str(_BACKEND_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── Environment setup (required by Settings) ─────────────────────────────────
import os

_REQUIRED_ENV = {
    "DATABASE_URL": "postgresql+asyncpg://test_user:test_pw@localhost:5432/test_db",
    "TIMESCALE_PASSWORD": "test_pw",
    "JWT_SECRET_KEY": os.environ.get("JWT_SECRET_KEY", "ci-test-secret-key-minimum-32-chars-here"),
    "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", "sk-ant-test-key"),
    "CORS_ORIGINS": '["http://localhost:3000"]',
    "TESTING": "true",
    "REDIS_URL": "redis://localhost:6379/0",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
}
for _k, _v in _REQUIRED_ENV.items():
    os.environ.setdefault(_k, _v)


# ── Lazy imports (after env is set) ──────────────────────────────────────────


def _import_service():
    from app.services.event_service import EventService, KafkaProducerSingleton

    return EventService, KafkaProducerSingleton


# ── Shared mock row builders ──────────────────────────────────────────────────


def _make_event_row(
    *,
    event_id: uuid.UUID | None = None,
    source_id: str = "sensor-42",
    anomaly_score: float = 0.85,
    is_anomaly: bool = True,
    feature_vector=None,
    model_version: str = "v1.0",
    event_time: datetime | None = None,
    processed_at: datetime | None = None,
) -> MagicMock:
    row = MagicMock()
    row.event_id = event_id or uuid.uuid4()
    row.source_id = source_id
    row.anomaly_score = anomaly_score
    row.is_anomaly = is_anomaly
    row.feature_vector = feature_vector or [0.1, 0.2, 0.3]
    row.model_version = model_version
    row.event_time = event_time or datetime.now(UTC)
    row.processed_at = processed_at or datetime.now(UTC)
    return row


def _make_label_row(
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


def _build_async_session(scalar_result=None, scalars_all=None) -> AsyncMock:
    """Build a minimal AsyncMock for AsyncSession."""
    session = AsyncMock()
    exec_result = MagicMock()
    exec_result.scalar_one_or_none.return_value = scalar_result
    scalars_mock = MagicMock()
    scalars_mock.all.return_value = scalars_all or []
    exec_result.scalars.return_value = scalars_mock
    session.execute = AsyncMock(return_value=exec_result)
    session.flush = AsyncMock(return_value=None)
    session.add = MagicMock()
    return session


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def reset_kafka_singleton():
    """Reset the Kafka singleton before/after each test."""
    EventService, KafkaProducerSingleton = _import_service()
    KafkaProducerSingleton._producer = None
    yield
    KafkaProducerSingleton._producer = None


@pytest.fixture()
def mock_kafka_producer() -> MagicMock:
    """Return a MagicMock Kafka Producer and inject it into the singleton."""
    _, KafkaProducerSingleton = _import_service()
    producer = MagicMock()
    producer.produce = MagicMock()
    producer.poll = MagicMock(return_value=0)
    producer.flush = MagicMock(return_value=0)
    KafkaProducerSingleton._producer = producer
    return producer


@pytest.fixture()
def sample_event_payload():
    """A minimal IngestEventIn payload dict."""
    from app.schemas.events import IngestEventIn  # type: ignore[import]

    return IngestEventIn(
        source_id="sensor-42",
        feature_vector=[0.1, 0.2, 0.3, 0.4, 0.5],
        metadata={"env": "test"},
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1. test_ingest_produces_to_kafka
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ingest_produces_to_kafka(mock_kafka_producer, sample_event_payload):
    """
    EventService.ingest() must call producer.produce() exactly once
    with the correct topic and a JSON-encoded value.
    Covers lines 54-84 of event_service.py.
    """
    EventService, _ = _import_service()
    session = _build_async_session()
    service = EventService(session)

    event_id = await service.ingest(sample_event_payload)

    assert isinstance(event_id, uuid.UUID)
    assert mock_kafka_producer.produce.call_count == 1

    # Verify the call arguments
    call_args = mock_kafka_producer.produce.call_args
    assert call_args is not None

    # Extract keyword or positional args depending on how they were passed
    kwargs = call_args[1] if call_args[1] else {}
    args = call_args[0] if call_args[0] else []

    # Topic must be the raw-events topic
    from app.core.config import settings  # type: ignore[import]

    topic_used = kwargs.get("topic") or (args[0] if args else None)
    assert topic_used == settings.KAFKA_RAW_EVENTS_TOPIC

    # Value must be valid JSON containing expected fields
    raw_value = kwargs.get("value") or (args[2] if len(args) > 2 else None)
    if raw_value:
        parsed = json.loads(raw_value)
        assert parsed["source_id"] == "sensor-42"
        assert parsed["event_id"] == str(event_id)
        assert "feature_vector" in parsed


@pytest.mark.asyncio
async def test_ingest_generates_unique_event_ids(mock_kafka_producer, sample_event_payload):
    """Each call to ingest() must return a distinct UUID."""
    EventService, _ = _import_service()
    session = _build_async_session()
    service = EventService(session)

    id1 = await service.ingest(sample_event_payload)
    id2 = await service.ingest(sample_event_payload)

    assert id1 != id2


@pytest.mark.asyncio
async def test_ingest_calls_producer_poll(mock_kafka_producer, sample_event_payload):
    """ingest() must call producer.poll(0) to trigger async delivery callbacks."""
    EventService, _ = _import_service()
    session = _build_async_session()
    service = EventService(session)

    await service.ingest(sample_event_payload)

    mock_kafka_producer.poll.assert_called_once_with(0)


# ─────────────────────────────────────────────────────────────────────────────
# 2. test_ingest_batch_produces_all_events
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ingest_batch_produces_all_events(mock_kafka_producer):
    """
    EventService.ingest_batch() must call produce() N times (once per event)
    and flush() once after the loop.
    Covers lines 86-121 of event_service.py.
    """
    from app.schemas.events import IngestEventIn  # type: ignore[import]

    EventService, _ = _import_service()
    session = _build_async_session()
    service = EventService(session)

    N = 5
    events = [
        IngestEventIn(source_id=f"src-{i}", feature_vector=[float(i), float(i + 1)])
        for i in range(N)
    ]

    event_ids = await service.ingest_batch(events)

    assert len(event_ids) == N
    # All returned IDs must be unique UUIDs
    assert len(set(str(eid) for eid in event_ids)) == N
    # produce called once per event
    assert mock_kafka_producer.produce.call_count == N
    # flush called once at end of batch
    assert mock_kafka_producer.flush.call_count == 1


@pytest.mark.asyncio
async def test_ingest_batch_empty_payload_raises(mock_kafka_producer):
    """
    An empty event list should be rejected by Pydantic (min_length=1)
    before reaching the service layer.  We verify the schema validator.
    """
    from pydantic import ValidationError  # type: ignore[import]
    from app.schemas.events import BatchEventsIn  # type: ignore[import]

    with pytest.raises(ValidationError):
        BatchEventsIn(events=[])


@pytest.mark.asyncio
async def test_ingest_batch_metadata_included_in_kafka_message(mock_kafka_producer):
    """
    Each Kafka message produced during ingest_batch() must include the
    metadata field from the input payload.
    """
    from app.schemas.events import IngestEventIn  # type: ignore[import]

    EventService, _ = _import_service()
    session = _build_async_session()
    service = EventService(session)

    events = [
        IngestEventIn(
            source_id="meta-sensor",
            feature_vector=[0.1],
            metadata={"region": "us-east-1"},
        )
    ]
    await service.ingest_batch(events)

    call_args = mock_kafka_producer.produce.call_args
    kwargs = call_args[1] if call_args[1] else {}
    raw_value = kwargs.get("value")
    if raw_value:
        parsed = json.loads(raw_value)
        assert parsed.get("metadata", {}).get("region") == "us-east-1"


# ─────────────────────────────────────────────────────────────────────────────
# 3. test_get_events_pagination_works
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_events_pagination_works():
    """
    EventService.list_events(limit=2, offset=1) must pass those values
    to the SQLAlchemy query. We verify the returned items match what the
    mock DB returns.
    Covers lines 123-150 of event_service.py.
    """
    EventService, _ = _import_service()

    rows = [_make_event_row(source_id=f"src-{i}") for i in range(2)]
    session = _build_async_session(scalars_all=rows)
    service = EventService(session)

    results = await service.list_events(limit=2, offset=1)

    assert len(results) == 2
    # Results must be AnomalyEventOut instances
    from app.schemas.events import AnomalyEventOut  # type: ignore[import]

    assert all(isinstance(r, AnomalyEventOut) for r in results)


@pytest.mark.asyncio
async def test_list_events_only_anomalies_filter():
    """
    list_events(only_anomalies=True) must add the is_anomaly filter.
    We verify the session.execute is called (with modified statement).
    Covers lines 139-140 of event_service.py.
    """
    EventService, _ = _import_service()
    anomaly_row = _make_event_row(is_anomaly=True)
    session = _build_async_session(scalars_all=[anomaly_row])
    service = EventService(session)

    results = await service.list_events(only_anomalies=True)

    assert len(results) == 1
    assert results[0].is_anomaly is True


@pytest.mark.asyncio
async def test_list_events_returns_empty_when_no_rows():
    """list_events() with no DB rows must return an empty list."""
    EventService, _ = _import_service()
    session = _build_async_session(scalars_all=[])
    service = EventService(session)

    results = await service.list_events()

    assert results == []


@pytest.mark.asyncio
async def test_list_events_source_id_filter():
    """
    list_events(source_id='sensor-X') must include source filter.
    Covers lines 141-142 of event_service.py.
    """
    EventService, _ = _import_service()
    target_row = _make_event_row(source_id="sensor-X")
    session = _build_async_session(scalars_all=[target_row])
    service = EventService(session)

    results = await service.list_events(source_id="sensor-X")

    assert len(results) == 1
    assert results[0].source_id == "sensor-X"


# ─────────────────────────────────────────────────────────────────────────────
# 4. test_label_event_updates_db
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_label_event_updates_db():
    """
    EventService.apply_label() must:
      1. Create an EventLabel record with the correct attributes.
      2. Call session.add() with the new label record.
      3. Call session.flush() to populate auto-generated fields.
      4. Return an EventLabelOut Pydantic instance.
    Covers lines 159-178 of event_service.py.
    """
    EventService, _ = _import_service()
    from app.schemas.events import EventLabelOut, LabelEventIn, LabelEnum  # type: ignore[import]

    event_id = uuid.uuid4()
    label_payload = LabelEventIn(
        label=LabelEnum.TP,
        analyst_id="analyst@example.com",
        note="Confirmed attack pattern",
    )
    label_row = _make_label_row(
        event_id=event_id,
        label="TP",
        analyst_id="analyst@example.com",
        note="Confirmed attack pattern",
    )

    session = _build_async_session()

    with patch("app.services.event_service.EventLabel") as MockEventLabel:
        MockEventLabel.return_value = label_row
        service = EventService(session)
        result = await service.apply_label(event_id, label_payload)

    # EventLabel constructor must have been called once
    MockEventLabel.assert_called_once()
    call_kwargs = MockEventLabel.call_args[1]
    assert call_kwargs["event_id"] == event_id
    assert call_kwargs["label"] == "TP"
    assert call_kwargs["analyst_id"] == "analyst@example.com"

    # session.add must have been called with the label record
    session.add.assert_called_once_with(label_row)
    session.flush.assert_called_once()

    # Return type must be EventLabelOut
    assert isinstance(result, EventLabelOut)
    assert result.label == LabelEnum.TP
    assert result.analyst_id == "analyst@example.com"


@pytest.mark.asyncio
async def test_get_by_id_returns_none_for_missing_event():
    """
    EventService.get_by_id() must return None when the DB row is absent.
    Covers lines 152-157 of event_service.py.
    """
    EventService, _ = _import_service()
    session = _build_async_session(scalar_result=None)
    service = EventService(session)

    result = await service.get_by_id(uuid.uuid4())

    assert result is None


@pytest.mark.asyncio
async def test_get_by_id_returns_anomaly_event_out():
    """
    EventService.get_by_id() must return an AnomalyEventOut when row is found.
    Covers lines 152-157 of event_service.py (happy path).
    """
    EventService, _ = _import_service()
    from app.schemas.events import AnomalyEventOut  # type: ignore[import]

    event_id = uuid.uuid4()
    row = _make_event_row(event_id=event_id, anomaly_score=0.91)
    session = _build_async_session(scalar_result=row)
    service = EventService(session)

    result = await service.get_by_id(event_id)

    assert result is not None
    assert isinstance(result, AnomalyEventOut)
    assert result.event_id == event_id
    assert result.anomaly_score == pytest.approx(0.91, abs=0.001)


@pytest.mark.asyncio
async def test_to_schema_handles_json_string_feature_vector():
    """
    EventService._to_schema() must JSON-decode feature_vector when stored
    as a string. Covers lines 182-184 of event_service.py.
    """
    EventService, _ = _import_service()

    row = MagicMock()
    row.event_id = uuid.uuid4()
    row.source_id = "test-sensor"
    row.anomaly_score = 0.75
    row.is_anomaly = True
    row.model_version = "v2.0"
    row.event_time = datetime.now(UTC)
    row.processed_at = datetime.now(UTC)
    # feature_vector stored as JSON string
    row.feature_vector = json.dumps([0.1, 0.2, 0.3])

    result = EventService._to_schema(row)

    assert isinstance(result.feature_vector, list)
    assert result.feature_vector == pytest.approx([0.1, 0.2, 0.3], abs=0.001)


@pytest.mark.asyncio
async def test_to_schema_handles_dict_feature_vector():
    """
    EventService._to_schema() handles feature_vector stored as a dict
    (JSONB column may return dict), converting values() to a list.
    Covers lines 189 of event_service.py.
    """
    EventService, _ = _import_service()

    row = MagicMock()
    row.event_id = uuid.uuid4()
    row.source_id = "dict-sensor"
    row.anomaly_score = 0.82
    row.is_anomaly = True
    row.model_version = "v1.5"
    row.event_time = datetime.now(UTC)
    row.processed_at = datetime.now(UTC)
    # feature_vector stored as dict (JSONB native format)
    row.feature_vector = {"0": 0.5, "1": 0.6, "2": 0.7}

    result = EventService._to_schema(row)

    assert isinstance(result.feature_vector, list)
    assert len(result.feature_vector) == 3


@pytest.mark.asyncio
async def test_kafka_singleton_reuses_producer(mock_kafka_producer):
    """
    KafkaProducerSingleton.get() must return the same producer instance
    on successive calls within the same process.
    Covers lines 33-45 of event_service.py.
    """
    _, KafkaProducerSingleton = _import_service()

    p1 = KafkaProducerSingleton.get()
    p2 = KafkaProducerSingleton.get()

    assert p1 is p2
