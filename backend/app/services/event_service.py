"""
Event Service — business logic for event ingestion, querying, and ground-truth labelling
"""
from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import structlog
from confluent_kafka import Producer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings

try:
    from database.models import AnomalyEvent, EventLabel
except ModuleNotFoundError:
    from backend.database.models import AnomalyEvent, EventLabel
from app.schemas.events import AnomalyEventOut, EventLabelOut, IngestEventIn, LabelEventIn

logger = structlog.get_logger(__name__)


class KafkaProducerSingleton:
    """Lazy singleton Kafka producer for the FastAPI process."""

    _producer: Producer | None = None

    @classmethod
    def get(cls) -> Producer:
        if cls._producer is None:
            cls._producer = Producer(
                {
                    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                    "acks": "all",
                    "enable.idempotence": True,
                    "linger.ms": 5,
                    "compression.type": "lz4",
                }
            )
        return cls._producer


class EventService:
    """Handles raw event ingestion, scored event querying, and ground-truth feedback."""

    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    async def ingest(self, payload: IngestEventIn) -> uuid.UUID:
        """
        Publish a raw event to the Kafka raw-events topic.
        Returns the generated event_id.
        """
        event_id = uuid.uuid4()
        event_time = payload.timestamp or datetime.now(tz=UTC)

        message: dict[str, Any] = {
            "event_id": str(event_id),
            "source_id": payload.source_id,
            "feature_vector": payload.feature_vector,
            "timestamp": event_time.isoformat(),
            "metadata": payload.metadata or {},
        }

        producer = KafkaProducerSingleton.get()
        producer.produce(
            topic=settings.KAFKA_RAW_EVENTS_TOPIC,
            key=str(event_id),
            value=json.dumps(message),
        )
        producer.poll(0)

        logger.info(
            "Event ingested to Kafka",
            event_id=str(event_id),
            source_id=payload.source_id,
            topic=settings.KAFKA_RAW_EVENTS_TOPIC,
        )
        return event_id

    async def ingest_batch(self, events: list[IngestEventIn]) -> list[uuid.UUID]:
        """
        Publish a batch of raw events to the Kafka raw-events topic.
        Returns list of generated event_ids.
        """
        producer = KafkaProducerSingleton.get()
        event_ids = []

        for payload in events:
            event_id = uuid.uuid4()
            event_time = payload.timestamp or datetime.now(tz=UTC)

            message: dict[str, Any] = {
                "event_id": str(event_id),
                "source_id": payload.source_id,
                "feature_vector": payload.feature_vector,
                "timestamp": event_time.isoformat(),
                "metadata": payload.metadata or {},
            }

            producer.produce(
                topic=settings.KAFKA_RAW_EVENTS_TOPIC,
                key=str(event_id),
                value=json.dumps(message),
            )
            event_ids.append(event_id)

        # Trigger batch delivery
        producer.flush(timeout=2)

        logger.info(
            "Batch events ingested to Kafka",
            count=len(events),
            topic=settings.KAFKA_RAW_EVENTS_TOPIC,
        )
        return event_ids

    async def list_events(
        self,
        limit: int = 50,
        offset: int = 0,
        only_anomalies: bool = False,
        source_id: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[AnomalyEventOut]:
        """Query anomaly_events hypertable with optional filters and time ranges."""
        stmt = (
            select(AnomalyEvent)
            .order_by(AnomalyEvent.event_time.desc())
            .limit(limit)
            .offset(offset)
        )
        if only_anomalies:
            stmt = stmt.where(AnomalyEvent.is_anomaly.is_(True))
        if source_id:
            stmt = stmt.where(AnomalyEvent.source_id == source_id)
        if start:
            stmt = stmt.where(AnomalyEvent.event_time >= start)
        if end:
            stmt = stmt.where(AnomalyEvent.event_time <= end)

        result = await self._db.execute(stmt)
        rows = result.scalars().all()
        return [self._to_schema(row) for row in rows]

    async def get_by_id(self, event_id: uuid.UUID) -> AnomalyEventOut | None:
        """Fetch a single event by its UUID."""
        stmt = select(AnomalyEvent).where(AnomalyEvent.event_id == event_id)
        result = await self._db.execute(stmt)
        row = result.scalar_one_or_none()
        return self._to_schema(row) if row else None

    async def apply_label(self, event_id: uuid.UUID, payload: LabelEventIn) -> EventLabelOut:
        """Apply a ground-truth feedback label to a scored event."""
        # Create a new label mapping
        label_record = EventLabel(
            event_id=event_id,
            label=payload.label.value,
            analyst_id=payload.analyst_id,
            note=payload.note,
            labeled_at=datetime.now(tz=UTC),
        )
        self._db.add(label_record)
        await self._db.flush()  # populate autogenerated fields

        logger.info(
            "Event ground-truth label applied",
            event_id=str(event_id),
            label=payload.label.value,
            analyst_id=payload.analyst_id,
        )
        return EventLabelOut.model_validate(label_record)

    @staticmethod
    def _to_schema(row: AnomalyEvent) -> AnomalyEventOut:
        fv = row.feature_vector
        if isinstance(fv, str):
            fv = json.loads(fv)
        return AnomalyEventOut(
            event_id=row.event_id,
            event_time=row.event_time,
            source_id=row.source_id,
            feature_vector=list(fv) if isinstance(fv, list) else list(fv.values()),
            anomaly_score=row.anomaly_score,
            is_anomaly=row.is_anomaly,
            model_version=row.model_version,
            processed_at=row.processed_at,
        )
