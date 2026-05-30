"""
TimescaleDB Writer Agent
Architecture: Section 5 — Sink agent
Trigger: scored-events Kafka topic consumption
Output: Hypertable row insert into anomaly.anomaly_events
Uses: SQLAlchemy 2.0 async + asyncpg 0.29
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import UTC, datetime
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from agents.shared.state import AgentState

logger = structlog.get_logger(__name__)


INSERT_SQL = text("""
    INSERT INTO anomaly.anomaly_events (
        event_id, event_time, source_id, feature_vector,
        anomaly_score, is_anomaly, model_version, processed_at
    ) VALUES (
        :event_id, :event_time, :source_id, CAST(:feature_vector AS jsonb),
        :anomaly_score, :is_anomaly, :model_version, :processed_at
    )
    ON CONFLICT (event_id, event_time) DO NOTHING
""")


class TimescaleDBWriterWorker:
    """
    Standalone Kafka consumer that writes scored events to TimescaleDB.
    Runs as a long-lived async loop in the ml-inference-worker container.
    """

    def __init__(self) -> None:
        bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        self._topic = os.environ.get("KAFKA_SCORED_EVENTS_TOPIC", "scored-events")
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": os.environ.get("KAFKA_WRITER_GROUP_ID", "timescaledb-writer-group"),
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        database_url = os.environ["DATABASE_URL"]
        engine = create_async_engine(database_url, pool_size=5, max_overflow=10, pool_pre_ping=True)
        self._session_factory = async_sessionmaker(engine, expire_on_commit=False)
        self._running = False

    async def run(self) -> None:
        """Main consumer loop."""
        self._consumer.subscribe([self._topic])
        self._running = True
        logger.info("TimescaleDB writer worker started", topic=self._topic)

        try:
            while self._running:
                msg = self._consumer.poll(timeout=0.1)
                if msg is None:
                    await asyncio.sleep(0.01)
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                val = msg.value()
                if val is None:
                    await asyncio.sleep(0.01)
                    continue
                event: dict[str, Any] = json.loads(val)
                await self._write_event(event)
                self._consumer.commit(message=msg)
                await asyncio.sleep(0.01)
        finally:
            self._consumer.close()

    async def _write_event(self, event: dict[str, Any]) -> None:
        """Insert a single scored event into the anomaly_events hypertable."""
        async with self._session_factory() as session:
            try:
                # Parse timestamp if it's a string, otherwise default to now
                ts_raw = event.get("timestamp")
                if isinstance(ts_raw, str):
                    if ts_raw.endswith("Z"):
                        ts_raw = ts_raw[:-1] + "+00:00"
                    event_time = datetime.fromisoformat(ts_raw)
                else:
                    event_time = datetime.now(tz=UTC)

                await session.execute(
                    INSERT_SQL,
                    {
                        "event_id": event.get("event_id", str(uuid.uuid4())),
                        "event_time": event_time,
                        "source_id": event.get("source_id", "unknown"),
                        "feature_vector": json.dumps(event.get("feature_vector", [])),
                        "anomaly_score": float(event.get("anomaly_score", 0.0)),
                        "is_anomaly": bool(event.get("is_anomaly", False)),
                        "model_version": event.get("model_version", "unknown"),
                        "processed_at": datetime.now(tz=UTC),
                    },
                )
                await session.commit()
                logger.debug("Event written to TimescaleDB", event_id=event.get("event_id"))
            except Exception as exc:
                await session.rollback()
                logger.error(
                    "Failed to write event", error=str(exc), event_id=event.get("event_id")
                )
                raise

    def stop(self) -> None:
        self._running = False


async def timescaledb_writer_node(state: AgentState) -> AgentState:
    """
    LangGraph node: persist the current scored event to TimescaleDB.
    Used when the writer is embedded in the orchestrator graph.
    """
    logger.info(
        "TimescaleDB writer node: writing event",
        event_id=state.get("event_id"),
        score=state.get("anomaly_score"),
    )
    # In the graph context the write is delegated to the API service;
    # the standalone worker handles the bulk writes from Kafka.
    return state
