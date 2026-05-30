"""
Producer Agent
Architecture: Section 5 — Event generator
Trigger: Scheduled (APScheduler) or external webhook
Output: Kafka raw-events topic messages (Avro-encoded)
"""
from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any

import numpy as np
import structlog
from confluent_kafka import Producer

from agents.shared.state import AgentState

logger = structlog.get_logger(__name__)


class KafkaEventProducer:
    """
    Produces synthetic or forwarded events to the raw-events Kafka topic.
    Uses confluent-kafka 2.4 (librdkafka under the hood).
    """

    def __init__(self) -> None:
        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self._topic = os.environ.get("KAFKA_RAW_EVENTS_TOPIC", "raw-events")
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "retries": 5,
                "retry.backoff.ms": 300,
                "linger.ms": 5,
                "compression.type": "lz4",
                "enable.idempotence": True,
            }
        )

    def produce_event(self, payload: dict[str, Any]) -> None:
        """Serialize and produce a single event to raw-events."""
        self._producer.produce(
            topic=self._topic,
            key=payload.get("event_id", str(uuid.uuid4())),
            value=json.dumps(payload),
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()

    @staticmethod
    def _delivery_report(err: Any, msg: Any) -> None:
        if err is not None:
            logger.error("Kafka delivery failed", error=str(err))
        else:
            logger.debug(
                "Message delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )


def generate_synthetic_event(
    source_id: str | None = None,
    n_features: int = 10,
    anomaly_probability: float = 0.05,
) -> dict[str, Any]:
    """
    Generate a synthetic event payload for development/testing.
    Injects anomalies at the given probability by sampling from a
    high-variance distribution.
    """
    rng = np.random.default_rng()
    is_injected_anomaly = rng.random() < anomaly_probability

    if is_injected_anomaly:
        feature_vector = rng.normal(loc=5.0, scale=3.0, size=n_features).tolist()
    else:
        feature_vector = rng.normal(loc=0.0, scale=1.0, size=n_features).tolist()

    return {
        "event_id": str(uuid.uuid4()),
        "source_id": source_id or f"source-{rng.integers(1, 21)}",
        "feature_vector": feature_vector,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "injected_anomaly": is_injected_anomaly,
    }


async def producer_node(state: AgentState) -> AgentState:
    """
    LangGraph node: generate and produce a synthetic event to Kafka.
    """
    producer = KafkaEventProducer()
    event = generate_synthetic_event(
        source_id=state.get("source_id"),
    )
    producer.produce_event(event)
    producer.flush()

    logger.info(
        "Producer node: event published",
        event_id=event["event_id"],
        source_id=event["source_id"],
        injected_anomaly=event.get("injected_anomaly"),
    )

    state["event_id"] = event["event_id"]
    state["source_id"] = event["source_id"]
    state["feature_vector"] = event["feature_vector"]
    state["raw_event"] = event
    state["needs_inference"] = True

    return state
