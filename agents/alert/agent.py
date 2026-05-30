"""
Alert Agent
Architecture: Section 5 — Reactive, triggered on score threshold breach
Input: scored-events Kafka topic (also called from LangGraph node)
Output: alerts Kafka topic message
"""
from __future__ import annotations

import json
import os
import uuid
from typing import Any

import structlog
from confluent_kafka import Producer

from agents.shared.state import AgentState

logger = structlog.get_logger(__name__)


def _classify_severity(score: float) -> str:
    """Map anomaly score to a severity tier."""
    if score >= 0.95:
        return "critical"
    elif score >= 0.85:
        return "high"
    elif score >= 0.75:
        return "medium"
    return "low"


class AlertPublisher:
    """Publishes alert messages to the alerts Kafka topic."""

    def __init__(self) -> None:
        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self._topic = os.environ.get("KAFKA_ALERTS_TOPIC", "alerts")
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    def publish(self, alert: dict[str, Any]) -> None:
        self._producer.produce(
            topic=self._topic,
            key=alert.get("alert_id", str(uuid.uuid4())),
            value=json.dumps(alert),
        )
        self._producer.flush()
        logger.info(
            "Alert published",
            alert_id=alert["alert_id"],
            severity=alert["severity"],
            score=alert["anomaly_score"],
        )


async def alert_node(state: AgentState) -> AgentState:
    """
    LangGraph node: fire alert when anomaly score exceeds threshold.
    Publishes to alerts Kafka topic and updates state.
    """
    score: float = state.get("anomaly_score", 0.0)
    event_id: str = state.get("event_id", "unknown")
    source_id: str = state.get("source_id", "unknown")
    model_version: str = state.get("model_version", "unknown")
    severity = _classify_severity(score)

    alert = {
        "alert_id": str(uuid.uuid4()),
        "event_id": event_id,
        "source_id": source_id,
        "anomaly_score": score,
        "severity": severity,
        "model_version": model_version,
        "llm_explanation": state.get("llm_explanation", ""),
        "feature_vector": state.get("feature_vector", []),
    }

    publisher = AlertPublisher()
    publisher.publish(alert)

    state["alert_severity"] = severity
    state["alert_sent"] = True

    return state
