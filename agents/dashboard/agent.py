"""
Dashboard Agent — Bridge
Architecture: Section 5 — scored-events poll → Grafana API annotation + WebSocket push
Uses: Grafana Live WebSocket, Grafana Annotations REST API
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

import httpx
import structlog

logger = structlog.get_logger(__name__)

GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://grafana:3000")
GRAFANA_USER = os.environ.get("GRAFANA_ADMIN_USER", "admin")
GRAFANA_PASSWORD = os.environ.get("GRAFANA_ADMIN_PASSWORD", "admin")


async def push_grafana_annotation(event: dict[str, Any]) -> bool:
    """
    Create a Grafana annotation on the anomaly dashboard
    whenever an anomaly is detected (is_anomaly=True).
    Uses Grafana Annotations REST API v1.
    """
    if not event.get("is_anomaly"):
        return False

    severity = _score_to_severity(event.get("anomaly_score", 0.0))
    tag_list = ["anomaly", severity, f"source:{event.get('source_id', 'unknown')}"]

    payload = {
        "dashboardUID": os.environ.get("GRAFANA_DASHBOARD_UID", "anomaly-overview"),
        "time": int(time.time() * 1000),
        "tags": tag_list,
        "text": (
            f"Anomaly detected | Score: {event.get('anomaly_score', 0):.4f} | "
            f"Severity: {severity} | Source: {event.get('source_id')} | "
            f"Model: {event.get('model_version')}"
        ),
    }

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                f"{GRAFANA_URL}/api/annotations",
                json=payload,
                auth=(GRAFANA_USER, GRAFANA_PASSWORD),
            )
            if resp.status_code in {200, 201}:
                logger.debug("Grafana annotation created", score=event.get("anomaly_score"))
                return True
            logger.warning("Grafana annotation failed", status=resp.status_code)
            return False
    except Exception as exc:
        logger.error("Grafana annotation error", error=str(exc))
        return False


def _score_to_severity(score: float) -> str:
    if score >= 0.95:
        return "critical"
    elif score >= 0.85:
        return "high"
    elif score >= 0.75:
        return "medium"
    return "low"


class DashboardBridgeWorker:
    """
    Kafka consumer that polls scored-events and pushes
    real-time annotations to Grafana for any detected anomalies.
    """

    def __init__(self) -> None:
        from confluent_kafka import Consumer

        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": os.environ.get("KAFKA_DASHBOARD_GROUP_ID", "dashboard-bridge-group"),
                "auto.offset.reset": "latest",  # Only new events for live dashboard
                "enable.auto.commit": True,
            }
        )
        self._topic = os.environ.get("KAFKA_SCORED_EVENTS_TOPIC", "scored-events")
        self._running = False

    async def run(self) -> None:
        """Main loop — push anomaly annotations to Grafana."""
        self._consumer.subscribe([self._topic])
        self._running = True
        logger.info("Dashboard bridge worker started", topic=self._topic)

        try:
            while self._running:
                msg = self._consumer.poll(timeout=0.1)
                if msg is None:
                    await asyncio.sleep(0.01)
                    continue
                if msg.error():
                    from confluent_kafka import KafkaException

                    raise KafkaException(msg.error())
                val = msg.value()
                if val is None:
                    await asyncio.sleep(0.01)
                    continue
                event = json.loads(val)
                await push_grafana_annotation(event)
                await asyncio.sleep(0.01)
        finally:
            self._consumer.close()

    def stop(self) -> None:
        self._running = False
