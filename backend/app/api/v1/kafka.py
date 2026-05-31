"""
Kafka test endpoints — for end-to-end integration testing and verification.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, Field

from app.api.v1.auth import check_viewer
from app.core.config import settings
from app.schemas.auth import TokenData
from app.services.event_service import KafkaProducerSingleton

router = APIRouter()


class TestProducePayload(BaseModel):
    score: float = Field(default=0.97, ge=0.0, le=1.0, description="Synthetic anomaly score")
    source_id: str | None = Field(default=None, description="Optional custom source identifier")


class TestProduceResponse(BaseModel):
    event_id: str
    source_id: str
    topic: str
    status: str


@router.post(
    "/produce-test",
    response_model=TestProduceResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Produce a synthetic scored event directly into Kafka scored-events topic",
)
async def produce_test_event(
    payload: TestProducePayload,
    current_user: Annotated[TokenData, Depends(check_viewer)],
) -> TestProduceResponse:
    """
    Directly injects a synthetic scored event into the `scored-events` topic.
    This bypasses raw ingestion and ML inference, pushing straight to downstream alerting.
    """
    event_id = str(uuid.uuid4())
    source_id = payload.source_id or f"synthetic-test-{uuid.uuid4().hex[:8]}"
    now_ms = int(time.time() * 1000)

    # Classify severity inline for topic metadata
    score = payload.score
    if score >= 0.95:
        severity = "CRITICAL"
    elif score >= 0.90:
        severity = "HIGH"
    elif score >= 0.80:
        severity = "MEDIUM"
    elif score >= 0.70:
        severity = "LOW"
    else:
        severity = "NONE"

    scored_event = {
        "event_id": event_id,
        "source_id": source_id,
        "event_type": "synthetic-test",
        "features": {
            "f0": score,
            "f1": 0.98,
            "f2": 0.96,
            "f3": 0.99,
            "f4": score,
        },
        "event_time": now_ms,
        "anomaly_score": score,
        "is_anomaly": score >= settings.ANOMALY_SCORE_THRESHOLD,
        "severity": severity,
        "model_version": "synthetic-test-v1",
        "processed_at": now_ms,
    }

    producer = KafkaProducerSingleton.get()
    producer.produce(
        topic=settings.KAFKA_SCORED_EVENTS_TOPIC,
        key=event_id.encode("utf-8"),
        value=json.dumps(scored_event).encode("utf-8"),
    )
    producer.flush(timeout=5)

    return TestProduceResponse(
        event_id=event_id,
        source_id=source_id,
        topic=settings.KAFKA_SCORED_EVENTS_TOPIC,
        status="produced",
    )
