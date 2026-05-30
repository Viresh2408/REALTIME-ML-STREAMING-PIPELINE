"""
Event schemas — Pydantic v2
Covers: single/batch ingestion request, anomaly event response, ground-truth labelling
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class LabelEnum(str, Enum):
    TP = "TP"  # True Positive
    FP = "FP"  # False Positive
    TN = "TN"  # True Negative
    FN = "FN"  # False Negative


class IngestEventIn(BaseModel):
    """Request body for POST /api/v1/events."""

    source_id: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Origin system or user ID",
        examples=["sensor-42", "api-gateway-us-east-1"],
    )
    feature_vector: List[float] = Field(
        ...,
        min_length=1,
        max_length=1024,
        description="Numeric feature array passed to the ML model",
        examples=[[0.12, -0.45, 1.23, 0.87, -0.33]],
    )
    timestamp: Optional[datetime] = Field(
        default=None,
        description="Event occurrence time (UTC). Defaults to server time if omitted.",
    )
    metadata: Optional[dict[str, Any]] = Field(
        default=None,
        description="Optional arbitrary metadata attached to the event",
    )

    @field_validator("feature_vector")
    @classmethod
    def validate_feature_vector(cls, v: List[float]) -> List[float]:
        import math
        for val in v:
            if math.isnan(val) or math.isinf(val):
                raise ValueError("feature_vector must not contain NaN or Inf values")
        return v


class BatchEventsIn(BaseModel):
    """Request body for batch event ingestion (up to 1000 events)."""

    events: List[IngestEventIn] = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="List of events to ingest in batch",
    )


class IngestEventResponse(BaseModel):
    """Response after a raw event is queued to Kafka."""

    event_id: str = Field(..., description="Generated unique event ID")
    status: str = Field(default="queued", description="Ingestion status")


class BatchIngestEventResponse(BaseModel):
    """Response after batch events are queued to Kafka."""

    event_ids: List[str] = Field(..., description="List of generated unique event IDs")
    status: str = Field(default="queued", description="Ingestion status")
    count: int = Field(..., description="Number of events successfully queued")


class AnomalyEventOut(BaseModel):
    """Scored anomaly event from TimescaleDB."""

    event_id: UUID = Field(..., description="Unique event identifier")
    event_time: datetime = Field(..., description="Event occurrence time")
    source_id: str = Field(..., description="Origin system or user ID")
    feature_vector: List[float] = Field(..., description="Numeric features passed to ML model")
    anomaly_score: float = Field(..., ge=0.0, le=1.0, description="Model anomaly score")
    is_anomaly: bool = Field(..., description="Threshold-applied classification")
    model_version: str = Field(..., description="ML model version used for scoring")
    processed_at: datetime = Field(..., description="Ingestion processing timestamp")

    model_config = {"from_attributes": True}


class LabelEventIn(BaseModel):
    """Request schema to apply ground-truth feedback to an event."""

    label: LabelEnum = Field(..., description="Ground-truth feedback label: TP, FP, TN, FN")
    analyst_id: str = Field(..., description="ID of the analyst submitting feedback")
    note: Optional[str] = Field(default=None, description="Optional annotations regarding the label decision")


class EventLabelOut(BaseModel):
    """Response schema containing applied ground-truth label details."""

    label_id: UUID
    event_id: UUID
    label: LabelEnum
    analyst_id: str
    note: Optional[str]
    labeled_at: datetime

    model_config = {"from_attributes": True}
