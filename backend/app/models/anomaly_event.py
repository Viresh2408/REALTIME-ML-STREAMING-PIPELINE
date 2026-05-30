"""
SQLAlchemy 2.0 ORM model — anomaly.anomaly_events hypertable
Columns mirror architecture.docx Section 6 schema exactly.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Float, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class AnomalyEvent(Base):
    """
    Maps to the TimescaleDB hypertable: anomaly.anomaly_events
    Partitioned by event_time (1-day chunks).
    """

    __tablename__ = "anomaly_events"
    __table_args__ = {"schema": "anomaly"}

    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="Unique event identifier",
    )
    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,  # composite PK for hypertable
        comment="Partition key — event occurrence time",
    )
    source_id: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        index=True,
        comment="Origin system or user ID",
    )
    feature_vector: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        comment="Raw numeric features passed to ML model",
    )
    anomaly_score: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        comment="Model output (0–1, higher = more anomalous)",
    )
    is_anomaly: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="Threshold-applied label",
    )
    model_version: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Model artifact version used for inference",
    )
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="When the consumer processed this event",
    )
