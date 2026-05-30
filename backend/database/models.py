"""
backend/database/models.py
──────────────────────────────────────────────────────────────────────────────
SQLAlchemy 2.0 async ORM models for the Real-Time Anomaly Detection System.

Tables reflected:
  • anomaly.anomaly_events       — TimescaleDB hypertable
  • anomaly.hourly_anomaly_stats  — continuous aggregate
  • anomaly.event_labels          — ground-truth labels for events
  • anomaly.alerts                — system alerts for high-severity anomalies
  • anomaly.alert_silences        — silencing configurations for alerts
  • anomaly.retrain_jobs          — training and retraining jobs tracking
──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from app.core.database import Base
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    Index,
    Integer,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import Mapped, mapped_column

__all__ = [
    "Alert",
    "AlertSilence",
    "AnomalyEvent",
    "Base",
    "EventLabel",
    "HourlyAnomalyStat",
    "RetrainJob",
]


# ─────────────────────────────────────────────────────────────────────────────
# Model: AnomalyEvent
# ─────────────────────────────────────────────────────────────────────────────
class AnomalyEvent(AsyncAttrs, Base):
    """
    ORM model for the ``anomaly.anomaly_events`` hypertable.
    """

    __tablename__ = "anomaly_events"
    __table_args__ = (
        Index("idx_anomaly_events_event_time", text("event_time DESC")),
        Index("idx_anomaly_events_source_time", "source_id", text("event_time DESC")),
        Index(
            "idx_anomaly_events_is_anomaly_time",
            "is_anomaly",
            text("event_time DESC"),
            postgresql_where=text("is_anomaly = true"),
        ),
        CheckConstraint(
            "anomaly_score >= 0.0 AND anomaly_score <= 1.0",
            name="chk_anomaly_score_range",
        ),
        {"schema": "anomaly"},
    )

    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
        nullable=False,
    )
    source_id: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )
    feature_vector: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )
    anomaly_score: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )
    is_anomaly: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    model_version: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=text("NOW()"),
    )

    def __repr__(self) -> str:
        return (
            f"<AnomalyEvent "
            f"event_id={self.event_id!s:.8} "
            f"event_time={self.event_time.isoformat() if self.event_time else 'N/A'} "
            f"source_id={self.source_id!r} "
            f"score={self.anomaly_score:.4f} "
            f"is_anomaly={self.is_anomaly}>"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": str(self.event_id),
            "event_time": self.event_time.isoformat() if self.event_time else None,
            "source_id": self.source_id,
            "feature_vector": self.feature_vector,
            "anomaly_score": self.anomaly_score,
            "is_anomaly": self.is_anomaly,
            "model_version": self.model_version,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Model: HourlyAnomalyStat
# ─────────────────────────────────────────────────────────────────────────────
class HourlyAnomalyStat(AsyncAttrs, Base):
    """
    Read-only ORM model for continuous aggregates.
    """

    __tablename__ = "hourly_anomaly_stats"
    __table_args__ = (
        Index("idx_hourly_anomaly_stats_bucket_source", "source_id", text("bucket DESC")),
        Index("idx_hourly_anomaly_stats_bucket", text("bucket DESC")),
        {"schema": "anomaly"},
    )

    bucket: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )
    source_id: Mapped[str] = mapped_column(
        Text,
        primary_key=True,
    )
    event_count: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )
    avg_score: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
    )
    max_score: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket.isoformat() if self.bucket else None,
            "source_id": self.source_id,
            "event_count": self.event_count,
            "avg_score": self.avg_score,
            "max_score": self.max_score,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Model: EventLabel (Ground-truth labels)
# ─────────────────────────────────────────────────────────────────────────────
class EventLabel(AsyncAttrs, Base):
    """
    Stores ground-truth feedback (TP, FP, TN, FN) applied by analysts on scored events.
    """

    __tablename__ = "event_labels"
    __table_args__ = (
        Index("idx_event_labels_event_id", "event_id"),
        {"schema": "anomaly"},
    )

    label_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        comment="Linked event_id",
    )
    label: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="One of: TP, FP, TN, FN",
    )
    analyst_id: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )
    note: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    labeled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=text("NOW()"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Model: Alert
# ─────────────────────────────────────────────────────────────────────────────
class Alert(AsyncAttrs, Base):
    """
    Alerts triggered by high/critical severity anomaly score threshold breaches.
    """

    __tablename__ = "alerts"
    __table_args__ = (
        Index("idx_alerts_status", "status"),
        Index("idx_alerts_severity", "severity"),
        Index("idx_alerts_source_id", "source_id"),
        {"schema": "anomaly"},
    )

    alert_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    source_id: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )
    severity: Mapped[str] = mapped_column(
        Text,
        nullable=False,  # LOW, MEDIUM, HIGH, CRITICAL
    )
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,  # ACTIVE, ACKNOWLEDGED, RESOLVED
        default="ACTIVE",
        server_default=text("'ACTIVE'"),
    )
    score: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )
    analyst_id: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    note: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    resolution: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=text("NOW()"),
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    resolved_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Model: AlertSilence
# ─────────────────────────────────────────────────────────────────────────────
class AlertSilence(AsyncAttrs, Base):
    """
    Maintains silence configurations for specific source systems or time windows.
    """

    __tablename__ = "alert_silences"
    __table_args__ = (
        Index("idx_alert_silences_expires", "expires_at"),
        {"schema": "anomaly"},
    )

    silence_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    source_id: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,  # If NULL, silences all alert generators
    )
    duration_minutes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    reason: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=text("NOW()"),
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Model: RetrainJob
# ─────────────────────────────────────────────────────────────────────────────
class RetrainJob(AsyncAttrs, Base):
    """
    Logs retraining job invocations and execution history.
    """

    __tablename__ = "retrain_jobs"
    __table_args__ = (
        Index("idx_retrain_jobs_status", "status"),
        {"schema": "anomaly"},
    )

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,  # PENDING, RUNNING, COMPLETED, FAILED
        default="PENDING",
        server_default=text("'PENDING'"),
    )
    reason: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )
    force: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=text("NOW()"),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
