"""
Alerts schemas — Pydantic v2
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field


class AlertSeverity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

class AlertStatus(str, Enum):
    ACTIVE = "ACTIVE"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"

class AlertOut(BaseModel):
    """Output details of a system alert."""

    alert_id: UUID = Field(..., description="Unique alert UUID")
    source_id: str = Field(..., description="Target source identifier")
    severity: AlertSeverity = Field(..., description="Classification: LOW, MEDIUM, HIGH, CRITICAL")
    status: AlertStatus = Field(..., description="State: ACTIVE, ACKNOWLEDGED, RESOLVED")
    score: float = Field(..., description="Maximum triggering score")
    analyst_id: str | None = Field(default=None, description="Assigned analyst")
    note: str | None = Field(default=None, description="Feedback comment")
    resolution: str | None = Field(default=None, description="Action taken to resolve alert")
    created_at: datetime = Field(..., description="Alert creation time")
    acknowledged_at: datetime | None = Field(default=None, description="Time acknowledged")
    resolved_at: datetime | None = Field(default=None, description="Time resolved")

    model_config = {"from_attributes": True}

class AlertAcknowledgeIn(BaseModel):
    """Acknowledge alert payload."""

    analyst_id: str = Field(..., description="Assigned analyst ID")
    note: str | None = Field(default=None, description="Optional triage comment")

class AlertResolveIn(BaseModel):
    """Resolve alert payload."""

    analyst_id: str = Field(..., description="Assigned analyst ID")
    resolution: str = Field(..., description="Resolution statement/actions")

class AlertSilenceIn(BaseModel):
    """Request silencing alert parameters."""

    source_id: str | None = Field(default=None, description="Optionally silence specific source")
    duration_minutes: int = Field(..., ge=1, le=1440, description="Duration in minutes to silence")
    reason: str = Field(..., description="Reasoning behind suppression")

class AlertSilenceOut(BaseModel):
    """Silencing action result."""

    silence_id: UUID
    source_id: str | None
    duration_minutes: int
    reason: str
    created_at: datetime
    expires_at: datetime

    model_config = {"from_attributes": True}
