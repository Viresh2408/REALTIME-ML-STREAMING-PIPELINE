"""
agents/alerting/models.py
==========================
Shared data models for the alert pipeline.

``AlertEvent`` is the canonical wire format produced by the Alert Agent
and consumed by Slack, email, and PagerDuty notifiers.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from agents.alerting.severity_classifier import Severity


class AlertEvent(BaseModel):
    """Canonical alert payload flowing through the alert pipeline."""

    alert_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique alert identifier (UUID4).",
    )
    event_id: str = Field(..., description="Source anomaly event_id from TimescaleDB.")
    source_id: str = Field(..., description="Originating system / device identifier.")
    score: float = Field(..., ge=0.0, le=1.0, description="Anomaly score in [0.0, 1.0].")
    severity: Severity = Field(..., description="Classified severity level.")
    burst_count: int = Field(
        default=0,
        ge=0,
        description="Number of HIGH+ alerts in the last 60 s for this source_id.",
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(datetime.UTC),
        description="UTC timestamp when this alert was created.",
    )
    dashboard_url: str | None = Field(
        default=None,
        description="Deep-link to Grafana panel for this event (auto-built if absent).",
    )

    # ── Convenience ────────────────────────────────────────────────────────────

    @property
    def grafana_deep_link(self) -> str:
        """Return an explicit or auto-generated Grafana deep-link."""
        if self.dashboard_url:
            return self.dashboard_url
        # Default: Grafana running on localhost, panel for the anomaly events view
        import os

        grafana_host = os.getenv("GRAFANA_URL", "http://localhost:3000")
        return (
            f"{grafana_host}/d/anomaly-events"
            f"?var-source_id={self.source_id}"
            f"&var-event_id={self.event_id}"
        )

    @property
    def severity_emoji(self) -> str:
        """Unicode emoji that matches the alert severity for Slack messages."""
        return {
            Severity.NONE: "✅",
            Severity.LOW: "🟡",
            Severity.MEDIUM: "🟠",
            Severity.HIGH: "🔴",
            Severity.CRITICAL: "🚨",
        }.get(self.severity, "❓")
