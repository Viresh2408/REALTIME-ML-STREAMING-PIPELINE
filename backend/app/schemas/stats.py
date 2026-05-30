"""
Statistics schemas — Pydantic v2
"""
from __future__ import annotations

from pydantic import BaseModel, Field


class AnomalyStatsOut(BaseModel):
    """Aggregated anomaly statistics for a time window."""

    window_minutes: int = Field(description="Query window in minutes")
    total_events: int = Field(description="Total events in window")
    anomaly_count: int = Field(description="Events flagged as anomalous")
    anomaly_rate_pct: float = Field(description="Anomaly rate as percentage (0–100)")
    avg_score: float = Field(description="Mean anomaly score across all events")
    max_score: float = Field(description="Maximum anomaly score in window")
