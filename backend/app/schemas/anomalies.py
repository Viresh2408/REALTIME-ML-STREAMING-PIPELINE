"""
Anomalies schemas — Pydantic v2
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class AnomalyStatsOut(BaseModel):
    """Aggregated anomaly statistics for a time window."""

    window_minutes: int = Field(..., description="Query window in minutes")
    total_events: int = Field(..., description="Total events in window")
    anomaly_count: int = Field(..., description="Events flagged as anomalous")
    anomaly_rate_pct: float = Field(..., description="Anomaly rate as percentage (0–100)")
    avg_score: float = Field(..., description="Mean anomaly score across all events")
    max_score: float = Field(..., description="Maximum anomaly score in window")


class HeatmapItem(BaseModel):
    """A single bucket in a time × source_id heatmap."""

    bucket: datetime = Field(..., description="Hourly or resolution-truncated time bucket")
    source_id: str = Field(..., description="Origin system or user ID")
    event_count: int = Field(..., description="Total events in this bucket")
    anomaly_count: int = Field(..., description="Flagged anomalies in this bucket")
    avg_score: float = Field(..., description="Average anomaly score in this bucket")
    max_score: float = Field(..., description="Maximum anomaly score in this bucket")


class HeatmapOut(BaseModel):
    """Heatmap dataset containing bucketed time-series values."""

    resolution: str = Field(..., description="Bucket resolution, e.g. 1h, 5m")
    start: datetime = Field(..., description="Start range boundary")
    end: datetime = Field(..., description="End range boundary")
    data: list[HeatmapItem] = Field(..., description="Bucket list")
