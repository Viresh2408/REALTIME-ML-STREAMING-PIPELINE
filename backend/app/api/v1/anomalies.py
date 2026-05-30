"""
Anomaly Query APIs
Provides rich querying, stats aggregation, and heatmap generation.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.auth import check_viewer
from app.core.database import get_db_session
from app.schemas.anomalies import AnomalyStatsOut, HeatmapItem, HeatmapOut
from app.schemas.auth import TokenData
from app.schemas.events import AnomalyEventOut

logger = structlog.get_logger(__name__)

router = APIRouter()

# Severity to anomaly score mapping (workflow.docx Section 4)
SEVERITY_RANGES = {
    "LOW": (0.70, 0.799999),
    "MEDIUM": (0.80, 0.899999),
    "HIGH": (0.90, 0.949999),
    "CRITICAL": (0.95, 1.0),
}


@router.get(
    "",
    response_model=list[AnomalyEventOut],
    summary="List scored anomalies with filtering options",
)
async def list_anomalies(
    current_user: Annotated[TokenData, Depends(check_viewer)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    min_score: float | None = Query(default=None, ge=0.0, le=1.0),
    severity: str | None = Query(default=None, description="LOW, MEDIUM, HIGH, CRITICAL"),
    source_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[AnomalyEventOut]:
    """
    Query scored anomalies. Automatically restricts search to is_anomaly = true.
    Supports filtering by min_score, severity, origin source_id, and time range.
    """
    conditions = ["is_anomaly = true"]
    params = {"limit": limit, "offset": offset}

    if min_score is not None:
        conditions.append("anomaly_score >= :min_score")
        params["min_score"] = min_score

    if severity:
        sev_upper = severity.upper()
        if sev_upper in SEVERITY_RANGES:
            low, high = SEVERITY_RANGES[sev_upper]
            conditions.append("anomaly_score >= :sev_low AND anomaly_score <= :sev_high")
            params["sev_low"] = low
            params["sev_high"] = high
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid severity. Must be one of: LOW, MEDIUM, HIGH, CRITICAL",
            )

    if source_id:
        conditions.append("source_id = :source_id")
        params["source_id"] = source_id

    if start:
        conditions.append("event_time >= :start")
        params["start"] = start

    if end:
        conditions.append("event_time <= :end")
        params["end"] = end

    where_clause = " AND ".join(conditions)
    sql = text(f"""
        SELECT event_id, event_time, source_id, feature_vector, anomaly_score, is_anomaly, model_version, processed_at
        FROM anomaly.anomaly_events
        WHERE {where_clause}
        ORDER BY event_time DESC
        LIMIT :limit OFFSET :offset
    """)

    result = await db.execute(sql, params)
    rows = result.fetchall()

    out = []
    for r in rows:
        import json

        fv = r.feature_vector
        if isinstance(fv, str):
            fv = json.loads(fv)
        out.append(
            AnomalyEventOut(
                event_id=r.event_id,
                event_time=r.event_time,
                source_id=r.source_id,
                feature_vector=list(fv) if isinstance(fv, list) else list(fv.values()),
                anomaly_score=r.anomaly_score,
                is_anomaly=r.is_anomaly,
                model_version=r.model_version,
                processed_at=r.processed_at,
            )
        )
    return out


@router.get(
    "/stats",
    response_model=AnomalyStatsOut,
    summary="Get aggregated anomaly statistics for a window",
)
async def get_anomaly_stats(
    current_user: Annotated[TokenData, Depends(check_viewer)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    bucket: str = Query(default="5m", description="Bucket window, e.g. 5m, 1h, 1d"),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
) -> AnomalyStatsOut:
    """
    Return aggregated metrics (total count, anomaly rate, avg/max scores)
    within the specified start and end time boundaries.
    """
    # Default to last 24 hours if start not provided
    end_time = end or datetime.now(UTC)
    start_time = start or (end_time - timedelta(days=1))

    sql = text("""
        SELECT
            COUNT(*)                                            AS total_events,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)        AS anomaly_count,
            COALESCE(AVG(anomaly_score), 0.0)                   AS avg_score,
            COALESCE(MAX(anomaly_score), 0.0)                   AS max_score,
            ROUND(
                100.0 * SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
                2
            )                                                   AS anomaly_rate_pct
        FROM anomaly.anomaly_events
        WHERE event_time >= :start AND event_time <= :end
    """)

    result = await db.execute(sql, {"start": start_time, "end": end_time})
    row = result.mappings().one()

    # Determine window in minutes
    window_minutes = int((end_time - start_time).total_seconds() / 60)

    return AnomalyStatsOut(
        window_minutes=window_minutes,
        total_events=int(row["total_events"] or 0),
        anomaly_count=int(row["anomaly_count"] or 0),
        anomaly_rate_pct=float(row["anomaly_rate_pct"] or 0.0),
        avg_score=float(row["avg_score"] or 0.0),
        max_score=float(row["max_score"] or 0.0),
    )


@router.get(
    "/heatmap",
    response_model=HeatmapOut,
    summary="Get time × source_id heatmap data for Grafana",
)
async def get_heatmap(
    current_user: Annotated[TokenData, Depends(check_viewer)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    resolution: str = Query(default="1h", description="Bucket resolution: 5m, 1h, 1d"),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
) -> HeatmapOut:
    """
    Provides aggregated time-series buckets cross-referenced by source_id.
    Powers heatmap visualisations on analytical dashboards.
    """
    end_time = end or datetime.now(UTC)
    start_time = start or (end_time - timedelta(days=7))  # Default 7 days

    # Safely convert resolution to Postgres INTERVAL syntax
    res_lower = resolution.lower()
    if res_lower.endswith("m"):
        interval_str = f"{res_lower[:-1]} minutes"
    elif res_lower.endswith("h"):
        interval_str = f"{res_lower[:-1]} hours"
    elif res_lower.endswith("d"):
        interval_str = f"{res_lower[:-1]} days"
    else:
        interval_str = "1 hour"

    sql = text("""
        SELECT
            time_bucket(INTERVAL :interval, event_time)        AS time_bucket,
            source_id,
            COUNT(*)                                            AS event_count,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)        AS anomaly_count,
            COALESCE(AVG(anomaly_score), 0.0)                   AS avg_score,
            COALESCE(MAX(anomaly_score), 0.0)                   AS max_score
        FROM anomaly.anomaly_events
        WHERE event_time >= :start AND event_time <= :end
        GROUP BY time_bucket, source_id
        ORDER BY time_bucket DESC, source_id ASC
    """)

    result = await db.execute(sql, {"interval": interval_str, "start": start_time, "end": end_time})
    rows = result.fetchall()

    heatmap_items = []
    for r in rows:
        heatmap_items.append(
            HeatmapItem(
                bucket=r.time_bucket,
                source_id=r.source_id,
                event_count=r.event_count,
                anomaly_count=r.anomaly_count,
                avg_score=r.avg_score,
                max_score=r.max_score,
            )
        )

    return HeatmapOut(
        resolution=resolution,
        start=start_time,
        end=end_time,
        data=heatmap_items,
    )


@router.get(
    "/{id}",
    response_model=AnomalyEventOut,
    summary="Get single anomaly with full context",
)
async def get_anomaly(
    id: UUID,
    current_user: Annotated[TokenData, Depends(check_viewer)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> AnomalyEventOut:
    """
    Fetch a specific anomaly event by its unique UUID.
    """
    sql = text("""
        SELECT event_id, event_time, source_id, feature_vector, anomaly_score, is_anomaly, model_version, processed_at
        FROM anomaly.anomaly_events
        WHERE event_id = :id AND is_anomaly = true
    """)
    result = await db.execute(sql, {"id": id})
    r = result.fetchone()
    if not r:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Anomaly event not found.",
        )

    import json

    fv = r.feature_vector
    if isinstance(fv, str):
        fv = json.loads(fv)

    return AnomalyEventOut(
        event_id=r.event_id,
        event_time=r.event_time,
        source_id=r.source_id,
        feature_vector=list(fv) if isinstance(fv, list) else list(fv.values()),
        anomaly_score=r.anomaly_score,
        is_anomaly=r.is_anomaly,
        model_version=r.model_version,
        processed_at=r.processed_at,
    )
