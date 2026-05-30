"""
Statistics endpoint — aggregated anomaly metrics for Grafana panels
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.auth import get_current_user
from app.core.database import get_db_session
from app.schemas.auth import TokenData
from app.schemas.stats import AnomalyStatsOut

router = APIRouter()


@router.get("", response_model=AnomalyStatsOut, summary="Aggregate anomaly statistics")
async def get_stats(
    current_user: Annotated[TokenData, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    window_minutes: int = Query(default=60, ge=1, le=10080),
) -> AnomalyStatsOut:
    """
    Return aggregated anomaly statistics for the specified time window.
    Backed by the anomaly_rate_hourly continuous aggregate in TimescaleDB.
    """
    sql = text("""
        SELECT
            COUNT(*)                                            AS total_events,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)        AS anomaly_count,
            AVG(anomaly_score)                                  AS avg_score,
            MAX(anomaly_score)                                  AS max_score,
            ROUND(
                100.0 * SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
                2
            )                                                   AS anomaly_rate_pct
        FROM anomaly.anomaly_events
        WHERE event_time > NOW() - INTERVAL ':window_minutes minutes'
    """)

    result = await db.execute(sql, {"window_minutes": window_minutes})
    row = result.mappings().one()

    return AnomalyStatsOut(
        window_minutes=window_minutes,
        total_events=int(row["total_events"] or 0),
        anomaly_count=int(row["anomaly_count"] or 0),
        anomaly_rate_pct=float(row["anomaly_rate_pct"] or 0.0),
        avg_score=float(row["avg_score"] or 0.0),
        max_score=float(row["max_score"] or 0.0),
    )
