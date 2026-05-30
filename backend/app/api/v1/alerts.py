"""
Alerts management APIs
Enables alert triage lifecycle: querying, acknowledging, resolving, and setting silencing rules.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.auth import check_analyst, check_viewer
from app.core.database import get_db_session
from app.schemas.alerts import (
    AlertAcknowledgeIn,
    AlertOut,
    AlertResolveIn,
    AlertSeverity,
    AlertSilenceIn,
    AlertSilenceOut,
    AlertStatus,
)
from app.schemas.auth import TokenData
from app.schemas.events import AnomalyEventOut

try:
    from database.models import Alert, AlertSilence, AnomalyEvent
except ModuleNotFoundError:
    from backend.database.models import Alert, AlertSilence, AnomalyEvent

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.get(
    "",
    response_model=list[AlertOut],
    summary="List active and resolved alerts",
)
async def list_alerts(
    current_user: Annotated[TokenData, Depends(check_viewer)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    status: AlertStatus | None = Query(default=None, description="ACTIVE, ACKNOWLEDGED, RESOLVED"),
    severity: AlertSeverity | None = Query(default=None, description="LOW, MEDIUM, HIGH, CRITICAL"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[AlertOut]:
    """
    Retrieve list of active or historic alerts from PostgreSQL.
    Supports filtering by severity level and status flags.
    """
    stmt = select(Alert).order_by(Alert.created_at.desc()).limit(limit).offset(offset)

    if status:
        stmt = stmt.where(Alert.status == status.value)
    if severity:
        stmt = stmt.where(Alert.severity == severity.value)

    result = await db.execute(stmt)
    rows = result.scalars().all()

    return [AlertOut.model_validate(r) for r in rows]


class AlertWithEventsOut(BaseModel):
    alert: AlertOut
    linked_events: list[AnomalyEventOut]


@router.get(
    "/{alert_id}",
    response_model=AlertWithEventsOut,
    summary="Get single alert with linked anomaly events",
)
async def get_alert(
    alert_id: UUID,
    current_user: Annotated[TokenData, Depends(check_viewer)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> Any:
    """
    Retrieve single alert details by alert UUID, along with the most recent
    linked anomaly events that match the alert's source ID.
    """
    stmt = select(Alert).where(Alert.alert_id == alert_id)
    result = await db.execute(stmt)
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert not found",
        )

    # Fetch recent anomaly events for this source_id
    event_stmt = (
        select(AnomalyEvent)
        .where(AnomalyEvent.source_id == alert.source_id)
        .where(AnomalyEvent.is_anomaly.is_(True))
        .order_by(AnomalyEvent.event_time.desc())
        .limit(10)
    )
    event_result = await db.execute(event_stmt)
    events = event_result.scalars().all()

    # Helper function to match EventService._to_schema
    import json

    parsed_events = []
    for row in events:
        fv = row.feature_vector
        if isinstance(fv, str):
            fv = json.loads(fv)
        parsed_events.append(
            AnomalyEventOut(
                event_id=row.event_id,
                event_time=row.event_time,
                source_id=row.source_id,
                feature_vector=list(fv) if isinstance(fv, list) else list(fv.values()),
                anomaly_score=row.anomaly_score,
                is_anomaly=row.is_anomaly,
                model_version=row.model_version,
                processed_at=row.processed_at,
            )
        )

    return {
        "alert": AlertOut.model_validate(alert),
        "linked_events": parsed_events,
    }


from pydantic import BaseModel


@router.patch(
    "/{alert_id}/acknowledge",
    response_model=AlertOut,
    summary="Acknowledge alert",
)
async def acknowledge_alert(
    alert_id: UUID,
    payload: AlertAcknowledgeIn,
    current_user: Annotated[TokenData, Depends(check_analyst)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> AlertOut:
    """
    Mark an active alert as Acknowledged. Assigns an analyst and sets timestamp.
    """
    stmt = select(Alert).where(Alert.alert_id == alert_id)
    result = await db.execute(stmt)
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert not found",
        )

    if alert.status == "RESOLVED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot acknowledge a resolved alert.",
        )

    alert.status = "ACKNOWLEDGED"
    alert.analyst_id = payload.analyst_id
    alert.note = payload.note
    alert.acknowledged_at = datetime.now(tz=UTC)

    await db.flush()
    logger.info("Alert acknowledged", alert_id=str(alert_id), analyst=payload.analyst_id)
    return AlertOut.model_validate(alert)


@router.patch(
    "/{alert_id}/resolve",
    response_model=AlertOut,
    summary="Mark alert resolved",
)
async def resolve_alert(
    alert_id: UUID,
    payload: AlertResolveIn,
    current_user: Annotated[TokenData, Depends(check_analyst)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> AlertOut:
    """
    Mark alert as Resolved. Requires providing resolution reason and analyst ID.
    """
    stmt = select(Alert).where(Alert.alert_id == alert_id)
    result = await db.execute(stmt)
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Alert not found",
        )

    alert.status = "RESOLVED"
    alert.analyst_id = payload.analyst_id
    alert.resolution = payload.resolution
    alert.resolved_at = datetime.now(tz=UTC)

    await db.flush()
    logger.info("Alert resolved", alert_id=str(alert_id), analyst=payload.analyst_id)
    return AlertOut.model_validate(alert)


@router.post(
    "/silence",
    response_model=AlertSilenceOut,
    status_code=status.HTTP_201_CREATED,
    summary="Silence alerts for a source/time window",
)
async def silence_alerts(
    payload: AlertSilenceIn,
    current_user: Annotated[TokenData, Depends(check_analyst)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> AlertSilenceOut:
    """
    Register a silencing configuration to suppress alert notifications for a specified
    duration in minutes (e.g. during maintenance windows).
    """
    created_at = datetime.now(tz=UTC)
    expires_at = created_at + timedelta(minutes=payload.duration_minutes)

    silence_record = AlertSilence(
        source_id=payload.source_id,
        duration_minutes=payload.duration_minutes,
        reason=payload.reason,
        created_at=created_at,
        expires_at=expires_at,
    )

    db.add(silence_record)
    await db.flush()

    logger.info(
        "Alert silencing scheduled",
        silence_id=str(silence_record.silence_id),
        source_id=payload.source_id,
        duration=payload.duration_minutes,
    )
    return AlertSilenceOut.model_validate(silence_record)
