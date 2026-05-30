"""
Events endpoints — Ingest raw events, query scored anomaly events, apply ground-truth feedback
"""
from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.auth import get_current_user, check_admin, check_viewer
from app.core.database import get_db_session
from app.schemas.auth import TokenData
from app.schemas.events import (
    AnomalyEventOut,
    IngestEventIn,
    IngestEventResponse,
    BatchEventsIn,
    BatchIngestEventResponse,
    LabelEventIn,
    EventLabelOut,
)
from app.services.event_service import EventService

router = APIRouter()


@router.post(
    "",
    response_model=IngestEventResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a single raw event into the pipeline",
)
async def ingest_event(
    payload: IngestEventIn,
    current_user: Annotated[TokenData, Depends(check_viewer)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> IngestEventResponse:
    """
    Publish a single raw event to the Kafka raw-events topic.
    The ML Inference Agent will consume it, score it, and write it to TimescaleDB.
    """
    service = EventService(db)
    event_id = await service.ingest(payload)
    return IngestEventResponse(event_id=str(event_id), status="queued")


@router.post(
    "/batch",
    response_model=BatchIngestEventResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest multiple raw events in one call (limit 1000)",
)
async def ingest_events_batch(
    payload: BatchEventsIn,
    current_user: Annotated[TokenData, Depends(check_viewer)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> BatchIngestEventResponse:
    """
    Ingest up to 1000 events simultaneously.
    Validates features and publishes all events to the Kafka raw-events topic.
    """
    service = EventService(db)
    event_ids = await service.ingest_batch(payload.events)
    return BatchIngestEventResponse(
        event_ids=[str(eid) for eid in event_ids],
        status="queued",
        count=len(event_ids),
    )


@router.get(
    "",
    response_model=list[AnomalyEventOut],
    summary="List recent scored events with filters and pagination",
)
async def list_events(
    current_user: Annotated[TokenData, Depends(check_viewer)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    only_anomalies: bool = Query(default=False),
    source_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None, description="Start date/time in ISO-8601 format"),
    end: datetime | None = Query(default=None, description="End date/time in ISO-8601 format"),
) -> list[AnomalyEventOut]:
    """
    Fetch historical scored events from TimescaleDB ordered by event_time DESC.
    Supports range selection, source filtering, and limiting to anomalies only.
    """
    service = EventService(db)
    return await service.list_events(
        limit=limit,
        offset=offset,
        only_anomalies=only_anomalies,
        source_id=source_id,
        start=start,
        end=end,
    )


@router.get(
    "/{event_id}",
    response_model=AnomalyEventOut,
    summary="Get full event detail including features",
)
async def get_event(
    event_id: UUID,
    current_user: Annotated[TokenData, Depends(check_viewer)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> AnomalyEventOut:
    """
    Retrieve full scored event information by event UUID.
    """
    service = EventService(db)
    event = await service.get_by_id(event_id)
    if not event:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Event not found")
    return event


@router.patch(
    "/{event_id}/label",
    response_model=EventLabelOut,
    status_code=status.HTTP_200_OK,
    summary="Apply analyst ground-truth feedback label to a scored event",
)
async def label_event(
    event_id: UUID,
    payload: LabelEventIn,
    current_user: Annotated[TokenData, Depends(check_admin)],
    db: Annotated[AsyncSession, Depends(get_db_session)],
) -> EventLabelOut:
    """
    Apply analyst labels (TP, FP, TN, FN) to scored events to provide active learning feedback.
    Requires Admin privileges.
    """
    service = EventService(db)
    # Check if event exists
    event = await service.get_by_id(event_id)
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cannot apply feedback. Targeted event not found.",
        )
    return await service.apply_label(event_id, payload)
