"""
API v1 Router — aggregates all endpoint modules
"""
from __future__ import annotations

from fastapi import APIRouter

from app.api.v1 import alerts, anomalies, auth, events, model, websocket

api_router = APIRouter()

api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(events.router, prefix="/events", tags=["Events"])
api_router.include_router(anomalies.router, prefix="/anomalies", tags=["Anomalies"])
api_router.include_router(alerts.router, prefix="/alerts", tags=["Alerts"])
api_router.include_router(model.router, prefix="/model", tags=["Model Management"])
api_router.include_router(websocket.router, prefix="/ws", tags=["WebSocket"])
