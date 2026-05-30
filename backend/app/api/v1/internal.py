"""
Internal endpoints — service-to-service only (not for external clients)
Covers: model hot-reload signaling, health aggregation
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from ml.inference.engine import InferenceEngine

router = APIRouter()


class ModelReloadRequest(BaseModel):
    model_version: str


class ModelReloadResponse(BaseModel):
    status: str
    model_version: str


@router.post(
    "/model-reload",
    response_model=ModelReloadResponse,
    status_code=status.HTTP_200_OK,
    summary="Signal ML inference engine to hot-reload model artifact",
)
async def model_reload(payload: ModelReloadRequest) -> ModelReloadResponse:
    """
    Called by the Model Retraining Agent after a new artifact is available.
    Triggers InferenceEngine.hot_reload() without restarting the container.
    """
    try:
        engine = InferenceEngine.get_instance()
        await engine.hot_reload()
        return ModelReloadResponse(status="reloaded", model_version=payload.model_version)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Model reload failed: {exc}",
        )
