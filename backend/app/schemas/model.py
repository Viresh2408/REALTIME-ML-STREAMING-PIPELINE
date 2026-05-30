"""
Model management schemas — Pydantic v2
"""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from uuid import UUID
from pydantic import BaseModel, Field


class ModelStatusOut(BaseModel):
    """Details of the currently running ML model."""

    model_version: str = Field(..., description="Active model identifier")
    load_time: datetime = Field(..., description="When the current model was loaded")
    avg_inference_latency_ms: float = Field(..., description="Average inference latency in milliseconds")
    total_inferences: int = Field(..., description="Cumulative number of predictions made")


class ModelVersionOut(BaseModel):
    """Model version log entry."""

    model_version: str = Field(..., description="Unique model identifier")
    accuracy: Optional[float] = Field(default=None, description="Accuracy rating")
    f1_score: Optional[float] = Field(default=None, description="F1 Score")
    registered_at: datetime = Field(..., description="Registry registration timestamp")
    active: bool = Field(..., description="Is this currently the running model")


class ModelRetrainIn(BaseModel):
    """Request immediate retraining pipeline execution."""

    reason: str = Field(..., description="Justification for running retraining")
    force: bool = Field(default=False, description="Bypass training validation score safety checks")


class ModelRetrainJobOut(BaseModel):
    """Retraining execution job state details."""

    job_id: UUID = Field(..., description="Assigned retraining job ID")
    status: str = Field(..., description="Job state: PENDING, RUNNING, COMPLETED, FAILED")
    reason: str = Field(..., description="Retrain motivation reason")
    force: bool = Field(..., description="Force parameter used")
    created_at: datetime = Field(..., description="Triggered timestamp")
    completed_at: Optional[datetime] = Field(default=None, description="Completion timestamp")
    error: Optional[str] = Field(default=None, description="Errors logged during processing")

    model_config = {"from_attributes": True}


class ModelRollbackIn(BaseModel):
    """Request active model rollback to a prior version."""

    version: str = Field(..., description="Target model version identifier to rollback to")
    reason: str = Field(..., description="Rollback request justification")


class ModelMetricsOut(BaseModel):
    """Evaluation metrics for a model version."""

    model_version: str = Field(..., description="Target model identifier")
    precision: float = Field(..., description="Model Precision score")
    recall: float = Field(..., description="Model Recall score")
    f1_score: float = Field(..., description="Model F1 score")
    auc_roc: float = Field(..., description="AUC ROC metric score")
    evaluation_date: datetime = Field(..., description="When evaluation tests were run")
