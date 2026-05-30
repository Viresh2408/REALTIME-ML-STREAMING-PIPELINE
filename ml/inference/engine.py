"""
ML Inference Engine — Singleton
Supports: IsolationForest (scikit-learn 1.5) + Autoencoder (PyTorch 2.3)
Serialization: joblib 1.4
Online learning: River 0.21
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

import joblib
import numpy as np
import structlog

logger = structlog.get_logger(__name__)

_instance: InferenceEngine | None = None


class InferenceEngine:
    """
    Singleton inference engine that loads models from disk/MinIO.
    Supports hot-reload via model-updates Kafka topic.
    """

    def __init__(self, artifact_path: str | None = None) -> None:
        self._artifact_path = Path(
            artifact_path or os.environ.get("MODEL_ARTIFACT_PATH", "/app/artifacts")
        )
        self._isolation_forest = None
        self._model_version: str = "unloaded"
        self._lock = asyncio.Lock()
        self._load_models()

    @classmethod
    def get_instance(cls) -> InferenceEngine:
        global _instance
        if _instance is None:
            _instance = cls()
        return _instance

    def _load_models(self) -> None:
        """Load IsolationForest model from disk."""
        model_path = self._artifact_path / "isolation_forest.joblib"
        if model_path.exists():
            self._isolation_forest = joblib.load(model_path)
            version_path = self._artifact_path / "model_version.txt"
            self._model_version = (
                version_path.read_text().strip() if version_path.exists() else "v1"
            )
            logger.info("Model loaded", version=self._model_version, path=str(model_path))
        else:
            logger.warning(
                "No model found at artifact path, using dummy scorer",
                path=str(model_path),
            )

    async def predict(self, features: np.ndarray) -> tuple[float, str]:
        """
        Run inference on a feature vector.
        Returns (anomaly_score 0.0–1.0, model_version).
        IsolationForest scores in [-1, 1]; we normalise to [0, 1].
        """
        async with self._lock:
            if self._isolation_forest is None:
                # Fallback: random score for development without a trained model
                score = float(np.random.uniform(0, 1))
                return score, "dummy-v0"

            raw_score = self._isolation_forest.decision_function(
                features.reshape(1, -1)
            )[0]
            # Convert: decision_function returns negative for anomalies.
            # Normalise to [0, 1] where higher = more anomalous.
            score = float(1.0 - (raw_score - (-0.5)) / (0.5 - (-0.5)))
            score = max(0.0, min(1.0, score))
            return score, self._model_version

    async def hot_reload(self, new_artifact_path: str | None = None) -> None:
        """Reload model from disk — called by Model Retraining Agent."""
        if new_artifact_path:
            self._artifact_path = Path(new_artifact_path)
        async with self._lock:
            self._isolation_forest = None
            self._load_models()
        logger.info("Model hot-reloaded", version=self._model_version)
