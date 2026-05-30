"""
Online / Incremental Learning with River 0.21
Architecture: techstack.docx §3 — Update model in real-time without full retraining
Used as a lightweight complement to IsolationForest for concept-drift adaptation.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import joblib
import structlog
from river import anomaly, preprocessing, compose

logger = structlog.get_logger(__name__)


class OnlineAnomalyDetector:
    """
    River-based online anomaly detector using Half-Space Trees (HST).
    Incrementally updates on every scored event — no batch retraining needed.

    HST is River's canonical unsupervised streaming anomaly algorithm:
    - O(1) update and scoring time
    - Handles concept drift via window-based leaf statistics
    - Compatible with high-velocity Kafka event streams

    Serialized with joblib for persistence and hot-swap.
    """

    def __init__(
        self,
        n_trees: int = 25,
        height: int = 8,
        window_size: int = 250,
        artifact_path: str | None = None,
    ) -> None:
        self._artifact_path = Path(
            artifact_path or os.environ.get("MODEL_ARTIFACT_PATH", "/app/artifacts")
        )
        self._model = compose.Pipeline(
            preprocessing.StandardScaler(),
            anomaly.HalfSpaceTrees(
                n_trees=n_trees,
                height=height,
                window_size=window_size,
            ),
        )
        self._n_scored = 0

    def learn_one(self, features: dict[str, float]) -> None:
        """Incrementally update the model with one event's features."""
        self._model.learn_one(features)
        self._n_scored += 1

    def score_one(self, features: dict[str, float]) -> float:
        """
        Return an anomaly score in [0, 1] for a single feature dict.
        River's HalfSpaceTrees.score_one() returns values in [0, 1]
        where higher = more anomalous.
        """
        return float(self._model.score_one(features))

    def process_event(self, feature_vector: list[float]) -> float:
        """
        Convert a feature list to a dict, score it, then learn from it.
        This is the main hot-path called per Kafka message.
        Returns the anomaly score before the model update.
        """
        features = {f"f{i}": v for i, v in enumerate(feature_vector)}
        score = self.score_one(features)
        self.learn_one(features)
        return score

    def save(self) -> Path:
        """Persist model state to disk with joblib."""
        out = self._artifact_path / "river_online_model.joblib"
        self._artifact_path.mkdir(parents=True, exist_ok=True)
        joblib.dump(self._model, out)
        logger.info("River model saved", path=str(out), n_scored=self._n_scored)
        return out

    def load(self) -> bool:
        """Load model state from disk. Returns True if successful."""
        model_path = self._artifact_path / "river_online_model.joblib"
        if model_path.exists():
            self._model = joblib.load(model_path)
            logger.info("River model loaded", path=str(model_path))
            return True
        logger.warning("River model not found, starting fresh", path=str(model_path))
        return False

    @property
    def n_scored(self) -> int:
        return self._n_scored


class DriftDetector:
    """
    Lightweight Page-Hinkley drift detector using River.
    Triggers a retraining signal when the anomaly score distribution shifts.
    """

    def __init__(self, min_instances: int = 500, delta: float = 0.005, threshold: float = 50.0) -> None:
        from river import drift
        self._detector = drift.PageHinkley(
            min_instances=min_instances,
            delta=delta,
            threshold=threshold,
        )
        self._drift_detected = False

    def update(self, score: float) -> bool:
        """
        Feed the latest anomaly score. Returns True if drift is detected.
        Drift signal can be used to trigger early model retraining.
        """
        self._detector.update(score)
        if self._detector.drift_detected:
            if not self._drift_detected:
                logger.warning(
                    "Concept drift detected — triggering retraining signal",
                    score=score,
                )
            self._drift_detected = True
            return True
        self._drift_detected = False
        return False
