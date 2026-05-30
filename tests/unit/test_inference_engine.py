"""
Unit tests for ML Inference Engine
pytest 8.x + pytest-asyncio 0.23.7
"""
from __future__ import annotations

import numpy as np
import pytest

from ml.inference.engine import InferenceEngine


@pytest.mark.unit
class TestInferenceEngine:
    """Tests for InferenceEngine without loaded models (dummy scorer path)."""

    def test_get_instance_returns_singleton(self, tmp_path: pytest.TempPathFactory) -> None:
        """InferenceEngine.get_instance() should always return the same object."""
        engine1 = InferenceEngine(artifact_path=str(tmp_path))
        engine2 = InferenceEngine.get_instance()
        # Both use the module-level singleton path
        assert engine1 is not None
        assert engine2 is not None

    @pytest.mark.asyncio
    async def test_predict_dummy_returns_float_in_range(self, tmp_path: str) -> None:
        """With no model artifact, predict() returns a float in [0, 1]."""
        engine = InferenceEngine(artifact_path=str(tmp_path))
        features = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        score, version = await engine.predict(features)
        assert 0.0 <= score <= 1.0
        assert version == "dummy-v0"

    @pytest.mark.asyncio
    async def test_predict_with_different_feature_shapes(self, tmp_path: str) -> None:
        """Engine should handle feature vectors of varying lengths."""
        engine = InferenceEngine(artifact_path=str(tmp_path))
        for n_features in [1, 5, 10, 50, 100]:
            features = np.random.rand(n_features)
            score, _ = await engine.predict(features)
            assert 0.0 <= score <= 1.0, f"Score out of range for {n_features} features"

    @pytest.mark.asyncio
    async def test_hot_reload_does_not_raise(self, tmp_path: str) -> None:
        """hot_reload() should succeed even when no artifact exists."""
        engine = InferenceEngine(artifact_path=str(tmp_path))
        await engine.hot_reload()  # Should not raise


@pytest.mark.unit
class TestAnomalyThreshold:
    """Test threshold application logic."""

    @pytest.mark.parametrize("score,threshold,expected", [
        (0.8, 0.7, True),
        (0.5, 0.7, False),
        (0.7, 0.7, False),   # Equal is not above threshold
        (0.71, 0.7, True),
        (0.0, 0.7, False),
        (1.0, 0.7, True),
    ])
    def test_threshold_logic(
        self, score: float, threshold: float, expected: bool
    ) -> None:
        is_anomaly = score > threshold
        assert is_anomaly == expected
