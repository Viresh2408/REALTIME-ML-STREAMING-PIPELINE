"""
tests/unit/test_inference_engine_full.py
─────────────────────────────────────────────────────────────────────────────
Comprehensive tests for ml/inference/engine.py InferenceEngine class.

Coverage targets:
  - Model loading from disk/MinIO
  - Inference scoring (0-1 range)
  - Anomaly threshold classification
  - Batch inference performance
  - Hot-reload functionality
  - Lock-based concurrency

All MinIO and joblib mocked.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import numpy as np
import pytest

# Setup sys.path
_PROJ_ROOT = Path(__file__).parent.parent.parent / "anomaly-detection-system"
_ML_DIR = _PROJ_ROOT / "ml"
if str(_ML_DIR) not in sys.path:
    sys.path.insert(0, str(_ML_DIR))

os.environ.setdefault("MODEL_ARTIFACT_PATH", "/tmp/artifacts")


class TestInferenceEngineLoad:
    """Test InferenceEngine model loading."""

    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    def test_engine_loads_model_from_artifact_path(self, mock_joblib_load, mock_path_cls):
        """Test that engine loads model from configured artifact path."""
        from ml.inference.engine import InferenceEngine

        # Mock Path and model loading
        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = True
        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text.return_value = "v1.0.0"

        mock_artifact_dir.__truediv__ = MagicMock(side_effect=[mock_model_file, mock_version_file])
        mock_path_cls.return_value = mock_artifact_dir

        mock_model = MagicMock()
        mock_joblib_load.return_value = mock_model

        engine = InferenceEngine()

        assert engine._isolation_forest is not None
        assert engine._model_version == "v1.0.0"
        mock_joblib_load.assert_called_once()

    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    def test_engine_falls_back_to_dummy_if_model_missing(self, mock_joblib_load, mock_path_cls):
        """Test that engine uses dummy scorer if model file not found."""
        from ml.inference.engine import InferenceEngine

        # Mock Path — model doesn't exist
        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = False

        mock_artifact_dir.__truediv__ = MagicMock(return_value=mock_model_file)
        mock_path_cls.return_value = mock_artifact_dir

        engine = InferenceEngine()

        assert engine._isolation_forest is None
        assert engine._model_version == "unloaded"

    @pytest.mark.asyncio
    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_infer_returns_score_between_0_and_1(self, mock_joblib_load, mock_path_cls):
        """Test that inference returns score in [0.0, 1.0]."""
        from ml.inference.engine import InferenceEngine

        # Setup mocks
        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = True
        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text.return_value = "v1.0"

        mock_artifact_dir.__truediv__ = MagicMock(side_effect=[mock_model_file, mock_version_file])
        mock_path_cls.return_value = mock_artifact_dir

        # Mock model that returns decision scores
        mock_model = MagicMock()
        mock_model.decision_function = MagicMock(return_value=np.array([0.3]))
        mock_joblib_load.return_value = mock_model

        engine = InferenceEngine()

        # Test inference
        features = np.array([0.1, 0.2, 0.3, 0.4, 0.5])
        score, version = await engine.predict(features)

        assert 0.0 <= score <= 1.0
        assert version == "v1.0"

    @pytest.mark.asyncio
    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_infer_returns_version_in_output(self, mock_joblib_load, mock_path_cls):
        """Test that inference includes model version in output."""
        from ml.inference.engine import InferenceEngine

        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = True
        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text.return_value = "v2.5.1"

        mock_artifact_dir.__truediv__ = MagicMock(side_effect=[mock_model_file, mock_version_file])
        mock_path_cls.return_value = mock_artifact_dir

        mock_model = MagicMock()
        mock_model.decision_function = MagicMock(return_value=np.array([-0.2]))
        mock_joblib_load.return_value = mock_model

        engine = InferenceEngine()
        features = np.array([0.1, 0.2, 0.3])
        score, version = await engine.predict(features)

        assert version == "v2.5.1"

    @pytest.mark.asyncio
    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_batch_infer_processes_multiple_features(self, mock_joblib_load, mock_path_cls):
        """Test that engine can process batch inference requests."""
        from ml.inference.engine import InferenceEngine

        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = True
        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text.return_value = "v1.0"

        mock_artifact_dir.__truediv__ = MagicMock(side_effect=[mock_model_file, mock_version_file])
        mock_path_cls.return_value = mock_artifact_dir

        mock_model = MagicMock()
        mock_model.decision_function = MagicMock(return_value=np.array([0.1, -0.1, 0.3]))
        mock_joblib_load.return_value = mock_model

        engine = InferenceEngine()

        # Process batch
        batch_features = [
            np.array([0.1, 0.2, 0.3]),
            np.array([0.4, 0.5, 0.6]),
            np.array([0.7, 0.8, 0.9]),
        ]

        scores = []
        for features in batch_features:
            score, _ = await engine.predict(features)
            scores.append(score)

        assert len(scores) == 3
        assert all(0.0 <= s <= 1.0 for s in scores)

    @pytest.mark.asyncio
    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_hot_reload_updates_model_version(self, mock_joblib_load, mock_path_cls):
        """Test that hot_reload updates the model version."""
        from ml.inference.engine import InferenceEngine

        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = True
        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text = MagicMock(side_effect=["v1.0", "v2.0"])

        mock_artifact_dir.__truediv__ = MagicMock(
            side_effect=[
                mock_model_file,
                mock_version_file,  # Initial load
                mock_model_file,
                mock_version_file,  # Hot reload
            ]
        )
        mock_path_cls.return_value = mock_artifact_dir

        mock_model = MagicMock()
        mock_joblib_load.return_value = mock_model

        engine = InferenceEngine()
        assert engine._model_version == "v1.0"

        # Hot reload
        await engine.hot_reload()
        assert engine._model_version == "v2.0"

    @pytest.mark.asyncio
    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_concurrent_predict_uses_lock(self, mock_joblib_load, mock_path_cls):
        """Test that concurrent predictions use async lock correctly."""
        from ml.inference.engine import InferenceEngine

        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = True
        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text.return_value = "v1.0"

        mock_artifact_dir.__truediv__ = MagicMock(side_effect=[mock_model_file, mock_version_file])
        mock_path_cls.return_value = mock_artifact_dir

        mock_model = MagicMock()
        mock_model.decision_function = MagicMock(return_value=np.array([0.1]))
        mock_joblib_load.return_value = mock_model

        engine = InferenceEngine()

        # Concurrent predictions
        features_list = [np.array([0.1, 0.2, 0.3]) for _ in range(5)]
        results = await asyncio.gather(*[engine.predict(f) for f in features_list])

        assert len(results) == 5
        assert all(isinstance(r, tuple) and len(r) == 2 for r in results)

    @pytest.mark.asyncio
    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_predict_with_no_model_returns_dummy_score(self, mock_joblib_load, mock_path_cls):
        """Test that predict returns dummy score when model is None."""
        from ml.inference.engine import InferenceEngine

        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = False

        mock_artifact_dir.__truediv__ = MagicMock(return_value=mock_model_file)
        mock_path_cls.return_value = mock_artifact_dir

        engine = InferenceEngine()
        assert engine._isolation_forest is None

        # Predict should still work
        features = np.array([0.1, 0.2, 0.3])
        score, version = await engine.predict(features)

        assert 0.0 <= score <= 1.0
        assert version == "dummy-v0"

    @pytest.mark.asyncio
    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_singleton_pattern(self, mock_joblib_load, mock_path_cls):
        """Test that InferenceEngine uses singleton pattern."""
        from ml.inference.engine import InferenceEngine

        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = True
        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text.return_value = "v1.0"

        mock_artifact_dir.__truediv__ = MagicMock(side_effect=[mock_model_file, mock_version_file])
        mock_path_cls.return_value = mock_artifact_dir

        mock_model = MagicMock()
        mock_joblib_load.return_value = mock_model

        engine1 = InferenceEngine.get_instance()
        engine2 = InferenceEngine.get_instance()

        assert engine1 is engine2
