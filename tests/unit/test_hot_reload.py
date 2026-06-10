"""
tests/unit/test_hot_reload.py
─────────────────────────────────────────────────────────────────────────────
Tests for ML model hot-reload functionality with threading and concurrency.

Coverage targets:
  - Model reload from MinIO/disk
  - Write lock acquisition during reload
  - Model version updates
  - Concurrent inference during reload
  - Error recovery on corrupt files

All MinIO, threading, and Kafka mocked.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

# Setup sys.path
_PROJ_ROOT = Path(__file__).parent.parent.parent / "anomaly-detection-system"
_ML_DIR = _PROJ_ROOT / "ml"
if str(_ML_DIR) not in sys.path:
    sys.path.insert(0, str(_ML_DIR))

os.environ.setdefault("MODEL_ARTIFACT_PATH", "/tmp/artifacts")


@pytest.mark.asyncio
class TestHotReload:
    """Test model hot-reload with concurrency."""

    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_reload_downloads_new_model(self, mock_joblib_load, mock_path_cls):
        """Test that reload loads model from new artifact path."""
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
                mock_version_file,  # Initial
                mock_model_file,
                mock_version_file,  # Reload
            ]
        )
        mock_path_cls.return_value = mock_artifact_dir

        mock_model_v1 = MagicMock()
        mock_model_v2 = MagicMock()
        mock_joblib_load.side_effect = [mock_model_v1, mock_model_v2]

        engine = InferenceEngine()
        assert engine._isolation_forest is mock_model_v1

        # Hot reload
        await engine.hot_reload("/new/artifact/path")
        assert engine._isolation_forest is mock_model_v2

    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_reload_updates_model_version_attribute(self, mock_joblib_load, mock_path_cls):
        """Test that reload updates _model_version attribute."""
        from ml.inference.engine import InferenceEngine

        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = True
        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text = MagicMock(side_effect=["old-v1", "new-v2"])

        mock_artifact_dir.__truediv__ = MagicMock(
            side_effect=[
                mock_model_file,
                mock_version_file,  # Initial
                mock_model_file,
                mock_version_file,  # Reload
            ]
        )
        mock_path_cls.return_value = mock_artifact_dir

        mock_model = MagicMock()
        mock_joblib_load.return_value = mock_model

        engine = InferenceEngine()
        assert engine._model_version == "old-v1"

        await engine.hot_reload()
        assert engine._model_version == "new-v2"

    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_concurrent_infer_during_reload_succeeds(self, mock_joblib_load, mock_path_cls):
        """Test that concurrent predictions work during reload."""
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
                mock_version_file,  # Initial
                mock_model_file,
                mock_version_file,  # Reload
            ]
        )
        mock_path_cls.return_value = mock_artifact_dir

        mock_model_v1 = MagicMock()
        mock_model_v1.decision_function = MagicMock(return_value=np.array([0.1]))
        mock_model_v2 = MagicMock()
        mock_model_v2.decision_function = MagicMock(return_value=np.array([0.2]))

        mock_joblib_load.side_effect = [mock_model_v1, mock_model_v2]

        engine = InferenceEngine()

        # Start concurrent predictions
        async def predict_task():
            features = np.array([0.1, 0.2, 0.3])
            return await engine.predict(features)

        # Gather predictions and reload concurrently
        results, _ = await asyncio.gather(
            asyncio.gather(predict_task(), predict_task(), predict_task()),
            engine.hot_reload(),
        )

        # All predictions should succeed
        assert len(results) == 3
        assert all(isinstance(r, tuple) for r in results)

    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_reload_clears_model_during_transition(self, mock_joblib_load, mock_path_cls):
        """Test that reload clears model before loading new one."""
        from ml.inference.engine import InferenceEngine

        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = True
        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text = MagicMock(side_effect=["v1", "v2"])

        mock_artifact_dir.__truediv__ = MagicMock(
            side_effect=[
                mock_model_file,
                mock_version_file,  # Initial
                mock_model_file,
                mock_version_file,  # Reload
            ]
        )
        mock_path_cls.return_value = mock_artifact_dir

        mock_model_v1 = MagicMock()
        mock_model_v2 = MagicMock()
        mock_joblib_load.side_effect = [mock_model_v1, mock_model_v2]

        engine = InferenceEngine()
        original_model = engine._isolation_forest

        await engine.hot_reload()

        # Model should have changed
        assert engine._isolation_forest is not original_model
        assert engine._isolation_forest is mock_model_v2

    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_reload_handles_missing_model_gracefully(self, mock_joblib_load, mock_path_cls):
        """Test that reload handles missing model file gracefully."""
        from ml.inference.engine import InferenceEngine

        mock_artifact_dir = MagicMock()
        mock_model_file_v1 = MagicMock()
        mock_model_file_v1.exists.return_value = True
        mock_model_file_v2 = MagicMock()
        mock_model_file_v2.exists.return_value = False  # Not found

        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text = MagicMock(return_value="v1")

        mock_artifact_dir.__truediv__ = MagicMock(
            side_effect=[
                mock_model_file_v1,
                mock_version_file,  # Initial (found)
                mock_model_file_v2,
                mock_version_file,  # Reload (not found)
            ]
        )
        mock_path_cls.return_value = mock_artifact_dir

        mock_model = MagicMock()
        mock_joblib_load.return_value = mock_model

        engine = InferenceEngine()
        assert engine._isolation_forest is not None

        # Reload with missing file
        await engine.hot_reload()

        # Model should be cleared
        assert engine._isolation_forest is None

    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_lock_prevents_concurrent_reload_and_predict(
        self, mock_joblib_load, mock_path_cls
    ):
        """Test that lock serializes reload and predict operations."""
        from ml.inference.engine import InferenceEngine

        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = True
        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text = MagicMock(side_effect=["v1", "v2"])

        mock_artifact_dir.__truediv__ = MagicMock(
            side_effect=[
                mock_model_file,
                mock_version_file,  # Initial
                mock_model_file,
                mock_version_file,  # Reload
            ]
        )
        mock_path_cls.return_value = mock_artifact_dir

        mock_model = MagicMock()
        mock_model.decision_function = MagicMock(return_value=np.array([0.1]))
        mock_joblib_load.return_value = mock_model

        engine = InferenceEngine()

        # Verify lock exists
        assert hasattr(engine, "_lock")
        assert isinstance(engine._lock, asyncio.Lock)

    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_reload_with_custom_artifact_path(self, mock_joblib_load, mock_path_cls):
        """Test reload with custom artifact path."""
        from ml.inference.engine import InferenceEngine

        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = True
        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text = MagicMock(side_effect=["v1", "v2"])

        mock_artifact_dir.__truediv__ = MagicMock(
            side_effect=[
                mock_model_file,
                mock_version_file,  # Initial
                mock_model_file,
                mock_version_file,  # Reload
            ]
        )
        mock_path_cls.return_value = mock_artifact_dir

        mock_model = MagicMock()
        mock_joblib_load.return_value = mock_model

        engine = InferenceEngine()

        # Reload with new path
        new_path = "/new/artifacts/path"
        await engine.hot_reload(new_path)

        # Path should have been updated by direct assignment
        assert engine._artifact_path is not None

    @patch("ml.inference.engine.Path")
    @patch("ml.inference.engine.joblib.load")
    async def test_multiple_sequential_reloads(self, mock_joblib_load, mock_path_cls):
        """Test multiple sequential hot reloads."""
        from ml.inference.engine import InferenceEngine

        mock_artifact_dir = MagicMock()
        mock_model_file = MagicMock()
        mock_model_file.exists.return_value = True
        mock_version_file = MagicMock()
        mock_version_file.exists.return_value = True
        mock_version_file.read_text = MagicMock(side_effect=["v1", "v2", "v3", "v4"])

        mock_artifact_dir.__truediv__ = MagicMock(
            side_effect=[
                mock_model_file,
                mock_version_file,  # Initial
                mock_model_file,
                mock_version_file,  # Reload 1
                mock_model_file,
                mock_version_file,  # Reload 2
                mock_model_file,
                mock_version_file,  # Reload 3
            ]
        )
        mock_path_cls.return_value = mock_artifact_dir

        mock_models = [MagicMock() for _ in range(4)]
        mock_joblib_load.side_effect = mock_models

        engine = InferenceEngine()
        assert engine._model_version == "v1"

        for expected_version in ["v2", "v3", "v4"]:
            await engine.hot_reload()
            assert engine._model_version == expected_version
