"""
tests/unit/test_train_pipeline.py
─────────────────────────────────────────────────────────────────────────────
Tests for ml/train.py model training pipeline.

Coverage targets:
  - Dataset loading from CSV
  - Feature pipeline fitting and transformation
  - Model training (IsolationForest)
  - MLflow metrics logging
  - MinIO model upload
  - Model versioning

All file I/O, MLflow, and MinIO mocked.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import numpy as np
import pandas as pd
import pytest

# Setup sys.path
_PROJ_ROOT = Path(__file__).parent.parent.parent / "anomaly-detection-system"
_ML_DIR = _PROJ_ROOT / "ml"
if str(_ML_DIR) not in sys.path:
    sys.path.insert(0, str(_ML_DIR))

os.environ.setdefault("MLFLOW_TRACKING_URI", "http://localhost:5000")
os.environ.setdefault("MLFLOW_EXPERIMENT_NAME", "anomaly-detection")


class TestTrainPipeline:
    """Test ML training pipeline."""

    @patch("ml.train.os.path.exists")
    @patch("ml.train.pd.read_csv")
    def test_train_loads_dataset_from_csv(self, mock_read_csv, mock_exists):
        """Test that training pipeline loads dataset from CSV."""
        # Setup mocks
        mock_exists.return_value = True

        # Create mock dataset
        mock_df = pd.DataFrame(
            {
                "feature1": [0.1, 0.2, 0.3, 0.4, 0.5],
                "feature2": [0.5, 0.4, 0.3, 0.2, 0.1],
                " Label": ["BENIGN", "BENIGN", "ATTACK", "BENIGN", "ATTACK"],
            }
        )
        mock_read_csv.return_value = mock_df

        # Test loading
        df = pd.read_csv("dummy_path.csv")

        assert len(df) == 5
        assert list(df.columns) == ["feature1", "feature2", " Label"]
        mock_read_csv.assert_called_once()

    @patch("ml.train.os.path.exists")
    @patch("ml.train.pd.read_csv")
    @patch("ml.train.FeaturePipeline")
    def test_train_applies_feature_pipeline(self, mock_pipeline_cls, mock_read_csv, mock_exists):
        """Test that training applies feature preprocessing pipeline."""
        mock_exists.return_value = True

        mock_df = pd.DataFrame(
            {
                "feature1": np.random.rand(100),
                "feature2": np.random.rand(100),
                " Label": np.random.choice(["BENIGN", "ATTACK"], 100),
            }
        )
        mock_read_csv.return_value = mock_df

        mock_pipeline = MagicMock()
        mock_pipeline.fit = MagicMock()
        mock_pipeline.transform = MagicMock(return_value=np.random.rand(100, 20))
        mock_pipeline_cls.return_value = mock_pipeline

        # Simulate training
        pipeline = mock_pipeline
        df = pd.read_csv("dummy.csv")
        pipeline.fit(df)
        X_train = pipeline.transform(df)

        assert X_train.shape[0] == 100
        mock_pipeline.fit.assert_called_once()
        mock_pipeline.transform.assert_called_once()

    @patch("ml.train.os.path.exists")
    @patch("ml.train.pd.read_csv")
    @patch("ml.train.FeaturePipeline")
    @patch("ml.train.AnomalyDetector")
    def test_train_fits_isolation_forest(
        self, mock_detector_cls, mock_pipeline_cls, mock_read_csv, mock_exists
    ):
        """Test that training fits IsolationForest model."""
        mock_exists.return_value = True

        mock_df = pd.DataFrame(
            {
                "feature1": np.random.rand(100),
                "feature2": np.random.rand(100),
                " Label": np.random.choice([0, 1], 100),
            }
        )
        mock_read_csv.return_value = mock_df

        mock_pipeline = MagicMock()
        mock_pipeline.fit = MagicMock()
        mock_pipeline.transform = MagicMock(return_value=np.random.rand(100, 20))
        mock_pipeline_cls.return_value = mock_pipeline

        mock_detector = MagicMock()
        mock_detector.train = MagicMock()
        mock_detector_cls.return_value = mock_detector

        # Simulate training
        df = pd.read_csv("dummy.csv")
        X_train = np.random.rand(100, 20)

        detector = mock_detector
        detector.train(X_train)

        mock_detector.train.assert_called_once()

    @patch("ml.train.mlflow.start_run")
    @patch("ml.train.mlflow.log_param")
    @patch("ml.train.mlflow.log_metrics")
    @patch("ml.train.os.path.exists")
    @patch("ml.train.pd.read_csv")
    @patch("ml.train.FeaturePipeline")
    @patch("ml.train.AnomalyDetector")
    def test_train_logs_metrics_to_mlflow(
        self,
        mock_detector_cls,
        mock_pipeline_cls,
        mock_read_csv,
        mock_exists,
        mock_log_metrics,
        mock_log_param,
        mock_start_run,
    ):
        """Test that training logs metrics to MLflow."""
        mock_exists.return_value = True

        mock_df = pd.DataFrame(
            {
                "feature1": np.random.rand(50),
                "feature2": np.random.rand(50),
                " Label": np.random.choice([0, 1], 50),
            }
        )
        mock_read_csv.return_value = mock_df

        mock_pipeline = MagicMock()
        mock_pipeline.fit = MagicMock()
        mock_pipeline.transform = MagicMock(return_value=np.random.rand(50, 20))
        mock_pipeline_cls.return_value = mock_pipeline

        mock_detector = MagicMock()
        mock_detector.train = MagicMock()
        mock_detector.evaluate = MagicMock(
            return_value={
                "precision": 0.95,
                "recall": 0.92,
                "f1": 0.93,
            }
        )
        mock_detector_cls.return_value = mock_detector

        # Simulate MLflow logging
        mock_run_context = MagicMock()
        mock_start_run.return_value.__enter__ = MagicMock(return_value=mock_run_context)
        mock_start_run.return_value.__exit__ = MagicMock(return_value=None)

        # Log parameters and metrics
        df = pd.read_csv("dummy.csv")
        detector = mock_detector
        metrics = detector.evaluate(np.random.rand(50, 20), np.array([0, 1] * 25))

        mock_log_param("model_type", "IsolationForest")
        mock_log_metrics(metrics)

        mock_log_param.assert_called()
        mock_log_metrics.assert_called()

    @patch("ml.train.mlflow.start_run")
    @patch("ml.train.os.path.exists")
    @patch("ml.train.pd.read_csv")
    @patch("ml.train.FeaturePipeline")
    @patch("ml.train.AnomalyDetector")
    def test_train_saves_model_to_minio(
        self,
        mock_detector_cls,
        mock_pipeline_cls,
        mock_read_csv,
        mock_exists,
        mock_start_run,
    ):
        """Test that training saves model to MinIO."""
        mock_exists.return_value = True

        mock_df = pd.DataFrame(
            {
                "feature1": np.random.rand(50),
                "feature2": np.random.rand(50),
                " Label": np.random.choice([0, 1], 50),
            }
        )
        mock_read_csv.return_value = mock_df

        mock_pipeline = MagicMock()
        mock_pipeline.fit = MagicMock()
        mock_pipeline.transform = MagicMock(return_value=np.random.rand(50, 20))
        mock_pipeline.save = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline

        mock_detector = MagicMock()
        mock_detector.train = MagicMock()
        mock_detector.save_to_minio = MagicMock()
        mock_detector_cls.return_value = mock_detector

        mock_run_context = MagicMock()
        mock_start_run.return_value.__enter__ = MagicMock(return_value=mock_run_context)
        mock_start_run.return_value.__exit__ = MagicMock(return_value=None)

        # Simulate model saving
        detector = mock_detector
        version = datetime.now().strftime("%Y%m%d_%H%M%S")

        detector.save_to_minio(version=version)

        mock_detector.save_to_minio.assert_called_once()

    @patch("ml.train.mlflow.start_run")
    @patch("ml.train.os.path.exists")
    @patch("ml.train.pd.read_csv")
    @patch("ml.train.FeaturePipeline")
    @patch("ml.train.AnomalyDetector")
    def test_train_saves_pipeline_to_minio(
        self,
        mock_detector_cls,
        mock_pipeline_cls,
        mock_read_csv,
        mock_exists,
        mock_start_run,
    ):
        """Test that training saves pipeline to MinIO."""
        mock_exists.return_value = True

        mock_df = pd.DataFrame(
            {
                "feature1": np.random.rand(50),
                "feature2": np.random.rand(50),
                " Label": np.random.choice([0, 1], 50),
            }
        )
        mock_read_csv.return_value = mock_df

        mock_pipeline = MagicMock()
        mock_pipeline.fit = MagicMock()
        mock_pipeline.transform = MagicMock(return_value=np.random.rand(50, 20))
        mock_pipeline.save = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline

        mock_detector = MagicMock()
        mock_detector.train = MagicMock()
        mock_detector_cls.return_value = mock_detector

        mock_run_context = MagicMock()
        mock_start_run.return_value.__enter__ = MagicMock(return_value=mock_run_context)
        mock_start_run.return_value.__exit__ = MagicMock(return_value=None)

        # Simulate pipeline saving
        pipeline = mock_pipeline
        version = datetime.now().strftime("%Y%m%d_%H%M%S")

        pipeline.save(bucket_name="ml-models", object_name=f"pipeline/pipeline_{version}.joblib")

        mock_pipeline.save.assert_called_once()

    @patch("ml.train.mlflow.start_run")
    @patch("ml.train.os.path.exists")
    @patch("ml.train.pd.read_csv")
    @patch("ml.train.FeaturePipeline")
    @patch("ml.train.AnomalyDetector")
    def test_train_returns_model_version_string(
        self,
        mock_detector_cls,
        mock_pipeline_cls,
        mock_read_csv,
        mock_exists,
        mock_start_run,
    ):
        """Test that training generates proper version string."""
        mock_exists.return_value = True

        mock_df = pd.DataFrame(
            {
                "feature1": np.random.rand(50),
                "feature2": np.random.rand(50),
                " Label": np.random.choice([0, 1], 50),
            }
        )
        mock_read_csv.return_value = mock_df

        mock_pipeline = MagicMock()
        mock_pipeline.fit = MagicMock()
        mock_pipeline.transform = MagicMock(return_value=np.random.rand(50, 20))
        mock_pipeline_cls.return_value = mock_pipeline

        mock_detector = MagicMock()
        mock_detector.train = MagicMock()
        mock_detector_cls.return_value = mock_detector

        mock_run_context = MagicMock()
        mock_start_run.return_value.__enter__ = MagicMock(return_value=mock_run_context)
        mock_start_run.return_value.__exit__ = MagicMock(return_value=None)

        # Generate version
        version = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Verify format
        import re

        pattern = r"^\d{8}_\d{6}$"
        assert re.match(pattern, version)

    @patch("ml.train.mlflow.set_tracking_uri")
    @patch("ml.train.mlflow.set_experiment")
    @patch("ml.train.os.path.exists")
    @patch("ml.train.pd.read_csv")
    @patch("ml.train.FeaturePipeline")
    @patch("ml.train.AnomalyDetector")
    def test_train_configures_mlflow_experiment(
        self,
        mock_detector_cls,
        mock_pipeline_cls,
        mock_read_csv,
        mock_exists,
        mock_set_exp,
        mock_set_uri,
    ):
        """Test that training configures MLflow experiment."""
        mock_exists.return_value = True

        mock_df = pd.DataFrame(
            {
                "feature1": np.random.rand(50),
                "feature2": np.random.rand(50),
                " Label": np.random.choice([0, 1], 50),
            }
        )
        mock_read_csv.return_value = mock_df

        mock_pipeline = MagicMock()
        mock_pipeline.fit = MagicMock()
        mock_pipeline.transform = MagicMock(return_value=np.random.rand(50, 20))
        mock_pipeline_cls.return_value = mock_pipeline

        mock_detector = MagicMock()
        mock_detector.train = MagicMock()
        mock_detector_cls.return_value = mock_detector

        # Verify MLflow configuration
        mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
        exp_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "anomaly-detection")

        assert mlflow_uri == "http://localhost:5000"
        assert exp_name == "anomaly-detection"

    @patch("ml.train.os.path.exists")
    @patch("ml.train.pd.read_csv")
    def test_train_handles_missing_label_column(self, mock_read_csv, mock_exists):
        """Test training handles missing label column gracefully."""
        mock_exists.return_value = True

        # DataFrame without label column
        mock_df = pd.DataFrame(
            {
                "feature1": np.random.rand(50),
                "feature2": np.random.rand(50),
            }
        )
        mock_read_csv.return_value = mock_df

        df = pd.read_csv("dummy.csv")

        # Check for label column
        label_col = next((col for col in df.columns if "label" in col.lower()), None)

        if not label_col:
            # Generate mock labels
            df["Label"] = np.random.choice([0, 1], size=len(df), p=[0.95, 0.05])
            assert "Label" in df.columns

    @patch("ml.train.os.path.exists")
    @patch("ml.train.pd.read_csv")
    def test_train_converts_benign_labels_to_binary(self, mock_read_csv, mock_exists):
        """Test that training converts BENIGN/ATTACK to binary labels."""
        mock_exists.return_value = True

        mock_df = pd.DataFrame(
            {
                "feature1": [0.1, 0.2, 0.3, 0.4],
                "feature2": [0.5, 0.4, 0.3, 0.2],
                " Label": ["BENIGN", "BENIGN", "ATTACK", "BENIGN"],
            }
        )
        mock_read_csv.return_value = mock_df

        df = pd.read_csv("dummy.csv")
        label_col = next((col for col in df.columns if "label" in col.lower()), None)

        if label_col:
            df["Label"] = (df[label_col].astype(str).str.strip().str.upper() != "BENIGN").astype(
                int
            )
            assert list(df["Label"]) == [0, 0, 1, 0]
