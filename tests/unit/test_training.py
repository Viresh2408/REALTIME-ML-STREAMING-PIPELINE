"""
Unit tests — IsolationForest training pipeline
"""

from __future__ import annotations

import numpy as np
import pytest


@pytest.mark.unit
class TestIsolationForestTraining:
    """Tests for the IsolationForest training script (no MLflow server required)."""

    def _make_synthetic_data(self, n_samples: int = 500, n_features: int = 5) -> np.ndarray:
        rng = np.random.default_rng(42)
        normal = rng.standard_normal((n_samples, n_features))
        anomalies = rng.standard_normal((25, n_features)) * 3
        return np.vstack([normal, anomalies])

    def test_isolation_forest_fits_and_saves(self, tmp_path, monkeypatch) -> None:
        """Training should produce a joblib artifact and a version file."""
        # Monkeypatch MLflow to avoid needing a tracking server
        import mlflow

        monkeypatch.setattr(mlflow, "set_tracking_uri", lambda *a, **kw: None)
        monkeypatch.setattr(mlflow, "set_experiment", lambda *a, **kw: None)

        class _FakeRun:
            class info:
                run_id = "fake-run-id"

        from contextlib import contextmanager

        @contextmanager
        def _fake_start_run(*a, **kw):
            yield _FakeRun()

        monkeypatch.setattr(mlflow, "start_run", _fake_start_run)
        monkeypatch.setattr(mlflow, "log_params", lambda *a, **kw: None)
        monkeypatch.setattr(mlflow, "log_metrics", lambda *a, **kw: None)

        import mlflow.sklearn

        monkeypatch.setattr(mlflow.sklearn, "log_model", lambda *a, **kw: None)

        from ml.training.train_isolation_forest import train_isolation_forest

        data = self._make_synthetic_data()
        metrics = train_isolation_forest(
            data=data,
            contamination=0.05,
            n_estimators=20,
            artifact_path=str(tmp_path),
            model_version="test-v1",
        )

        assert "anomaly_rate" in metrics
        assert 0.0 <= metrics["anomaly_rate"] <= 1.0

        model_file = tmp_path / "isolation_forest.joblib"
        version_file = tmp_path / "model_version.txt"
        assert model_file.exists(), "joblib artifact must be created"
        assert version_file.exists(), "model_version.txt must be created"
        assert version_file.read_text().strip() == "test-v1"

    def test_metrics_are_numeric(self, tmp_path, monkeypatch) -> None:
        import mlflow
        import mlflow.sklearn

        monkeypatch.setattr(mlflow, "set_tracking_uri", lambda *a, **kw: None)
        monkeypatch.setattr(mlflow, "set_experiment", lambda *a, **kw: None)

        from contextlib import contextmanager

        class _FakeRun:
            class info:
                run_id = "fake"

        @contextmanager
        def _fake(*a, **kw):
            yield _FakeRun()

        monkeypatch.setattr(mlflow, "start_run", _fake)
        monkeypatch.setattr(mlflow, "log_params", lambda *a, **kw: None)
        monkeypatch.setattr(mlflow, "log_metrics", lambda *a, **kw: None)
        monkeypatch.setattr(mlflow.sklearn, "log_model", lambda *a, **kw: None)

        from ml.training.train_isolation_forest import train_isolation_forest

        data = self._make_synthetic_data()
        metrics = train_isolation_forest(data=data, artifact_path=str(tmp_path))

        for key, val in metrics.items():
            assert isinstance(val, float), f"Metric {key} should be float, got {type(val)}"
            assert not np.isnan(val), f"Metric {key} must not be NaN"
