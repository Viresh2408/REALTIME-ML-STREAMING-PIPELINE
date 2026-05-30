"""
Unit tests — Online Learner (River 0.21)
"""

from __future__ import annotations

import pytest


@pytest.mark.unit
class TestOnlineAnomalyDetector:
    """Tests for the River-based incremental anomaly detector."""

    def test_import_and_instantiate(self, tmp_path) -> None:
        from ml.training.online_learner import OnlineAnomalyDetector

        detector = OnlineAnomalyDetector(artifact_path=str(tmp_path))
        assert detector.n_scored == 0

    def test_score_returns_float_in_range(self, tmp_path) -> None:
        from ml.training.online_learner import OnlineAnomalyDetector

        detector = OnlineAnomalyDetector(artifact_path=str(tmp_path))
        features = {"f0": 1.0, "f1": -0.5, "f2": 2.3}
        score = detector.score_one(features)
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0

    def test_process_event_increments_counter(self, tmp_path) -> None:
        from ml.training.online_learner import OnlineAnomalyDetector

        detector = OnlineAnomalyDetector(artifact_path=str(tmp_path))
        for _ in range(5):
            detector.process_event([0.1, 0.2, 0.3])
        assert detector.n_scored == 5

    def test_save_and_load_roundtrip(self, tmp_path) -> None:
        from ml.training.online_learner import OnlineAnomalyDetector

        d1 = OnlineAnomalyDetector(artifact_path=str(tmp_path))
        for _ in range(10):
            d1.process_event([0.5, -0.3, 1.2])
        d1.save()

        d2 = OnlineAnomalyDetector(artifact_path=str(tmp_path))
        loaded = d2.load()
        assert loaded is True

    def test_normal_events_score_lower_than_anomalies(self, tmp_path) -> None:
        """After warm-up, anomalous events should score higher on average."""
        import random

        from ml.training.online_learner import OnlineAnomalyDetector

        detector = OnlineAnomalyDetector(artifact_path=str(tmp_path), window_size=100)
        # Warm up on normal data
        for _ in range(200):
            detector.process_event([random.gauss(0, 0.5) for _ in range(5)])

        normal_scores = [
            detector.process_event([random.gauss(0, 0.5) for _ in range(5)]) for _ in range(30)
        ]
        anomaly_scores = [
            detector.process_event([random.gauss(10, 1.0) for _ in range(5)]) for _ in range(30)
        ]

        avg_normal = sum(normal_scores) / len(normal_scores)
        avg_anomaly = sum(anomaly_scores) / len(anomaly_scores)
        assert avg_anomaly > avg_normal, (
            f"Expected anomaly avg ({avg_anomaly:.4f}) > normal avg ({avg_normal:.4f})"
        )


@pytest.mark.unit
class TestDriftDetector:
    """Tests for the Page-Hinkley concept drift detector."""

    def test_no_drift_on_stable_stream(self, tmp_path) -> None:
        from ml.training.online_learner import DriftDetector

        detector = DriftDetector(min_instances=50)
        # Feed stable scores — no drift expected
        drifts = [detector.update(0.3 + 0.01 * (i % 5)) for i in range(200)]
        assert not any(drifts), "Stable stream should not trigger drift"

    def test_drift_detected_on_shift(self, tmp_path) -> None:
        from ml.training.online_learner import DriftDetector

        detector = DriftDetector(min_instances=50, threshold=5.0)
        # Warm up with low scores
        for _ in range(100):
            detector.update(0.1)
        # Sudden shift to high scores — drift should be detected
        drift_flags = [detector.update(0.95) for _ in range(100)]
        assert any(drift_flags), "Sharp distribution shift should trigger drift detection"
