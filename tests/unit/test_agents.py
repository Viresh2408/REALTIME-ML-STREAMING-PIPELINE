"""
Unit tests — Producer Agent and event generation
"""
from __future__ import annotations

import math
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from agents.producer.agent import generate_synthetic_event


@pytest.mark.unit
class TestSyntheticEventGeneration:
    """Tests for the synthetic event generator (no Kafka needed)."""

    def test_event_has_required_keys(self) -> None:
        event = generate_synthetic_event()
        assert "event_id" in event
        assert "source_id" in event
        assert "feature_vector" in event
        assert "timestamp" in event

    def test_event_id_is_valid_uuid(self) -> None:
        import uuid
        event = generate_synthetic_event()
        uuid.UUID(event["event_id"])  # raises ValueError if invalid

    def test_feature_vector_default_length(self) -> None:
        event = generate_synthetic_event(n_features=10)
        assert len(event["feature_vector"]) == 10

    @pytest.mark.parametrize("n_features", [1, 5, 20, 50])
    def test_feature_vector_custom_length(self, n_features: int) -> None:
        event = generate_synthetic_event(n_features=n_features)
        assert len(event["feature_vector"]) == n_features

    def test_feature_vector_has_no_nan(self) -> None:
        for _ in range(20):
            event = generate_synthetic_event()
            for val in event["feature_vector"]:
                assert not math.isnan(val), "Feature vector must not contain NaN"
                assert not math.isinf(val), "Feature vector must not contain Inf"

    def test_custom_source_id(self) -> None:
        event = generate_synthetic_event(source_id="my-sensor")
        assert event["source_id"] == "my-sensor"

    def test_random_source_id_generated_when_none(self) -> None:
        event = generate_synthetic_event(source_id=None)
        assert event["source_id"].startswith("source-")

    def test_anomaly_probability_zero_never_injects(self) -> None:
        for _ in range(50):
            event = generate_synthetic_event(anomaly_probability=0.0)
            assert event["injected_anomaly"] is False

    def test_anomaly_probability_one_always_injects(self) -> None:
        for _ in range(20):
            event = generate_synthetic_event(anomaly_probability=1.0)
            assert event["injected_anomaly"] is True

    def test_high_anomaly_features_have_larger_values(self) -> None:
        """Injected anomalies should have feature values sampled from loc=5, not loc=0."""
        anomaly_events = [
            generate_synthetic_event(anomaly_probability=1.0) for _ in range(100)
        ]
        mean_abs = sum(
            sum(abs(v) for v in e["feature_vector"]) / len(e["feature_vector"])
            for e in anomaly_events
        ) / len(anomaly_events)
        # Normal events average near 0; anomaly events average near 5
        assert mean_abs > 2.0, f"Expected mean > 2.0 for anomaly events, got {mean_abs:.2f}"


@pytest.mark.unit
class TestAlertSeverityClassification:
    """Tests for the severity tiering in the Alert Agent."""

    def test_severity_classification(self) -> None:
        from agents.alert.agent import _classify_severity
        assert _classify_severity(0.99) == "critical"
        assert _classify_severity(0.95) == "critical"
        assert _classify_severity(0.90) == "high"
        assert _classify_severity(0.85) == "high"
        assert _classify_severity(0.80) == "medium"
        assert _classify_severity(0.75) == "medium"
        assert _classify_severity(0.70) == "low"
        assert _classify_severity(0.50) == "low"
        assert _classify_severity(0.0) == "low"
