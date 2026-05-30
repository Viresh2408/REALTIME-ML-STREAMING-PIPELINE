"""
Unit tests — Pydantic v2 request/response schemas
pytest 8.x
"""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from backend.app.schemas.auth import Token, TokenData
from backend.app.schemas.events import AnomalyEventOut, IngestEventIn
from backend.app.schemas.stats import AnomalyStatsOut


@pytest.mark.unit
class TestIngestEventIn:
    """Validation tests for the IngestEventIn request schema."""

    def test_valid_payload_passes(self) -> None:
        payload = IngestEventIn(
            source_id="sensor-42",
            feature_vector=[0.1, -0.2, 1.5, 0.7],
        )
        assert payload.source_id == "sensor-42"
        assert len(payload.feature_vector) == 4

    def test_empty_source_id_rejected(self) -> None:
        with pytest.raises(Exception):  # pydantic ValidationError
            IngestEventIn(source_id="", feature_vector=[1.0])

    def test_empty_feature_vector_rejected(self) -> None:
        with pytest.raises(Exception):
            IngestEventIn(source_id="src", feature_vector=[])

    def test_nan_in_feature_vector_rejected(self) -> None:
        with pytest.raises(Exception):
            IngestEventIn(source_id="src", feature_vector=[float("nan"), 1.0])

    def test_inf_in_feature_vector_rejected(self) -> None:
        with pytest.raises(Exception):
            IngestEventIn(source_id="src", feature_vector=[float("inf"), 1.0])

    def test_optional_timestamp_defaults_to_none(self) -> None:
        payload = IngestEventIn(source_id="src", feature_vector=[1.0])
        assert payload.timestamp is None

    def test_optional_metadata_defaults_to_none(self) -> None:
        payload = IngestEventIn(source_id="src", feature_vector=[1.0])
        assert payload.metadata is None

    def test_metadata_accepts_arbitrary_keys(self) -> None:
        payload = IngestEventIn(
            source_id="src",
            feature_vector=[1.0],
            metadata={"region": "us-east-1", "version": 2},
        )
        assert payload.metadata["region"] == "us-east-1"

    @pytest.mark.parametrize("n_features", [1, 10, 100, 1024])
    def test_accepts_various_feature_lengths(self, n_features: int) -> None:
        payload = IngestEventIn(
            source_id="src",
            feature_vector=[0.0] * n_features,
        )
        assert len(payload.feature_vector) == n_features

    def test_feature_vector_exceeding_max_rejected(self) -> None:
        with pytest.raises(Exception):
            IngestEventIn(source_id="src", feature_vector=[0.0] * 1025)


@pytest.mark.unit
class TestAnomalyEventOut:
    """Serialisation tests for the AnomalyEventOut response schema."""

    def _make_event(self, **kwargs) -> AnomalyEventOut:
        defaults = {
            "event_id": uuid4(),
            "event_time": datetime.now(tz=UTC),
            "source_id": "sensor-1",
            "feature_vector": [0.1, 0.2],
            "anomaly_score": 0.85,
            "is_anomaly": True,
            "model_version": "v1",
            "processed_at": datetime.now(tz=UTC),
        }
        defaults.update(kwargs)
        return AnomalyEventOut(**defaults)

    def test_valid_event_constructs(self) -> None:
        event = self._make_event()
        assert event.anomaly_score == 0.85
        assert event.is_anomaly is True

    def test_score_below_zero_rejected(self) -> None:
        with pytest.raises(Exception):
            self._make_event(anomaly_score=-0.1)

    def test_score_above_one_rejected(self) -> None:
        with pytest.raises(Exception):
            self._make_event(anomaly_score=1.1)

    def test_score_boundary_zero_allowed(self) -> None:
        event = self._make_event(anomaly_score=0.0)
        assert event.anomaly_score == 0.0

    def test_score_boundary_one_allowed(self) -> None:
        event = self._make_event(anomaly_score=1.0)
        assert event.anomaly_score == 1.0


@pytest.mark.unit
class TestAnomalyStatsOut:
    """Tests for the statistics response schema."""

    def test_valid_stats(self) -> None:
        stats = AnomalyStatsOut(
            window_minutes=60,
            total_events=1000,
            anomaly_count=42,
            anomaly_rate_pct=4.2,
            avg_score=0.32,
            max_score=0.97,
        )
        assert stats.anomaly_count == 42
        assert stats.window_minutes == 60


@pytest.mark.unit
class TestAuthSchemas:
    """Tests for JWT token schemas."""

    def test_token_schema(self) -> None:
        token = Token(access_token="jwt.token.value")
        assert token.token_type == "bearer"
        assert token.access_token == "jwt.token.value"

    def test_token_data_schema(self) -> None:
        data = TokenData(sub="user@example.com")
        assert data.sub == "user@example.com"
