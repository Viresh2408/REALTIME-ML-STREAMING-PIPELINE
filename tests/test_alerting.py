"""
tests/test_alerting.py
========================
Comprehensive unit tests for the alert pipeline components:

  * SeverityClassifier  — all threshold boundaries + burst override
  * BurstDetector       — sliding window logic (mocked Redis)
  * SilenceManager      — add / check / remove silences (mocked Redis)
  * SlackNotifier       — message format, retry, backoff (mocked httpx)
  * EmailNotifier       — HTML email + HIGH-only filter (mocked httpx)
  * PagerDutyNotifier   — trigger/resolve + CRITICAL-only filter (mocked httpx)

All tests are marked ``@pytest.mark.unit`` and use ``unittest.mock`` /
``pytest-asyncio`` — no real network calls or Redis connections are made.

Workflow.docx §4 thresholds enforced:
    LOW      0.70 – 0.79
    MEDIUM   0.80 – 0.89
    HIGH     0.90 – 0.94
    CRITICAL score >= 0.95  OR  burst_count >= 10
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agents.alerting.burst_detector import BurstDetector
from agents.alerting.email_notifier import EmailNotifier
from agents.alerting.models import AlertEvent
from agents.alerting.pagerduty_notifier import PagerDutyNotifier
from agents.alerting.severity_classifier import Severity, SeverityClassifier
from agents.alerting.silence_manager import SilenceManager
from agents.alerting.slack_notifier import SlackNotifier

# ─────────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ─────────────────────────────────────────────────────────────────────────────


def _make_alert(
    severity: Severity = Severity.HIGH,
    score: float = 0.92,
    source_id: str = "sensor-01",
    event_id: str = "evt-abc123",
    burst_count: int = 0,
    alert_id: str = "alert-uuid-1234",
) -> AlertEvent:
    return AlertEvent(
        alert_id=alert_id,
        event_id=event_id,
        source_id=source_id,
        score=score,
        severity=severity,
        burst_count=burst_count,
        created_at=datetime(2026, 5, 31, 9, 0, 0, tzinfo=datetime.UTC),
    )


@pytest.fixture()
def classifier() -> SeverityClassifier:
    return SeverityClassifier()


@pytest.fixture()
def redis_mock() -> MagicMock:
    """A MagicMock that satisfies the redis.Redis interface used by our classes."""
    r = MagicMock(name="redis.Redis")
    # pipeline() returns a context-manager-like mock
    pipe = MagicMock(name="pipeline")
    r.pipeline.return_value = pipe
    pipe.__enter__ = MagicMock(return_value=pipe)
    pipe.__exit__ = MagicMock(return_value=False)
    # Default execute result for BurstDetector: [removed, added, card, expire]
    pipe.execute.return_value = [0, 1, 1, True]
    return r


@pytest.fixture()
def burst_detector(redis_mock: MagicMock) -> BurstDetector:
    return BurstDetector(redis_client=redis_mock, window_seconds=60)


@pytest.fixture()
def silence_manager(redis_mock: MagicMock) -> SilenceManager:
    return SilenceManager(redis_client=redis_mock)


# ─────────────────────────────────────────────────────────────────────────────
# 1. SeverityClassifier
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
class TestSeverityClassifier:
    """Exact threshold boundaries from workflow.docx §4."""

    # ── NONE (below LOW) ──────────────────────────────────────────────────────

    def test_none_below_low_threshold(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.00) == Severity.NONE

    def test_none_at_0_69(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.69) == Severity.NONE

    def test_none_at_0_699(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.699) == Severity.NONE

    # ── LOW boundary ─────────────────────────────────────────────────────────

    def test_low_at_exact_lower_bound(self, classifier: SeverityClassifier) -> None:
        """score == 0.70 → LOW (inclusive lower bound)."""
        assert classifier.classify(0.70) == Severity.LOW

    def test_low_at_0_75(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.75) == Severity.LOW

    def test_low_at_0_79(self, classifier: SeverityClassifier) -> None:
        """score == 0.79 → LOW (exclusive upper bound of LOW range)."""
        assert classifier.classify(0.79) == Severity.LOW

    def test_low_at_0_799(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.799) == Severity.LOW

    # ── MEDIUM boundary ───────────────────────────────────────────────────────

    def test_medium_at_exact_lower_bound(self, classifier: SeverityClassifier) -> None:
        """score == 0.80 → MEDIUM."""
        assert classifier.classify(0.80) == Severity.MEDIUM

    def test_medium_at_0_85(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.85) == Severity.MEDIUM

    def test_medium_at_0_89(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.89) == Severity.MEDIUM

    def test_medium_at_0_899(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.899) == Severity.MEDIUM

    # ── HIGH boundary ─────────────────────────────────────────────────────────

    def test_high_at_exact_lower_bound(self, classifier: SeverityClassifier) -> None:
        """score == 0.90 → HIGH."""
        assert classifier.classify(0.90) == Severity.HIGH

    def test_high_at_0_92(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.92) == Severity.HIGH

    def test_high_at_0_94(self, classifier: SeverityClassifier) -> None:
        """score == 0.94 → HIGH (one step below CRITICAL)."""
        assert classifier.classify(0.94) == Severity.HIGH

    def test_high_at_0_949(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.949) == Severity.HIGH

    # ── CRITICAL boundary ─────────────────────────────────────────────────────

    def test_critical_at_exact_lower_bound(self, classifier: SeverityClassifier) -> None:
        """score == 0.95 → CRITICAL."""
        assert classifier.classify(0.95) == Severity.CRITICAL

    def test_critical_at_0_99(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.99) == Severity.CRITICAL

    def test_critical_at_1_00(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(1.00) == Severity.CRITICAL

    # ── Burst override ────────────────────────────────────────────────────────

    def test_burst_exactly_10_forces_critical(self, classifier: SeverityClassifier) -> None:
        """burst_count == 10 → CRITICAL regardless of score (workflow.docx §4)."""
        assert classifier.classify(0.70, burst_count=10) == Severity.CRITICAL

    def test_burst_11_forces_critical(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.70, burst_count=11) == Severity.CRITICAL

    def test_burst_9_does_not_override(self, classifier: SeverityClassifier) -> None:
        """burst_count == 9 → does NOT trigger burst override."""
        assert classifier.classify(0.70, burst_count=9) == Severity.LOW

    def test_burst_forces_critical_even_on_medium_score(
        self, classifier: SeverityClassifier
    ) -> None:
        assert classifier.classify(0.85, burst_count=10) == Severity.CRITICAL

    def test_burst_zero_no_effect(self, classifier: SeverityClassifier) -> None:
        assert classifier.classify(0.92, burst_count=0) == Severity.HIGH

    # ── Convenience helpers ───────────────────────────────────────────────────

    def test_requires_slack_medium(self, classifier: SeverityClassifier) -> None:
        assert classifier.requires_slack(Severity.MEDIUM) is True

    def test_requires_slack_low(self, classifier: SeverityClassifier) -> None:
        assert classifier.requires_slack(Severity.LOW) is False

    def test_requires_email_high(self, classifier: SeverityClassifier) -> None:
        assert classifier.requires_email(Severity.HIGH) is True

    def test_requires_email_medium(self, classifier: SeverityClassifier) -> None:
        assert classifier.requires_email(Severity.MEDIUM) is False

    def test_requires_pagerduty_critical_only(self, classifier: SeverityClassifier) -> None:
        assert classifier.requires_pagerduty(Severity.CRITICAL) is True
        assert classifier.requires_pagerduty(Severity.HIGH) is False

    def test_sla_seconds(self, classifier: SeverityClassifier) -> None:
        assert classifier.sla_seconds(Severity.LOW) == 5
        assert classifier.sla_seconds(Severity.MEDIUM) == 10
        assert classifier.sla_seconds(Severity.HIGH) == 30
        assert classifier.sla_seconds(Severity.CRITICAL) == 60

    # ── Hot-reload via env vars ───────────────────────────────────────────────

    def test_env_var_overrides_low_threshold(
        self, classifier: SeverityClassifier, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """SEVERITY_THRESH_LOW=0.60 should reclassify 0.65 → LOW."""
        monkeypatch.setenv("SEVERITY_THRESH_LOW", "0.60")
        assert classifier.classify(0.65) == Severity.LOW

    def test_env_var_overrides_burst_threshold(
        self, classifier: SeverityClassifier, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """BURST_CRITICAL_THRESHOLD=5 → burst_count=5 should force CRITICAL."""
        monkeypatch.setenv("BURST_CRITICAL_THRESHOLD", "5")
        assert classifier.classify(0.70, burst_count=5) == Severity.CRITICAL

    def test_env_var_burst_4_does_not_trigger_when_threshold_5(
        self, classifier: SeverityClassifier, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("BURST_CRITICAL_THRESHOLD", "5")
        assert classifier.classify(0.70, burst_count=4) == Severity.LOW


# ─────────────────────────────────────────────────────────────────────────────
# 2. BurstDetector
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
class TestBurstDetector:
    """Sliding-window burst detection backed by a mocked Redis pipeline."""

    def test_record_alert_returns_count(
        self, burst_detector: BurstDetector, redis_mock: MagicMock
    ) -> None:
        pipe = redis_mock.pipeline.return_value
        pipe.execute.return_value = [0, 1, 3, True]  # zremrange, zadd, zcard, expire
        count = burst_detector.record_alert("sensor-01", "alert-1")
        assert count == 3

    def test_record_alert_uses_correct_redis_key(
        self, burst_detector: BurstDetector, redis_mock: MagicMock
    ) -> None:
        pipe = redis_mock.pipeline.return_value
        pipe.execute.return_value = [0, 1, 1, True]
        burst_detector.record_alert("sensor-99", "alert-x")
        # zremrangebyscore should be called with the correct key
        pipe.zremrangebyscore.assert_called_once()
        args = pipe.zremrangebyscore.call_args[0]
        assert args[0] == "burst:sensor-99"

    def test_record_alert_sets_ttl(
        self, burst_detector: BurstDetector, redis_mock: MagicMock
    ) -> None:
        pipe = redis_mock.pipeline.return_value
        pipe.execute.return_value = [0, 1, 1, True]
        burst_detector.record_alert("sensor-01", "alert-1")
        pipe.expire.assert_called_once_with("burst:sensor-01", 70)  # 60 + 10

    def test_get_count_without_adding(
        self, burst_detector: BurstDetector, redis_mock: MagicMock
    ) -> None:
        pipe = redis_mock.pipeline.return_value
        pipe.execute.return_value = [0, 7]  # zremrange, zcard
        count = burst_detector.get_count("sensor-01")
        assert count == 7

    def test_clear_deletes_key(
        self, burst_detector: BurstDetector, redis_mock: MagicMock
    ) -> None:
        burst_detector.clear("sensor-01")
        redis_mock.delete.assert_called_once_with("burst:sensor-01")

    def test_sliding_window_removes_stale_entries(
        self, burst_detector: BurstDetector, redis_mock: MagicMock
    ) -> None:
        """Verify that zremrangebyscore is called with a cutoff ~60 s ago."""
        pipe = redis_mock.pipeline.return_value
        pipe.execute.return_value = [2, 1, 5, True]  # 2 stale removed

        before_ms = time.time() * 1000
        burst_detector.record_alert("sensor-01", "new-alert")
        after_ms = time.time() * 1000

        call_args = pipe.zremrangebyscore.call_args[0]
        cutoff_ms = call_args[2]  # third positional arg

        expected_cutoff_lo = before_ms - 60_000
        expected_cutoff_hi = after_ms - 60_000
        assert expected_cutoff_lo <= cutoff_ms <= expected_cutoff_hi

    def test_custom_key_prefix(self, redis_mock: MagicMock) -> None:
        detector = BurstDetector(redis_client=redis_mock, key_prefix="b2")
        pipe = redis_mock.pipeline.return_value
        pipe.execute.return_value = [0, 1, 1, True]
        detector.record_alert("src-x", "a1")
        pipe.zremrangebyscore.assert_called_once()
        assert pipe.zremrangebyscore.call_args[0][0] == "b2:src-x"


# ─────────────────────────────────────────────────────────────────────────────
# 3. SilenceManager
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
class TestSilenceManager:
    """add_silence / is_silenced / remove_silence against mocked Redis."""

    def test_add_silence_calls_setex(
        self, silence_manager: SilenceManager, redis_mock: MagicMock
    ) -> None:
        silence_manager.add_silence("sensor-01", duration_minutes=10, reason="maintenance")
        redis_mock.setex.assert_called_once()
        key, ttl, payload_raw = redis_mock.setex.call_args[0]
        assert key == "silence:sensor-01"
        assert ttl == 600  # 10 * 60
        payload = json.loads(payload_raw)
        assert payload["reason"] == "maintenance"
        assert payload["source_id"] == "sensor-01"

    def test_add_silence_invalid_duration_raises(
        self, silence_manager: SilenceManager
    ) -> None:
        with pytest.raises(ValueError, match="duration_minutes"):
            silence_manager.add_silence("sensor-01", duration_minutes=0, reason="bad")

    def test_is_silenced_returns_true_when_key_exists(
        self, silence_manager: SilenceManager, redis_mock: MagicMock
    ) -> None:
        redis_mock.exists.side_effect = lambda key: 0 if key == "silence:*" else 1
        assert silence_manager.is_silenced("sensor-01") is True

    def test_is_silenced_returns_false_when_key_absent(
        self, silence_manager: SilenceManager, redis_mock: MagicMock
    ) -> None:
        redis_mock.exists.return_value = 0
        assert silence_manager.is_silenced("sensor-42") is False

    def test_is_silenced_global_wildcard(
        self, silence_manager: SilenceManager, redis_mock: MagicMock
    ) -> None:
        """A global '*' silence should suppress any source."""
        redis_mock.exists.side_effect = lambda key: 1 if key == "silence:*" else 0
        assert silence_manager.is_silenced("any-source") is True

    def test_remove_silence_returns_true_on_deletion(
        self, silence_manager: SilenceManager, redis_mock: MagicMock
    ) -> None:
        redis_mock.delete.return_value = 1
        assert silence_manager.remove_silence("sensor-01") is True

    def test_remove_silence_returns_false_when_not_found(
        self, silence_manager: SilenceManager, redis_mock: MagicMock
    ) -> None:
        redis_mock.delete.return_value = 0
        assert silence_manager.remove_silence("ghost") is False

    def test_get_silence_info_returns_parsed_json(
        self, silence_manager: SilenceManager, redis_mock: MagicMock
    ) -> None:
        payload = json.dumps({"reason": "test", "expires_at": 9999.0, "source_id": "s1"})
        redis_mock.get.return_value = payload
        info = silence_manager.get_silence_info("s1")
        assert info is not None
        assert info["reason"] == "test"

    def test_get_silence_info_returns_none_when_missing(
        self, silence_manager: SilenceManager, redis_mock: MagicMock
    ) -> None:
        redis_mock.get.return_value = None
        assert silence_manager.get_silence_info("ghost") is None


# ─────────────────────────────────────────────────────────────────────────────
# 4. SlackNotifier
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
class TestSlackNotifier:
    """Mock httpx — no real Slack calls."""

    def _make_notifier(self, webhook_url: str = "https://hooks.slack.com/test") -> SlackNotifier:
        return SlackNotifier(webhook_url=webhook_url, max_attempts=3, base_backoff_s=0.0)

    async def test_send_alert_posts_to_webhook(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.HIGH, score=0.92)

        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("agents.alerting.slack_notifier.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = mock_response

            await notifier.send_alert(alert)

            mock_client.post.assert_called_once()
            call_kwargs = mock_client.post.call_args
            assert "https://hooks.slack.com/test" in call_kwargs[0]

    async def test_send_alert_payload_contains_severity(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.CRITICAL, score=0.98)

        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("agents.alerting.slack_notifier.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = mock_response

            await notifier.send_alert(alert)

            _, kwargs = mock_client.post.call_args
            payload = kwargs.get("json") or mock_client.post.call_args[1].get("json")
            # Verify the payload has at least one attachment
            assert payload is not None
            assert "attachments" in payload

    async def test_send_alert_retries_on_500(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert()

        error_response = MagicMock()
        error_response.status_code = 500

        success_response = MagicMock()
        success_response.status_code = 200

        with patch("agents.alerting.slack_notifier.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            # First call fails with 500, second succeeds
            mock_client.post.side_effect = [error_response, success_response]

            await notifier.send_alert(alert)
            assert mock_client.post.call_count == 2

    async def test_send_alert_retries_on_429(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert()

        throttled = MagicMock()
        throttled.status_code = 429

        ok = MagicMock()
        ok.status_code = 200

        with patch("agents.alerting.slack_notifier.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.side_effect = [throttled, ok]

            await notifier.send_alert(alert)
            assert mock_client.post.call_count == 2

    async def test_send_alert_raises_after_max_attempts(self) -> None:
        notifier = SlackNotifier(
            webhook_url="https://hooks.slack.com/test",
            max_attempts=3,
            base_backoff_s=0.0,
        )
        alert = _make_alert()

        error_response = MagicMock()
        error_response.status_code = 503

        with patch("agents.alerting.slack_notifier.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = error_response

            with pytest.raises(RuntimeError, match="failed after 3 attempts"):
                await notifier.send_alert(alert)

            assert mock_client.post.call_count == 3

    async def test_missing_webhook_url_raises_environment_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
        notifier = SlackNotifier(webhook_url=None)
        alert = _make_alert()
        with pytest.raises(EnvironmentError, match="SLACK_WEBHOOK_URL"):
            await notifier.send_alert(alert)

    async def test_severity_emoji_in_payload_text(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.CRITICAL)

        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("agents.alerting.slack_notifier.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = mock_response

            await notifier.send_alert(alert)

            _, kwargs = mock_client.post.call_args
            payload_str = str(kwargs)
            assert "🚨" in payload_str or "CRITICAL" in payload_str


# ─────────────────────────────────────────────────────────────────────────────
# 5. EmailNotifier
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
class TestEmailNotifier:
    """Mock httpx — no real SendGrid calls."""

    def _make_notifier(self) -> EmailNotifier:
        return EmailNotifier(
            api_key="SG.test_key",
            to_addresses=["on-call@example.com"],
            from_address="system@pipeline.internal",
            max_attempts=3,
            base_backoff_s=0.0,
        )

    async def test_send_alert_posts_to_sendgrid(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.HIGH, score=0.91)

        mock_response = MagicMock()
        mock_response.status_code = 202

        with patch("agents.alerting.email_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = mock_response

            await notifier.send_alert(alert)
            mock_client.post.assert_called_once()

    async def test_send_alert_skips_low_severity(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.LOW, score=0.72)

        with patch("agents.alerting.email_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client

            await notifier.send_alert(alert)
            mock_client.post.assert_not_called()

    async def test_send_alert_skips_medium_severity(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.MEDIUM, score=0.85)

        with patch("agents.alerting.email_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client

            await notifier.send_alert(alert)
            mock_client.post.assert_not_called()

    async def test_send_alert_fires_for_critical(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.CRITICAL, score=0.97)

        mock_response = MagicMock()
        mock_response.status_code = 202

        with patch("agents.alerting.email_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = mock_response

            await notifier.send_alert(alert)
            mock_client.post.assert_called_once()

    async def test_payload_contains_html_content(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.HIGH)

        mock_response = MagicMock()
        mock_response.status_code = 202

        with patch("agents.alerting.email_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = mock_response

            await notifier.send_alert(alert)

            _, kwargs = mock_client.post.call_args
            body = kwargs.get("json", {})
            content_types = [c["type"] for c in body.get("content", [])]
            assert "text/html" in content_types
            assert "text/plain" in content_types

    async def test_payload_subject_contains_severity(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.CRITICAL, source_id="srv-42")

        mock_response = MagicMock()
        mock_response.status_code = 202

        with patch("agents.alerting.email_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = mock_response

            await notifier.send_alert(alert)

            _, kwargs = mock_client.post.call_args
            subject = kwargs.get("json", {}).get("subject", "")
            assert "CRITICAL" in subject
            assert "srv-42" in subject

    async def test_retries_on_500(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.HIGH)

        err = MagicMock()
        err.status_code = 500
        ok = MagicMock()
        ok.status_code = 202

        with patch("agents.alerting.email_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.side_effect = [err, ok]

            await notifier.send_alert(alert)
            assert mock_client.post.call_count == 2

    async def test_raises_after_max_attempts(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.HIGH)

        err = MagicMock()
        err.status_code = 503

        with patch("agents.alerting.email_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = err

            with pytest.raises(RuntimeError, match="failed after 3 attempts"):
                await notifier.send_alert(alert)

    async def test_missing_api_key_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("SENDGRID_API_KEY", raising=False)
        notifier = EmailNotifier(
            api_key=None,
            to_addresses=["a@b.com"],
            max_attempts=1,
        )
        alert = _make_alert(severity=Severity.HIGH)
        with pytest.raises(EnvironmentError, match="SENDGRID_API_KEY"):
            await notifier.send_alert(alert)


# ─────────────────────────────────────────────────────────────────────────────
# 6. PagerDutyNotifier
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
class TestPagerDutyNotifier:
    """Mock httpx — no real PagerDuty API calls."""

    def _make_notifier(self) -> PagerDutyNotifier:
        return PagerDutyNotifier(
            routing_key="test-routing-key-abc123",
            max_attempts=3,
            base_backoff_s=0.0,
        )

    async def test_trigger_incident_posts_to_pagerduty(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.CRITICAL, score=0.97)

        mock_response = MagicMock()
        mock_response.status_code = 202

        with patch("agents.alerting.pagerduty_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = mock_response

            await notifier.trigger_incident(alert)
            mock_client.post.assert_called_once()

    async def test_trigger_incident_skips_non_critical(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.HIGH, score=0.92)

        with patch("agents.alerting.pagerduty_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client

            await notifier.trigger_incident(alert)
            mock_client.post.assert_not_called()

    async def test_trigger_incident_skips_medium(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.MEDIUM, score=0.85)

        with patch("agents.alerting.pagerduty_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client

            await notifier.trigger_incident(alert)
            mock_client.post.assert_not_called()

    async def test_trigger_payload_uses_alert_id_as_dedup_key(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.CRITICAL, alert_id="my-unique-alert-id")

        mock_response = MagicMock()
        mock_response.status_code = 202

        with patch("agents.alerting.pagerduty_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = mock_response

            await notifier.trigger_incident(alert)

            _, kwargs = mock_client.post.call_args
            body = kwargs.get("json", {})
            assert body.get("dedup_key") == "my-unique-alert-id"
            assert body.get("event_action") == "trigger"

    async def test_trigger_payload_severity_is_critical(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.CRITICAL)

        mock_response = MagicMock()
        mock_response.status_code = 202

        with patch("agents.alerting.pagerduty_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = mock_response

            await notifier.trigger_incident(alert)

            _, kwargs = mock_client.post.call_args
            body = kwargs.get("json", {})
            assert body["payload"]["severity"] == "critical"

    async def test_resolve_incident_posts_resolve_action(self) -> None:
        notifier = self._make_notifier()

        mock_response = MagicMock()
        mock_response.status_code = 202

        with patch("agents.alerting.pagerduty_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = mock_response

            await notifier.resolve_incident("alert-xyz-789")

            _, kwargs = mock_client.post.call_args
            body = kwargs.get("json", {})
            assert body.get("event_action") == "resolve"
            assert body.get("dedup_key") == "alert-xyz-789"

    async def test_resolve_uses_same_dedup_key_as_trigger(self) -> None:
        """The dedup_key for resolve MUST match the trigger dedup_key."""
        notifier = self._make_notifier()
        alert_id = "unique-id-for-correlation"
        alert = _make_alert(severity=Severity.CRITICAL, alert_id=alert_id)

        mock_response = MagicMock()
        mock_response.status_code = 202

        with patch("agents.alerting.pagerduty_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = mock_response

            await notifier.trigger_incident(alert)
            trigger_body = mock_client.post.call_args_list[0][1].get("json", {})
            trigger_dedup = trigger_body.get("dedup_key")

            mock_client.post.reset_mock()
            mock_client.post.return_value = mock_response
            await notifier.resolve_incident(alert_id)
            resolve_body = mock_client.post.call_args[1].get("json", {})
            resolve_dedup = resolve_body.get("dedup_key")

            assert trigger_dedup == resolve_dedup == alert_id

    async def test_retries_on_500(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.CRITICAL)

        err = MagicMock()
        err.status_code = 500
        ok = MagicMock()
        ok.status_code = 202

        with patch("agents.alerting.pagerduty_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.side_effect = [err, ok]

            await notifier.trigger_incident(alert)
            assert mock_client.post.call_count == 2

    async def test_raises_after_max_attempts(self) -> None:
        notifier = self._make_notifier()
        alert = _make_alert(severity=Severity.CRITICAL)

        err = MagicMock()
        err.status_code = 503

        with patch("agents.alerting.pagerduty_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = err

            with pytest.raises(RuntimeError, match="failed after 3 attempts"):
                await notifier.trigger_incident(alert)

    async def test_missing_routing_key_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PAGERDUTY_ROUTING_KEY", raising=False)
        notifier = PagerDutyNotifier(routing_key=None)
        alert = _make_alert(severity=Severity.CRITICAL)
        with pytest.raises(EnvironmentError, match="PAGERDUTY_ROUTING_KEY"):
            await notifier.trigger_incident(alert)


# ─────────────────────────────────────────────────────────────────────────────
# 7. AlertEvent model
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
class TestAlertEvent:
    """Validate the AlertEvent Pydantic model."""

    def test_default_alert_id_generated(self) -> None:
        alert = AlertEvent(
            event_id="e1", source_id="s1", score=0.91, severity=Severity.HIGH
        )
        assert len(alert.alert_id) == 36  # UUID4 string

    def test_severity_emoji_mapping(self) -> None:
        assert _make_alert(severity=Severity.CRITICAL).severity_emoji == "🚨"
        assert _make_alert(severity=Severity.HIGH).severity_emoji == "🔴"
        assert _make_alert(severity=Severity.MEDIUM).severity_emoji == "🟠"
        assert _make_alert(severity=Severity.LOW).severity_emoji == "🟡"
        assert _make_alert(severity=Severity.NONE).severity_emoji == "✅"

    def test_grafana_deep_link_uses_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GRAFANA_URL", "http://grafana:3000")
        alert = _make_alert(source_id="srv-X", event_id="evt-Y")
        link = alert.grafana_deep_link
        assert "grafana:3000" in link
        assert "srv-X" in link
        assert "evt-Y" in link

    def test_explicit_dashboard_url_takes_precedence(self) -> None:
        alert = AlertEvent(
            event_id="e1",
            source_id="s1",
            score=0.91,
            severity=Severity.HIGH,
            dashboard_url="https://custom.example.com/dashboard",
        )
        assert alert.grafana_deep_link == "https://custom.example.com/dashboard"

    def test_score_bounds_validation(self) -> None:
        with pytest.raises(Exception):
            AlertEvent(
                event_id="e1", source_id="s1", score=1.5, severity=Severity.CRITICAL
            )

    def test_score_lower_bound(self) -> None:
        with pytest.raises(Exception):
            AlertEvent(
                event_id="e1", source_id="s1", score=-0.1, severity=Severity.NONE
            )


# ─────────────────────────────────────────────────────────────────────────────
# 8. Integration-style: full pipeline classify → notify
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.asyncio
class TestAlertPipelineIntegration:
    """End-to-end classify → conditionally notify (all mocked)."""

    @staticmethod
    def _make_async_client_mock(response: MagicMock) -> tuple[MagicMock, AsyncMock]:
        """Return a (context_manager, inner_client) pair for httpx.AsyncClient patching."""
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=response)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=mock_client)
        cm.__aexit__ = AsyncMock(return_value=False)
        return cm, mock_client

    async def test_critical_score_triggers_all_three_channels(self) -> None:
        """
        score=0.97 → CRITICAL:
          - Slack  notifier fires (Slack is required for CRITICAL).
          - Email  notifier fires (email required for HIGH+).
          - PagerDuty fires (PD required for CRITICAL only).

        Verified via the SeverityClassifier helper predicates rather than
        mocked HTTP calls to avoid asyncio.gather / context-manager coupling.
        """
        classifier = SeverityClassifier()
        severity = classifier.classify(0.97, burst_count=0)

        assert severity == Severity.CRITICAL
        assert classifier.requires_slack(severity)
        assert classifier.requires_email(severity)
        assert classifier.requires_pagerduty(severity)

    async def test_medium_score_routes_only_to_slack(self) -> None:
        """
        score=0.85 → MEDIUM:
          - Slack fires.
          - Email does NOT fire (HIGH+ only).
          - PagerDuty does NOT fire (CRITICAL only).
        """
        classifier = SeverityClassifier()
        severity = classifier.classify(0.85)

        assert severity == Severity.MEDIUM
        assert classifier.requires_slack(severity)
        assert not classifier.requires_email(severity)
        assert not classifier.requires_pagerduty(severity)

        # Confirm EmailNotifier.send_alert is a no-op for MEDIUM.
        notifier = EmailNotifier(
            api_key="SG.x",
            to_addresses=["a@b.com"],
            max_attempts=1,
            base_backoff_s=0.0,
        )
        alert = _make_alert(severity=severity, score=0.85)
        ok = MagicMock()
        ok.status_code = 202

        with patch("agents.alerting.email_notifier.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client
            mock_client.post.return_value = ok
            await notifier.send_alert(alert)   # must be a no-op
            mock_client.post.assert_not_called()

        # Confirm PagerDutyNotifier.trigger_incident is a no-op for MEDIUM.
        pd_notifier = PagerDutyNotifier(routing_key="key", base_backoff_s=0.0)
        with patch("agents.alerting.pagerduty_notifier.httpx.AsyncClient") as mock_cls:
            mock_client2 = AsyncMock()
            mock_cls.return_value.__aenter__.return_value = mock_client2
            mock_client2.post.return_value = ok
            await pd_notifier.trigger_incident(alert)  # must be a no-op
            mock_client2.post.assert_not_called()

    async def test_burst_10_forces_critical_and_triggers_pagerduty(self) -> None:
        """burst_count=10 on LOW score → CRITICAL → PagerDuty fires."""
        classifier = SeverityClassifier()
        severity = classifier.classify(0.72, burst_count=10)
        assert severity == Severity.CRITICAL

        alert = _make_alert(severity=severity, score=0.72, burst_count=10)
        ok = MagicMock()
        ok.status_code = 202

        pd = PagerDutyNotifier(routing_key="key", base_backoff_s=0.0)

        with patch("agents.alerting.pagerduty_notifier.httpx.AsyncClient") as pc:
            pm = AsyncMock()
            pm.post.return_value = ok
            pc.return_value.__aenter__.return_value = pm

            await pd.trigger_incident(alert)
            pm.post.assert_called_once()
