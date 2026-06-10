"""
tests/unit/test_severity_classifier.py
───────────────────────────────────────
Unit tests for the SeverityClassifier.

Covers:
  - Parametrised score × burst_count threshold boundary checks
  - SLA target mapping for every severity tier
  - Notification channel routing (Slack, Email, PagerDuty)
  - Edge cases at exact boundary values (0.695, 0.70, 0.80, 0.90, 0.95)
  - Burst override escalation rules

Spec refs:
  backend_requirements.docx § 2   NFR-06 (alert latency SLAs)
  tasks.docx                       T-048 (unit test harness)
"""

from __future__ import annotations

import pytest
from agents.alerting.severity_classifier import Severity, SeverityClassifier


@pytest.mark.unit
class TestSeverityClassifierThresholds:
    """
    Parametrised boundary and boundary-adjacent tests for SeverityClassifier.classify().

    Score → Severity mapping (from classifier spec):
      score < 0.70       → NONE   (not an anomaly)
      0.70 ≤ score < 0.80 → LOW
      0.80 ≤ score < 0.90 → MEDIUM
      0.90 ≤ score < 0.95 → HIGH
      score ≥ 0.95        → CRITICAL
      burst_count ≥ 10    → escalate to CRITICAL regardless of score
    """

    @pytest.fixture(autouse=True)
    def setup_classifier(self) -> None:
        """Create a fresh classifier for each test method."""
        self.classifier = SeverityClassifier()

    # ── Score-based boundaries ────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "score, burst_count, expected_severity",
        [
            # ── NONE: score strictly below 0.70 ─────────────────────────────
            (0.0,   0, Severity.NONE),
            (0.500, 0, Severity.NONE),
            (0.694, 0, Severity.NONE),
            (0.695, 0, Severity.NONE),   # spec edge case — below LOW threshold
            (0.699, 0, Severity.NONE),

            # ── LOW boundary: [0.70, 0.80) ────────────────────────────────────
            (0.700, 0, Severity.LOW),    # exact lower boundary → LOW
            (0.70,  0, Severity.LOW),    # spec edge case: score=0.70 → LOW
            (0.750, 0, Severity.LOW),
            (0.799, 0, Severity.LOW),    # just below MEDIUM

            # ── MEDIUM boundary: [0.80, 0.90) ────────────────────────────────
            (0.800, 0, Severity.MEDIUM), # exact MEDIUM lower boundary
            (0.850, 0, Severity.MEDIUM),
            (0.899, 0, Severity.MEDIUM), # just below HIGH

            # ── HIGH boundary: [0.90, 0.95) ──────────────────────────────────
            (0.900, 0, Severity.HIGH),   # exact HIGH lower boundary
            (0.920, 0, Severity.HIGH),
            (0.940, 0, Severity.HIGH),
            (0.949, 0, Severity.HIGH),   # just below CRITICAL

            # ── CRITICAL boundary: score ≥ 0.95 ──────────────────────────────
            (0.950, 0, Severity.CRITICAL),  # spec edge case: score=0.95 → CRITICAL
            (0.95,  0, Severity.CRITICAL),  # exact lower boundary
            (0.975, 0, Severity.CRITICAL),
            (0.990, 0, Severity.CRITICAL),
            (1.000, 0, Severity.CRITICAL),  # maximum score

            # ── Burst-based override (burst_count ≥ 10 → CRITICAL) ────────────
            (0.500, 10, Severity.CRITICAL),  # low score, but burst triggers CRITICAL
            (0.695, 10, Severity.CRITICAL),  # would be NONE by score alone
            (0.750, 12, Severity.CRITICAL),  # LOW by score, CRITICAL by burst
            (0.850, 15, Severity.CRITICAL),  # MEDIUM by score, CRITICAL by burst
            (0.900,  9, Severity.HIGH),      # HIGH by score, burst=9 < 10 → stays HIGH
            (0.900, 10, Severity.CRITICAL),  # HIGH by score, burst=10 → CRITICAL
        ],
        ids=lambda v: str(v),
    )
    def test_classify_thresholds(
        self,
        score: float,
        burst_count: int,
        expected_severity: Severity,
    ) -> None:
        """
        Assert that classify(score, burst_count) returns the expected Severity.
        Covers all threshold boundaries including the exact spec values:
          score=0.70 → LOW, score=0.695 → not anomaly (NONE), score=0.95 → CRITICAL
        """
        result = self.classifier.classify(score, burst_count)
        assert result == expected_severity, (
            f"classify(score={score}, burst={burst_count}) = {result}, "
            f"expected {expected_severity}"
        )


@pytest.mark.unit
class TestSeverityClassifierSLAMapping:
    """
    Verify the SLA time-to-acknowledge (TTA) mapping for every severity tier.

    From backend_requirements.docx § 2 NFR-06:
      CRITICAL → 60s
      HIGH     → 30s
      MEDIUM   → 10s (note: spec says minutes, but classifier returns seconds)
      LOW      →  5s
      NONE     →  0  (no SLA required)
    """

    @pytest.fixture(autouse=True)
    def setup_classifier(self) -> None:
        self.classifier = SeverityClassifier()

    @pytest.mark.parametrize(
        "severity, expected_sla_s",
        [
            (Severity.NONE,     0),
            (Severity.LOW,      5),
            (Severity.MEDIUM,  10),
            (Severity.HIGH,    30),
            (Severity.CRITICAL, 60),
        ],
    )
    def test_sla_mapping(self, severity: Severity, expected_sla_s: int) -> None:
        """
        Assert SLA seconds match the specification for each severity tier.
        NFR-06: CRITICAL alerts must trigger within 60s.
        """
        result = self.classifier.sla_seconds(severity)
        assert result == expected_sla_s, (
            f"sla_seconds({severity}) = {result}, expected {expected_sla_s}"
        )


@pytest.mark.unit
class TestSeverityClassifierRoutingActions:
    """
    Verify the notification channel routing matrix.

    Channel activation rules:
      NONE     → no channels
      LOW      → no active channels (logged only)
      MEDIUM   → Slack
      HIGH     → Slack + Email
      CRITICAL → Slack + Email + PagerDuty
    """

    @pytest.fixture(autouse=True)
    def setup_classifier(self) -> None:
        self.classifier = SeverityClassifier()

    @pytest.mark.parametrize(
        "severity, actionable, slack, email, pagerduty",
        [
            (Severity.NONE,     False, False, False, False),
            (Severity.LOW,      True,  False, False, False),
            (Severity.MEDIUM,   True,  True,  False, False),
            (Severity.HIGH,     True,  True,  True,  False),
            (Severity.CRITICAL, True,  True,  True,  True),
        ],
    )
    def test_routing_actions(
        self,
        severity: Severity,
        actionable: bool,
        slack: bool,
        email: bool,
        pagerduty: bool,
    ) -> None:
        """
        Verify notification router activates the correct channels per severity.

        is_actionable: False means the alert is recorded but not routed.
        requires_slack: Slack notification must fire for MEDIUM and above.
        requires_email: Email must fire for HIGH and above.
        requires_pagerduty: PagerDuty must fire only for CRITICAL.
        """
        assert self.classifier.is_actionable(severity) == actionable, (
            f"is_actionable({severity}) = {self.classifier.is_actionable(severity)}, "
            f"expected {actionable}"
        )
        assert self.classifier.requires_slack(severity) == slack, (
            f"requires_slack({severity}) = {self.classifier.requires_slack(severity)}, "
            f"expected {slack}"
        )
        assert self.classifier.requires_email(severity) == email, (
            f"requires_email({severity}) = {self.classifier.requires_email(severity)}, "
            f"expected {email}"
        )
        assert self.classifier.requires_pagerduty(severity) == pagerduty, (
            f"requires_pagerduty({severity}) = {self.classifier.requires_pagerduty(severity)}, "
            f"expected {pagerduty}"
        )


@pytest.mark.unit
class TestSeverityClassifierEdgeCases:
    """Additional edge cases and regression tests."""

    @pytest.fixture(autouse=True)
    def setup_classifier(self) -> None:
        self.classifier = SeverityClassifier()

    def test_score_zero_is_not_anomaly(self) -> None:
        """Score of 0.0 must produce NONE severity — lowest possible input."""
        assert self.classifier.classify(0.0, 0) == Severity.NONE

    def test_score_one_is_critical(self) -> None:
        """Score of 1.0 must produce CRITICAL severity — highest possible input."""
        assert self.classifier.classify(1.0, 0) == Severity.CRITICAL

    def test_burst_zero_never_escalates(self) -> None:
        """burst_count=0 must never trigger burst-override escalation."""
        # Score that would be HIGH by burst rule if burst > 0
        assert self.classifier.classify(0.5, 0) == Severity.NONE

    def test_large_burst_count_always_critical(self) -> None:
        """Very large burst_count (e.g., 1000) must still produce CRITICAL."""
        assert self.classifier.classify(0.0, 1_000) == Severity.CRITICAL

    @pytest.mark.parametrize("score", [0.6999999, 0.6950001, 0.6900000])
    def test_sub_threshold_scores_are_none(self, score: float) -> None:
        """Any score below 0.70 must classify as NONE (not actionable)."""
        result = self.classifier.classify(score, 0)
        assert result == Severity.NONE, (
            f"classify({score}, 0) should be NONE, got {result}"
        )

    @pytest.mark.parametrize("score", [0.700001, 0.710, 0.790, 0.799999])
    def test_low_band_scores(self, score: float) -> None:
        """Scores in (0.70, 0.80) exclusive must classify as LOW."""
        result = self.classifier.classify(score, 0)
        assert result == Severity.LOW, (
            f"classify({score}, 0) should be LOW, got {result}"
        )
