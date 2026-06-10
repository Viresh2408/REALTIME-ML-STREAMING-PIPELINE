"""
agents/alerting/severity_classifier.py
=======================================
Classifies anomaly score + burst count into a Severity level following the
exact thresholds from workflow.docx Section 4 — Alert Escalation Workflow:

    Severity | Trigger Condition                              | SLA
    ---------|------------------------------------------------|-------
    LOW      | score 0.70 – 0.79                              | < 5 s
    MEDIUM   | score 0.80 – 0.89                              | < 10 s
    HIGH     | score 0.90 – 0.94                              | < 30 s
    CRITICAL | score >= 0.95 OR burst >= 10 HIGH alerts/min  | < 60 s

All boundary values are hot-reloadable via environment variables so that
ops can tighten / loosen thresholds without a redeploy.  The classifier
re-reads os.getenv() on every call, meaning a SIGHUP + env update is
sufficient for hot-reload (useful with docker-compose env_file or Vault
agent).
"""

from __future__ import annotations

import os
from enum import StrEnum


class Severity(StrEnum):
    """Alert severity levels — matches AlertSeverity in backend schemas."""

    NONE = "NONE"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class SeverityClassifier:
    """
    Classify an (anomaly_score, burst_count) pair into a Severity level.

    Thresholds are read from environment variables on every :py:meth:`classify`
    call so that hot-reload works without restarting the process.

    Environment variables (all optional — defaults match workflow.docx §4):
        SEVERITY_THRESH_LOW       float  default 0.70  — lower bound for LOW
        SEVERITY_THRESH_MEDIUM    float  default 0.80  — lower bound for MEDIUM
        SEVERITY_THRESH_HIGH      float  default 0.90  — lower bound for HIGH
        SEVERITY_THRESH_CRITICAL  float  default 0.95  — lower bound for CRITICAL
        BURST_CRITICAL_THRESHOLD  int    default 10    — HIGH alerts/min → CRITICAL
    """

    # ── Compile-time defaults (overridden by env at runtime) ──────────────────
    _DEFAULT_LOW = 0.70
    _DEFAULT_MEDIUM = 0.80
    _DEFAULT_HIGH = 0.90
    _DEFAULT_CRITICAL = 0.95
    _DEFAULT_BURST = 10

    def _thresholds(self) -> tuple[float, float, float, float, int]:
        """Return (low, medium, high, critical, burst) thresholds from env."""
        low = float(os.getenv("SEVERITY_THRESH_LOW", str(self._DEFAULT_LOW)))
        medium = float(os.getenv("SEVERITY_THRESH_MEDIUM", str(self._DEFAULT_MEDIUM)))
        high = float(os.getenv("SEVERITY_THRESH_HIGH", str(self._DEFAULT_HIGH)))
        critical = float(os.getenv("SEVERITY_THRESH_CRITICAL", str(self._DEFAULT_CRITICAL)))
        burst = int(os.getenv("BURST_CRITICAL_THRESHOLD", str(self._DEFAULT_BURST)))
        return low, medium, high, critical, burst

    def classify(self, score: float, burst_count: int = 0) -> Severity:
        """
        Return the Severity for the given anomaly score and burst count.

        Parameters
        ----------
        score:
            Anomaly score in [0.0, 1.0] produced by the ML inference engine.
        burst_count:
            Number of HIGH-or-above alerts seen in the last 60 seconds for
            the same source_id (provided by :class:`BurstDetector`).

        Returns
        -------
        Severity
            The determined severity level.  Returns ``Severity.NONE`` when the
            score is below the LOW threshold and no burst condition is active.
        """
        low, medium, high, critical, burst_limit = self._thresholds()

        # Burst override — any source with >= burst_limit HIGH alerts/min
        # is immediately escalated to CRITICAL regardless of current score.
        if burst_count >= burst_limit:
            return Severity.CRITICAL

        # Score-based classification (workflow.docx §4 table, exact boundaries)
        if score >= critical:
            return Severity.CRITICAL
        if score >= high:
            return Severity.HIGH
        if score >= medium:
            return Severity.MEDIUM
        if score >= low:
            return Severity.LOW

        return Severity.NONE

    # ── Convenience helpers ───────────────────────────────────────────────────

    def is_actionable(self, severity: Severity) -> bool:
        """Return True when severity warrants any notification action."""
        return severity not in (Severity.NONE,)

    def requires_slack(self, severity: Severity) -> bool:
        """Slack fires for MEDIUM, HIGH, and CRITICAL (workflow.docx §4)."""
        return severity in (Severity.MEDIUM, Severity.HIGH, Severity.CRITICAL)

    def requires_email(self, severity: Severity) -> bool:
        """Email fires for HIGH and CRITICAL (workflow.docx §4)."""
        return severity in (Severity.HIGH, Severity.CRITICAL)

    def requires_pagerduty(self, severity: Severity) -> bool:
        """PagerDuty fires for CRITICAL only (workflow.docx §4)."""
        return severity == Severity.CRITICAL

    def sla_seconds(self, severity: Severity) -> int:
        """Return the SLA target in seconds from workflow.docx §4."""
        mapping = {
            Severity.LOW: 5,
            Severity.MEDIUM: 10,
            Severity.HIGH: 30,
            Severity.CRITICAL: 60,
        }
        return mapping.get(severity, 0)
