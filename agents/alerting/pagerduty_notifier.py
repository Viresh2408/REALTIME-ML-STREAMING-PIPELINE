"""
agents/alerting/pagerduty_notifier.py
=======================================
PagerDuty Events API v2 notifier — triggers CRITICAL incidents and resolves
them on alert resolution.

Design
------
* Fires only for ``CRITICAL`` severity (workflow.docx §4).
* ``trigger_incident`` maps directly to PagerDuty's ``trigger`` action.
* ``resolve_incident`` maps to the ``resolve`` action with the same
  ``dedup_key`` (alert_id) so that PagerDuty can correlate trigger ↔ resolve.
* Uses ``httpx.AsyncClient`` with 3-attempt exponential back-off.

PagerDuty Events v2 endpoint: ``https://events.pagerduty.com/v2/enqueue``

Environment variables
---------------------
PAGERDUTY_ROUTING_KEY   Required — Service integration routing key.
PAGERDUTY_TIMEOUT_S     Optional — HTTP timeout (default 15 s).
GRAFANA_URL             Optional — Grafana base URL for deep-links in PD.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import httpx

from agents.alerting.models import AlertEvent
from agents.alerting.severity_classifier import Severity

logger = logging.getLogger(__name__)

_PD_EVENTS_URL = "https://events.pagerduty.com/v2/enqueue"

# Map our Severity to PagerDuty's severity vocabulary
_PD_SEVERITY_MAP: dict[str, str] = {
    "NONE": "info",
    "LOW": "info",
    "MEDIUM": "warning",
    "HIGH": "error",
    "CRITICAL": "critical",
}


class PagerDutyNotifier:
    """
    Trigger and resolve PagerDuty incidents via the Events API v2.

    Only ``trigger_incident`` fires for CRITICAL alerts; lower severities
    are silently ignored.

    Parameters
    ----------
    routing_key:
        PagerDuty integration/routing key.  If *None*, read from
        ``PAGERDUTY_ROUTING_KEY`` at call time.
    max_attempts:
        Total send attempts.  Default 3.
    base_backoff_s:
        Initial back-off seconds; doubles each retry.  Default 1.
    timeout_s:
        HTTP timeout.  Reads ``PAGERDUTY_TIMEOUT_S``, else 15 s.
    """

    def __init__(
        self,
        routing_key: str | None = None,
        max_attempts: int = 3,
        base_backoff_s: float = 1.0,
        timeout_s: float | None = None,
    ) -> None:
        self._routing_key = routing_key
        self._max_attempts = max_attempts
        self._base_backoff = base_backoff_s
        self._timeout_s = timeout_s

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _resolved_key(self) -> str:
        key = self._routing_key or os.getenv("PAGERDUTY_ROUTING_KEY", "")
        if not key:
            raise EnvironmentError(
                "PAGERDUTY_ROUTING_KEY is not configured. "
                "Set the env var or pass routing_key to PagerDutyNotifier."
            )
        return key

    def _resolved_timeout(self) -> float:
        if self._timeout_s is not None:
            return self._timeout_s
        return float(os.getenv("PAGERDUTY_TIMEOUT_S", "15"))

    def _build_trigger_payload(self, alert: AlertEvent) -> dict[str, Any]:
        """Construct a PagerDuty Events v2 ``trigger`` payload."""
        pd_severity = _PD_SEVERITY_MAP.get(alert.severity.value, "error")
        ts = alert.created_at.isoformat()

        return {
            "routing_key": self._resolved_key(),
            "dedup_key": alert.alert_id,        # idempotent across retries
            "event_action": "trigger",
            "payload": {
                "summary": (
                    f"[{alert.severity.value}] Anomaly on source_id={alert.source_id} "
                    f"score={alert.score:.4f}"
                ),
                "source": alert.source_id,
                "severity": pd_severity,
                "timestamp": ts,
                "component": "ML Inference Engine",
                "group": "anomaly-detection-pipeline",
                "class": "anomaly",
                "custom_details": {
                    "alert_id": alert.alert_id,
                    "event_id": alert.event_id,
                    "anomaly_score": alert.score,
                    "burst_count": alert.burst_count,
                    "severity": alert.severity.value,
                    "dashboard_url": alert.grafana_deep_link,
                },
            },
            "links": [
                {
                    "href": alert.grafana_deep_link,
                    "text": "Grafana Dashboard",
                }
            ],
        }

    def _build_resolve_payload(self, alert_id: str) -> dict[str, Any]:
        """Construct a PagerDuty Events v2 ``resolve`` payload."""
        return {
            "routing_key": self._resolved_key(),
            "dedup_key": alert_id,
            "event_action": "resolve",
        }

    async def _send(self, payload: dict[str, Any]) -> None:
        """POST *payload* to PagerDuty with retry logic."""
        timeout = self._resolved_timeout()

        async with httpx.AsyncClient(timeout=timeout) as client:
            last_exc: Exception | None = None
            for attempt in range(1, self._max_attempts + 1):
                try:
                    response = await client.post(_PD_EVENTS_URL, json=payload)
                    # PagerDuty returns 202 on success
                    if response.status_code in (200, 202):
                        return
                    if response.status_code in (429,) or response.status_code >= 500:
                        logger.warning(
                            "PagerDuty transient error %d (attempt %d/%d)",
                            response.status_code,
                            attempt,
                            self._max_attempts,
                        )
                        last_exc = httpx.HTTPStatusError(
                            f"HTTP {response.status_code}",
                            request=response.request,
                            response=response,
                        )
                    else:
                        response.raise_for_status()
                        return
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    logger.warning(
                        "PagerDuty network error (attempt %d/%d): %s",
                        attempt,
                        self._max_attempts,
                        exc,
                    )
                    last_exc = exc

                if attempt < self._max_attempts:
                    await asyncio.sleep(self._base_backoff * (2 ** (attempt - 1)))

            raise RuntimeError(
                f"PagerDuty request failed after {self._max_attempts} attempts"
            ) from last_exc

    # ── Public API ────────────────────────────────────────────────────────────

    async def trigger_incident(self, alert: AlertEvent) -> None:
        """
        Trigger a PagerDuty incident for *alert*.

        Silently returns (no-op) when severity is not CRITICAL.

        Parameters
        ----------
        alert:
            The ``AlertEvent`` that caused the incident.  The alert's
            ``alert_id`` is used as the PagerDuty ``dedup_key`` so that
            duplicate triggers are de-duplicated by PagerDuty automatically.

        Raises
        ------
        EnvironmentError
            If ``PAGERDUTY_ROUTING_KEY`` is not configured.
        RuntimeError
            If all retry attempts are exhausted.
        """
        if alert.severity != Severity.CRITICAL:
            logger.debug(
                "PagerDutyNotifier: skipping severity=%s (not CRITICAL)",
                alert.severity.value,
            )
            return

        payload = self._build_trigger_payload(alert)
        await self._send(payload)
        logger.info(
            "PagerDuty incident triggered",
            extra={
                "alert_id": alert.alert_id,
                "source_id": alert.source_id,
                "score": alert.score,
            },
        )

    async def resolve_incident(self, alert_id: str) -> None:
        """
        Resolve the PagerDuty incident that was opened for *alert_id*.

        Uses the same ``dedup_key`` (``alert_id``) that was used at trigger
        time so PagerDuty correlates the resolve correctly.

        Parameters
        ----------
        alert_id:
            The ``alert_id`` (UUID) from the original ``AlertEvent``.

        Raises
        ------
        EnvironmentError
            If ``PAGERDUTY_ROUTING_KEY`` is not configured.
        RuntimeError
            If all retry attempts are exhausted.
        """
        payload = self._build_resolve_payload(alert_id)
        await self._send(payload)
        logger.info(
            "PagerDuty incident resolved",
            extra={"alert_id": alert_id},
        )
