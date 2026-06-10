"""
agents/alerting/email_notifier.py
===================================
SendGrid API v3 email notifier for HIGH and CRITICAL severity alerts.

Design
------
* Uses ``httpx.AsyncClient`` for non-blocking HTTP calls.
* Sends a multi-part email (HTML + plain-text fallback) via the
  SendGrid v3 ``/mail/send`` endpoint.
* HTML body is generated inline — no template files required.
* Fires only for HIGH and CRITICAL severity (workflow.docx §4).

Environment variables
---------------------
SENDGRID_API_KEY    Required — SendGrid API key (Bearer token).
ALERT_EMAIL_TO      Required — Recipient address(es), comma-separated.
ALERT_EMAIL_FROM    Optional — Sender address (default system@anomaly-pipeline.internal).
SENDGRID_TIMEOUT_S  Optional — HTTP timeout in seconds (default 15).
GRAFANA_URL         Optional — Grafana base URL for deep-links.
"""

from __future__ import annotations

import asyncio
import logging
import os
from textwrap import dedent
from typing import Any

import httpx

from agents.alerting.models import AlertEvent
from agents.alerting.severity_classifier import Severity

logger = logging.getLogger(__name__)

_SENDGRID_SEND_URL = "https://api.sendgrid.com/v3/mail/send"

# Severities that trigger email (workflow.docx §4: HIGH + CRITICAL)
_EMAIL_SEVERITIES: frozenset[Severity] = frozenset({Severity.HIGH, Severity.CRITICAL})

_SEVERITY_BADGE_COLOR: dict[str, str] = {
    "HIGH": "#e01e5a",
    "CRITICAL": "#7b0000",
}


def _build_html(alert: AlertEvent) -> str:
    """Generate a self-contained HTML email body for *alert*."""
    badge_color = _SEVERITY_BADGE_COLOR.get(alert.severity.value, "#555555")
    link = alert.grafana_deep_link
    score_pct = f"{alert.score * 100:.1f}%"
    ts = alert.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")

    return dedent(f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>Anomaly Alert — {alert.severity.value}</title>
      <style>
        body {{ font-family: Arial, sans-serif; background:#f4f6f8; color:#222; margin:0; padding:0; }}
        .wrapper {{ max-width:640px; margin:32px auto; background:#fff;
                    border-radius:8px; overflow:hidden;
                    box-shadow:0 2px 8px rgba(0,0,0,.12); }}
        .header {{ background:{badge_color}; color:#fff; padding:24px 32px; }}
        .header h1 {{ margin:0; font-size:22px; letter-spacing:.5px; }}
        .header .badge {{ display:inline-block; background:rgba(255,255,255,.2);
                          border-radius:4px; padding:2px 10px; font-size:13px;
                          margin-top:8px; }}
        .body {{ padding:28px 32px; }}
        table {{ width:100%; border-collapse:collapse; margin-top:16px; }}
        th {{ text-align:left; font-size:12px; color:#888; text-transform:uppercase;
              padding:6px 8px; border-bottom:2px solid #eee; }}
        td {{ padding:10px 8px; border-bottom:1px solid #eee; font-size:14px; }}
        td.label {{ font-weight:bold; width:38%; color:#444; }}
        .cta {{ margin-top:28px; text-align:center; }}
        .cta a {{ background:{badge_color}; color:#fff; text-decoration:none;
                  padding:12px 32px; border-radius:6px; font-size:15px;
                  display:inline-block; }}
        .footer {{ padding:16px 32px; font-size:11px; color:#aaa;
                   border-top:1px solid #eee; text-align:center; }}
      </style>
    </head>
    <body>
      <div class="wrapper">
        <div class="header">
          <h1>{alert.severity_emoji} Anomaly Alert — {alert.severity.value}</h1>
          <div class="badge">Real-Time ML Streaming Pipeline</div>
        </div>
        <div class="body">
          <p>An anomaly has been detected that requires your attention.</p>
          <table>
            <tr><th>Field</th><th>Value</th></tr>
            <tr><td class="label">Alert ID</td><td><code>{alert.alert_id}</code></td></tr>
            <tr><td class="label">Event ID</td><td><code>{alert.event_id}</code></td></tr>
            <tr><td class="label">Source ID</td><td>{alert.source_id}</td></tr>
            <tr><td class="label">Anomaly Score</td>
                <td><strong>{alert.score:.4f}</strong> ({score_pct})</td></tr>
            <tr><td class="label">Severity</td>
                <td><strong style="color:{badge_color}">{alert.severity.value}</strong></td></tr>
            <tr><td class="label">Burst Count</td>
                <td>{alert.burst_count} HIGH+ alerts / 60 s</td></tr>
            <tr><td class="label">Detected At</td><td>{ts}</td></tr>
          </table>
          <div class="cta">
            <a href="{link}">🔗 Open Grafana Dashboard</a>
          </div>
        </div>
        <div class="footer">
          This alert was generated automatically by the Real-Time Anomaly Detection System.
          Please review and label the event via the API:
          <code>PATCH /api/events/{alert.event_id}/label</code>
        </div>
      </div>
    </body>
    </html>
    """).strip()


def _build_plain(alert: AlertEvent) -> str:
    """Plain-text fallback for email clients that don't render HTML."""
    ts = alert.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
    return (
        f"ANOMALY ALERT — {alert.severity.value}\n"
        f"{'=' * 50}\n"
        f"Alert ID    : {alert.alert_id}\n"
        f"Event ID    : {alert.event_id}\n"
        f"Source ID   : {alert.source_id}\n"
        f"Score       : {alert.score:.4f}\n"
        f"Severity    : {alert.severity.value}\n"
        f"Burst Count : {alert.burst_count} HIGH+/60 s\n"
        f"Detected At : {ts}\n"
        f"\nDashboard   : {alert.grafana_deep_link}\n"
        f"\nPlease review via: PATCH /api/events/{alert.event_id}/label\n"
    )


class EmailNotifier:
    """
    Send HTML alert emails via the SendGrid API v3.

    Only fires for ``HIGH`` and ``CRITICAL`` severity (workflow.docx §4).

    Parameters
    ----------
    api_key:
        SendGrid API key.  If *None*, read from ``SENDGRID_API_KEY`` at
        call time (supports hot-reload / secrets rotation).
    to_addresses:
        List of recipient email addresses.  If *None*, read from
        ``ALERT_EMAIL_TO`` (comma-separated) at call time.
    from_address:
        Sender address.  Falls back to ``ALERT_EMAIL_FROM`` env var.
    max_attempts:
        Total send attempts (1 = no retry).  Default 3.
    base_backoff_s:
        Initial back-off seconds; doubles each retry.  Default 1.
    timeout_s:
        HTTP timeout.  Reads ``SENDGRID_TIMEOUT_S``, else 15 s.
    """

    def __init__(
        self,
        api_key: str | None = None,
        to_addresses: list[str] | None = None,
        from_address: str | None = None,
        max_attempts: int = 3,
        base_backoff_s: float = 1.0,
        timeout_s: float | None = None,
    ) -> None:
        self._api_key = api_key
        self._to_addresses = to_addresses
        self._from_address = from_address
        self._max_attempts = max_attempts
        self._base_backoff = base_backoff_s
        self._timeout_s = timeout_s

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _resolved_api_key(self) -> str:
        key = self._api_key or os.getenv("SENDGRID_API_KEY", "")
        if not key:
            raise OSError(
                "SENDGRID_API_KEY is not configured. "
                "Set the env var or pass api_key to EmailNotifier."
            )
        return key

    def _resolved_to(self) -> list[str]:
        if self._to_addresses:
            return self._to_addresses
        raw = os.getenv("ALERT_EMAIL_TO", "")
        if not raw:
            raise OSError(
                "ALERT_EMAIL_TO is not configured. "
                "Set the env var or pass to_addresses to EmailNotifier."
            )
        return [addr.strip() for addr in raw.split(",") if addr.strip()]

    def _resolved_from(self) -> str:
        if self._from_address:
            return self._from_address
        return os.getenv("ALERT_EMAIL_FROM", "system@anomaly-pipeline.internal")

    def _resolved_timeout(self) -> float:
        if self._timeout_s is not None:
            return self._timeout_s
        return float(os.getenv("SENDGRID_TIMEOUT_S", "15"))

    def _build_sendgrid_payload(self, alert: AlertEvent) -> dict[str, Any]:
        to_list = [{"email": addr} for addr in self._resolved_to()]
        subject = f"[{alert.severity.value}] Anomaly Alert — source_id={alert.source_id}"

        return {
            "personalizations": [{"to": to_list}],
            "from": {"email": self._resolved_from()},
            "subject": subject,
            "content": [
                {"type": "text/plain", "value": _build_plain(alert)},
                {"type": "text/html", "value": _build_html(alert)},
            ],
        }

    # ── Public API ────────────────────────────────────────────────────────────

    async def send_alert(self, alert: AlertEvent) -> None:
        """
        Send an HTML alert email for *alert*.

        Silently returns (no-op) when severity is below HIGH.

        Parameters
        ----------
        alert:
            The ``AlertEvent`` to email.

        Raises
        ------
        EnvironmentError
            If SendGrid credentials or recipient addresses are missing.
        RuntimeError
            If all retry attempts are exhausted.
        """
        if alert.severity not in _EMAIL_SEVERITIES:
            logger.debug(
                "EmailNotifier: skipping severity=%s (below HIGH threshold)",
                alert.severity.value,
            )
            return

        api_key = self._resolved_api_key()
        payload = self._build_sendgrid_payload(alert)
        timeout = self._resolved_timeout()
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        async with httpx.AsyncClient(timeout=timeout) as client:
            last_exc: Exception | None = None
            for attempt in range(1, self._max_attempts + 1):
                try:
                    response = await client.post(_SENDGRID_SEND_URL, headers=headers, json=payload)
                    # SendGrid returns 202 Accepted on success
                    if response.status_code in (200, 202):
                        logger.info(
                            "Email alert sent via SendGrid",
                            extra={
                                "alert_id": alert.alert_id,
                                "severity": alert.severity.value,
                                "attempt": attempt,
                            },
                        )
                        return
                    if response.status_code in (429,) or response.status_code >= 500:
                        logger.warning(
                            "SendGrid transient error %d (attempt %d/%d)",
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
                        "SendGrid network error (attempt %d/%d): %s",
                        attempt,
                        self._max_attempts,
                        exc,
                    )
                    last_exc = exc

                if attempt < self._max_attempts:
                    await asyncio.sleep(self._base_backoff * (2 ** (attempt - 1)))

            raise RuntimeError(
                f"Email notification failed after {self._max_attempts} attempts"
            ) from last_exc
