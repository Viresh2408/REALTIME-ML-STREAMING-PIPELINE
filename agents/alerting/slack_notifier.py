"""
agents/alerting/slack_notifier.py
===================================
Async Slack notifier that posts rich alert messages to an incoming webhook.

Design
------
* Uses ``httpx.AsyncClient`` (already a project dependency — httpx 0.27).
* Retries up to 3 times with exponential back-off (1 s, 2 s, 4 s) on
  transient HTTP 429 / 5xx responses — matches the retry requirement.
* Message format (workflow.docx §4):
    ``<emoji> [<SEVERITY>] source_id=<id>  score=<0.000>``
    Dashboard deep-link rendered as a Slack hyperlink.

Environment variables
---------------------
SLACK_WEBHOOK_URL   Required — Slack incoming webhook endpoint.
SLACK_TIMEOUT_S     Optional — HTTP timeout in seconds (default 10).
GRAFANA_URL         Optional — Grafana base URL for deep-links.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import httpx

from agents.alerting.models import AlertEvent

logger = logging.getLogger(__name__)

_SEVERITY_COLORS: dict[str, str] = {
    "NONE": "#36a64f",  # green
    "LOW": "#f0e130",  # yellow
    "MEDIUM": "#ffa500",  # orange
    "HIGH": "#e01e5a",  # red
    "CRITICAL": "#7b0000",  # dark red
}


class SlackNotifier:
    """
    Post alert notifications to a Slack incoming webhook.

    Parameters
    ----------
    webhook_url:
        Slack webhook URL.  If *None*, the value is read from
        ``SLACK_WEBHOOK_URL`` at call time (supports hot-reload).
    max_attempts:
        Total number of send attempts (1 = no retries).  Default 3.
    base_backoff_s:
        Initial back-off interval in seconds; doubles each retry. Default 1.
    timeout_s:
        HTTP request timeout.  Reads ``SLACK_TIMEOUT_S`` env var, else 10 s.
    """

    SENDGRID_API = "https://api.sendgrid.com/v3/mail/send"

    def __init__(
        self,
        webhook_url: str | None = None,
        max_attempts: int = 3,
        base_backoff_s: float = 1.0,
        timeout_s: float | None = None,
    ) -> None:
        self._webhook_url = webhook_url
        self._max_attempts = max_attempts
        self._base_backoff = base_backoff_s
        self._timeout_s = timeout_s

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _resolved_url(self) -> str:
        url = self._webhook_url or os.getenv("SLACK_WEBHOOK_URL", "")
        if not url:
            raise OSError(
                "SLACK_WEBHOOK_URL is not configured. "
                "Set the env var or pass webhook_url to SlackNotifier."
            )
        return url

    def _resolved_timeout(self) -> float:
        if self._timeout_s is not None:
            return self._timeout_s
        return float(os.getenv("SLACK_TIMEOUT_S", "10"))

    def _build_payload(self, alert: AlertEvent) -> dict[str, Any]:
        """Build a Slack Block Kit message payload from an AlertEvent."""
        color = _SEVERITY_COLORS.get(alert.severity.value, "#cccccc")
        link = alert.grafana_deep_link

        header_text = (
            f"{alert.severity_emoji}  *[{alert.severity.value}]*  "
            f"`source_id={alert.source_id}`  —  score: *{alert.score:.4f}*"
        )

        return {
            "attachments": [
                {
                    "color": color,
                    "blocks": [
                        {
                            "type": "section",
                            "text": {"type": "mrkdwn", "text": header_text},
                        },
                        {
                            "type": "section",
                            "fields": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Alert ID*\n`{alert.alert_id}`",
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Event ID*\n`{alert.event_id}`",
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Anomaly Score*\n{alert.score:.4f}",
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Burst Count*\n{alert.burst_count} HIGH/min",
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Severity*\n{alert.severity.value}",
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": (
                                        f"*Created At*\n"
                                        f"{alert.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}"
                                    ),
                                },
                            ],
                        },
                        {
                            "type": "actions",
                            "elements": [
                                {
                                    "type": "button",
                                    "text": {"type": "plain_text", "text": "🔗 Open Dashboard"},
                                    "url": link,
                                    "style": "primary",
                                }
                            ],
                        },
                    ],
                }
            ]
        }

    # ── Public API ────────────────────────────────────────────────────────────

    async def send_alert(self, alert: AlertEvent) -> None:
        """
        Post *alert* to the Slack webhook.

        Retries up to ``max_attempts`` times with exponential back-off on
        HTTP 429 / 5xx responses.

        Parameters
        ----------
        alert:
            The ``AlertEvent`` to post.

        Raises
        ------
        httpx.HTTPStatusError
            If all retry attempts fail with a non-2xx status code.
        EnvironmentError
            If ``SLACK_WEBHOOK_URL`` is not configured.
        """
        url = self._resolved_url()
        payload = self._build_payload(alert)
        timeout = self._resolved_timeout()

        async with httpx.AsyncClient(timeout=timeout) as client:
            last_exc: Exception | None = None
            for attempt in range(1, self._max_attempts + 1):
                try:
                    response = await client.post(url, json=payload)
                    if response.status_code == 200:
                        logger.info(
                            "Slack alert sent",
                            extra={
                                "alert_id": alert.alert_id,
                                "severity": alert.severity.value,
                                "attempt": attempt,
                            },
                        )
                        return
                    # Retry on 429 / 5xx
                    if response.status_code in (429,) or response.status_code >= 500:
                        logger.warning(
                            "Slack webhook transient error %d (attempt %d/%d)",
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
                        # 4xx (except 429) — do not retry
                        response.raise_for_status()
                        return
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    logger.warning(
                        "Slack webhook network error (attempt %d/%d): %s",
                        attempt,
                        self._max_attempts,
                        exc,
                    )
                    last_exc = exc

                if attempt < self._max_attempts:
                    backoff = self._base_backoff * (2 ** (attempt - 1))
                    await asyncio.sleep(backoff)

            raise RuntimeError(
                f"Slack notification failed after {self._max_attempts} attempts"
            ) from last_exc
