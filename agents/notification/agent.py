"""
Notification Agent
Architecture: Section 5 — Reactive, triggered by alerts topic
Output: Email / Slack / PagerDuty webhook call
Uses: LangChain 0.2 + Claude claude-sonnet-4-20250514 for human-readable alert summaries
"""
from __future__ import annotations

import os
from typing import Any

import httpx
import structlog
from langchain.schema import HumanMessage
from langchain_anthropic import ChatAnthropic

from agents.shared.state import AgentState

logger = structlog.get_logger(__name__)


async def _generate_alert_summary(alert: dict[str, Any]) -> str:
    """
    Use Claude claude-sonnet-4-20250514 to produce a human-readable anomaly explanation
    from the raw alert payload. Returns a markdown-formatted summary.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return (
            f"Anomaly detected: score={alert.get('anomaly_score', 0):.3f}, "
            f"severity={alert.get('severity', 'unknown')}, "
            f"source={alert.get('source_id', 'unknown')}."
        )

    llm = ChatAnthropic(
        model=os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
        api_key=api_key,
        max_tokens=512,
        temperature=0.0,
    )

    prompt = (
        f"You are an SRE assistant. An anomaly was detected in the real-time streaming pipeline.\n\n"
        f"Alert details:\n"
        f"- Event ID: {alert.get('event_id')}\n"
        f"- Source: {alert.get('source_id')}\n"
        f"- Anomaly Score: {alert.get('anomaly_score', 0):.4f} (threshold: 0.7)\n"
        f"- Severity: {alert.get('severity')}\n"
        f"- Model Version: {alert.get('model_version')}\n"
        f"- Feature Vector (first 5): {alert.get('feature_vector', [])[:5]}\n\n"
        "Write a concise (2-3 sentences) human-readable incident summary suitable "
        "for a Slack alert or PagerDuty incident title. Be specific and actionable."
    )

    response = await llm.ainvoke([HumanMessage(content=prompt)])
    return str(response.content)


async def _send_slack(summary: str, alert: dict[str, Any]) -> bool:
    """Post alert to Slack webhook."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not configured — skipping Slack notification")
        return False

    severity_emoji = {
        "low": "🟡", "medium": "🟠", "high": "🔴", "critical": "🚨"
    }.get(alert.get("severity", "low"), "⚠️")

    payload = {
        "text": f"{severity_emoji} *Anomaly Alert* [{alert.get('severity', '').upper()}]",
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{severity_emoji} Anomaly Detected*\n{summary}"},
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"Score: `{alert.get('anomaly_score', 0):.4f}` | "
                                                f"Source: `{alert.get('source_id')}` | "
                                                f"Model: `{alert.get('model_version')}`"}
                ],
            },
        ],
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(webhook_url, json=payload)
            return resp.status_code == 200
    except Exception as exc:
        logger.error("Slack notification failed", error=str(exc))
        return False


async def _send_pagerduty(summary: str, alert: dict[str, Any]) -> bool:
    """Send PagerDuty Events API v2 alert."""
    integration_key = os.environ.get("PAGERDUTY_INTEGRATION_KEY", "")
    if not integration_key:
        logger.warning("PAGERDUTY_INTEGRATION_KEY not configured — skipping PagerDuty")
        return False

    severity_map = {"low": "warning", "medium": "warning", "high": "error", "critical": "critical"}
    payload = {
        "routing_key": integration_key,
        "event_action": "trigger",
        "dedup_key": alert.get("alert_id"),
        "payload": {
            "summary": summary[:1024],
            "source": alert.get("source_id", "anomaly-detection-system"),
            "severity": severity_map.get(alert.get("severity", "low"), "warning"),
            "custom_details": {
                "anomaly_score": alert.get("anomaly_score"),
                "model_version": alert.get("model_version"),
                "event_id": alert.get("event_id"),
            },
        },
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "https://events.pagerduty.com/v2/enqueue", json=payload
            )
            return resp.status_code in {200, 202}
    except Exception as exc:
        logger.error("PagerDuty notification failed", error=str(exc))
        return False


async def notification_node(state: AgentState) -> AgentState:
    """
    LangGraph node: generate an LLM summary and dispatch notifications
    via Slack and/or PagerDuty based on configured severity thresholds.
    """
    alert = {
        "alert_id": state.get("event_id", ""),
        "event_id": state.get("event_id", ""),
        "source_id": state.get("source_id", ""),
        "anomaly_score": state.get("anomaly_score", 0.0),
        "severity": state.get("alert_severity", "low"),
        "model_version": state.get("model_version", "unknown"),
        "feature_vector": state.get("feature_vector", []),
    }

    summary = await _generate_alert_summary(alert)
    state["llm_explanation"] = summary

    severity = alert["severity"]
    if severity in {"low", "medium", "high", "critical"}:
        await _send_slack(summary, alert)
    if severity in {"high", "critical"}:
        await _send_pagerduty(summary, alert)

    logger.info(
        "Notification dispatched",
        severity=severity,
        source_id=alert["source_id"],
        score=alert["anomaly_score"],
    )
    return state
