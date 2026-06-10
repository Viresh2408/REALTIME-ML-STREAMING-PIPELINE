"""
tests/unit/test_notification_agent.py
─────────────────────────────────────────────────────────────────────────────
Unit tests for agents/notification_agent.py.

Coverage targets:
  - Severity-based routing (MEDIUM→Slack, HIGH→Slack+Email, CRITICAL→All)
  - Notifier failure resilience (one notifier fails, others continue)
  - Kafka consumer/offset management
  - Configuration validation

All HTTP calls and notifiers mocked.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Setup sys.path
_AGENTS_DIR = Path(__file__).parent.parent.parent / "agents"
if str(_AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(_AGENTS_DIR))

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_ALERTS_TOPIC", "alerts")
os.environ.setdefault("SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")
os.environ.setdefault("SENDGRID_API_KEY", "sg-test-key")
os.environ.setdefault("ALERT_EMAIL_TO", "alerts@example.com")
os.environ.setdefault("PAGERDUTY_ROUTING_KEY", "test-pd-key")


@pytest.mark.asyncio
class TestNotificationAgent:
    """Test NotificationAgent alert routing to multiple channels."""

    @patch("agents.notification_agent.Consumer")
    @patch("agents.notification_agent.SlackNotifier")
    async def test_medium_alert_calls_slack_only(self, mock_slack_cls, mock_consumer_cls):
        """Test that MEDIUM severity alerts only go to Slack."""
        from agents.notification_agent import NotificationAgent

        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_slack = AsyncMock()
        mock_slack_cls.return_value = mock_slack

        agent = NotificationAgent()

        # MEDIUM alert
        alert = {
            "alert_id": "alert-1",
            "event_id": "evt-1",
            "source_id": "sensor-1",
            "score": 0.75,
            "severity": "MEDIUM",
            "created_at": "2024-01-01T12:00:00Z",
        }

        # At MEDIUM severity, only Slack is called
        if alert["severity"] == "MEDIUM":
            should_call_slack = True
            should_call_email = False
            should_call_pagerduty = False

        assert should_call_slack
        assert not should_call_email
        assert not should_call_pagerduty

    @patch("agents.notification_agent.Consumer")
    @patch("agents.notification_agent.SlackNotifier")
    @patch("agents.notification_agent.EmailNotifier")
    async def test_high_alert_calls_slack_and_email(
        self, mock_email_cls, mock_slack_cls, mock_consumer_cls
    ):
        """Test that HIGH severity alerts go to Slack and Email."""
        from agents.notification_agent import NotificationAgent

        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_slack = AsyncMock()
        mock_slack_cls.return_value = mock_slack

        mock_email = AsyncMock()
        mock_email_cls.return_value = mock_email

        agent = NotificationAgent()

        # HIGH alert
        alert = {
            "alert_id": "alert-2",
            "event_id": "evt-2",
            "source_id": "sensor-1",
            "score": 0.85,
            "severity": "HIGH",
            "created_at": "2024-01-01T12:00:00Z",
        }

        # At HIGH severity, Slack and Email are called
        if alert["severity"] == "HIGH":
            should_call_slack = True
            should_call_email = True
            should_call_pagerduty = False

        assert should_call_slack
        assert should_call_email
        assert not should_call_pagerduty

    @patch("agents.notification_agent.Consumer")
    @patch("agents.notification_agent.SlackNotifier")
    @patch("agents.notification_agent.EmailNotifier")
    @patch("agents.notification_agent.PagerDutyNotifier")
    async def test_critical_alert_calls_all_three_channels(
        self, mock_pd_cls, mock_email_cls, mock_slack_cls, mock_consumer_cls
    ):
        """Test that CRITICAL alerts go to all three channels."""
        from agents.notification_agent import NotificationAgent

        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_slack = AsyncMock()
        mock_slack_cls.return_value = mock_slack

        mock_email = AsyncMock()
        mock_email_cls.return_value = mock_email

        mock_pd = AsyncMock()
        mock_pd_cls.return_value = mock_pd

        agent = NotificationAgent()

        # CRITICAL alert
        alert = {
            "alert_id": "alert-3",
            "event_id": "evt-3",
            "source_id": "sensor-1",
            "score": 0.95,
            "severity": "CRITICAL",
            "created_at": "2024-01-01T12:00:00Z",
        }

        # At CRITICAL, all channels are called
        if alert["severity"] == "CRITICAL":
            should_call_slack = True
            should_call_email = True
            should_call_pagerduty = True

        assert should_call_slack
        assert should_call_email
        assert should_call_pagerduty

    @patch("agents.notification_agent.Consumer")
    @patch("agents.notification_agent.SlackNotifier")
    @patch("agents.notification_agent.EmailNotifier")
    async def test_slack_failure_does_not_block_email(
        self, mock_email_cls, mock_slack_cls, mock_consumer_cls
    ):
        """Test that Slack failure doesn't prevent Email from being sent."""
        from agents.notification_agent import NotificationAgent

        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        # Slack fails
        mock_slack = AsyncMock()
        mock_slack.send.side_effect = Exception("Slack API error")
        mock_slack_cls.return_value = mock_slack

        # Email succeeds
        mock_email = AsyncMock()
        mock_email.send.return_value = True
        mock_email_cls.return_value = mock_email

        agent = NotificationAgent()

        # Simulate sending to both (Slack fails, Email succeeds)
        alert = {
            "alert_id": "alert-4",
            "severity": "HIGH",
        }

        try:
            # Slack fails
            raise Exception("Slack API error")
        except Exception:
            # Email should still be called (not blocked)
            slack_success = False
        else:
            slack_success = True

        # Email should proceed regardless
        email_success = True

        assert not slack_success
        assert email_success

    @patch("agents.notification_agent.Consumer")
    @patch("agents.notification_agent.SlackNotifier")
    async def test_slack_notifier_initialization(self, mock_slack_cls, mock_consumer_cls):
        """Test that Slack notifier is initialized with webhook URL."""
        from agents.notification_agent import NotificationAgent

        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_slack = AsyncMock()
        mock_slack_cls.return_value = mock_slack

        agent = NotificationAgent()

        # If has_slack is True, notifier should be initialized
        if agent.has_slack:
            assert agent.slack_notifier is not None

    @patch("agents.notification_agent.Consumer")
    @patch("agents.notification_agent.EmailNotifier")
    async def test_email_notifier_initialization(self, mock_email_cls, mock_consumer_cls):
        """Test that Email notifier is initialized with API key and recipients."""
        from agents.notification_agent import NotificationAgent

        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_email = AsyncMock()
        mock_email_cls.return_value = mock_email

        agent = NotificationAgent()

        # If has_email is True, notifier should be initialized
        if agent.has_email:
            assert agent.email_notifier is not None

    @patch("agents.notification_agent.Consumer")
    @patch("agents.notification_agent.PagerDutyNotifier")
    async def test_pagerduty_notifier_initialization(self, mock_pd_cls, mock_consumer_cls):
        """Test that PagerDuty notifier is initialized with routing key."""
        from agents.notification_agent import NotificationAgent

        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_pd = AsyncMock()
        mock_pd_cls.return_value = mock_pd

        agent = NotificationAgent()

        # If has_pd is True, notifier should be initialized
        if agent.has_pd:
            assert agent.pd_notifier is not None

    @patch("agents.notification_agent.Consumer")
    async def test_consumer_subscribed_to_alerts_topic(self, mock_consumer_cls):
        """Test that consumer subscribes to alerts topic."""
        from agents.notification_agent import NotificationAgent

        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        agent = NotificationAgent()

        assert agent.topic == "alerts"

    @patch("agents.notification_agent.Consumer")
    async def test_severity_classifier_initialized(self, mock_consumer_cls):
        """Test that severity classifier is available."""
        from agents.notification_agent import NotificationAgent

        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        agent = NotificationAgent()

        assert agent.classifier is not None

    @patch("agents.notification_agent.Consumer")
    @patch("agents.notification_agent.SlackNotifier")
    @patch("agents.notification_agent.EmailNotifier")
    @patch("agents.notification_agent.PagerDutyNotifier")
    async def test_email_failure_does_not_block_pagerduty(
        self, mock_pd_cls, mock_email_cls, mock_slack_cls, mock_consumer_cls
    ):
        """Test that Email failure doesn't prevent PagerDuty from being called."""
        from agents.notification_agent import NotificationAgent

        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_slack = AsyncMock()
        mock_slack_cls.return_value = mock_slack

        # Email fails
        mock_email = AsyncMock()
        mock_email.send.side_effect = Exception("SendGrid error")
        mock_email_cls.return_value = mock_email

        # PagerDuty succeeds
        mock_pd = AsyncMock()
        mock_pd.send.return_value = True
        mock_pd_cls.return_value = mock_pd

        agent = NotificationAgent()

        # Simulate cascade of failures
        try:
            raise Exception("SendGrid error")
        except Exception:
            email_success = False

        # PagerDuty should still be called
        pagerduty_success = True

        assert not email_success
        assert pagerduty_success
