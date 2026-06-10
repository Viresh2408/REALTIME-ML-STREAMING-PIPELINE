"""
agents/alerting — Alert pipeline components for the Real-Time Anomaly Detection System.

Exports:
    AlertEvent          — canonical alert payload shared by all notifiers
    Severity            — severity level enum (NONE / LOW / MEDIUM / HIGH / CRITICAL)
    SeverityClassifier  — classify anomaly score + burst count → Severity
    BurstDetector       — Redis sliding-window HIGH-alert counter
    SilenceManager      — Redis-backed alert silencing
    SlackNotifier       — async httpx Slack webhook poster
    EmailNotifier       — SendGrid API v3 HTML email sender
    PagerDutyNotifier   — PagerDuty Events v2 trigger / resolve
"""

from agents.alerting.burst_detector import BurstDetector
from agents.alerting.email_notifier import EmailNotifier
from agents.alerting.models import AlertEvent
from agents.alerting.pagerduty_notifier import PagerDutyNotifier
from agents.alerting.severity_classifier import Severity, SeverityClassifier
from agents.alerting.silence_manager import SilenceManager
from agents.alerting.slack_notifier import SlackNotifier

__all__ = [
    "AlertEvent",
    "BurstDetector",
    "EmailNotifier",
    "PagerDutyNotifier",
    "Severity",
    "SeverityClassifier",
    "SilenceManager",
    "SlackNotifier",
]
