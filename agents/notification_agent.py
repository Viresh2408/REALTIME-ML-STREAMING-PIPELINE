import asyncio
import json
import os
import sys

from confluent_kafka import Consumer

# Ensure project root is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.alerting import (
    AlertEvent,
    EmailNotifier,
    PagerDutyNotifier,
    SeverityClassifier,
    SlackNotifier,
)
from agents.env_loader import load_env

load_env()


class NotificationAgent:
    def __init__(self) -> None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("KAFKA_ALERTS_TOPIC", "alerts")

        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": os.getenv("KAFKA_NOTIFY_GROUP_ID", "notification-agent-group"),
                "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
                "enable.auto.commit": False,
            }
        )

        self.classifier = SeverityClassifier()

        # Slack Configuration
        self.slack_webhook = os.getenv("SLACK_WEBHOOK_URL", "")
        self.has_slack = bool(
            self.slack_webhook and "placeholder" not in self.slack_webhook.lower()
        )
        if self.has_slack:
            self.slack_notifier = SlackNotifier(webhook_url=self.slack_webhook)

        # Email Configuration
        self.sendgrid_key = os.getenv("SENDGRID_API_KEY", "")
        self.email_to = os.getenv("ALERT_EMAIL_TO") or os.getenv("ALERT_EMAIL_RECIPIENTS", "")
        self.has_email = bool(
            self.sendgrid_key and self.email_to and "placeholder" not in self.sendgrid_key.lower()
        )
        if self.has_email:
            email_to: str = self.email_to  # type: ignore[assignment]  # guarded by has_email
            emails = [addr.strip() for addr in email_to.split(",") if addr.strip()]
            self.email_notifier = EmailNotifier(api_key=self.sendgrid_key, to_addresses=emails)

        # PagerDuty Configuration
        self.pd_key = os.getenv("PAGERDUTY_ROUTING_KEY") or os.getenv(
            "PAGERDUTY_INTEGRATION_KEY", ""
        )
        self.has_pd = bool(self.pd_key and "placeholder" not in self.pd_key.lower())
        if self.has_pd:
            self.pd_notifier = PagerDutyNotifier(routing_key=self.pd_key)

        self.running = False

    async def run(self) -> None:
        self.running = True
        self.consumer.subscribe([self.topic])
        print("NotificationAgent started...")

        try:
            while self.running:
                msg = self.consumer.poll(0.1)
                if msg is None:
                    await asyncio.sleep(0.01)
                    continue

                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    await asyncio.sleep(0.01)
                    continue

                try:
                    val = msg.value()
                    if val is None:
                        self.consumer.commit(message=msg, asynchronous=True)
                        continue

                    alert_dict = json.loads(val.decode("utf-8"))
                    alert = AlertEvent.model_validate(alert_dict)
                    print(
                        f"Received alert event: {alert.alert_id} (severity={alert.severity.value})"
                    )

                    tasks = []

                    # 1. Slack routing (MEDIUM+)
                    if self.classifier.requires_slack(alert.severity):
                        if self.has_slack:
                            tasks.append(self.slack_notifier.send_alert(alert))
                        else:
                            print(
                                f"[MOCK SLACK] [{alert.severity.value}] "
                                f"source_id={alert.source_id} - score: {alert.score:.4f} "
                                f"({alert.burst_count} HIGH/min)"
                            )

                    # 2. Email routing (HIGH+)
                    if self.classifier.requires_email(alert.severity):
                        if self.has_email:
                            tasks.append(self.email_notifier.send_alert(alert))
                        else:
                            print(
                                f"[MOCK EMAIL] Anomaly Alert - {alert.severity.value} "
                                f"source_id={alert.source_id} - score: {alert.score:.4f}"
                            )

                    # 3. PagerDuty routing (CRITICAL only)
                    if self.classifier.requires_pagerduty(alert.severity):
                        if self.has_pd:
                            tasks.append(self.pd_notifier.trigger_incident(alert))
                        else:
                            print(
                                f"[MOCK PAGERDUTY] CRITICAL Anomaly: "
                                f"source_id={alert.source_id} - score: {alert.score:.4f}"
                            )

                    if tasks:
                        # Gather all active notifications concurrently
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        for r in results:
                            if isinstance(r, Exception):
                                print(f"Notification channel dispatch error: {r}")

                    self.consumer.commit(message=msg, asynchronous=True)

                except Exception as e:
                    print(f"Error in NotificationAgent loop: {e}")
                    await asyncio.sleep(0.01)

        finally:
            self.running = False
            self.consumer.close()

    def stop(self) -> None:
        self.running = False


if __name__ == "__main__":
    agent = NotificationAgent()
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        print("Stopping NotificationAgent...")
