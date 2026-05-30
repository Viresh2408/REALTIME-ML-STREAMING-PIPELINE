import asyncio
import json
import os
import sys

import aiohttp
from confluent_kafka import Consumer

# Ensure project root is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        self.slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
        self.sendgrid_api_key = os.getenv("SENDGRID_API_KEY", "")
        self.pagerduty_routing_key = os.getenv("PAGERDUTY_ROUTING_KEY", "")
        self.running = False

    async def notify_slack(self, session: aiohttp.ClientSession, alert: dict) -> None:
        if not self.slack_webhook_url:
            print(f"[MOCK SLACK] {alert['severity']} alert for {alert['event_id']}")
            return

        payload = {
            "text": f"*{alert['severity']} Anomaly Detected!*\nScore: {alert['score']}\nEvent ID: {alert['event_id']}"
        }
        async with session.post(self.slack_webhook_url, json=payload) as resp:
            if resp.status >= 400:
                print(f"Slack notification failed: {resp.status}")

    async def notify_email(self, session: aiohttp.ClientSession, alert: dict) -> None:
        if not self.sendgrid_api_key:
            print(f"[MOCK EMAIL] {alert['severity']} alert for {alert['event_id']}")
            return

        headers = {
            "Authorization": f"Bearer {self.sendgrid_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "personalizations": [
                {"to": [{"email": os.getenv("ALERT_EMAIL_TO", "admin@example.com")}]}
            ],
            "from": {"email": os.getenv("ALERT_EMAIL_FROM", "system@example.com")},
            "subject": f"[{alert['severity']}] Anomaly Alert",
            "content": [
                {
                    "type": "text/plain",
                    "value": f"Anomaly score: {alert['score']}\nEvent ID: {alert['event_id']}",
                }
            ],
        }
        async with session.post(
            "https://api.sendgrid.com/v3/mail/send", headers=headers, json=payload
        ) as resp:
            if resp.status >= 400:
                print(f"SendGrid email failed: {resp.status}")

    async def notify_pagerduty(self, session: aiohttp.ClientSession, alert: dict) -> None:
        if not self.pagerduty_routing_key:
            print(f"[MOCK PAGERDUTY] {alert['severity']} alert for {alert['event_id']}")
            return

        payload = {
            "routing_key": self.pagerduty_routing_key,
            "event_action": "trigger",
            "payload": {
                "summary": f"CRITICAL Anomaly: {alert['score']}",
                "source": alert.get("source_id", "ML-Streaming-Pipeline"),
                "severity": "critical",
            },
        }
        async with session.post("https://events.pagerduty.com/v2/enqueue", json=payload) as resp:
            if resp.status >= 400:
                print(f"PagerDuty trigger failed: {resp.status}")

    async def run(self) -> None:
        self.running = True
        self.consumer.subscribe([self.topic])
        print("NotificationAgent started...")

        async with aiohttp.ClientSession() as session:
            try:
                while self.running:
                    msg = self.consumer.poll(1.0)
                    if msg is None or msg.error():
                        await asyncio.sleep(0.01)
                        continue

                    val = msg.value()
                    if val is None:
                        continue

                    try:
                        alert = json.loads(val.decode("utf-8"))
                        severity = alert.get("severity")

                        tasks = []
                        if severity in ["MEDIUM", "HIGH", "CRITICAL"]:
                            tasks.append(self.notify_slack(session, alert))

                        if severity in ["HIGH", "CRITICAL"]:
                            tasks.append(self.notify_email(session, alert))

                        if severity == "CRITICAL":
                            tasks.append(self.notify_pagerduty(session, alert))

                        if tasks:
                            await asyncio.gather(*tasks)

                        self.consumer.commit(message=msg)
                    except Exception as e:
                        print(f"Notification error: {e}")
            finally:
                self.running = False
                self.consumer.close()

    def stop(self) -> None:
        self.running = False


if __name__ == "__main__":
    agent = NotificationAgent()
    asyncio.run(agent.run())
