import asyncio
import json
import os
import sys
import time
import uuid

from confluent_kafka import Consumer, Producer

# Ensure project root is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.env_loader import load_env

load_env()


class AlertAgent:
    def __init__(self) -> None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.in_topic = os.getenv("KAFKA_SCORED_EVENTS_TOPIC", "scored-events")
        self.out_topic = os.getenv("KAFKA_ALERTS_TOPIC", "alerts")

        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": os.getenv("KAFKA_ALERT_GROUP_ID", "alert-agent-group"),
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.running = False

    def _determine_severity(self, score: float) -> str:
        """Thresholds based on workflow.docx Section 4."""
        if score >= 0.95:
            return "CRITICAL"
        elif score >= 0.90:
            return "HIGH"
        elif score >= 0.80:
            return "MEDIUM"
        elif score >= 0.70:
            return "LOW"
        return "NONE"

    async def run(self) -> None:
        self.running = True
        self.consumer.subscribe([self.in_topic])
        print("AlertAgent started...")

        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is None or msg.error():
                    await asyncio.sleep(0.01)
                    continue

                try:
                    val = msg.value()
                    if val is None:
                        continue
                    event = json.loads(val.decode("utf-8"))
                    score = event.get("anomaly_score", 0.0)
                    severity = self._determine_severity(score)

                    if severity != "NONE":
                        alert = {
                            "alert_id": str(uuid.uuid4()),
                            "severity": severity,
                            "event_id": event.get("event_id"),
                            "score": score,
                            "source_id": event.get("source_id"),
                            "created_at": int(time.time() * 1000),
                            "acknowledged": False,
                        }

                        self.producer.produce(
                            topic=self.out_topic, key=alert["alert_id"], value=json.dumps(alert)
                        )
                        self.producer.poll(0)

                    self.consumer.commit(message=msg)
                except Exception as e:
                    print(f"Alert generation error: {e}")
        finally:
            self.running = False
            self.consumer.close()
            self.producer.flush()

    def stop(self) -> None:
        self.running = False


if __name__ == "__main__":
    agent = AlertAgent()
    asyncio.run(agent.run())
