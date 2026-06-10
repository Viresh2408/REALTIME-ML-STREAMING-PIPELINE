import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import asyncpg
import redis
from confluent_kafka import Consumer, Producer

# Ensure project root is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.alerting import AlertEvent, BurstDetector, SeverityClassifier, SilenceManager
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
                "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
                "enable.auto.commit": False,
            }
        )
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})

        # DB connection
        self.db_url = os.getenv(
            "DATABASE_URL", "postgresql://worker_rw:WorkerRw_SecurePass2!@localhost:5432/anomaly_db"
        )
        if self.db_url.startswith("postgresql+asyncpg://"):
            self.db_url = self.db_url.replace("postgresql+asyncpg://", "postgresql://")

        # Redis connection
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.redis_client = redis.Redis.from_url(redis_url)

        # Alerting pipeline helpers
        self.classifier = SeverityClassifier()
        self.burst_detector = BurstDetector(self.redis_client)
        self.silence_manager = SilenceManager(self.redis_client)

        self.running = False

    async def run(self) -> None:
        self.running = True
        self.consumer.subscribe([self.in_topic])
        print("AlertAgent started...")

        # Create TimescaleDB pool
        pool = await asyncpg.create_pool(self.db_url)
        print("TimescaleDB pool connected.")

        try:
            while self.running:
                # Poll in a non-blocking way using an executor or asyncio.sleep for context yield
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

                    event = json.loads(val.decode("utf-8"))
                    event_id = event.get("event_id")
                    source_id = event.get("source_id")
                    score = event.get("anomaly_score", 0.0)
                    is_anomaly = event.get("is_anomaly", False)

                    # Only process anomalies
                    if not is_anomaly:
                        self.consumer.commit(message=msg, asynchronous=True)
                        continue

                    # 1. Silencing check
                    if self.silence_manager.is_silenced(source_id):
                        print(f"Alert suppressed for source_id={source_id} (active silence rule)")
                        self.consumer.commit(message=msg, asynchronous=True)
                        continue

                    # 2. Burst counting
                    # If score triggers HIGH or above, record alert to zset sliding window
                    high_thresh = float(os.getenv("SEVERITY_THRESH_HIGH", "0.90"))
                    is_high_or_above = score >= high_thresh

                    alert_id = str(uuid.uuid4())
                    if is_high_or_above:
                        burst_count = self.burst_detector.record_alert(source_id, alert_id)
                    else:
                        burst_count = self.burst_detector.get_count(source_id)

                    # 3. Classify severity
                    severity = self.classifier.classify(score, burst_count)

                    # Skip low/no severity alerts
                    if not self.classifier.is_actionable(severity):
                        self.consumer.commit(message=msg, asynchronous=True)
                        continue

                    # 4. Construct canonical AlertEvent
                    alert = AlertEvent(
                        alert_id=alert_id,
                        event_id=event_id,
                        source_id=source_id,
                        score=score,
                        severity=severity,
                        burst_count=burst_count,
                        created_at=datetime.now(timezone.utc),
                    )

                    # 5. Write to TimescaleDB
                    async with pool.acquire() as conn:
                        await conn.execute(
                            """
                            INSERT INTO anomaly.alerts
                                (alert_id, source_id, severity, status, score, created_at)
                            VALUES ($1, $2, $3, 'ACTIVE', $4, $5)
                            ON CONFLICT DO NOTHING
                            """,
                            uuid.UUID(alert.alert_id),
                            alert.source_id,
                            alert.severity.value,
                            alert.score,
                            alert.created_at,
                        )
                    print(
                        f"Saved active alert {alert.alert_id} (severity={alert.severity.value}) to DB"
                    )

                    # 6. Publish to Kafka topic 'alerts'
                    self.producer.produce(
                        topic=self.out_topic,
                        key=alert.alert_id.encode("utf-8"),
                        value=alert.model_dump_json().encode("utf-8"),
                    )
                    self.producer.poll(0)
                    print(f"Published alert event {alert.alert_id} to topic {self.out_topic}")

                    self.consumer.commit(message=msg, asynchronous=True)

                except Exception as e:
                    print(f"Error processing scored event: {e}")
                    # Allow loop to continue
                    await asyncio.sleep(0.01)

        finally:
            self.running = False
            self.consumer.close()
            self.producer.flush()
            await pool.close()

    def stop(self) -> None:
        self.running = False


if __name__ == "__main__":
    agent = AlertAgent()
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        print("Stopping AlertAgent...")
