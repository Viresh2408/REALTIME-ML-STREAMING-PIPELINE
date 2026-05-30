import asyncio
import json
import os
import sys
from typing import Any

import asyncpg
from confluent_kafka import Consumer

# Ensure project root is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.env_loader import load_env

load_env()


class TimescaleDBWriterAgent:
    def __init__(self) -> None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("KAFKA_SCORED_EVENTS_TOPIC", "scored-events")

        # asyncpg requires postgresql:// instead of postgresql+asyncpg://
        self.db_url = os.getenv(
            "DATABASE_URL", "postgresql://worker_rw:WorkerRw_SecurePass2!@localhost:5432/anomaly_db"
        )
        if self.db_url.startswith("postgresql+asyncpg://"):
            self.db_url = self.db_url.replace("postgresql+asyncpg://", "postgresql://")

        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": os.getenv("KAFKA_WRITER_GROUP_ID", "timescaledb-writer-group"),
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        self.batch_size = 500
        self.flush_interval = 0.1  # 100ms
        self.running = False
        self.buffer: list[dict[str, Any]] = []

    async def _flush_buffer(self, pool: asyncpg.Pool) -> None:
        """Flushes the buffered events directly to TimescaleDB asynchronously."""
        if not self.buffer:
            return

        batch = self.buffer.copy()
        self.buffer.clear()

        try:
            async with pool.acquire() as conn:
                query = """
                    INSERT INTO anomaly.anomaly_events
                    (event_id, source_id, feature_vector, event_time, anomaly_score, is_anomaly, model_version, processed_at)
                    VALUES ($1, $2, $3, to_timestamp($4 / 1000.0), $5, $6, $7, NOW())
                    ON CONFLICT (event_id, event_time) DO NOTHING
                """

                records = [
                    (
                        e["event_id"],
                        e["source_id"],
                        json.dumps(e.get("features", {})),
                        e["event_time"],
                        e["anomaly_score"],
                        e["is_anomaly"],
                        e["model_version"],
                    )
                    for e in batch
                ]

                await conn.executemany(query, records)
                print(f"Flushed {len(batch)} events to TimescaleDB")

        except Exception as e:
            print(f"Failed to flush to DB: {e}")
            # Real-world system would dead-letter or retry with backoff here

    async def run(self) -> None:
        self.running = True
        self.consumer.subscribe([self.topic])

        # Connect to TimescaleDB
        pool = await asyncpg.create_pool(self.db_url)
        last_flush_time = asyncio.get_event_loop().time()

        print("TimescaleDBWriterAgent started...")
        try:
            while self.running:
                msg = self.consumer.poll(0.01)  # Short poll

                if msg is not None and not msg.error():
                    val = msg.value()
                    if val is not None:
                        event = json.loads(val.decode("utf-8"))
                        self.buffer.append(event)
                        self.consumer.commit(message=msg, asynchronous=True)

                current_time = asyncio.get_event_loop().time()
                time_elapsed = current_time - last_flush_time

                if len(self.buffer) >= self.batch_size or (
                    time_elapsed >= self.flush_interval and self.buffer
                ):
                    await self._flush_buffer(pool)
                    last_flush_time = current_time

        finally:
            self.running = False
            await self._flush_buffer(pool)  # Final flush
            await pool.close()
            self.consumer.close()

    def stop(self) -> None:
        self.running = False


if __name__ == "__main__":
    agent = TimescaleDBWriterAgent()
    asyncio.run(agent.run())
