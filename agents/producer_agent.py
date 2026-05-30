import asyncio
import json
import os
import sys
import time
import uuid
from typing import Any

import pandas as pd
import uvicorn
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Request

# Ensure project root is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.env_loader import load_env

load_env()

app = FastAPI(title="Producer Agent Webhook")


class ProducerAgent:
    def __init__(self) -> None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "batch.size": 65536,
                "linger.ms": 5,
                "acks": "1",
            }
        )
        self.topic = os.getenv("KAFKA_RAW_EVENTS_TOPIC", "raw-events")
        self.running = False

    def _delivery_report(self, err: Any, msg: Any) -> None:
        if err is not None:
            print(f"Failed to deliver message: {err}")

    async def replay_cicids(self, events_per_second: int = 100) -> None:
        """Async loop to replay CICIDS dataset at controlled rate."""
        self.running = True
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_path = os.path.join(base_dir, "ml", "data", "raw", "cicids2017.csv")

        if not os.path.exists(data_path):
            print(f"Data not found at {data_path}. Replay skipped.")
            return

        chunksize = 1000
        interval = 1.0 / events_per_second
        print(f"Starting replay of {data_path} at {events_per_second} eps.")

        try:
            for chunk in pd.read_csv(data_path, chunksize=chunksize):
                if not self.running:
                    break
                for _, row in chunk.iterrows():
                    if not self.running:
                        break

                    event_dict = {
                        "event_id": str(uuid.uuid4()),
                        "source_id": "cicids-replay",
                        "event_type": "network_flow",
                        "features": row.to_dict(),
                        "event_time": int(time.time() * 1000),
                    }

                    self.producer.produce(
                        topic=self.topic,
                        key=event_dict["event_id"],
                        value=json.dumps(event_dict),
                        on_delivery=self._delivery_report,
                    )
                    self.producer.poll(0)
                    await asyncio.sleep(interval)
        except Exception as e:
            print(f"Replay error: {e}")
        finally:
            self.producer.flush()

    def stop(self) -> None:
        self.running = False


agent = ProducerAgent()


@app.post("/webhook")
async def webhook(request: Request) -> dict[str, str]:
    """HTTP webhook endpoint for external event injection."""
    try:
        data = await request.json()
        event_dict = {
            "event_id": str(uuid.uuid4()),
            "source_id": data.get("source_id", "webhook"),
            "event_type": data.get("event_type", "custom"),
            "features": data.get("features", {}),
            "event_time": int(time.time() * 1000),
        }
        agent.producer.produce(
            topic=agent.topic,
            key=event_dict["event_id"],
            value=json.dumps(event_dict),
            on_delivery=agent._delivery_report,
        )
        agent.producer.poll(0)
        return {"status": "success", "event_id": event_dict["event_id"]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def main() -> None:
    # Run the replay loop as a background task
    asyncio.create_task(agent.replay_cicids(events_per_second=100))
    # Start FastAPI server
    config = uvicorn.Config(app, host="0.0.0.0", port=8082)
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
