import asyncio
import json
import os
import sys

import aiohttp
import uvicorn
from confluent_kafka import Consumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# Ensure project root is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.env_loader import load_env

load_env()

app = FastAPI(title="Dashboard Agent Websocket")

class DashboardAgent:
    def __init__(self) -> None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("KAFKA_SCORED_EVENTS_TOPIC", "scored-events")

        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': os.getenv("KAFKA_DASHBOARD_GROUP_ID", "dashboard-agent-group"),
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False
        })

        self.grafana_url = os.getenv("GRAFANA_URL", "http://localhost:3000")
        self.grafana_token = os.getenv("GRAFANA_SERVICE_ACCOUNT_TOKEN", "")
        self.running = False

        self.active_connections: set[WebSocket] = set()

    async def connect_client(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect_client(self, websocket: WebSocket) -> None:
        self.active_connections.discard(websocket)

    async def broadcast(self, message: str) -> None:
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message)
            except WebSocketDisconnect:
                self.disconnect_client(connection)

    async def create_grafana_annotation(self, event: dict) -> None:
        if not self.grafana_token:
            return

        headers = {
            "Authorization": f"Bearer {self.grafana_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "time": event["event_time"],
            "text": f"CRITICAL Anomaly (Score: {event['anomaly_score']})",
            "tags": ["critical", "anomaly", event.get("source_id", "unknown")]
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.grafana_url}/api/annotations", headers=headers, json=payload) as resp:
                    if resp.status >= 400:
                        print(f"Failed to create Grafana annotation: {resp.status}")
        except Exception as e:
            print(f"Grafana API error: {e}")

    async def run(self) -> None:
        self.running = True
        self.consumer.subscribe([self.topic])
        print("DashboardAgent started...")

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
                    event_str = val.decode('utf-8')
                    event = json.loads(event_str)

                    # 1. Broadcast to all active websocket clients
                    await self.broadcast(event_str)

                    # 2. Check for CRITICAL anomaly annotation
                    if event.get("anomaly_score", 0.0) >= 0.95:
                        await self.create_grafana_annotation(event)

                    self.consumer.commit(message=msg)
                except Exception as e:
                    print(f"Dashboard processing error: {e}")
        finally:
            self.running = False
            self.consumer.close()

    def stop(self) -> None:
        self.running = False

agent = DashboardAgent()

@app.websocket("/ws/events")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await agent.connect_client(websocket)
    try:
        while True:
            await websocket.receive_text() # keep alive
    except WebSocketDisconnect:
        agent.disconnect_client(websocket)

async def main() -> None:
    # Run Kafka consumer loop in background
    asyncio.create_task(agent.run())

    # Start WebSocket server
    config = uvicorn.Config(app, host="0.0.0.0", port=8083)
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
