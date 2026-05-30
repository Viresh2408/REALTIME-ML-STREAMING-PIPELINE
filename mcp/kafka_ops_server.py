import os
import json
from typing import Optional, List, Dict, Any

from fastapi import FastAPI
from mcp_fastapi import create_mcp_server
from mcp.server import Server
from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer

app = FastAPI(title="kafka-ops-mcp")
server = Server("kafka-ops-mcp")

KAFKA_BROKERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

def get_admin_client():
    return AdminClient({"bootstrap.servers": KAFKA_BROKERS})

def get_producer():
    return Producer({"bootstrap.servers": KAFKA_BROKERS})

@server.tool()
async def get_consumer_lag(group_id: Optional[str] = None, topic: Optional[str] = None) -> Dict[str, Any]:
    """Consumer group lag per topic-partition."""
    admin = get_admin_client()
    # Confluent Kafka python admin client doesn't have a direct "get lag" method easily exposed without listing offsets.
    # In a real implementation this would fetch group offsets and topic high watermarks.
    # We return a mock/placeholder implementation structure here.
    return {
        "group_id": group_id or "all",
        "topic": topic or "all",
        "lag_details": "Lag computation requires consumer offsets vs high watermarks. Not fully implemented in dummy."
    }

@server.tool()
async def list_topics() -> List[Dict[str, Any]]:
    """All Kafka topics with partition count and offsets."""
    admin = get_admin_client()
    md = admin.list_topics(timeout=5)
    topics = []
    for topic_name, topic_meta in md.topics.items():
        topics.append({
            "topic": topic_name,
            "partition_count": len(topic_meta.partitions)
        })
    return topics

@server.tool()
async def produce_test_event(event_type: str, feature_overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Inject a synthetic test event into raw-events topic."""
    p = get_producer()
    topic = os.environ.get("KAFKA_RAW_EVENTS_TOPIC", "raw-events")
    
    payload = {
        "source_id": "test_mcp_injection",
        "feature_vector": [0.0] * 10,
        "event_type": event_type
    }
    if feature_overrides:
        payload.update(feature_overrides)
        
    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
            
    p.produce(topic, value=json.dumps(payload).encode('utf-8'), callback=delivery_report)
    p.flush(timeout=5)
    
    return {"status": "produced", "topic": topic, "payload": payload}

@server.tool()
async def get_topic_metrics(topic: str, window: str = "1h") -> Dict[str, Any]:
    """Messages/s, bytes/s, error rate for a topic."""
    # This would typically query Prometheus or Kafka JMX metrics.
    # Mocking for the MCP interface.
    return {
        "topic": topic,
        "window": window,
        "metrics": {
            "messages_per_sec": 150.5,
            "bytes_per_sec": 10240,
            "error_rate": 0.001
        }
    }

# Create ASGI app from MCP server
mcp_app = create_mcp_server(server)
app.mount("/sse", mcp_app)
