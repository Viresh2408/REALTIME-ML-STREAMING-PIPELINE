"""
Integration tests — Kafka producer/consumer round-trip
Uses testcontainers-python 0.12 to spin up a real Kafka broker.
pytest 8.x + pytest-asyncio 0.23.7
"""
from __future__ import annotations

import json
import time
import uuid
from collections.abc import Generator

import pytest

try:
    from testcontainers.kafka import KafkaContainer

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

SKIP_REASON = "testcontainers or Docker not available in this environment"


@pytest.fixture(scope="module")
def kafka_container() -> Generator:
    if not KAFKA_AVAILABLE:
        pytest.skip(SKIP_REASON)

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as container:
        yield container


@pytest.mark.integration
@pytest.mark.skipif(not KAFKA_AVAILABLE, reason=SKIP_REASON)
class TestKafkaRoundTrip:
    """Integration: produce to raw-events, consume and verify message."""

    def test_produce_and_consume_event(self, kafka_container) -> None:
        from confluent_kafka import Consumer, Producer

        bootstrap = kafka_container.get_bootstrap_server()
        topic = "test-raw-events"
        event_id = str(uuid.uuid4())

        # Producer
        producer = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
        message = {
            "event_id": event_id,
            "source_id": "test-source",
            "feature_vector": [0.1, 0.2, 0.3],
            "timestamp": "2024-01-01T00:00:00Z",
        }
        producer.produce(topic=topic, key=event_id, value=json.dumps(message))
        producer.flush(timeout=10)

        # Consumer
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "test-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic])

        received = None
        deadline = time.time() + 15.0
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg and not msg.error():
                received = json.loads(msg.value())
                break

        consumer.close()

        assert received is not None, "Should have received the message within 15 seconds"
        assert received["event_id"] == event_id
        assert received["source_id"] == "test-source"
        assert len(received["feature_vector"]) == 3

    def test_multiple_events_all_consumed(self, kafka_container) -> None:
        from confluent_kafka import Consumer, Producer

        bootstrap = kafka_container.get_bootstrap_server()
        topic = "test-multi-events"
        n_messages = 10

        producer = Producer({"bootstrap.servers": bootstrap, "acks": "1"})
        sent_ids = set()
        for i in range(n_messages):
            eid = str(uuid.uuid4())
            sent_ids.add(eid)
            producer.produce(topic=topic, key=eid, value=json.dumps({"event_id": eid, "idx": i}))
        producer.flush(timeout=15)

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"test-multi-{uuid.uuid4()}",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic])

        received_ids: set[str] = set()
        deadline = time.time() + 20.0
        while len(received_ids) < n_messages and time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg and not msg.error():
                data = json.loads(msg.value())
                received_ids.add(data["event_id"])

        consumer.close()
        assert received_ids == sent_ids, f"Missing messages: {sent_ids - received_ids}"
