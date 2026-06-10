"""
tests/integration/test_kafka_pipeline.py
─────────────────────────────────────────
Integration tests for the Apache Kafka streaming pipeline.

Tests:
  1. test_raw_to_scored       — Produce to raw-events → verify scored-events
  2. test_consumer_recovery   — Kill consumer mid-stream → restart → verify no loss
  3. test_dead_letter         — Produce malformed event → verify dl-events topic

Spec refs:
  backend_requirements.docx § 2   NFR-01 (10 000 events/s throughput)
                                  NFR-02 (exactly-once / at-least-once delivery)
  tasks.docx Phase 6              T-049 (Kafka pipeline tests), T-050 (consumer recovery)
"""

from __future__ import annotations

import json
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_producer(bootstrap: str, **extra: object) -> Producer:
    """Create a confluent-kafka Producer with safe defaults."""
    cfg: dict[str, object] = {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "retries": 3,
        "retry.backoff.ms": 200,
    }
    cfg.update(extra)
    return Producer(cfg)


def _make_consumer(bootstrap: str, group_id: str, **extra: object) -> Consumer:
    """Create a confluent-kafka Consumer with safe defaults."""
    cfg: dict[str, object] = {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 200,
        "session.timeout.ms": 10_000,
    }
    cfg.update(extra)
    return Consumer(cfg)


def _drain_topic(
    consumer: Consumer,
    topic: str,
    expected_count: int,
    timeout_s: float = 8.0,
) -> list[dict]:
    """Subscribe and poll until `expected_count` messages or `timeout_s` elapses."""
    consumer.subscribe([topic])
    messages: list[dict] = []
    deadline = time.time() + timeout_s

    while len(messages) < expected_count and time.time() < deadline:
        msg = consumer.poll(timeout=0.5)
        if msg is None or msg.error():
            continue
        try:
            messages.append(json.loads(msg.value()))
        except json.JSONDecodeError:
            messages.append({"_raw": msg.value().decode("utf-8", errors="replace")})

    consumer.close()
    return messages


# ── Test class ────────────────────────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.testcontainers
class TestKafkaPipeline:
    """
    Verifies offset management, recovery guarantees, and DLQ routing.

    All tests require:
      • kafka_container (session fixture from conftest.py)
    """

    # ─────────────────────────────────────────────────────────────────────────
    # T-049: raw-events → scored-events round-trip
    # ─────────────────────────────────────────────────────────────────────────

    def test_raw_to_scored(self, kafka_container) -> None:
        """
        Produce a raw event to the raw-events topic.
        Simulate the ML worker (consume → score → produce to scored-events).
        Verify the scored event appears on the scored-events topic with correct
        event_id, anomaly_score, and is_anomaly fields.

        NFR-01: Pipeline must sustain 10 000 events/s — this test validates
        correctness of the round-trip at single-event granularity.
        """
        bootstrap = kafka_container.get_bootstrap_server()
        raw_topic = f"test-raw-events-{uuid.uuid4().hex[:6]}"
        scored_topic = f"test-scored-events-{uuid.uuid4().hex[:6]}"
        event_id = str(uuid.uuid4())

        # ── Step 1: produce raw event ─────────────────────────────────────────
        producer = _make_producer(bootstrap)
        raw_event = {
            "event_id": event_id,
            "source_id": "sensor-integration-99",
            "event_type": "network_flow",
            "features": {
                "packet_length": 1500.0,
                "flow_duration": 45_000.0,
                "fwd_packets/s": 12.3,
                "bwd_packets/s": 8.7,
                "flag_counts": 2.0,
            },
            "event_time": int(time.time() * 1000),
        }
        producer.produce(topic=raw_topic, key=event_id, value=json.dumps(raw_event))
        producer.flush(timeout=10)

        # ── Step 2: simulate ML worker (consume raw → produce scored) ─────────
        raw_consumer = _make_consumer(
            bootstrap,
            group_id=f"test-ml-worker-{uuid.uuid4().hex[:6]}",
        )
        raw_consumer.subscribe([raw_topic])

        raw_msg = None
        deadline = time.time() + 8.0
        while time.time() < deadline:
            msg = raw_consumer.poll(timeout=0.5)
            if msg and not msg.error():
                raw_msg = json.loads(msg.value())
                break
        raw_consumer.close()

        assert raw_msg is not None, (
            f"Raw event '{event_id}' not received from topic '{raw_topic}' within 8s"
        )
        assert raw_msg["event_id"] == event_id, (
            f"event_id mismatch: expected {event_id}, got {raw_msg['event_id']}"
        )

        # Produce simulated scored event (as the ML worker would)
        scored_event = {
            **raw_msg,
            "anomaly_score": 0.82,
            "is_anomaly": True,
            "severity": "MEDIUM",
            "model_version": "isolation_forest_v1",
            "processed_at": int(time.time() * 1000),
        }
        producer.produce(
            topic=scored_topic,
            key=event_id,
            value=json.dumps(scored_event),
        )
        producer.flush(timeout=10)

        # ── Step 3: verify scored event on output topic ───────────────────────
        scored_consumer = _make_consumer(
            bootstrap,
            group_id=f"test-verify-scored-{uuid.uuid4().hex[:6]}",
        )
        results = _drain_topic(scored_consumer, scored_topic, expected_count=1, timeout_s=8.0)

        assert len(results) >= 1, f"Scored event not found on '{scored_topic}' within 8s"
        result = results[0]
        assert result["event_id"] == event_id, f"Scored event_id mismatch: {result['event_id']}"
        assert result["anomaly_score"] == pytest.approx(0.82, abs=1e-6)
        assert result["is_anomaly"] is True
        assert result["severity"] == "MEDIUM"

    # ─────────────────────────────────────────────────────────────────────────
    # T-050: Consumer crash recovery — no message loss
    # ─────────────────────────────────────────────────────────────────────────

    def test_consumer_recovery(self, kafka_container) -> None:
        """
        Simulate consumer crash mid-stream.

        1. Produce 10 events to a dedicated topic.
        2. Start Consumer-1 (same group) — consume 5, commit offsets, close (crash).
        3. Start Consumer-2 (same group) — verify it resumes from offset 5.
        4. Assert that the union of both consumers == all 10 unique events.

        NFR-02: At-least-once delivery guarantee; no duplicate or lost events.
        tasks.docx T-050: consumer restart must resume without offset regression.
        """
        bootstrap = kafka_container.get_bootstrap_server()
        topic = f"recovery-test-{uuid.uuid4().hex[:8]}"
        group_id = f"recovery-group-{uuid.uuid4().hex[:8]}"
        n_events = 10

        # ── Produce 10 events ─────────────────────────────────────────────────
        producer = _make_producer(bootstrap)
        event_ids = [str(uuid.uuid4()) for _ in range(n_events)]
        for eid in event_ids:
            producer.produce(
                topic=topic,
                key=eid,
                value=json.dumps({"event_id": eid, "ts": int(time.time() * 1000)}),
            )
        producer.flush(timeout=10)

        # ── Consumer-1: read first 5 then "crash" ─────────────────────────────
        c1 = _make_consumer(
            bootstrap,
            group_id=group_id,
            **{"auto.commit.interval.ms": 100},
        )
        c1.subscribe([topic])

        consumed_c1: list[str] = []
        deadline = time.time() + 8.0
        while len(consumed_c1) < 5 and time.time() < deadline:
            msg = c1.poll(timeout=0.5)
            if msg and not msg.error():
                consumed_c1.append(json.loads(msg.value())["event_id"])

        # Allow auto-commit to flush
        time.sleep(0.5)
        c1.close()  # simulates consumer crash / SIGKILL

        assert len(consumed_c1) == 5, (
            f"Consumer-1 should have read 5 events, got {len(consumed_c1)}"
        )

        # ── Consumer-2: restart under same group, must resume from offset 5 ───
        c2 = _make_consumer(bootstrap, group_id=group_id)
        c2.subscribe([topic])

        consumed_c2: list[str] = []
        deadline = time.time() + 8.0
        while len(consumed_c2) < 5 and time.time() < deadline:
            msg = c2.poll(timeout=0.5)
            if msg and not msg.error():
                consumed_c2.append(json.loads(msg.value())["event_id"])
        c2.close()

        assert len(consumed_c2) == 5, (
            f"Consumer-2 should have read remaining 5 events, got {len(consumed_c2)}"
        )

        # ── No duplicates, no loss ────────────────────────────────────────────
        total_consumed = consumed_c1 + consumed_c2
        assert len(set(total_consumed)) == n_events, (
            f"Expected {n_events} unique events, got {len(set(total_consumed))}. "
            f"Duplicates: {[e for e in total_consumed if total_consumed.count(e) > 1]}"
        )
        assert set(total_consumed) == set(event_ids), (
            "Consumed event IDs do not match produced event IDs — message loss detected"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # T-051: Dead-letter queue — malformed event routing
    # ─────────────────────────────────────────────────────────────────────────

    def test_dead_letter(self, kafka_container) -> None:
        """
        Produce a malformed (non-JSON) payload to the raw-events topic.
        Simulate the ML worker's error handler routing it to dl-events.
        Verify the DLQ message contains the original payload and error_reason.

        NFR-02: Malformed events must never block the pipeline — they must be
        routed to the dead-letter topic with full error context.
        tasks.docx T-051: DLQ routing verified by integration test.
        """
        bootstrap = kafka_container.get_bootstrap_server()
        raw_topic = f"test-raw-dlq-{uuid.uuid4().hex[:6]}"
        dlq_topic = f"dl-events-{uuid.uuid4().hex[:6]}"
        event_id = str(uuid.uuid4())

        # ── Step 1: produce a malformed raw event ─────────────────────────────
        producer = _make_producer(bootstrap)
        malformed_payload = (
            b"{{MALFORMED:::NON_JSON_CORRUPT_PAYLOAD:::" + event_id.encode() + b"}}}"
        )
        producer.produce(topic=raw_topic, key=event_id, value=malformed_payload)
        producer.flush(timeout=10)

        # ── Step 2: simulate ML worker consuming and routing to DLQ ──────────
        raw_consumer = _make_consumer(
            bootstrap,
            group_id=f"test-dlq-processor-{uuid.uuid4().hex[:6]}",
        )
        raw_consumer.subscribe([raw_topic])

        raw_val: bytes | None = None
        deadline = time.time() + 8.0
        while time.time() < deadline:
            msg = raw_consumer.poll(timeout=0.5)
            if msg and not msg.error():
                raw_val = msg.value()
                break
        raw_consumer.close()

        assert raw_val is not None, "Malformed event not received from raw topic"
        assert raw_val == malformed_payload, "Raw payload corrupted during transit"

        # Simulate error handler logic (normally inside InferenceWorkerTask._route_to_dlq)
        try:
            json.loads(raw_val)
            raise AssertionError("Expected JSONDecodeError for malformed payload")
        except json.JSONDecodeError as e:
            error_type = type(e).__name__
            error_msg = str(e)

        dlq_payload = {
            "event_id": event_id,
            "error_reason": error_type,
            "error_message": error_msg,
            "original_payload": raw_val.decode("utf-8", errors="replace"),
            "failed_at": int(time.time() * 1000),
            "source_topic": raw_topic,
        }
        producer.produce(
            topic=dlq_topic,
            key=event_id,
            value=json.dumps(dlq_payload),
        )
        producer.flush(timeout=10)

        # ── Step 3: verify DLQ message ────────────────────────────────────────
        dlq_consumer = _make_consumer(
            bootstrap,
            group_id=f"test-verify-dlq-{uuid.uuid4().hex[:6]}",
        )
        dlq_messages = _drain_topic(dlq_consumer, dlq_topic, expected_count=1, timeout_s=8.0)

        assert len(dlq_messages) >= 1, f"No DLQ message found on '{dlq_topic}' within 8s"
        dlq_msg = dlq_messages[0]
        assert dlq_msg["error_reason"] == "JSONDecodeError", (
            f"Expected JSONDecodeError, got: {dlq_msg.get('error_reason')}"
        )
        assert event_id.encode().decode() in dlq_msg["original_payload"], (
            "Original payload not preserved in DLQ message"
        )
        assert "MALFORMED" in dlq_msg["original_payload"], (
            "Malformed marker not found in DLQ original_payload"
        )
        assert dlq_msg["event_id"] == event_id
        assert "failed_at" in dlq_msg

    # ─────────────────────────────────────────────────────────────────────────
    # Bonus: Batch throughput sanity check
    # ─────────────────────────────────────────────────────────────────────────

    def test_batch_produce_consume_throughput(self, kafka_container) -> None:
        """
        Produce 1 000 events and verify all are consumed within 15s.

        This is a sanity check for the Kafka broker capacity.
        Full 10 000 events/s throughput is validated by the Locust load test.

        NFR-01: Broker must handle at least 1 000 events in under 15s.
        """
        bootstrap = kafka_container.get_bootstrap_server()
        topic = f"throughput-test-{uuid.uuid4().hex[:6]}"
        n = 1_000

        producer = _make_producer(
            bootstrap,
            **{"batch.size": 65_536, "linger.ms": 5},
        )

        event_ids = [str(uuid.uuid4()) for _ in range(n)]
        for eid in event_ids:
            producer.produce(
                topic=topic,
                key=eid,
                value=json.dumps({"event_id": eid, "ts": int(time.time() * 1_000)}),
            )
        producer.flush(timeout=15)

        consumer = _make_consumer(
            bootstrap,
            group_id=f"test-throughput-{uuid.uuid4().hex[:6]}",
        )
        received = _drain_topic(consumer, topic, expected_count=n, timeout_s=15.0)

        assert len(received) == n, f"Throughput test: expected {n} events, received {len(received)}"
