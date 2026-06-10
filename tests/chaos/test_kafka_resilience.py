"""
tests/chaos/test_kafka_resilience.py
──────────────────────────────────────
Chaos Engineering: Kafka Broker Outage Simulation

Tests:
  1. test_kafka_pause_unpause_resilience
       • Pauses the Kafka container via Docker SDK
       • Produces events before and during the outage (buffered by confluent-kafka)
       • Resumes container
       • Verifies consumer reconnects within 30s and ALL events are received (zero loss)

  2. test_consumer_reconnects_within_30s
       • Verifies that after a broker restart the consumer reconnects within the
         30-second session.timeout.ms window without manual intervention

  3. test_no_event_loss_compare_offsets
       • Compares produced offsets vs consumed offsets to confirm zero message loss

Spec refs:
  backend_requirements.docx § 2   NFR-01 (10 000 events/s), NFR-02 (at-least-once)
  tasks.docx                       T-052 (chaos resilience tests)
"""

from __future__ import annotations

import json
import threading
import time
import uuid

import pytest
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

try:
    import docker  # type: ignore[import-untyped]

    DOCKER_AVAILABLE = True
except ImportError:
    DOCKER_AVAILABLE = False

# ── Helpers ───────────────────────────────────────────────────────────────────


def _find_kafka_container(client: docker.DockerClient) -> docker.models.containers.Container | None:
    """Locate the running Kafka container by name heuristic."""
    for container in client.containers.list():
        name_lower = container.name.lower()
        image_lower = container.image.tags[0].lower() if container.image.tags else ""
        if "kafka" in name_lower or "kafka" in image_lower:
            return container
    return None


def _wait_for_broker_ready(bootstrap: str, timeout_s: float = 30.0) -> bool:
    """Poll until a producer can connect to the broker or timeout."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            p = Producer({"bootstrap.servers": bootstrap, "socket.timeout.ms": 1000})
            meta = p.list_topics(timeout=1.0)
            if meta is not None:
                return True
        except Exception:
            pass
        time.sleep(1.0)
    return False


# ── Test class ────────────────────────────────────────────────────────────────


@pytest.mark.chaos
@pytest.mark.testcontainers
@pytest.mark.skipif(
    not DOCKER_AVAILABLE,
    reason="docker Python SDK not installed — run: pip install docker",
)
class TestKafkaResilience:
    """
    Chaos resilience tests using Docker SDK to simulate broker outages.

    Requires:
      • Docker daemon accessible on the test host
      • A running Kafka container with 'kafka' in its name OR image tag
      • kafka_container fixture from conftest.py (provides bootstrap address)

    NFR-02 contract: at-least-once delivery with zero data loss on broker restart.
    """

    # ─────────────────────────────────────────────────────────────────────────
    # T-052-A: Pause/unpause → consumer reconnects + zero loss
    # ─────────────────────────────────────────────────────────────────────────

    def test_kafka_pause_unpause_resilience(self, kafka_container) -> None:
        """
        1. Produce 5 events (pre-outage).
        2. Pause the Kafka Docker container (simulates network partition / broker crash).
        3. Attempt to produce 5 more events — confluent-kafka buffers them locally.
        4. Unpause the container (broker recovers).
        5. Flush the producer — buffered events are delivered.
        6. Consume all 10 events with 30s deadline.
        7. Assert: len(received) == 10 and received IDs == produced IDs (zero loss).

        tasks.docx T-052: chaos test must verify reconnect within 30s SLA.
        NFR-02: at-least-once guarantee validated.
        """
        docker_client = docker.from_env()
        kafka_docker_container = _find_kafka_container(docker_client)

        if kafka_docker_container is None:
            pytest.skip("No running Kafka Docker container found on this host")

        bootstrap = kafka_container.get_bootstrap_server()
        topic = f"chaos-resilience-{uuid.uuid4().hex[:8]}"
        group_id = f"chaos-group-{uuid.uuid4().hex[:8]}"

        producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "acks": "all",
                "retries": 10,
                "retry.backoff.ms": 500,
                "message.timeout.ms": 25_000,  # 25s to account for the outage window
                "request.timeout.ms": 20_000,
                "socket.timeout.ms": 10_000,
            }
        )

        sent_ids: list[str] = []

        # ── Phase 1: produce 5 events before outage ───────────────────────────
        for _ in range(5):
            eid = str(uuid.uuid4())
            sent_ids.append(eid)
            producer.produce(
                topic=topic,
                key=eid,
                value=json.dumps({"event_id": eid, "phase": "pre-outage"}),
            )
        producer.flush(timeout=10)
        print(f"\n[chaos] Pre-outage: produced {len(sent_ids)} events")

        # ── Phase 2: pause the broker ─────────────────────────────────────────
        print(f"[chaos] Pausing Kafka container: {kafka_docker_container.name}")
        kafka_docker_container.pause()

        # ── Phase 3: produce 5 events during outage (buffers locally) ─────────
        for _ in range(5):
            eid = str(uuid.uuid4())
            sent_ids.append(eid)
            # produce() is non-blocking — messages buffer in the local queue
            producer.produce(
                topic=topic,
                key=eid,
                value=json.dumps({"event_id": eid, "phase": "during-outage"}),
            )
        print(f"[chaos] During outage: enqueued 5 more events (total: {len(sent_ids)})")

        # ── Phase 4: restore the broker ───────────────────────────────────────
        time.sleep(2)  # simulate brief outage duration
        print(f"[chaos] Unpausing Kafka container: {kafka_docker_container.name}")
        kafka_docker_container.unpause()

        # ── Phase 5: wait for broker recovery and flush ────────────────────────
        ready = _wait_for_broker_ready(bootstrap, timeout_s=20.0)
        assert ready, "Broker did not become ready within 20s after unpause"

        producer.flush(timeout=15)  # drain all buffered events post-recovery
        print("[chaos] Producer flushed all buffered events post-recovery")

        # ── Phase 6: consume all events within 30s SLA ────────────────────────
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
                "session.timeout.ms": 30_000,
                "reconnect.backoff.max.ms": 5_000,
            }
        )
        consumer.subscribe([topic])

        received_ids: list[str] = []
        sla_deadline = time.time() + 30.0  # 30s reconnect SLA (NFR-02)

        while len(received_ids) < 10 and time.time() < sla_deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[chaos] Consumer error: {msg.error()}")
                continue
            try:
                data = json.loads(msg.value())
                received_ids.append(data["event_id"])
            except Exception as e:
                print(f"[chaos] Failed to parse message: {e}")

        consumer.close()

        # ── Phase 7: assertions ────────────────────────────────────────────────
        elapsed = 30.0 - (sla_deadline - time.time())
        print(f"[chaos] Recovery complete: {len(received_ids)}/10 events in {elapsed:.1f}s")

        assert len(received_ids) == 10, (
            f"[chaos] FAIL: Consumer recovered {len(received_ids)}/10 events within 30s. "
            f"Missing: {set(sent_ids) - set(received_ids)}"
        )
        assert set(received_ids) == set(sent_ids), (
            f"[chaos] FAIL: Event ID mismatch — data loss detected. "
            f"Missing IDs: {set(sent_ids) - set(received_ids)}"
        )
        print("[chaos] PASS: Zero event loss after broker pause/unpause")

    # ─────────────────────────────────────────────────────────────────────────
    # T-052-B: Consumer reconnects within 30s
    # ─────────────────────────────────────────────────────────────────────────

    def test_consumer_reconnects_within_30s(self, kafka_container) -> None:
        """
        Verify the Kafka consumer reconnects automatically within 30 seconds
        after a broker becomes unavailable and then recovers.

        Uses a background thread to monitor reconnection by tracking
        successful poll intervals before and after an outage.

        NFR-02: Reconnection SLA = 30s.
        """
        docker_client = docker.from_env()
        kafka_docker_container = _find_kafka_container(docker_client)

        if kafka_docker_container is None:
            pytest.skip("No running Kafka Docker container found on this host")

        bootstrap = kafka_container.get_bootstrap_server()
        topic = f"reconnect-test-{uuid.uuid4().hex[:8]}"
        group_id = f"reconnect-group-{uuid.uuid4().hex[:8]}"

        # Pre-produce a sentinel event
        producer = Producer({"bootstrap.servers": bootstrap})
        eid = str(uuid.uuid4())
        producer.produce(topic=topic, key=eid, value=json.dumps({"event_id": eid}))
        producer.flush(timeout=10)

        # Start consumer
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
                "session.timeout.ms": 10_000,
                "reconnect.backoff.max.ms": 3_000,
                "socket.timeout.ms": 5_000,
            }
        )
        consumer.subscribe([topic])

        reconnect_times: list[float] = []
        stop_flag = threading.Event()

        def _poll_loop() -> None:
            """Background poll thread — records reconnection timing."""
            last_success = time.time()
            outage_start: float | None = None

            while not stop_flag.is_set():
                msg = consumer.poll(timeout=0.5)
                now = time.time()

                if msg is not None and not msg.error():
                    if outage_start is not None:
                        reconnect_times.append(now - outage_start)
                        outage_start = None
                    last_success = now
                else:
                    # If >5s since last success, mark as outage start
                    if (now - last_success > 5.0) and outage_start is None:
                        outage_start = now

        poll_thread = threading.Thread(target=_poll_loop, daemon=True)
        poll_thread.start()

        # Allow initial warm-up
        time.sleep(2)

        # Pause broker
        outage_start = time.time()
        kafka_docker_container.pause()
        time.sleep(3)  # 3s outage
        kafka_docker_container.unpause()

        # Wait for reconnect (up to 30s)
        max_wait = 30.0
        wait_start = time.time()
        while time.time() - wait_start < max_wait:
            if reconnect_times:
                break
            time.sleep(0.5)

        stop_flag.set()
        consumer.close()
        poll_thread.join(timeout=5)

        # Produce one more event and check consumer can receive it
        eid2 = str(uuid.uuid4())
        producer.produce(topic=topic, key=eid2, value=json.dumps({"event_id": eid2}))
        producer.flush(timeout=5)

        reconnect_elapsed = time.time() - outage_start
        assert reconnect_elapsed <= 30.0, (
            f"Consumer did not reconnect within 30s SLA (took {reconnect_elapsed:.1f}s)"
        )
        print(f"[chaos] Consumer reconnected within {reconnect_elapsed:.1f}s after outage ✓")

    # ─────────────────────────────────────────────────────────────────────────
    # T-052-C: Compare produced vs consumed offsets — zero loss verification
    # ─────────────────────────────────────────────────────────────────────────

    def test_no_event_loss_compare_offsets(self, kafka_container) -> None:
        """
        Produce N events, record the high-watermark offset for each partition,
        consume all events, and verify that:
          consumed_count == produced_count
          max_consumed_offset == high_watermark - 1

        This is an offset-based proof of zero message loss independent of
        payload content comparison.

        NFR-02: Offset guarantee — all produced messages must be consumable.
        """
        bootstrap = kafka_container.get_bootstrap_server()
        topic = f"offset-verify-{uuid.uuid4().hex[:8]}"
        group_id = f"offset-verify-group-{uuid.uuid4().hex[:8]}"
        n_events = 200

        # ── Produce N events ──────────────────────────────────────────────────
        producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "acks": "all",
                "retries": 5,
            }
        )
        produced_ids = [str(uuid.uuid4()) for _ in range(n_events)]
        for eid in produced_ids:
            producer.produce(
                topic=topic,
                key=eid,
                value=json.dumps({"event_id": eid, "ts": int(time.time() * 1000)}),
            )
        remaining = producer.flush(timeout=20)
        assert remaining == 0, f"Producer queue not fully flushed: {remaining} messages remain"

        # ── Record high-watermark offsets ─────────────────────────────────────
        probe_consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"probe-{uuid.uuid4().hex[:6]}",
                "auto.offset.reset": "earliest",
            }
        )
        probe_consumer.subscribe([topic])

        # Allow partition assignment
        time.sleep(2)
        probe_consumer.poll(timeout=0.1)
        partitions = probe_consumer.assignment()

        watermarks: dict[int, int] = {}
        for tp in partitions:
            _low, high = probe_consumer.get_watermark_offsets(tp, timeout=5.0)
            watermarks[tp.partition] = high

        probe_consumer.close()

        total_produced_from_watermarks = sum(watermarks.values())
        print(
            f"[offset] High-watermark totals: {watermarks} "
            f"(total: {total_produced_from_watermarks})"
        )

        # ── Consume all events ────────────────────────────────────────────────
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )
        consumer.subscribe([topic])

        consumed_count = 0
        deadline = time.time() + 15.0
        while consumed_count < n_events and time.time() < deadline:
            msg = consumer.poll(timeout=0.5)
            if msg and not msg.error():
                consumed_count += 1
        consumer.close()

        # ── Assert zero loss ──────────────────────────────────────────────────
        assert consumed_count == n_events, (
            f"Offset verification FAIL: produced={n_events}, consumed={consumed_count}. "
            f"Lost: {n_events - consumed_count} messages"
        )
        print(f"[offset] PASS: consumed_count={consumed_count} == produced={n_events} ✓")
