"""
tests/verify_pipeline.py
─────────────────────────
End-to-end pipeline verification.

Checks:
  1. Infrastructure readiness (Kafka, Schema Registry, Prometheus /metrics)
  2. Produce 1 000 raw events to raw-events topic
  3. Consume scored-events topic — all 1 000 must appear within 5 seconds
  4. Query /metrics — inference_latency_seconds P95 < 10 ms
  5. Print a detailed PASS / FAIL report

Usage (from project root):
    python tests/verify_pipeline.py [--bootstrap localhost:9092]
                                    [--metrics  http://localhost:8090]
                                    [--timeout  5]
                                    [--events   1000]

Requires: confluent-kafka, requests
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

# ── deps check ────────────────────────────────────────────────────────────────
try:
    import requests
    from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
except ImportError as e:
    print(f"[FATAL] Missing dependency: {e}")
    print("  pip install confluent-kafka requests")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
METRICS_URL = os.getenv("METRICS_URL", "http://localhost:8090/metrics")
RAW_TOPIC   = os.getenv("KAFKA_RAW_EVENTS_TOPIC", "raw-events")
SCORED_TOPIC = os.getenv("KAFKA_SCORED_EVENTS_TOPIC", "scored-events")
NUM_EVENTS  = 1_000
TIMEOUT_S   = 5.0          # all 1000 scored events must appear within this
P95_LIMIT_S = 0.010        # 10 ms
CONSUMER_GROUP = "verify-pipeline-test"

# ─────────────────────────────────────────────────────────────────────────────
# Result container
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class VerifyResult:
    name: str
    passed: bool
    detail: str
    duration_s: float = 0.0
    extra: dict[str, Any] = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now_ms() -> int:
    return int(time.time() * 1_000)


def _make_event(i: int) -> dict[str, Any]:
    """Build a realistic raw event dict matching the raw_event schema."""
    import random
    rng = random.Random(i)
    return {
        "event_id":   str(uuid.uuid4()),
        "source_id":  f"simulator-{i % 4}",
        "event_type": rng.choice(["network", "server", "financial"]),
        "event_time": _now_ms(),
        "features": {
            "packet_length":    rng.uniform(40, 1500),
            "flow_duration":    rng.uniform(0, 10),
            "fwd_packets/s":    rng.uniform(0, 1000),
            "bwd_packets/s":    rng.uniform(0, 500),
            "flag_counts":      rng.randint(0, 10),
            # Inject occasional anomaly-like values
            "cpu_pct":          rng.uniform(0, 100) if i % 50 != 0 else 99.9,
            "mem_pct":          rng.uniform(20, 80),
            "disk_io_bytes":    rng.uniform(0, 1e7),
            "net_rx_bytes":     rng.uniform(0, 1e8),
            "error_rate":       rng.uniform(0, 0.05) if i % 100 != 0 else 0.95,
            "price":            rng.uniform(100, 500),
            "volume":           rng.uniform(1000, 100000),
            "bid_ask_spread":   rng.uniform(0.01, 2.0),
            "price_return_1m":  rng.gauss(0, 0.01),
            "volume_z_score":   rng.gauss(0, 1),
            "hour_of_day":      rng.randint(0, 23),
            "day_of_week":      rng.randint(0, 6),
            "is_market_hours":  rng.choice([0, 1]),
            "rolling_mean_5m":  rng.uniform(50, 200),
            "rolling_std_5m":   rng.uniform(1, 20),
            "deviation_from_mean": rng.gauss(0, 2),
        },
    }


def _parse_prometheus_histogram(text: str, metric_name: str) -> dict[str, float] | None:
    """
    Parse a prometheus text exposition for a histogram metric.
    Returns dict: {'count': N, 'sum': S, 'buckets': {le: count, ...}}
    """
    buckets: dict[float, float] = {}
    total_count: float | None = None
    total_sum: float | None = None

    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#"):
            continue
        if f"{metric_name}_bucket" in line:
            # e.g. inference_latency_seconds_bucket{le="0.001"} 450
            le_part = line.split('le="')[1].split('"')[0]
            val_part = line.split("} ")[1].split()[0]
            le = float(le_part)  # "+Inf" → inf
            buckets[le] = float(val_part)
        elif f"{metric_name}_count" in line and not line.startswith("#"):
            total_count = float(line.split("} ")[1].split()[0]) if "}" in line else float(line.split(" ")[1])
        elif f"{metric_name}_sum" in line and not line.startswith("#"):
            total_sum = float(line.split("} ")[1].split()[0]) if "}" in line else float(line.split(" ")[1])

    if not buckets or total_count is None:
        return None
    return {"count": total_count, "sum": total_sum or 0.0, "buckets": buckets}


def _percentile_from_histogram(
    buckets: dict[float, float], total_count: float, pct: float
) -> float | None:
    """
    Linear interpolation within histogram buckets to estimate a percentile.
    pct is in [0, 100].
    """
    if total_count == 0:
        return None
    target = total_count * (pct / 100.0)
    sorted_le = sorted(k for k in buckets if k != math.inf)

    prev_le = 0.0
    prev_count = 0.0
    for le in sorted_le:
        count = buckets[le]
        if count >= target:
            # Linear interpolation
            if count == prev_count:
                return le
            frac = (target - prev_count) / (count - prev_count)
            return prev_le + frac * (le - prev_le)
        prev_le = le
        prev_count = count

    return None  # target falls in +Inf bucket


# ─────────────────────────────────────────────────────────────────────────────
# Check 0 — Infrastructure readiness
# ─────────────────────────────────────────────────────────────────────────────

def check_kafka_connectivity(bootstrap: str) -> VerifyResult:
    t0 = time.perf_counter()
    name = "Kafka connectivity"
    try:
        p = Producer({"bootstrap.servers": bootstrap, "socket.timeout.ms": 3000})
        p.list_topics(timeout=5)
        elapsed = time.perf_counter() - t0
        return VerifyResult(name, True, f"Connected to {bootstrap}", elapsed)
    except Exception as exc:
        return VerifyResult(name, False, f"Cannot connect to Kafka at {bootstrap}: {exc}", time.perf_counter() - t0)


def check_metrics_endpoint(metrics_url: str) -> VerifyResult:
    t0 = time.perf_counter()
    name = "Metrics endpoint /metrics"
    try:
        r = requests.get(metrics_url, timeout=5)
        r.raise_for_status()
        elapsed = time.perf_counter() - t0
        has_latency = "inference_latency_seconds" in r.text
        has_events  = "events_processed_total" in r.text
        if has_latency and has_events:
            return VerifyResult(name, True, f"HTTP 200 — expected metrics present ({len(r.text)} bytes)", elapsed)
        else:
            missing = []
            if not has_latency: missing.append("inference_latency_seconds")
            if not has_events:  missing.append("events_processed_total")
            return VerifyResult(name, False, f"Metrics endpoint up but missing: {missing}", elapsed)
    except Exception as exc:
        return VerifyResult(name, False, f"Cannot reach {metrics_url}: {exc}", time.perf_counter() - t0)


def check_topic_exists(bootstrap: str, topic: str) -> VerifyResult:
    t0 = time.perf_counter()
    name = f"Topic '{topic}' exists"
    try:
        p = Producer({"bootstrap.servers": bootstrap})
        meta = p.list_topics(timeout=5)
        if topic in meta.topics:
            parts = len(meta.topics[topic].partitions)
            return VerifyResult(name, True, f"Topic exists ({parts} partitions)", time.perf_counter() - t0)
        else:
            available = sorted(meta.topics.keys())
            return VerifyResult(name, False, f"Topic not found. Available: {available}", time.perf_counter() - t0)
    except Exception as exc:
        return VerifyResult(name, False, str(exc), time.perf_counter() - t0)


# ─────────────────────────────────────────────────────────────────────────────
# Check 1 — Produce 1 000 events
# ─────────────────────────────────────────────────────────────────────────────

def produce_test_events(
    bootstrap: str, topic: str, n: int
) -> tuple[VerifyResult, list[str]]:
    """Returns (result, list_of_produced_event_ids)."""
    t0 = time.perf_counter()
    name = f"Produce {n:,} events → {topic}"

    delivered: list[str] = []
    errors: list[str] = []

    def _on_delivery(err, msg):
        if err:
            errors.append(str(err))
        else:
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                delivered.append(payload["event_id"])
            except Exception:
                delivered.append("?")  # count it anyway

    try:
        p = Producer(
            {
                "bootstrap.servers": bootstrap,
                "acks": "all",
                "batch.size": 65536,
                "linger.ms": 5,
                "compression.type": "lz4",
            }
        )

        print(f"\n  [PRODUCE] Sending {n:,} events to '{topic}'...")
        for i in range(n):
            event = _make_event(i)
            p.produce(
                topic=topic,
                key=event["event_id"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=_on_delivery,
            )
            if i % 100 == 0:
                p.poll(0)

        remaining = p.flush(timeout=15)
        elapsed = time.perf_counter() - t0

        rate = n / elapsed if elapsed > 0 else 0
        if errors:
            return (
                VerifyResult(name, False,
                    f"Produced {len(delivered)}/{n}, {len(errors)} delivery errors. Rate: {rate:.0f}/s",
                    elapsed, {"errors": errors[:5]}),
                delivered,
            )

        return (
            VerifyResult(name, len(delivered) == n,
                f"All {len(delivered):,} delivered in {elapsed:.2f}s @ {rate:,.0f} events/s",
                elapsed, {"unflushed": remaining}),
            delivered,
        )
    except Exception as exc:
        return (
            VerifyResult(name, False, str(exc), time.perf_counter() - t0),
            delivered,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Check 2 — Consume scored-events and verify count within timeout
# ─────────────────────────────────────────────────────────────────────────────

def get_topic_watermarks(bootstrap: str, topic: str) -> list[TopicPartition]:
    """Retrieve the current high watermark offsets for all partitions of a topic."""
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"temp-watermark-{int(time.time())}",
        "auto.offset.reset": "latest"
    })
    try:
        meta = c.list_topics(topic, timeout=5)
        partitions = []
        if topic not in meta.topics:
            return []
        for pid in meta.topics[topic].partitions:
            tp = TopicPartition(topic, pid)
            _, high = c.get_watermark_offsets(tp, timeout=5)
            tp.offset = high
            partitions.append(tp)
        return partitions
    finally:
        c.close()


def consume_scored_events(
    bootstrap: str,
    scored_topic: str,
    expected_count: int,
    timeout_s: float,
    start_offsets: list[TopicPartition] | None = None,
) -> VerifyResult:
    t0 = time.perf_counter()
    name = f"Consume {expected_count:,} scored events within {timeout_s}s"

    scored_ids: list[str] = []
    anomaly_count = 0
    first_latency_ms: float | None = None

    # Start consuming from the end-of-topic snapshot taken just before producing
    # Use a fresh group-id to always read from now (not replay old messages)
    group_id = f"{CONSUMER_GROUP}-{int(time.time())}"

    c = Consumer(
        {
            "bootstrap.servers":   bootstrap,
            "group.id":            group_id,
            "auto.offset.reset":   "latest",   # only new messages
            "enable.auto.commit":  True,
            "session.timeout.ms":  6_000,
            "fetch.min.bytes":     1,
            "fetch.wait.max.ms":   50,
        }
    )

    if start_offsets is not None:
        c.assign(start_offsets)
    else:
        # Seek to end of all partitions before subscribing so we don't replay history
        meta = c.list_topics(scored_topic, timeout=5)
        partitions = [
            TopicPartition(scored_topic, pid)
            for pid in meta.topics[scored_topic].partitions
        ]
        c.assign(partitions)
        # Seek each partition to its high watermark
        for tp in partitions:
            _, high = c.get_watermark_offsets(tp, timeout=5)
            tp.offset = high
        c.assign(partitions)  # re-assign with updated offsets

    print(f"\n  [CONSUME] Waiting for {expected_count:,} scored events on '{scored_topic}' "
          f"(timeout={timeout_s}s)...")

    try:
        deadline = time.perf_counter() + timeout_s
        last_print = time.perf_counter()

        while time.perf_counter() < deadline:
            msg = c.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                continue

            raw = msg.value()
            if raw is None:
                continue

            try:
                payload = json.loads(raw.decode("utf-8"))
                scored_ids.append(payload.get("event_id", "?"))
                if payload.get("is_anomaly"):
                    anomaly_count += 1

                if first_latency_ms is None:
                    produced_ms = payload.get("event_time", _now_ms())
                    first_latency_ms = (time.perf_counter() - t0) * 1_000

            except Exception:
                scored_ids.append("?")

            # Progress print every 2s
            now = time.perf_counter()
            if now - last_print >= 2.0:
                elapsed = now - t0
                rate = len(scored_ids) / elapsed if elapsed > 0 else 0
                print(f"    {len(scored_ids):>5}/{expected_count} scored "
                      f"({rate:,.0f}/s) — {elapsed:.1f}s elapsed")
                last_print = now

            if len(scored_ids) >= expected_count:
                break

    finally:
        c.close()

    elapsed = time.perf_counter() - t0
    received = len(scored_ids)
    rate = received / elapsed if elapsed > 0 else 0
    passed = received >= expected_count and elapsed <= timeout_s

    detail_parts = [
        f"Received {received:,}/{expected_count:,} in {elapsed:.2f}s",
        f"@ {rate:,.0f} events/s",
        f"anomalies={anomaly_count}",
    ]
    if not passed:
        if received < expected_count:
            detail_parts.append(f"MISSING {expected_count - received} events")
        if elapsed > timeout_s:
            detail_parts.append(f"EXCEEDED {timeout_s}s timeout")

    return VerifyResult(
        name, passed, " | ".join(detail_parts), elapsed,
        {
            "received": received,
            "expected": expected_count,
            "anomaly_count": anomaly_count,
            "rate_per_s": round(rate, 1),
            "within_timeout": elapsed <= timeout_s,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# Check 3 — Prometheus P95 latency
# ─────────────────────────────────────────────────────────────────────────────

def check_p95_latency(metrics_url: str, limit_s: float) -> VerifyResult:
    t0 = time.perf_counter()
    name = f"Prometheus P95 inference_latency_seconds < {limit_s*1000:.0f}ms"

    try:
        r = requests.get(metrics_url, timeout=10)
        r.raise_for_status()
        elapsed = time.perf_counter() - t0

        parsed = _parse_prometheus_histogram(r.text, "inference_latency_seconds")
        if parsed is None:
            return VerifyResult(name, False, "Could not parse inference_latency_seconds histogram", elapsed)

        count = parsed["count"]
        total_sum = parsed["sum"]
        buckets = parsed["buckets"]

        if count == 0:
            return VerifyResult(name, False, "No inference observations recorded yet (count=0)", elapsed)

        p50 = _percentile_from_histogram(buckets, count, 50)
        p95 = _percentile_from_histogram(buckets, count, 95)
        p99 = _percentile_from_histogram(buckets, count, 99)
        avg = total_sum / count

        p95_ms = (p95 or 0) * 1_000
        passed = p95 is not None and p95 <= limit_s

        detail = (
            f"count={count:.0f} | avg={avg*1000:.2f}ms | "
            f"p50={((p50 or 0)*1000):.2f}ms | "
            f"p95={p95_ms:.2f}ms | "
            f"p99={((p99 or 0)*1000):.2f}ms | "
            f"limit={limit_s*1000:.0f}ms"
        )

        return VerifyResult(
            name, passed, detail, elapsed,
            {
                "count": count,
                "avg_ms": round(avg * 1000, 3),
                "p50_ms": round((p50 or 0) * 1000, 3),
                "p95_ms": round(p95_ms, 3),
                "p99_ms": round((p99 or 0) * 1000, 3),
            },
        )
    except Exception as exc:
        return VerifyResult(name, False, str(exc), time.perf_counter() - t0)


def check_events_processed_counter(metrics_url: str, expected: int) -> VerifyResult:
    """Verify the Prometheus counter matches what we produced."""
    t0 = time.perf_counter()
    name = f"events_processed_total success counter ≥ {expected:,}"
    try:
        r = requests.get(metrics_url, timeout=10)
        r.raise_for_status()

        success_count = 0
        error_count = 0
        for line in r.text.splitlines():
            if "events_processed_total" in line and not line.startswith("#"):
                if 'status="success"' in line:
                    success_count = int(float(line.split()[-1]))
                elif 'status="error"' in line:
                    error_count = int(float(line.split()[-1]))

        passed = success_count >= expected
        detail = (
            f"success={success_count:,} error={error_count:,} "
            f"(expected ≥ {expected:,})"
        )
        return VerifyResult(name, passed, detail, time.perf_counter() - t0,
                            {"success": success_count, "error": error_count})
    except Exception as exc:
        return VerifyResult(name, False, str(exc), time.perf_counter() - t0)


# ─────────────────────────────────────────────────────────────────────────────
# Report renderer
# ─────────────────────────────────────────────────────────────────────────────

def _render_report(results: list[VerifyResult]) -> bool:
    W = 80
    total = len(results)
    passed = sum(1 for r in results if r.passed)

    print("\n" + "═" * W)
    print("  PIPELINE VERIFICATION REPORT")
    print("═" * W)

    for i, r in enumerate(results, 1):
        icon  = "✅" if r.passed else "❌"
        label = "PASS" if r.passed else "FAIL"
        print(f"\n  {i}. {icon} [{label}] {r.name}")
        print(f"       {r.detail}")
        if r.duration_s > 0:
            print(f"       (completed in {r.duration_s:.3f}s)")
        if r.extra:
            for k, v in r.extra.items():
                print(f"       {k}: {v}")

    print("\n" + "─" * W)
    all_passed = passed == total
    status_icon = "🎉" if all_passed else "⚠️ "
    print(f"  {status_icon}  RESULT: {passed}/{total} checks passed")

    if not all_passed:
        print("\n  FAILED CHECKS:")
        for r in results:
            if not r.passed:
                print(f"    • {r.name}")
        print("\n  TROUBLESHOOTING:")
        print("    1. Is the ml-inference-worker container running?")
        print("       docker compose ps ml-inference-worker")
        print("    2. Check worker logs:")
        print("       docker compose logs --tail=50 ml-inference-worker")
        print("    3. Is the Prometheus metrics port mapped?")
        print("       curl http://localhost:8090/metrics | head -30")
        print("    4. Are the Kafka topics created?")
        print("       docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list")

    print("═" * W + "\n")
    return all_passed


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    global BOOTSTRAP, METRICS_URL, NUM_EVENTS, TIMEOUT_S

    parser = argparse.ArgumentParser(description="Pipeline end-to-end verification")
    parser.add_argument("--bootstrap", default=BOOTSTRAP,   help="Kafka bootstrap servers")
    parser.add_argument("--metrics",   default=METRICS_URL, help="Prometheus metrics URL")
    parser.add_argument("--timeout",   type=float, default=TIMEOUT_S, help="Max seconds for all events to be scored")
    parser.add_argument("--events",    type=int,   default=NUM_EVENTS,  help="Number of test events to send")
    args = parser.parse_args()

    BOOTSTRAP   = args.bootstrap
    METRICS_URL = args.metrics
    TIMEOUT_S   = args.timeout
    NUM_EVENTS  = args.events

    print(f"\n{'═'*80}")
    print(f"  ML PIPELINE VERIFICATION  —  {NUM_EVENTS:,} events / {TIMEOUT_S}s timeout")
    print(f"  Kafka:   {BOOTSTRAP}")
    print(f"  Metrics: {METRICS_URL}")
    print(f"{'═'*80}")

    results: list[VerifyResult] = []

    # ── Phase 0: Infrastructure readiness ─────────────────────────────────────
    print("\n[PHASE 0] Infrastructure readiness checks...")
    results.append(check_kafka_connectivity(BOOTSTRAP))
    if not results[-1].passed:
        print("  ❌ Cannot connect to Kafka — aborting (is docker-compose up?)")
        return (_render_report(results) and 0) or 1

    results.append(check_topic_exists(BOOTSTRAP, RAW_TOPIC))
    results.append(check_topic_exists(BOOTSTRAP, SCORED_TOPIC))
    results.append(check_metrics_endpoint(METRICS_URL))

    # Pre-query scored-events watermarks to avoid missing events during production
    start_offsets = get_topic_watermarks(BOOTSTRAP, SCORED_TOPIC)

    # ── Phase 1: Produce events ────────────────────────────────────────────────
    print("\n[PHASE 1] Producing test events...")
    produce_result, produced_ids = produce_test_events(BOOTSTRAP, RAW_TOPIC, NUM_EVENTS)
    results.append(produce_result)
    if not produce_result.passed:
        print("  ❌ Production failed — skipping consume check")
        return (_render_report(results) and 0) or 1

    # ── Phase 2: Consume scored-events ────────────────────────────────────────
    print("\n[PHASE 2] Verifying scored-events output...")
    consume_result = consume_scored_events(BOOTSTRAP, SCORED_TOPIC, NUM_EVENTS, TIMEOUT_S, start_offsets=start_offsets)
    results.append(consume_result)

    # ── Phase 3: Prometheus metrics ───────────────────────────────────────────
    print("\n[PHASE 3] Checking Prometheus metrics...")
    # Give the worker an extra second to flush metrics
    time.sleep(1)
    results.append(check_p95_latency(METRICS_URL, P95_LIMIT_S))
    results.append(check_events_processed_counter(METRICS_URL, NUM_EVENTS))

    # ── Final report ──────────────────────────────────────────────────────────
    all_ok = _render_report(results)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
