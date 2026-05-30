"""
ml/worker/metrics.py
─────────────────────
Prometheus metrics definitions for the ML inference worker.

Metrics (agents.docx § 3):
  inference_latency_seconds  — Histogram, buckets 1 ms → 1 s
  events_processed_total     — Counter, labels: status={success,error}
  anomalies_detected_total   — Counter, labels: severity={LOW,MEDIUM,HIGH,CRITICAL}
  model_version_info         — Gauge,   labels: version
  consumer_lag_events        — Gauge,   labels: partition

Exposed on port 8090 (default) via prometheus-client start_http_server.

Design notes:
  • CollectorRegistry is the default global registry; do NOT use multiprocess
    mode here — the worker runs as a single process with multiple coroutines.
  • INFERENCE_LATENCY buckets are tuned to the < 1 ms IsolationForest target
    while still detecting regressions up to 1 s.
  • All metric names follow the Prometheus naming convention:
      <namespace>_<subsystem>_<unit>  (unit suffix mandated by best practices)
"""

from __future__ import annotations

import structlog
from prometheus_client import (
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

logger = structlog.get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Histogram — inference end-to-end latency
#   Buckets span 1 ms → 1 s on a logarithmic scale.
#   Target p99 < 1 ms (IsolationForest); alert when p99 > 10 ms.
# ──────────────────────────────────────────────────────────────────────────────
INFERENCE_LATENCY = Histogram(
    name="inference_latency_seconds",
    documentation="End-to-end inference latency in seconds (preprocess + model.predict)",
    buckets=[
        0.0005,  # 0.5 ms  — sub-target (ideal IsolationForest path)
        0.001,  # 1 ms    — SLA target
        0.002,  # 2 ms
        0.005,  # 5 ms
        0.010,  # 10 ms   — soft alert threshold
        0.020,  # 20 ms
        0.050,  # 50 ms
        0.100,  # 100 ms  — hard alert threshold
        0.200,  # 200 ms
        0.500,  # 500 ms
        1.000,  # 1 s     — maximum bucket (overflow captured in +Inf)
    ],
)

# ──────────────────────────────────────────────────────────────────────────────
# Counter — total events through the pipeline, by outcome
#   Labels:
#     status="success"  — event processed, scored, written to DB, produced
#     status="error"    — any step failed; event routed to dl-events DLQ
# ──────────────────────────────────────────────────────────────────────────────
EVENTS_PROCESSED = Counter(
    name="events_processed_total",
    documentation="Total number of events processed by the ML worker",
    labelnames=["status"],
)

# Pre-initialise label combinations so Prometheus shows 0 before any events
EVENTS_PROCESSED.labels(status="success")
EVENTS_PROCESSED.labels(status="error")

# ──────────────────────────────────────────────────────────────────────────────
# Counter — anomalies detected, by severity tier
#   Severity tiers map to score ranges (see inference_worker.py _score_to_severity):
#     LOW      [0.00 – 0.50)
#     MEDIUM   [0.50 – 0.75)
#     HIGH     [0.75 – 0.90)
#     CRITICAL [0.90 – 1.00]
# ──────────────────────────────────────────────────────────────────────────────
ANOMALIES_DETECTED = Counter(
    name="anomalies_detected_total",
    documentation="Total anomalies detected, broken down by severity tier",
    labelnames=["severity"],
)

# Pre-initialise all four severity tiers
for _sev in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
    ANOMALIES_DETECTED.labels(severity=_sev)

# ──────────────────────────────────────────────────────────────────────────────
# Gauge — active model version
#   Usage: MODEL_VERSION_INFO.labels(version="v1.2.3").set(1)
#   Only the currently active version should be set to 1; previous versions
#   remain in the registry at 0 (useful for tracking hot-reload history in
#   Grafana with max_over_time).
# ──────────────────────────────────────────────────────────────────────────────
MODEL_VERSION_INFO = Gauge(
    name="model_version_info",
    documentation=(
        "Currently loaded model version. Set to 1 for the active version, 0 for inactive versions."
    ),
    labelnames=["version"],
)

# ──────────────────────────────────────────────────────────────────────────────
# Gauge — Kafka consumer lag per topic-partition
#   Label format: "raw-events:0", "raw-events:1", …
#   Updated every 5 s by the _monitor_consumer_lag background coroutine.
# ──────────────────────────────────────────────────────────────────────────────
CONSUMER_LAG = Gauge(
    name="consumer_lag_events",
    documentation="Number of unconsumed messages (lag) per Kafka topic-partition",
    labelnames=["partition"],
)


# ──────────────────────────────────────────────────────────────────────────────
# Server startup
# ──────────────────────────────────────────────────────────────────────────────


def start_metrics_server(port: int = 8090) -> None:
    """
    Start the Prometheus metrics HTTP server on *port*.

    The server is a lightweight WSGI app served by a daemon thread — it does
    not interfere with the asyncio event loop in the main process.

    Scrape endpoint: http://<host>:<port>/metrics
    """
    try:
        start_http_server(port, registry=REGISTRY)
        logger.info(
            "metrics_server.started",
            port=port,
            endpoint=f"http://0.0.0.0:{port}/metrics",
        )
    except OSError as exc:
        # Port already in use (e.g., dev restart without container rebuild)
        logger.error(
            "metrics_server.bind_failed",
            port=port,
            error=str(exc),
        )
        raise
