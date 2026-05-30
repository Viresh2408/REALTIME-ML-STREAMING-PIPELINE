#!/usr/bin/env bash
# ============================================================
# Kafka Topic Bootstrap Script
# Creates all topics defined in architecture.docx Section 4
# Run inside the kafka container or via `make topics`
# ============================================================

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Kafka Topic Bootstrap — Anomaly Detection System"
echo "  Broker: ${BOOTSTRAP}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── raw-events: 6 partitions, 24h retention ─────────────────
kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic raw-events \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config min.insync.replicas=1 \
  --config cleanup.policy=delete

echo "✅  raw-events created (6 partitions, 24h retention)"

# ── scored-events: 6 partitions, 72h retention ──────────────
kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic scored-events \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=259200000 \
  --config min.insync.replicas=1 \
  --config cleanup.policy=delete

echo "✅  scored-events created (6 partitions, 72h retention)"

# ── alerts: 3 partitions, 7d retention ──────────────────────
kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic alerts \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config min.insync.replicas=1 \
  --config cleanup.policy=delete

echo "✅  alerts created (3 partitions, 7d retention)"

# ── model-updates: 1 partition, 30d retention ───────────────
kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic model-updates \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=2592000000 \
  --config min.insync.replicas=1 \
  --config cleanup.policy=delete

echo "✅  model-updates created (1 partition, 30d retention)"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  All topics created. Current topic list:"
kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
