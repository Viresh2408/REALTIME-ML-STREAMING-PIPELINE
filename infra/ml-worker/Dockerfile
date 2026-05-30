# ============================================================
# ML Inference Worker — Production Dockerfile
# Base: python:3.12-slim
# Target throughput: 10 000 events/s across 4 async worker tasks
# ============================================================

# ── Stage 1: dependency builder ──────────────────────────────
FROM python:3.12-slim AS builder

# Install build tools needed to compile native extensions
# (confluent-kafka → librdkafka, asyncpg → Cython, scikit-learn → OpenMP)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libpq-dev \
        libgomp1 \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY requirements.txt ./

# Use a build venv so we can copy only the installed packages to the final stage
RUN pip install --upgrade pip==24.0 \
    && pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: minimal runtime image ───────────────────────────
FROM python:3.12-slim AS runtime

LABEL maintainer="ml-team" \
      description="ML Inference Worker — Kafka consumer + Isolation Forest anomaly detector" \
      org.opencontainers.image.title="ml-inference-worker" \
      org.opencontainers.image.version="1.0.0"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=1 \
    # Make the ml package importable as "ml.*" from /app
    PYTHONPATH=/app \
    # Prometheus metrics port
    METRICS_PORT=8090 \
    # Default Kafka settings (override via docker-compose / K8s env)
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    KAFKA_RAW_EVENTS_TOPIC=raw-events \
    KAFKA_SCORED_EVENTS_TOPIC=scored-events \
    KAFKA_DLQ_TOPIC=dl-events \
    KAFKA_MODEL_UPDATES_TOPIC=model-updates \
    KAFKA_CONSUMER_GROUP_ID=ml-inference-group \
    KAFKA_AUTO_OFFSET_RESET=earliest \
    # TimescaleDB
    TIMESCALE_HOST=timescaledb \
    TIMESCALE_PORT=5432 \
    TIMESCALE_DB=anomaly_db \
    TIMESCALE_USER=anomaly_admin \
    # MinIO
    MINIO_ENDPOINT=minio:9000 \
    MINIO_BUCKET_MODELS=ml-models \
    # Model inference
    ANOMALY_SCORE_THRESHOLD=0.7

# Runtime native libs (libgomp required by scikit-learn IsolationForest n_jobs=-1)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-built Python packages from builder stage
COPY --from=builder /install /usr/local

WORKDIR /app

# Copy the entire ml/ directory as the importable "ml" package
COPY . ./ml

# Non-root security: create dedicated user
RUN groupadd -r mlgroup \
    && useradd -r -g mlgroup -d /app -s /sbin/nologin mluser \
    && mkdir -p /app/artifacts /app/.cache \
    && chown -R mluser:mlgroup /app

USER mluser

# Prometheus metrics port
EXPOSE 8090

# Health check: verify the Prometheus metrics endpoint responds
# (proves the worker process started and the metrics server is up)
HEALTHCHECK \
    --interval=30s \
    --timeout=10s \
    --start-period=60s \
    --retries=3 \
    CMD curl -sf http://localhost:${METRICS_PORT}/metrics > /dev/null || exit 1

# Entry point: run as a Python module so the ml package path is correct
CMD ["python", "-m", "ml.worker.inference_worker"]
