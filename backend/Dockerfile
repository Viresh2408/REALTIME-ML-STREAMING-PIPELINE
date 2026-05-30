# ============================================================
# Backend Dockerfile — FastAPI + Uvicorn
# Python 3.12, from techstack.docx Section 2
# ============================================================
FROM python:3.12-slim AS base

LABEL maintainer="Anomaly Detection Team"
LABEL description="FastAPI backend — REST + WebSocket server"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# System dependencies for librdkafka (confluent-kafka) and asyncpg
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── Builder stage ──────────────────────────────────────────────────────────
FROM base AS builder

COPY requirements.txt ./
RUN pip install --upgrade pip && \
    pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt

# ── Runtime stage ──────────────────────────────────────────────────────────
FROM base AS runtime

COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .
RUN pip install --no-cache /wheels/*

COPY . .

# Non-root user for security
RUN groupadd -r appgroup && useradd -r -g appgroup -d /app -s /sbin/nologin appuser
RUN chown -R appuser:appgroup /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--log-level", "info", \
     "--access-log"]
