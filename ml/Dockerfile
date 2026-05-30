# ============================================================
# ML Inference Worker — Dockerfile
# Python 3.12, scikit-learn 1.5, PyTorch 2.3, River 0.21
# ============================================================
FROM python:3.12-slim AS base

LABEL description="ML Inference Worker — Kafka consumer + anomaly detection"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy into /app/ml to make the package importable as "ml"
COPY . ./ml

RUN groupadd -r mlgroup && useradd -r -g mlgroup -d /app -s /sbin/nologin mluser
RUN mkdir -p /app/artifacts && chown -R mluser:mlgroup /app
USER mluser

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

CMD ["python", "-m", "ml.worker.inference_worker"]
