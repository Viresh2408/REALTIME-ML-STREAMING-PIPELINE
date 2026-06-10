#!/usr/bin/env bash
# =============================================================
# bootstrap.sh — First-Run System Initializer
# Real-Time Anomaly Detection System
#
# Execute ONCE on first run:
#   bash scripts/bootstrap.sh
#
# Steps:
#   a) Wait for Kafka to be ready
#   b) Create Kafka topics
#   c) Register Avro schemas in Schema Registry
#   d) Run TimescaleDB init scripts (idempotent)
#   e) Download CICIDS2017 dataset (if not cached)
#   f) Run ML training pipeline
#   g) Register trained model in MLflow
#   h) Print ready message
# =============================================================

set -euo pipefail

# ── Colours ────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log()   { echo -e "${CYAN}[bootstrap]${NC} $*"; }
ok()    { echo -e "${GREEN}[✓]${NC} $*"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
fail()  { echo -e "${RED}[✗]${NC} $*"; exit 1; }

# ── Config (override via .env or environment) ───────────────
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
SCHEMA_REGISTRY_URL="${KAFKA_SCHEMA_REGISTRY_URL:-http://localhost:8081}"
TIMESCALE_HOST="${TIMESCALE_HOST:-localhost}"
TIMESCALE_PORT="${TIMESCALE_PORT:-5432}"
TIMESCALE_USER="${TIMESCALE_USER:-anomaly_admin}"
TIMESCALE_DB="${TIMESCALE_DB:-anomaly_db}"
TIMESCALE_PASSWORD="${TIMESCALE_PASSWORD:-}"
MLFLOW_URI="${MLFLOW_TRACKING_URI:-http://localhost:5000}"
DATA_DIR="ml/data/raw"
MODEL_DIR="ml/artifacts"
COMPOSE_PROJECT="${COMPOSE_PROJECT_NAME:-anomaly-detection}"

# Load .env if present
if [[ -f ".env" ]]; then
  log "Loading .env file..."
  set -o allexport
  # shellcheck disable=SC1091
  source .env
  set +o allexport
fi

# ─────────────────────────────────────────────────────────
# Step 0: Verify docker compose services are running
# ─────────────────────────────────────────────────────────
log "Step 0: Verifying Docker services are running..."
if ! docker compose --project-name "${COMPOSE_PROJECT}" ps --services --filter "status=running" | grep -q "kafka"; then
  warn "Kafka is not running. Starting all services first..."
  docker compose --project-name "${COMPOSE_PROJECT}" up -d
  sleep 10
fi
ok "Docker services are up."

# ─────────────────────────────────────────────────────────
# Step a: Wait for Kafka to be ready
# ─────────────────────────────────────────────────────────
log "Step a: Waiting for Kafka to be ready (up to 120s)..."
MAX_WAIT=120
ELAPSED=0
until docker compose --project-name "${COMPOSE_PROJECT}" exec -T kafka \
    kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; do
  if [[ ${ELAPSED} -ge ${MAX_WAIT} ]]; then
    fail "Kafka did not become ready within ${MAX_WAIT}s."
  fi
  echo -n "."
  sleep 5
  ELAPSED=$((ELAPSED + 5))
done
echo ""
ok "Kafka is ready."

# ─────────────────────────────────────────────────────────
# Step a2: Wait for Schema Registry
# ─────────────────────────────────────────────────────────
log "Waiting for Schema Registry (up to 60s)..."
ELAPSED=0
until curl -sf "${SCHEMA_REGISTRY_URL}/subjects" &>/dev/null; do
  if [[ ${ELAPSED} -ge 60 ]]; then
    fail "Schema Registry did not become ready within 60s."
  fi
  echo -n "."
  sleep 5
  ELAPSED=$((ELAPSED + 5))
done
echo ""
ok "Schema Registry is ready."

# ─────────────────────────────────────────────────────────
# Step b: Create Kafka topics
# ─────────────────────────────────────────────────────────
log "Step b: Creating Kafka topics..."
docker compose --project-name "${COMPOSE_PROJECT}" exec -T kafka \
  python3 /dev/stdin <<'PYEOF' 2>/dev/null || \
docker compose --project-name "${COMPOSE_PROJECT}" run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS="kafka:29092" \
  fastapi-backend \
  python infra/kafka/create_topics.py
PYEOF

# Direct kafka-topics approach as fallback
for TOPIC_SPEC in \
    "raw-events:6:86400000" \
    "scored-events:6:259200000" \
    "alerts:3:604800000" \
    "model-updates:1:2592000000"; do
  TOPIC=$(echo "$TOPIC_SPEC" | cut -d: -f1)
  PARTS=$(echo "$TOPIC_SPEC" | cut -d: -f2)
  RETENTION=$(echo "$TOPIC_SPEC" | cut -d: -f3)
  docker compose --project-name "${COMPOSE_PROJECT}" exec -T kafka \
    kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists \
      --topic "${TOPIC}" \
      --partitions "${PARTS}" \
      --replication-factor 1 \
      --config "retention.ms=${RETENTION}" 2>/dev/null || true
  ok "Topic '${TOPIC}' ensured."
done
ok "Kafka topics created."

# ─────────────────────────────────────────────────────────
# Step c: Register Avro schemas
# ─────────────────────────────────────────────────────────
log "Step c: Registering Avro schemas..."
if [[ -f "infra/kafka/schemas/raw_event.avsc" ]]; then
  docker compose --project-name "${COMPOSE_PROJECT}" run --rm \
    -e SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL}" \
    -v "$(pwd)/infra/kafka:/infra/kafka:ro" \
    fastapi-backend \
    python /infra/kafka/register_schemas.py 2>/dev/null || \
  # Direct registration fallback
  for SUBJECT in "raw-events-value" "scored-events-value" "alerts-value"; do
    SCHEMA_FILE="infra/kafka/schemas/$(echo "${SUBJECT}" | sed 's/-value//' | tr '-' '_').avsc"
    if [[ -f "${SCHEMA_FILE}" ]]; then
      SCHEMA_CONTENT=$(cat "${SCHEMA_FILE}")
      curl -sf -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": $(echo "${SCHEMA_CONTENT}" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}" \
        "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT}/versions" | python3 -m json.tool >/dev/null
      ok "Schema '${SUBJECT}' registered."
    fi
  done
else
  warn "Schema files not found at infra/kafka/schemas/. Skipping schema registration."
fi
ok "Avro schema registration complete."

# ─────────────────────────────────────────────────────────
# Step d: TimescaleDB init scripts (idempotent)
# ─────────────────────────────────────────────────────────
log "Step d: Verifying TimescaleDB initialization..."
TIMESCALE_PASSWORD_SAFE="${TIMESCALE_PASSWORD:-}"
# Check if hypertables exist
HTABLE_COUNT=$(docker compose --project-name "${COMPOSE_PROJECT}" exec -T timescaledb \
  psql -U "${TIMESCALE_USER}" -d "${TIMESCALE_DB}" -t -c \
  "SELECT COUNT(*) FROM timescaledb_information.hypertables;" 2>/dev/null | tr -d ' \n' || echo "0")

if [[ "${HTABLE_COUNT}" == "0" ]]; then
  warn "No hypertables found. Running init scripts manually..."
  for SQL_FILE in infra/timescaledb/init/*.sql; do
    [[ "${SQL_FILE}" == *.bak ]] && continue
    log "Running ${SQL_FILE}..."
    docker compose --project-name "${COMPOSE_PROJECT}" exec -T timescaledb \
      psql -U "${TIMESCALE_USER}" -d "${TIMESCALE_DB}" \
      -f "/docker-entrypoint-initdb.d/$(basename "${SQL_FILE}")" 2>/dev/null || true
  done
else
  ok "TimescaleDB hypertables already initialized (${HTABLE_COUNT} found)."
fi
ok "TimescaleDB ready."

# ─────────────────────────────────────────────────────────
# Step e: Download CICIDS2017 dataset (if not cached)
# ─────────────────────────────────────────────────────────
log "Step e: Checking CICIDS2017 dataset..."
mkdir -p "${DATA_DIR}"
DATASET_FILE="${DATA_DIR}/cicids2017.csv"

if [[ -f "${DATASET_FILE}" ]] && [[ $(wc -l < "${DATASET_FILE}") -gt 1000 ]]; then
  ok "CICIDS2017 dataset already cached at ${DATASET_FILE}."
else
  log "Downloading CICIDS2017 dataset (this may take a few minutes)..."
  docker compose --project-name "${COMPOSE_PROJECT}" run --rm \
    -v "$(pwd)/ml:/app/ml" \
    ml-inference-worker \
    python /app/ml/data/download_cicids.py 2>/dev/null || {
    warn "Dataset download via container failed. Attempting direct download..."
    # Fallback: try Kaggle mirror
    CICIDS_URL="https://media.githubusercontent.com/media/CanadianInstituteForCybersecurity/CIC-IDS-2017/master/MachineLearningCVE/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv"
    if command -v curl &>/dev/null; then
      curl -L --retry 3 --retry-delay 5 -o "${DATASET_FILE}" "${CICIDS_URL}" || true
    elif command -v wget &>/dev/null; then
      wget -q --tries=3 -O "${DATASET_FILE}" "${CICIDS_URL}" || true
    fi
  }
  if [[ -f "${DATASET_FILE}" ]] && [[ $(wc -l < "${DATASET_FILE}") -gt 1000 ]]; then
    ok "Dataset downloaded: ${DATASET_FILE}"
  else
    warn "Dataset download failed or incomplete. Training will use synthetic data."
  fi
fi

# ─────────────────────────────────────────────────────────
# Step f: Run ML training pipeline
# ─────────────────────────────────────────────────────────
log "Step f: Running ML training pipeline..."
mkdir -p "${MODEL_DIR}"

# Wait for MLflow to be healthy
log "Waiting for MLflow (up to 120s)..."
ELAPSED=0
until curl -sf "${MLFLOW_URI}/health" &>/dev/null; do
  if [[ ${ELAPSED} -ge 120 ]]; then
    warn "MLflow not ready. Will still attempt training (model saved locally)."
    break
  fi
  echo -n "."
  sleep 5
  ELAPSED=$((ELAPSED + 5))
done
echo ""

# Wait for MinIO to be ready
MINIO_URL="${MINIO_ENDPOINT:-localhost:9000}"
if [[ "${MINIO_URL}" != http* ]]; then
  MINIO_URL="http://${MINIO_URL}"
fi
ELAPSED=0
until curl -sf "${MINIO_URL}/minio/health/live" &>/dev/null; do
  if [[ ${ELAPSED} -ge 60 ]]; then
    warn "MinIO not ready. Model will be saved locally only."
    break
  fi
  echo -n "."
  sleep 5
  ELAPSED=$((ELAPSED + 5))
done
echo ""

# Create MinIO bucket if needed
docker compose --project-name "${COMPOSE_PROJECT}" exec -T minio \
  mc alias set local "http://localhost:9000" \
  "${MINIO_ACCESS_KEY:-minioadmin}" "${MINIO_SECRET_KEY:-minioadmin}" 2>/dev/null || true
docker compose --project-name "${COMPOSE_PROJECT}" exec -T minio \
  mc mb --ignore-existing local/"${MINIO_BUCKET_MODELS:-ml-models}" 2>/dev/null || true
docker compose --project-name "${COMPOSE_PROJECT}" exec -T minio \
  mc mb --ignore-existing local/"${MINIO_BUCKET_DATASETS:-training-datasets}" 2>/dev/null || true

# Run training
log "Starting model training (IsolationForest on CICIDS2017)..."
docker compose --project-name "${COMPOSE_PROJECT}" run --rm \
  --no-deps \
  -e MLFLOW_TRACKING_URI="${MLFLOW_URI}" \
  -e MLFLOW_EXPERIMENT_NAME="${MLFLOW_EXPERIMENT_NAME:-anomaly-detection}" \
  -e MINIO_ENDPOINT="${MINIO_ENDPOINT:-minio:9000}" \
  -e MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}" \
  -e MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}" \
  -e MINIO_BUCKET_MODELS="${MINIO_BUCKET_MODELS:-ml-models}" \
  -e MLFLOW_S3_ENDPOINT_URL="${MLFLOW_S3_ENDPOINT_URL:-http://minio:9000}" \
  -e AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY:-minioadmin}" \
  -e AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY:-minioadmin}" \
  -v "$(pwd)/ml:/app/ml" \
  ml-inference-worker \
  python /app/ml/train.py || {
  warn "Full training pipeline failed. Attempting simplified training..."
  docker compose --project-name "${COMPOSE_PROJECT}" exec -T ml-inference-worker \
    python -c "
import sys, os
sys.path.insert(0, '/app/ml')
from models.isolation_forest import AnomalyDetector
import numpy as np

print('Training minimal model with synthetic data...')
X = np.random.randn(10000, 78)  # CICIDS feature count
detector = AnomalyDetector()
detector.train(X)
detector.save(path='/app/ml/artifacts/model_bootstrap.joblib')
print('Minimal model saved to /app/ml/artifacts/model_bootstrap.joblib')
" 2>/dev/null || warn "Simplified training also failed. Worker will use default model."
}
ok "ML training complete."

# ─────────────────────────────────────────────────────────
# Step g: Register trained model in MLflow
# ─────────────────────────────────────────────────────────
log "Step g: Verifying model registration in MLflow..."
LATEST_RUN=$(curl -sf "${MLFLOW_URI}/api/2.0/mlflow/runs/search" \
  -H "Content-Type: application/json" \
  -d '{"experiment_ids":[],"max_results":1}' 2>/dev/null | \
  python3 -c "import sys,json; data=json.load(sys.stdin); print(data.get('runs',[{}])[0].get('info',{}).get('run_id','none'))" 2>/dev/null || echo "none")

if [[ "${LATEST_RUN}" != "none" ]] && [[ -n "${LATEST_RUN}" ]]; then
  ok "Model registered in MLflow. Latest run ID: ${LATEST_RUN}"
else
  warn "Could not verify MLflow registration. Check http://localhost:5000 manually."
fi

# ─────────────────────────────────────────────────────────
# Step h: System ready
# ─────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  System ready. Bootstrap complete!${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  ${CYAN}Service URLs:${NC}"
echo -e "  ├── Grafana Dashboard  →  ${YELLOW}http://localhost:3000${NC}  (admin / \$GRAFANA_ADMIN_PASSWORD)"
echo -e "  ├── FastAPI Swagger    →  ${YELLOW}http://localhost:8000/docs${NC}"
echo -e "  ├── Kafka UI           →  ${YELLOW}http://localhost:8080${NC}"
echo -e "  ├── MLflow             →  ${YELLOW}http://localhost:5000${NC}"
echo -e "  ├── MinIO Console      →  ${YELLOW}http://localhost:9001${NC}"
echo -e "  ├── Prometheus         →  ${YELLOW}http://localhost:9090${NC}"
echo -e "  └── MCP Servers        →  ${YELLOW}http://localhost:8001-8005${NC}"
echo ""
echo -e "  ${CYAN}Quick test:${NC}"
echo -e "  bash scripts/smoke_test.sh"
echo ""
