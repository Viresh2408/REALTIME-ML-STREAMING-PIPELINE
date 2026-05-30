# Real-Time Anomaly Detection System

> ML Streaming Pipeline — Kafka × FastAPI × LangGraph × TimescaleDB × Grafana

## Architecture Overview

```
Event Source → Kafka Producer Agent → raw-events (6 partitions)
                                          ↓
                               ML Inference Consumer Agent
                                    ↓             ↓
                            Anomaly Score    TimescaleDB Writer
                                    ↓
                     Score > threshold? → Alert Agent → Notification Agent
                                                          ↓
                                               Orchestrator Agent (LangGraph)
                                                          ↓
                                               Human Review API Endpoint
```

## Tech Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Streaming | Apache Kafka (Confluent) | 3.7 / 7.6 |
| API | FastAPI + Uvicorn | 0.111 / 0.29 |
| Validation | Pydantic v2 | 2.7 |
| ML — Detection | scikit-learn IsolationForest | 1.5 |
| ML — Deep | PyTorch Autoencoder | 2.3 |
| ML — Online | River | 0.21 |
| ML — Tracking | MLflow | 2.13 |
| Storage | TimescaleDB (PG16) | 2.15 |
| Cache | Redis | 7.2 |
| Artifacts | MinIO | RELEASE.2024 |
| Agents | LangGraph + LangChain | 0.1 / 0.2 |
| LLM | Claude claude-sonnet-4-20250514 | API |
| Dashboards | Grafana | 10.4 |
| Metrics | Prometheus | 2.52 |
| Logs | Loki | 3.0 |
| Cluster UI | Kafka UI | 0.7 |
| Testing | pytest + testcontainers | 8.x / 0.12 |
| Load Test | Locust | 2.28 |

## Quick Start

```bash
# 1. Clone and enter the project
cd anomaly-detection-system

# 2. Copy environment template and fill in secrets
cp .env.example .env
# Edit .env with your credentials

# 3. Build all Docker images
make build

# 4. Start all services
make up

# 5. Create Kafka topics (first time only)
make topics

# 6. Run database migrations (first time only)
make migrate

# 7. Train initial ML model (first time only)
docker compose exec ml-inference-worker \
    python -m training.train_isolation_forest --n-samples 50000

# 8. View the dashboard
open http://localhost:3000    # Grafana (admin/your-password)
open http://localhost:8000/docs  # FastAPI Swagger UI
open http://localhost:8080    # Kafka UI
open http://localhost:5000    # MLflow
open http://localhost:9001    # MinIO Console
```

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make up` | Start all Docker services (detached) |
| `make down` | Stop all containers (preserve volumes) |
| `make build` | Build all custom Docker images (no cache) |
| `make logs` | Tail all container logs |
| `make logs s=fastapi-backend` | Tail a specific service |
| `make test` | Run full pytest suite |
| `make test-unit` | Run unit tests only (no containers) |
| `make test-integration` | Run integration tests |
| `make test-load` | Run Locust load tests |
| `make shell-backend` | Shell into fastapi-backend |
| `make shell-db` | psql into timescaledb |
| `make topics` | Create all Kafka topics |
| `make migrate` | Apply DB schema migrations |
| `make lint` | Run ruff linter |
| `make format` | Auto-format with ruff |
| `make clean` | Remove build artifacts |

## Kafka Topics (architecture.docx §4)

| Topic | Partitions | Retention | Consumers |
|-------|-----------|-----------|-----------|
| `raw-events` | 6 | 24h | ML Inference Agent, Logger Agent |
| `scored-events` | 6 | 72h | TimescaleDB Writer Agent, Alert Agent |
| `alerts` | 3 | 7d | Notification Agent, Dashboard WebSocket bridge |
| `model-updates` | 1 | 30d | ML Inference Agent (hot-reload) |

## Agent Architecture (architecture.docx §5)

| Agent | Type | Trigger | Output |
|-------|------|---------|--------|
| Orchestrator | Supervisor | Always-on heartbeat | Route decisions, health checks |
| Producer | Event generator | Scheduled / webhook | Kafka raw-events messages |
| ML Inference | Stateless worker | Kafka poll loop | Anomaly score + label |
| TimescaleDB Writer | Sink | scored-events | Hypertable row insert |
| Alert | Reactive | Score threshold breach | Alert Kafka topic message |
| Notification | Reactive | alerts topic | Email / Slack / webhook |
| Model Retraining | Scheduled | Cron (daily) or drift | Updated model artifact |
| Dashboard | Bridge | scored-events poll | Grafana annotation + WebSocket push |

## Security Notes (architecture.docx §8)

- **Kafka**: SASL/SCRAM authentication (TLS in production)
- **TimescaleDB**: Role-based access — read-only Grafana user, write-only worker user
- **FastAPI**: JWT-based auth with OAuth2 password flow
- **Secrets**: Never hardcoded — all via `.env` / Docker secrets

## Project Structure

```
anomaly-detection-system/
├── backend/               # FastAPI app (REST + WebSocket)
│   ├── app/
│   │   ├── main.py        # Application entry point
│   │   ├── api/v1/        # Route handlers
│   │   ├── core/          # Config, DB, Kafka, Redis clients
│   │   ├── models/        # SQLAlchemy ORM models
│   │   ├── schemas/       # Pydantic v2 request/response models
│   │   └── services/      # Business logic layer
│   ├── Dockerfile
│   └── requirements.txt
├── agents/                # LangGraph agent modules
│   ├── orchestrator/      # Supervisor agent
│   ├── producer/          # Event producer agent
│   ├── ml_inference/      # Inference agent (also Kafka consumer loop)
│   ├── timescaledb_writer/# Sink agent
│   ├── alert/             # Threshold alert agent
│   ├── notification/      # Slack/Email/PagerDuty agent
│   ├── retraining/        # Scheduled model retraining agent
│   ├── dashboard/         # Grafana WebSocket bridge agent
│   └── shared/            # Shared state TypedDict + tools
├── ml/                    # Model training and inference
│   ├── training/          # Training scripts (IsolationForest, Autoencoder)
│   ├── inference/         # Inference engine (singleton)
│   ├── models/            # PyTorch model definitions
│   ├── artifacts/         # Local model artifacts (gitignored)
│   ├── Dockerfile
│   └── requirements.txt
├── infra/                 # Infrastructure configs
│   ├── kafka/             # Kafka configs
│   ├── timescaledb/init/  # SQL init scripts
│   ├── prometheus/        # prometheus.yml
│   ├── loki/              # loki-config.yaml
│   ├── redis/             # Redis config
│   └── minio/             # MinIO config
├── grafana/               # Dashboard provisioning
│   ├── provisioning/
│   │   ├── datasources/   # TimescaleDB, Prometheus, Loki
│   │   └── dashboards/    # Dashboard provider config
│   └── dashboards/        # Dashboard JSON files
├── tests/
│   ├── unit/              # Fast unit tests (no containers)
│   ├── integration/       # testcontainers-based tests
│   └── load/              # Locust load tests
├── docker-compose.yml     # All 9+ services
├── pyproject.toml         # Python dependencies + tool config
├── Makefile               # Developer commands
├── .env.example           # Environment variable template
└── .gitignore
```
