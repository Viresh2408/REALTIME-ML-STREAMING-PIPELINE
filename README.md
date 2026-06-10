# 🛡️ Real-Time Anomaly Detection System

> **Production-Grade ML Streaming Pipeline** — Kafka × FastAPI × LangGraph × TimescaleDB × Grafana × MLflow

[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://docs.docker.com/compose/)
[![Python](https://img.shields.io/badge/Python-3.11-green?logo=python)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-3.7-231F20?logo=apachekafka)](https://kafka.apache.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?logo=fastapi)](https://fastapi.tiangolo.com)
[![MLflow](https://img.shields.io/badge/MLflow-2.13-0194E2?logo=mlflow)](https://mlflow.org)
[![Grafana](https://img.shields.io/badge/Grafana-10.4-F46800?logo=grafana)](https://grafana.com)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.1-blueviolet)](https://github.com/langchain-ai/langgraph)
[![TimescaleDB](https://img.shields.io/badge/TimescaleDB-2.15-orange)](https://www.timescale.com)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Service URLs](#service-urls)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [API Reference](#api-reference)
- [WebSocket Channels](#websocket-channels)
- [Agent System](#agent-system)
- [Kafka Topics](#kafka-topics)
- [ML Pipeline](#ml-pipeline)
- [MCP Servers](#mcp-servers-claude-desktop-integration)
- [Infrastructure](#infrastructure)
- [Security & Auth](#security--auth)
- [Testing](#testing)
- [Makefile Targets](#makefile-targets)
- [Troubleshooting](#troubleshooting)
- [Production Checklist](#production-checklist)

---

## Overview

A fully containerized, production-ready anomaly detection platform that processes real-time network flow events through a multi-stage ML pipeline. The system uses **Apache Kafka** for event streaming, **FastAPI** for the REST/WebSocket backend, **LangGraph** agents for autonomous orchestration, and **TimescaleDB** for time-series storage — all observable through **Grafana** dashboards.

### Key Capabilities

| Capability | Description |
|-----------|-------------|
| 🔴 **Real-Time Detection** | Sub-10ms ML inference latency on streaming events |
| 🤖 **Agentic Orchestration** | LangGraph multi-agent system with Claude LLM reasoning |
| 📊 **Live Dashboards** | Grafana + WebSocket push for real-time visualization |
| 🔔 **Automated Alerting** | Threshold-based alerts with Slack/Email/PagerDuty notifications |
| 🔄 **Auto-Retraining** | Scheduled and drift-triggered model retraining pipeline |
| 🔑 **JWT RBAC** | Role-based access control (Admin / Analyst / Viewer) |
| 🧠 **MCP Integration** | Claude Desktop AI control over the full pipeline |
| 📦 **18 Dockerized Services** | Fully orchestrated via Docker Compose with health checks |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     REAL-TIME ANOMALY DETECTION SYSTEM                      │
│                                                                             │
│  ┌──────────────┐    ┌────────────────────────────────────────────────────┐ │
│  │  Event Source│───▶│          Apache Kafka (Confluent 7.6)              │ │
│  │  (REST POST) │    │  ┌──────────────┐  ┌───────────────┐              │ │
│  └──────────────┘    │  │  raw-events  │  │ scored-events │              │ │
│                       │  │  (6 parts)   │  │  (6 parts)    │              │ │
│  ┌──────────────┐    │  └──────┬───────┘  └───────┬───────┘              │ │
│  │  FastAPI     │    │         │                   │                      │ │
│  │  Backend     │───▶│  ┌──────▼──────────────┐   │                      │ │
│  │  :8000       │    │  │  ML Inference Worker│───┘                      │ │
│  │  REST + WS   │    │  │  (IsolationForest)  │──────▶ TimescaleDB       │ │
│  └──────┬───────┘    │  └─────────────────────┘       PG16 :5432         │ │
│         │             │                                                    │ │
│         │             │  ┌──────────────┐  ┌───────────┐                  │ │
│         │             │  │    alerts    │  │model-upd. │                  │ │
│         │             │  │  (3 parts)   │  │ (1 part)  │                  │ │
│         │             │  └──────┬───────┘  └─────┬─────┘                  │ │
│         │             └─────────│────────────────│────────────────────────┘ │
│         │                       │                │                          │
│  ┌──────▼───────────────────────▼────────┐       │                          │
│  │         LangGraph Agent System        │       │                          │
│  │  ┌──────────────┐ ┌────────────────┐  │       │                          │
│  │  │  Alert Agent │ │Notification    │  │       │                          │
│  │  │  (threshold) │ │Agent (Slack/   │  │       │                          │
│  │  └──────────────┘ │Email/PagerDuty)│  │       │                          │
│  │  ┌──────────────┐ └────────────────┘  │       │                          │
│  │  │  Retraining  │ ┌────────────────┐  │◀──────┘                          │
│  │  │  Agent (cron)│ │ Dashboard Agent│  │                                  │
│  │  └──────────────┘ └────────────────┘  │                                  │
│  └───────────────────────────────────────┘                                  │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │              Observability Stack                                    │    │
│  │  Grafana :3000  ←  TimescaleDB + Prometheus :9090 + Loki :3100     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │              MCP Server Fleet (Claude Desktop Integration)          │    │
│  │  anomaly :8001 │ model :8002 │ kafka :8003 │ grafana :8004          │    │
│  │  registry :8005                                                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │              Storage & Artifact Layer                               │    │
│  │  TimescaleDB (events) │ Redis (cache) │ MinIO (models) │ MLflow     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **FastAPI REST + Swagger** | http://localhost:8000/docs | JWT from `/api/v1/auth/token` |
| **Grafana Dashboards** | http://localhost:3000 | `admin` / `$GRAFANA_ADMIN_PASSWORD` |
| **Kafka UI** | http://localhost:8080 | `admin` / `$KAFKA_UI_PASSWORD` |
| **MLflow Tracking** | http://localhost:5000 | (no auth in dev) |
| **MinIO Console** | http://localhost:9001 | `$MINIO_ACCESS_KEY` / `$MINIO_SECRET_KEY` |
| **Prometheus Metrics** | http://localhost:9090 | (no auth) |
| **Loki Logs** | http://localhost:3100 | (internal) |
| **Schema Registry** | http://localhost:8081 | (no auth) |
| **MCP Anomaly Server** | http://localhost:8001 | (no auth) |
| **MCP Model Server** | http://localhost:8002 | (no auth) |
| **MCP Kafka Server** | http://localhost:8003 | (no auth) |
| **MCP Grafana Server** | http://localhost:8004 | (no auth) |
| **MCP Registry** | http://localhost:8005 | (no auth) |
| **ML Worker Health** | http://localhost:8090/health | (no auth) |

---

## Quick Start

```bash
# 1. Clone and enter the project
git clone <your-repo-url>
cd anomaly-detection-system

# 2. Copy environment template and fill in secrets
cp .env.example .env
# Edit .env with your credentials (at minimum: passwords and ANTHROPIC_API_KEY)

# 3. Start all services (builds images on first run)
make up

# 4. Initialize the system (first run only — ~5 min)
bash scripts/bootstrap.sh

# 5. Run the smoke test to verify everything works
bash scripts/smoke_test.sh
```

> **Windows users**: Use Git Bash or WSL2 to run `.sh` scripts.

### Default Test Credentials

| Email | Password | Role |
|-------|----------|------|
| `admin@example.com` | `admin123` | Admin |
| `analyst@example.com` | `analyst123` | Analyst |
| `viewer@example.com` | `viewer123` | Viewer |

---

## Project Structure

```
anomaly-detection-system/
├── backend/                         # FastAPI application
│   ├── app/
│   │   ├── main.py                  # Application entry point + lifespan
│   │   ├── api/v1/
│   │   │   ├── auth.py              # JWT auth, token issue/refresh/revoke
│   │   │   ├── events.py            # Event ingest + query endpoints
│   │   │   ├── anomalies.py         # Anomaly query, stats, heatmap
│   │   │   ├── alerts.py            # Alert lifecycle (ACK, resolve, silence)
│   │   │   ├── model.py             # Model status, retrain, rollback
│   │   │   ├── kafka.py             # Kafka topic management
│   │   │   ├── stats.py             # System-wide statistics
│   │   │   ├── websocket.py         # WS channels: events, alerts, metrics
│   │   │   └── internal.py          # Internal agent-to-backend calls
│   │   ├── core/
│   │   │   ├── config.py            # Settings (Pydantic BaseSettings)
│   │   │   ├── database.py          # Async SQLAlchemy session factory
│   │   │   ├── kafka.py             # Kafka producer/consumer clients
│   │   │   ├── redis_client.py      # Redis async connection pool
│   │   │   ├── lifespan.py          # Startup/shutdown hooks
│   │   │   └── migrate.py           # DB schema migration runner
│   │   ├── models/                  # SQLAlchemy ORM models
│   │   ├── schemas/                 # Pydantic v2 request/response schemas
│   │   ├── services/
│   │   │   └── event_service.py     # Business logic: ingest, score, label
│   │   └── middleware/              # Auth, CORS, logging middleware
│   ├── database/                    # DB session utilities
│   ├── kafka/                       # Kafka consumers (internal)
│   ├── ml/                          # ML inference engine (backend-side)
│   ├── agents/                      # Backend-side agent triggers
│   ├── Dockerfile
│   └── requirements.txt
│
├── agents/                          # LangGraph autonomous agent system
│   ├── orchestrator.py              # Supervisor: routes tasks, health checks
│   ├── producer_agent.py            # Kafka event producer agent
│   ├── inference_agent.py           # ML scoring agent (Kafka consumer)
│   ├── writer_agent.py              # TimescaleDB sink agent
│   ├── alert_agent.py               # Threshold alert detection agent
│   ├── notification_agent.py        # Slack/Email/PagerDuty dispatcher
│   ├── retraining_agent.py          # Scheduled + drift-triggered retraining
│   ├── dashboard_agent.py           # Grafana WebSocket bridge
│   ├── health_agent.py              # System health polling agent
│   ├── state.py                     # Shared agent state TypedDict
│   ├── env_loader.py                # Environment loader for agents
│   └── shared/                      # Shared tools, utilities
│
├── ml/                              # ML model training and inference
│   ├── train.py                     # Training entry point
│   ├── inference.py                 # Standalone inference script
│   ├── training/                    # IsolationForest + Autoencoder trainers
│   ├── inference/
│   │   ├── engine.py                # Singleton inference engine (hot-reload)
│   │   └── worker.py                # Kafka consumer worker loop
│   ├── models/                      # PyTorch Autoencoder model definitions
│   ├── features/                    # Feature extraction + normalization
│   ├── data/                        # CICIDS2017 download + preprocessing
│   ├── artifacts/                   # Local model artifacts (gitignored)
│   ├── Dockerfile
│   └── requirements.txt
│
├── mcp/                             # MCP Server Fleet (Claude Desktop)
│   ├── anomaly_detection_server.py  # Port 8001: anomaly queries
│   ├── model_management_server.py   # Port 8002: model control
│   ├── kafka_ops_server.py          # Port 8003: Kafka operations
│   ├── grafana_server.py            # Port 8004: dashboard control
│   ├── server_registry.py           # Port 8005: MCP server discovery
│   ├── mcp_fastapi.py               # FastAPI SSE transport base
│   ├── Dockerfile
│   └── requirements.txt
│
├── infra/                           # Infrastructure configuration files
│   ├── kafka/
│   │   ├── create_topics.py         # Kafka topic provisioner
│   │   └── schemas/                 # Avro schemas for events
│   ├── timescaledb/init/
│   │   ├── 01_hypertables.sql       # Schema + hypertable setup
│   │   ├── 02_aggregates.sql        # Continuous aggregate views
│   │   └── 03_users.sql             # DB role provisioning
│   ├── prometheus/
│   │   └── prometheus.yml           # Scrape targets config
│   ├── loki/
│   │   └── loki-config.yaml         # Log aggregation config
│   ├── redis/
│   │   └── redis.conf               # Redis config (LRU, persistence)
│   └── minio/                       # MinIO bucket policies
│
├── grafana/                         # Grafana dashboard provisioning
│   ├── provisioning/
│   │   ├── datasources/             # TimescaleDB, Prometheus, Loki YAML
│   │   └── dashboards/              # Dashboard provider config
│   ├── dashboards/                  # Pre-built dashboard JSON files
│   └── grafana.ini                  # Grafana server config
│
├── scripts/
│   ├── bootstrap.sh                 # First-run: topics, schema, train model
│   └── smoke_test.sh                # End-to-end pipeline verification
│
├── tests/
│   ├── unit/                        # Pure unit tests (no containers)
│   ├── integration/                 # testcontainers-based integration tests
│   ├── load/                        # Locust load testing scripts
│   └── chaos/                       # Chaos engineering tests
│
├── docker-compose.yml               # 18-service orchestration
├── docker-compose.override.yml      # Dev-mode overrides
├── Makefile                         # Developer CLI commands
├── pyproject.toml                   # Python deps + linting config
├── .env.example                     # Environment variable template
└── .gitignore
```

---

## Tech Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| **Streaming** | Apache Kafka (Confluent) | 3.7 / 7.6 |
| **Schema Registry** | Confluent Schema Registry | 7.6 |
| **API Framework** | FastAPI + Uvicorn | 0.111 / 0.29 |
| **Validation** | Pydantic v2 | 2.7 |
| **ML — Detection** | scikit-learn IsolationForest | 1.5 |
| **ML — Deep Learning** | PyTorch Autoencoder | 2.3 |
| **ML — Online Learning** | River | 0.21 |
| **ML — Tracking** | MLflow | 2.13 |
| **Time-Series DB** | TimescaleDB (PostgreSQL 16) | 2.15 |
| **Cache / Rate-Limit** | Redis | 7.2 |
| **Artifact Storage** | MinIO (S3-compatible) | RELEASE.2024 |
| **Agent Framework** | LangGraph + LangChain | 0.1 / 0.2 |
| **LLM** | Anthropic Claude | claude-sonnet-4 |
| **Dashboards** | Grafana | 10.4 |
| **Metrics** | Prometheus | 2.52 |
| **Log Aggregation** | Loki | 3.0 |
| **Kafka UI** | Kafka UI | 0.7 |
| **AI Integration** | Model Context Protocol (MCP) | 1.0 |
| **Unit Testing** | pytest + testcontainers | 8.x / 0.12 |
| **Load Testing** | Locust | 2.28 |
| **Container Runtime** | Docker + Docker Compose | — |

---

## API Reference

### Auth Endpoints — `/api/v1/auth`

| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| `POST` | `/token` | Public | OAuth2 password login — returns JWT access + refresh tokens |
| `POST` | `/refresh` | Public | Exchange refresh token for new access token |
| `POST` | `/revoke` | Any | Blacklist and revoke current access token |

### Events Endpoints — `/api/v1/events`

| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| `POST` | `/` | Viewer+ | Ingest a single raw event into the Kafka pipeline |
| `POST` | `/batch` | Viewer+ | Ingest batch of up to 1000 events |
| `GET` | `/` | Viewer+ | List scored events with filters (time range, source, anomaly-only) |
| `GET` | `/{event_id}` | Viewer+ | Get full event detail with feature vector |
| `PATCH` | `/{event_id}/label` | Admin | Apply analyst ground-truth label (TP/FP/TN/FN) |

### Anomalies Endpoints — `/api/v1/anomalies`

| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| `GET` | `/` | Viewer+ | List anomalies with severity/score/source/time filters |
| `GET` | `/stats` | Viewer+ | Aggregated stats: total count, anomaly rate %, avg/max score |
| `GET` | `/heatmap` | Viewer+ | Time × source_id heatmap data for Grafana |
| `GET` | `/{id}` | Viewer+ | Get single anomaly by UUID |

**Severity Ranges:**
| Severity | Score Range |
|----------|------------|
| `LOW` | 0.70 – 0.80 |
| `MEDIUM` | 0.80 – 0.90 |
| `HIGH` | 0.90 – 0.95 |
| `CRITICAL` | 0.95 – 1.00 |

### Alerts Endpoints — `/api/v1/alerts`

| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| `GET` | `/` | Viewer+ | List alerts (filter by status: ACTIVE/ACKNOWLEDGED/RESOLVED, severity) |
| `GET` | `/{alert_id}` | Viewer+ | Get alert with linked anomaly events |
| `PATCH` | `/{alert_id}/acknowledge` | Analyst+ | Acknowledge alert with analyst note |
| `PATCH` | `/{alert_id}/resolve` | Analyst+ | Mark alert resolved with resolution reason |
| `POST` | `/silence` | Analyst+ | Silence alerts for a source for N minutes |

### Model Management — `/api/v1/model`

| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| `GET` | `/status` | Viewer+ | Active model version, latency, total inferences |
| `GET` | `/versions` | Viewer+ | List all registered model versions with metrics |
| `POST` | `/retrain` | Admin | Trigger immediate model retraining job |
| `GET` | `/retrain/{job_id}` | Viewer+ | Poll retraining job status (PENDING/RUNNING/COMPLETED/FAILED) |
| `POST` | `/rollback` | Admin | Roll back to a historical model version |
| `GET` | `/metrics` | Viewer+ | Precision, Recall, F1, AUC-ROC for a model version |

### Kafka Management — `/api/v1/kafka`

| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| `GET` | `/topics` | Admin | List Kafka topics and partition counts |
| `POST` | `/produce` | Admin | Produce a raw message to a topic |

### Statistics — `/api/v1/stats`

| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| `GET` | `/` | Viewer+ | System-wide pipeline statistics |

---

## WebSocket Channels

Base URL: `ws://localhost:8000/api/v1/ws`

| Channel | Endpoint | Description | Message Format |
|---------|----------|-------------|----------------|
| **Events** | `/events` | Live ML-scored events stream | `{ event_id, score, is_anomaly, source_id, timestamp }` |
| **Alerts** | `/alerts` | Real-time alert triggers | `{ alert_id, severity, source_id, score, timestamp }` |
| **Metrics** | `/metrics` | System performance telemetry (every 5s) | `{ metric, value: { events_per_second, anomaly_rate_percent, consumer_lag }, timestamp }` |

**Keepalive:** Send `ping` text → receive `{"type": "pong"}`

---

## Agent System

The LangGraph multi-agent orchestration system runs as autonomous workers:

| Agent | Trigger | Input | Output |
|-------|---------|-------|--------|
| **Orchestrator** | Always-on heartbeat | System state | Route decisions, health checks |
| **Producer Agent** | Scheduled / webhook | Event config | Kafka `raw-events` messages |
| **Inference Agent** | Kafka `raw-events` poll | Raw event | Anomaly score + label → `scored-events` |
| **Writer Agent** | Kafka `scored-events` | Scored event | TimescaleDB hypertable row |
| **Alert Agent** | Score threshold breach | Scored event | Alert → Kafka `alerts` topic |
| **Notification Agent** | Kafka `alerts` topic | Alert payload | Slack / Email / PagerDuty |
| **Retraining Agent** | Daily cron (02:00) or drift | Model metrics | Updated model artifact in MinIO |
| **Dashboard Agent** | `scored-events` poll | Scored events | Grafana annotation + WebSocket push |
| **Health Agent** | Periodic poll | Service endpoints | System health status |

---

## Kafka Topics

| Topic | Partitions | Retention | Consumers |
|-------|-----------|-----------|-----------|
| `raw-events` | 6 | 24 hours | ML Inference Agent, Logger Agent |
| `scored-events` | 6 | 72 hours | TimescaleDB Writer, Alert Agent |
| `alerts` | 3 | 7 days | Notification Agent, Dashboard WS bridge |
| `model-updates` | 1 | 30 days | ML Inference Agent (hot-reload + rollback) |

---

## ML Pipeline

### Model Architecture

| Model | Algorithm | Use Case |
|-------|-----------|---------|
| **Primary Detector** | scikit-learn IsolationForest | Unsupervised anomaly detection on feature vectors |
| **Deep Detector** | PyTorch Autoencoder | Reconstruction-error based anomaly scoring |
| **Online Learner** | River (online ML) | Continuous learning from labeled feedback |

### Training Data

- **Dataset:** CICIDS2017 (Canadian Institute for Cybersecurity)
- **Features:** `duration`, `protocol_type`, `src_bytes`, `dst_bytes`, `land`, `wrong_fragment`, `urgent` + derived features
- **Storage:** MinIO (`ml-models` bucket), tracked with MLflow

### Inference Engine

- **Singleton pattern** with hot-reload on `model-updates` Kafka topic
- **Latency target:** < 10ms p99
- **Threshold:** configurable via `ANOMALY_SCORE_THRESHOLD` (default: 0.7)
- **Versioning:** Every deployed model version tracked in MLflow registry

---

## MCP Servers (Claude Desktop Integration)

Add to `~/.config/claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "anomaly-detection": {
      "url": "http://localhost:8001/sse",
      "transport": "sse"
    },
    "model-management": {
      "url": "http://localhost:8002/sse",
      "transport": "sse"
    },
    "kafka-ops": {
      "url": "http://localhost:8003/sse",
      "transport": "sse"
    },
    "grafana-control": {
      "url": "http://localhost:8004/sse",
      "transport": "sse"
    }
  }
}
```

| Server | Port | Capabilities |
|--------|------|-------------|
| `anomaly-detection` | 8001 | Query anomalies, get stats, view heatmaps |
| `model-management` | 8002 | Model status, trigger retrain, rollback |
| `kafka-ops` | 8003 | List topics, produce messages, view lag |
| `grafana-control` | 8004 | Create annotations, query dashboards |
| `mcp-registry` | 8005 | Service discovery for all MCP servers |

---

## Infrastructure

### Storage Volumes

| Volume | Service | Purpose |
|--------|---------|---------|
| `timescaledb-data` | TimescaleDB | Event + alert time-series data |
| `redis-data` | Redis | Token blacklist, rate limits, metrics cache |
| `minio-data` | MinIO | ML model artifacts, training datasets |
| `mlflow-data` | MLflow | Experiment tracking backend |
| `kafka-data` | Kafka | Topic log segments |
| `zookeeper-data` | Zookeeper | Kafka coordination metadata |
| `grafana-data` | Grafana | Dashboard state + user data |
| `prometheus-data` | Prometheus | Metric time-series (15d retention) |
| `loki-data` | Loki | Log aggregation data |

### Environment Variables (Key)

| Variable | Required | Description |
|----------|----------|-------------|
| `TIMESCALE_PASSWORD` | ✅ | TimescaleDB admin password |
| `REDIS_PASSWORD` | ✅ | Redis authentication password |
| `JWT_SECRET_KEY` | ✅ | JWT signing secret (min 32 chars) |
| `ANTHROPIC_API_KEY` | ✅ | Claude API key for LangGraph agents |
| `MINIO_ACCESS_KEY` | ✅ | MinIO access key |
| `MINIO_SECRET_KEY` | ✅ | MinIO secret key |
| `GRAFANA_ADMIN_PASSWORD` | ✅ | Grafana admin password |
| `KAFKA_UI_PASSWORD` | ✅ | Kafka UI login password |
| `SLACK_WEBHOOK_URL` | ⬜ | Slack alert webhook URL |
| `ANOMALY_SCORE_THRESHOLD` | ⬜ | Anomaly score threshold (default: 0.7) |
| `RETRAINING_CRON_HOUR` | ⬜ | Hour for daily retraining (default: 2) |
| `CORS_ORIGINS` | ⬜ | Allowed CORS origins for frontend |

---

## Security & Auth

### Authentication Flow

1. POST credentials to `/api/v1/auth/token` (OAuth2 password grant)
2. Receive `access_token` (1-hour validity) + `refresh_token` (7-day validity)
3. Include `Authorization: Bearer <access_token>` on all protected requests
4. Refresh with `/api/v1/auth/refresh` when access token expires
5. Logout with `/api/v1/auth/revoke` to blacklist the token (stored in Redis)

### Role Permissions

| Role | Events | Anomalies | Alerts | Model Control |
|------|--------|-----------|--------|---------------|
| **Viewer** | Read only | Read only | Read only | Read only |
| **Analyst** | Read only | Read only | ACK + Resolve + Silence | Read only |
| **Admin** | Full access | Full access | Full access | Retrain + Rollback + Label |

### Security Controls

- **Kafka:** SASL/SCRAM authentication (TLS in production)
- **TimescaleDB:** Role-based DB users — read-only Grafana user, write-only worker user
- **FastAPI:** JWT-based auth with bcrypt password hashing
- **Redis:** Password-protected with LRU eviction
- **Secrets:** Never hardcoded — all via `.env` / Docker secrets
- **CORS:** Configurable allow-list via `CORS_ORIGINS`

---

## Testing

```bash
# Unit tests (no containers required)
make test-unit

# Integration tests (requires running services)
make test-integration

# Full suite (unit + integration)
make test

# Load tests with Locust
make test-load

# End-to-end pipeline smoke test
bash scripts/smoke_test.sh
```

### Test Coverage Areas

| Category | Location | Description |
|----------|----------|-------------|
| Unit Tests | `tests/unit/` | Business logic, schemas, config validation |
| Integration Tests | `tests/integration/` | Full API tests with testcontainers (auth, events, anomalies) |
| Load Tests | `tests/load/` | Locust scenarios for throughput benchmarking |
| Chaos Tests | `tests/chaos/` | Fault injection and resilience verification |

---

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make up` | Start all 18 Docker services (detached) |
| `make down` | Stop all containers (preserve volumes) |
| `make down-v` | Stop all containers AND remove volumes (**destructive**) |
| `make build` | Build all custom Docker images (no cache) |
| `make build-cache` | Build Docker images (with layer cache) |
| `make logs` | Tail all container logs |
| `make logs s=fastapi-backend` | Tail a specific service |
| `make test` | Run full pytest suite (unit + integration) |
| `make test-unit` | Run unit tests only (no containers) |
| `make test-integration` | Run integration tests |
| `make test-load` | Run Locust load tests |
| `make bootstrap` | Run first-time system initialization |
| `make smoke-test` | Run end-to-end smoke test |
| `make shell-backend` | Shell into fastapi-backend |
| `make shell-db` | psql into timescaledb |
| `make topics` | Create all Kafka topics |
| `make migrate` | Apply DB schema migrations |
| `make train` | Run ML training pipeline |
| `make lint` | Run ruff linter |
| `make format` | Auto-format with ruff |
| `make clean` | Remove build artifacts |
| `make ps` | Show running containers |

---

## How to Inject a Test Anomaly

### Option 1 — REST API

```bash
# Get JWT token
TOKEN=$(curl -sf -X POST http://localhost:8000/api/v1/auth/token \
  -d "username=admin@example.com&password=admin123" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Post a high-anomaly event
curl -X POST http://localhost:8000/api/v1/events \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "source_id": "attacker-192-168-1-100",
    "event_type": "network_flow",
    "features": {
      "duration": 0,
      "protocol_type": 0,
      "src_bytes": 9999999,
      "dst_bytes": 0,
      "land": 1,
      "wrong_fragment": 3,
      "urgent": 1
    }
  }'

# Verify it was scored
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/anomalies?limit=1"
```

### Option 2 — Kafka Direct

```bash
docker compose exec kafka \
  kafka-console-producer --bootstrap-server localhost:9092 --topic raw-events <<'EOF'
{"event_id":"test-001","source_id":"test","event_time":"2024-01-01T00:00:00Z","features":{"duration":0,"src_bytes":9999999}}
EOF
```

---

## Troubleshooting

### Services not starting

```bash
docker compose ps                    # Check container status
docker compose logs <service>        # Check specific service logs
docker compose restart <service>     # Restart a specific service
```

### Kafka topics missing

```bash
make topics
```

### ML model not trained

```bash
make train
# or
bash scripts/bootstrap.sh
```

### TimescaleDB schema missing

```bash
make migrate
```

### Full reset (destructive)

```bash
make down-v          # Remove all containers and volumes
make up              # Fresh start
bash scripts/bootstrap.sh
```

---

## Production Checklist

- [ ] Set all passwords in `.env` to cryptographically strong values
- [ ] Generate JWT secret: `openssl rand -hex 32`
- [ ] Set `APP_ENV=production` and `LOG_LEVEL=WARNING`
- [ ] Enable Kafka SASL/TLS (`KAFKA_SECURITY_PROTOCOL=SASL_SSL`)
- [ ] Configure real `SLACK_WEBHOOK_URL` and `PAGERDUTY_INTEGRATION_KEY`
- [ ] Set `ANTHROPIC_API_KEY` for LangGraph agent reasoning
- [ ] Enable Grafana HTTPS (`GF_SERVER_PROTOCOL=https`)
- [ ] Set up MinIO with TLS (`MINIO_USE_SSL=true`)
- [ ] Configure Prometheus alerting rules
- [ ] Schedule daily retraining (`RETRAINING_CRON_HOUR=2`)
- [ ] Set `CORS_ORIGINS` to your actual frontend domain(s)
- [ ] Change default test user credentials

---

## License

MIT — see [LICENSE](./LICENSE)
