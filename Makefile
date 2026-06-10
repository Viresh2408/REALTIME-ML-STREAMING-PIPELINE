# ============================================================
# Makefile — Real-Time Anomaly Detection System
# Targets: up, down, build, logs, test, shell-backend, shell-db
# ============================================================

.PHONY: up down build logs test shell-backend shell-db \
        topics migrate lint format check clean ps help \
        bootstrap smoke-test train down-v build-cache \
        test-unit test-integration test-load

COMPOSE      := docker compose
ENV_FILE     := .env
PROJECT_NAME := anomaly-detection

# ─────────────────────────────────────────────────────────────
# help — list all targets with descriptions
# ─────────────────────────────────────────────────────────────
help: ## Show this help message
	@echo ""
	@echo "  Real-Time Anomaly Detection System — Makefile"
	@echo "  ──────────────────────────────────────────────"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ─────────────────────────────────────────────────────────────
# up — start all services (detached)
# ─────────────────────────────────────────────────────────────
up: ## Start all Docker services in detached mode
	@echo "🚀  Starting all services..."
	@test -f $(ENV_FILE) || { echo "❌  .env file not found. Copy .env.example to .env and fill in secrets."; exit 1; }
	$(COMPOSE) --project-name $(PROJECT_NAME) --env-file $(ENV_FILE) up -d
	@echo ""
	@echo "✅  Services started. Access points:"
	@echo "   FastAPI backend  →  http://localhost:8000/docs"
	@echo "   Grafana          →  http://localhost:3000"
	@echo "   Kafka UI         →  http://localhost:8080"
	@echo "   MLflow           →  http://localhost:5000"
	@echo "   Prometheus       →  http://localhost:9090"
	@echo "   MinIO Console    →  http://localhost:9001"
	@echo ""

# ─────────────────────────────────────────────────────────────
# down — stop and remove all containers
# ─────────────────────────────────────────────────────────────
down: ## Stop and remove all containers (preserves volumes)
	@echo "🛑  Stopping all services..."
	$(COMPOSE) --project-name $(PROJECT_NAME) down --remove-orphans
	@echo "✅  All containers stopped."

down-v: ## Stop all containers AND remove volumes (destructive!)
	@echo "⚠️   Stopping all services and removing volumes..."
	$(COMPOSE) --project-name $(PROJECT_NAME) down --volumes --remove-orphans
	@echo "✅  All containers and volumes removed."

# ─────────────────────────────────────────────────────────────
# build — build all custom Docker images
# ─────────────────────────────────────────────────────────────
build: ## Build all custom Docker images (no cache)
	@echo "🔨  Building Docker images..."
	$(COMPOSE) --project-name $(PROJECT_NAME) build --no-cache --parallel
	@echo "✅  All images built."

build-cache: ## Build Docker images (with layer cache)
	@echo "🔨  Building Docker images (with cache)..."
	$(COMPOSE) --project-name $(PROJECT_NAME) build --parallel
	@echo "✅  Build complete."

# ─────────────────────────────────────────────────────────────
# logs — tail logs for all services or a specific service
# Usage: make logs            → all services
#        make logs s=fastapi-backend  → specific service
# ─────────────────────────────────────────────────────────────
logs: ## Tail logs (all services). Use 's=<service>' for a specific one
ifdef s
	$(COMPOSE) --project-name $(PROJECT_NAME) logs -f $(s)
else
	$(COMPOSE) --project-name $(PROJECT_NAME) logs -f --tail=100
endif

# ─────────────────────────────────────────────────────────────
# test — run full test suite
# ─────────────────────────────────────────────────────────────
test: ## Run full pytest suite (unit + integration)
	@echo "🧪  Running test suite..."
	$(COMPOSE) --project-name $(PROJECT_NAME) run --rm \
		-e TESTING=true \
		fastapi-backend \
		python -m pytest tests/ -v --tb=short -m "not load"
	@echo "✅  Tests complete."

test-unit: ## Run unit tests only (fast, no containers required)
	@echo "🧪  Running unit tests..."
	python -m pytest tests/unit/ -v --tb=short -m unit
	@echo "✅  Unit tests complete."

test-integration: ## Run integration tests (requires running services)
	@echo "🧪  Running integration tests..."
	$(COMPOSE) --project-name $(PROJECT_NAME) run --rm \
		-e TESTING=true \
		fastapi-backend \
		python -m pytest tests/integration/ -v --tb=short -m integration
	@echo "✅  Integration tests complete."

test-load: ## Run Locust load tests (requires running services)
	@echo "⚡  Starting Locust load tests..."
	$(COMPOSE) --project-name $(PROJECT_NAME) run --rm \
		fastapi-backend \
		locust -f tests/load/locustfile.py \
		--host=http://fastapi-backend:8000 \
		--users=100 --spawn-rate=10 --run-time=60s --headless
	@echo "✅  Load tests complete."

# ─────────────────────────────────────────────────────────────
# shell-backend — open interactive shell in fastapi-backend
# ─────────────────────────────────────────────────────────────
shell-backend: ## Open an interactive shell in the fastapi-backend container
	@echo "🐚  Opening shell in fastapi-backend..."
	$(COMPOSE) --project-name $(PROJECT_NAME) exec fastapi-backend /bin/bash

# ─────────────────────────────────────────────────────────────
# shell-db — open psql in timescaledb
# ─────────────────────────────────────────────────────────────
shell-db: ## Open a psql session in the timescaledb container
	@echo "🗄️   Opening psql in timescaledb..."
	$(COMPOSE) --project-name $(PROJECT_NAME) exec timescaledb \
		psql -U $${TIMESCALE_USER:-anomaly_admin} -d $${TIMESCALE_DB:-anomaly_db}

# ─────────────────────────────────────────────────────────────
# topics — create all Kafka topics (from architecture.docx §4)
# ─────────────────────────────────────────────────────────────
topics: ## Create all Kafka topics defined in architecture.docx §4
	@echo "📋  Creating Kafka topics..."
	$(COMPOSE) --project-name $(PROJECT_NAME) exec kafka \
		kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
		--topic raw-events --partitions 6 --replication-factor 1 \
		--config retention.ms=86400000
	$(COMPOSE) --project-name $(PROJECT_NAME) exec kafka \
		kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
		--topic scored-events --partitions 6 --replication-factor 1 \
		--config retention.ms=259200000
	$(COMPOSE) --project-name $(PROJECT_NAME) exec kafka \
		kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
		--topic alerts --partitions 3 --replication-factor 1 \
		--config retention.ms=604800000
	$(COMPOSE) --project-name $(PROJECT_NAME) exec kafka \
		kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
		--topic model-updates --partitions 1 --replication-factor 1 \
		--config retention.ms=2592000000
	@echo "✅  Kafka topics created."
	@echo ""
	@echo "   Topic list:"
	$(COMPOSE) --project-name $(PROJECT_NAME) exec kafka \
		kafka-topics --bootstrap-server localhost:9092 --list

# ─────────────────────────────────────────────────────────────
# migrate — run TimescaleDB schema migrations
# ─────────────────────────────────────────────────────────────
migrate: ## Apply TimescaleDB schema migrations
	@echo "🗄️   Running database migrations..."
	$(COMPOSE) --project-name $(PROJECT_NAME) exec fastapi-backend \
		python -m backend.app.core.migrate
	@echo "✅  Migrations complete."

# ─────────────────────────────────────────────────────────────
# bootstrap — first-run system initialization
# ─────────────────────────────────────────────────────────────
bootstrap: ## Run first-time system initialization (topics + schemas + DB + training)
	@echo "Bootstrapping system..."
	@test -f $(ENV_FILE) || { echo "ERROR: .env not found. Copy .env.example to .env first."; exit 1; }
	@if [ -f scripts/bootstrap.sh ]; then bash scripts/bootstrap.sh; \
	 else powershell -ExecutionPolicy Bypass -File scripts/bootstrap.ps1; fi
	@echo "Bootstrap complete."

# ─────────────────────────────────────────────────────────────
# smoke-test — end-to-end smoke test
# ─────────────────────────────────────────────────────────────
smoke-test: ## Run end-to-end smoke test (PASS/FAIL per check)
	@echo "Running smoke test..."
	@if [ -f scripts/smoke_test.sh ]; then bash scripts/smoke_test.sh; \
	 else powershell -ExecutionPolicy Bypass -File scripts/smoke_test.ps1; fi

# ─────────────────────────────────────────────────────────────
# train — run ML training pipeline
# ─────────────────────────────────────────────────────────────
train: ## Run ML training pipeline (IsolationForest on CICIDS2017)
	@echo "🤖  Starting ML training pipeline..."
	$(COMPOSE) --project-name $(PROJECT_NAME) run --rm \
		-e MLFLOW_TRACKING_URI=http://mlflow:5000 \
		-e MLFLOW_S3_ENDPOINT_URL=http://minio:9000 \
		-e AWS_ACCESS_KEY_ID=$${MINIO_ACCESS_KEY} \
		-e AWS_SECRET_ACCESS_KEY=$${MINIO_SECRET_KEY} \
		-v "$$(pwd)/ml:/app/ml" \
		ml-inference-worker \
		python /app/ml/train.py
	@echo "✅  Training complete. Check MLflow at http://localhost:5000"

# ─────────────────────────────────────────────────────────────
# ps — show running containers
# ─────────────────────────────────────────────────────────────
ps: ## Show status of all project containers
	$(COMPOSE) --project-name $(PROJECT_NAME) ps

# ─────────────────────────────────────────────────────────────
# lint — run ruff linter
# ─────────────────────────────────────────────────────────────
lint: ## Run ruff linter across the entire codebase
	@echo "🔍  Running ruff linter..."
	ruff check .
	@echo "✅  Lint complete."

# ─────────────────────────────────────────────────────────────
# format — auto-format with ruff
# ─────────────────────────────────────────────────────────────
format: ## Auto-format code with ruff
	@echo "🎨  Formatting code..."
	ruff format .
	ruff check --fix .
	@echo "✅  Formatting complete."

# ─────────────────────────────────────────────────────────────
# check — full pre-commit check (lint + format + type check)
# ─────────────────────────────────────────────────────────────
check: lint ## Run all pre-commit checks (lint + type check)
	@echo "🔍  Running mypy type checking..."
	mypy backend/ agents/ ml/
	@echo "✅  All checks passed."

# ─────────────────────────────────────────────────────────────
# clean — remove build artifacts and cache
# ─────────────────────────────────────────────────────────────
clean: ## Remove build artifacts, __pycache__, and coverage files
	@echo "🧹  Cleaning build artifacts..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	find . -name ".coverage" -delete 2>/dev/null || true
	@echo "✅  Clean complete."

# Default target
.DEFAULT_GOAL := help
