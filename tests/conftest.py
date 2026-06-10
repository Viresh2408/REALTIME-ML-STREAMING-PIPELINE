"""
tests/conftest.py
─────────────────
Session-scoped pytest fixtures for the full test suite.

Infrastructure provided via Testcontainers (Docker):
  • kafka_container      — Confluent Kafka 7.6.1
  • timescaledb_container — TimescaleDB 2.15.3-pg16
  • redis_container      — Redis 7.2-alpine

Also provides:
  • event_loop           — session-scoped asyncio loop (pytest-asyncio)
  • client               — FastAPI TestClient wired to all three containers

Spec refs:
  backend_requirements.docx § 2 — Non-Functional Requirements (NFR-01..NFR-08)
  tasks.docx Phase 6 (T-048..T-052)
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient

# ── sys.path bootstrap ───────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).parent.parent
_BACKEND_DIR = _REPO_ROOT / "backend"

for _p in [str(_REPO_ROOT), str(_BACKEND_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

for _folder in ("agents", "ml"):
    _folderpath = _REPO_ROOT / _folder
    if str(_folderpath) not in sys.path:
        sys.path.insert(0, str(_folderpath))

# ── Environment defaults (overridden by container fixtures below) ────────────
os.environ.setdefault("TESTING", "true")
os.environ["CORS_ORIGINS"] = '["http://localhost:3000", "http://localhost:8080"]'
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://test_user:test_pw@localhost:5432/test_db",
)
os.environ.setdefault("TIMESCALE_PASSWORD", "test_pw")
os.environ.setdefault("JWT_SECRET_KEY", "ci-test-secret-key-minimum-32-chars-here")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test-key")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("ANOMALY_SCORE_THRESHOLD", "0.7")


# ── Async event loop (session-scoped) ────────────────────────────────────────

@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Tell anyio to use asyncio as the async backend."""
    return "asyncio"


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """
    Session-scoped asyncio event loop.

    pytest-asyncio >= 0.23 deprecates the module-scoped loop; a session-
    scoped loop is required so that Testcontainer fixtures (also session-
    scoped) can be awaited by async test functions without re-creating the
    loop per module.

    NFR-06: async tests must share the same loop to avoid event loop closed errors.
    """
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


# ── Testcontainers: Kafka ────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def kafka_container() -> Generator:
    """
    Spin up a Confluent Kafka broker container for test isolation.

    Exposes the bootstrap server address via the KAFKA_BOOTSTRAP_SERVERS
    environment variable so all consumer/producer code picks it up automatically.

    Guarded by a Docker availability check so the fixture skips gracefully
    (rather than timing out) when Docker is not running on the test host.

    NFR-01: Kafka must sustain 10 000 events/s — integration tests run against
    a real broker to validate the pipeline, not mocks.
    """
    # ── Docker availability guard ────────────────────────────────────────────
    try:
        import docker as _docker  # type: ignore[import-untyped]
        _docker.from_env().ping()
    except Exception as _exc:
        pytest.skip(f"Docker not available — skipping kafka_container fixture: {_exc}")

    from testcontainers.kafka import KafkaContainer

    container = KafkaContainer(image="confluentinc/cp-kafka:7.6.1")
    container.start(timeout=90)
    try:
        bootstrap = container.get_bootstrap_server()
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap
        if "app.core.config" in sys.modules:
            from app.core.config import settings
            settings.KAFKA_BOOTSTRAP_SERVERS = bootstrap

        # Pre-create all required topics so background workers don't crash
        from infra.kafka.create_topics import create_topics
        create_topics()

        # Reset cached Kafka producers
        if "app.core.kafka" in sys.modules:
            from app.core.kafka import kafka_producer_manager
            kafka_producer_manager._producer = None
        if "app.services.event_service" in sys.modules:
            from app.services.event_service import KafkaProducerSingleton
            KafkaProducerSingleton._producer = None

        yield container
    finally:
        container.stop()


# ── Testcontainers: TimescaleDB ──────────────────────────────────────────────

@pytest.fixture(scope="session")
def timescaledb_container() -> Generator:
    """
    Spin up a TimescaleDB container for test isolation.

    Overrides DATABASE_URL so SQLAlchemy AsyncEngine connects to the
    ephemeral container rather than any production DSN.

    NFR-03: Storage layer must support hypertable ingestion verified by tests.
    """
    # ── Docker availability guard ────────────────────────────────────────────
    try:
        import docker as _docker  # type: ignore[import-untyped]
        _docker.from_env().ping()
    except Exception as _exc:
        pytest.skip(f"Docker not available — skipping timescaledb_container fixture: {_exc}")

    from testcontainers.postgres import PostgresContainer

    image = "timescale/timescaledb:2.15.3-pg16"
    with PostgresContainer(
        image=image,
        username="test_user",
        password="test_pw",
        dbname="test_db",
    ) as container:
        # Replace psycopg2 driver with asyncpg for async SQLAlchemy
        url = container.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+asyncpg://"
        )
        os.environ["DATABASE_URL"] = url
        if "app.core.config" in sys.modules:
            from app.core.config import settings
            settings.DATABASE_URL = url

        # Reset cached db_manager engine to force re-initialization with correct container URL
        from backend.database.connection import db_manager
        db_manager._engine = None
        db_manager._session_factory = None

        # Reset raw connection pool
        import backend.database.connection as db_conn
        db_conn._raw_pool = None

        if "app.core.database" in sys.modules:
            import app.core.database
            from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
            new_engine = create_async_engine(
                url,
                echo=False,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
            )
            app.core.database.engine = new_engine
            app.core.database.async_session_factory.configure(bind=new_engine)

        yield container


# ── Testcontainers: Redis ────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def redis_container() -> Generator:
    """
    Spin up a Redis container for test isolation.

    Overrides REDIS_URL so the backend rate-limiter and cache connect to the
    ephemeral container.

    NFR-05: Redis cache must be tested end-to-end.
    """
    from testcontainers.redis import RedisContainer

    with RedisContainer(image="redis:7.2-alpine") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        url = f"redis://{host}:{port}/0"
        os.environ["REDIS_URL"] = url
        if "app.core.config" in sys.modules:
            from app.core.config import settings
            settings.REDIS_URL = url

        # Reset cached redis pool
        if "app.core.redis_client" in sys.modules:
            from app.core.redis_client import redis_pool
            redis_pool._client = None
            redis_pool._pool = None

        yield container


# ── FastAPI TestClient ───────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def client(
    timescaledb_container,
    redis_container,
    kafka_container,
) -> Generator[TestClient, None, None]:
    """
    FastAPI TestClient bound to a live application wired to all three containers.

    Dependency order guarantees containers are ready before the app boots.
    The lifespan context manager (app startup/shutdown) executes within the
    TestClient context manager.

    NFR-07: API endpoints must respond correctly under test isolation.
    """
    from app.main import app  # import after env vars are set by container fixtures

    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client
