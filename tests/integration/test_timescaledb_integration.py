"""
Integration tests — TimescaleDB schema and hypertable
Uses testcontainers-python 0.12 PostgreSQL (+ TimescaleDB extension).
pytest 8.x + pytest-asyncio 0.23.7
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Generator
from datetime import UTC, datetime

import pytest

TIMESCALE_IMAGE = "timescale/timescaledb:2.15.3-pg16"

try:
    from testcontainers.postgres import PostgresContainer

    TESTCONTAINERS_AVAILABLE = True
except ImportError:
    TESTCONTAINERS_AVAILABLE = False

SKIP_REASON = "testcontainers or Docker not available in this environment"


@pytest.fixture(scope="module")
def db_container() -> Generator:
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip(SKIP_REASON)

    with PostgresContainer(
        image=TIMESCALE_IMAGE,
        username="test_user",
        password="test_pw",
        dbname="test_db",
    ) as container:
        yield container


@pytest.mark.integration
@pytest.mark.skipif(not TESTCONTAINERS_AVAILABLE, reason=SKIP_REASON)
class TestTimescaleDBSchema:
    """Integration: verify hypertable creation and basic INSERT/SELECT."""

    @pytest.mark.asyncio
    async def test_create_hypertable_and_insert(self, db_container) -> None:
        import asyncpg

        dsn = db_container.get_connection_url().replace("postgresql://", "postgresql://")
        # asyncpg uses postgresql:// scheme
        dsn = dsn.replace("postgresql+psycopg2://", "postgresql://")

        conn = await asyncpg.connect(dsn)

        try:
            # Enable extension
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")

            # Create table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS anomaly_events (
                    event_id        UUID            NOT NULL,
                    event_time      TIMESTAMPTZ     NOT NULL,
                    source_id       TEXT            NOT NULL,
                    feature_vector  JSONB           NOT NULL,
                    anomaly_score   FLOAT8          NOT NULL,
                    is_anomaly      BOOLEAN         NOT NULL DEFAULT false,
                    model_version   TEXT            NOT NULL,
                    processed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (event_id, event_time)
                )
            """)

            # Create hypertable
            await conn.execute("""
                SELECT create_hypertable(
                    'anomaly_events', 'event_time',
                    chunk_time_interval => INTERVAL '1 day',
                    if_not_exists => TRUE
                )
            """)

            # Insert a test event
            event_id = uuid.uuid4()
            event_time = datetime.now(tz=UTC)
            await conn.execute(
                """
                INSERT INTO anomaly_events
                    (event_id, event_time, source_id, feature_vector, anomaly_score, is_anomaly, model_version)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                event_id,
                event_time,
                "test-sensor",
                json.dumps([0.1, 0.2, 0.3]),
                0.85,
                True,
                "v1",
            )

            # Verify retrieval
            row = await conn.fetchrow("SELECT * FROM anomaly_events WHERE event_id = $1", event_id)
            assert row is not None
            assert row["source_id"] == "test-sensor"
            assert abs(row["anomaly_score"] - 0.85) < 1e-6
            assert row["is_anomaly"] is True

        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_anomaly_score_constraint(self, db_container) -> None:
        """Verify CHECK constraint rejects scores outside [0, 1]."""
        import asyncpg

        dsn = db_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        conn = await asyncpg.connect(dsn)

        try:
            with pytest.raises(Exception):
                await conn.execute(
                    """
                    INSERT INTO anomaly_events
                        (event_id, event_time, source_id, feature_vector,
                         anomaly_score, is_anomaly, model_version)
                    VALUES (gen_random_uuid(), NOW(), 'src',
                            '[]'::jsonb, 1.5, false, 'v1')
                    """
                )
        finally:
            await conn.close()
