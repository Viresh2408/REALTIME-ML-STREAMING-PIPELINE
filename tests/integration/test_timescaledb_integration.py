"""
Integration tests — TimescaleDB schema and hypertable
Connects to the already-running TimescaleDB Docker container.
pytest 8.x + pytest-asyncio
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime

import pytest

# ── connection settings pulled from .env / environment ──────────────────────
DB_HOST = os.getenv("TIMESCALE_HOST", "localhost")
DB_PORT = int(os.getenv("TIMESCALE_PORT", "5432"))
DB_NAME = os.getenv("TIMESCALE_DB", "anomaly_db")
DB_USER = os.getenv("TIMESCALE_USER", "anomaly_admin")
DB_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "StrongPass123!")

DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ── shared connection fixture ────────────────────────────────────────────────
@pytest.fixture(scope="module")
async def conn(timescaledb_container):
    """
    Single asyncpg connection reused across all tests in this module.
    Creates a fresh test schema so tests are isolated from production data.
    """
    import asyncpg

    db_url = os.getenv("DATABASE_URL")
    if db_url:
        dsn = db_url.replace("postgresql+asyncpg://", "postgresql://")
    else:
        dsn = DSN

    connection = await asyncpg.connect(dsn)

    # Create isolated test schema
    await connection.execute("CREATE SCHEMA IF NOT EXISTS test_integration")
    await connection.execute("SET search_path TO test_integration, public")

    yield connection

    # Cleanup — drop test schema after all tests finish
    await connection.execute("DROP SCHEMA IF EXISTS test_integration CASCADE")
    await connection.close()


# ── helper: create the hypertable inside test schema ────────────────────────
async def create_test_table(conn) -> None:
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS test_integration.anomaly_events (
            event_id        UUID            NOT NULL,
            event_time      TIMESTAMPTZ     NOT NULL,
            source_id       TEXT            NOT NULL,
            feature_vector  JSONB           NOT NULL,
            anomaly_score   FLOAT8          NOT NULL
                            CHECK (anomaly_score >= 0.0 AND anomaly_score <= 1.0),
            is_anomaly      BOOLEAN         NOT NULL DEFAULT false,
            model_version   TEXT            NOT NULL,
            processed_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
            PRIMARY KEY (event_id, event_time)
        )
    """)

    # Convert to hypertable — ignore if already done
    await conn.execute("""
        SELECT create_hypertable(
            'test_integration.anomaly_events',
            'event_time',
            chunk_time_interval => INTERVAL '1 day',
            if_not_exists       => TRUE
        )
    """)


# ════════════════════════════════════════════════════════════════════════════
@pytest.mark.testcontainers
class TestTimescaleDBSchema:
    """Integration: verify hypertable creation and INSERT / SELECT."""

    @pytest.mark.asyncio
    async def test_timescaledb_extension_is_loaded(self, conn) -> None:
        """Confirm TimescaleDB extension is active in the database."""
        row = await conn.fetchrow("SELECT extname FROM pg_extension WHERE extname = 'timescaledb'")
        assert row is not None, (
            "TimescaleDB extension not found — make sure the timescaledb container is running"
        )

    @pytest.mark.asyncio
    async def test_create_hypertable_and_insert(self, conn) -> None:
        """Create hypertable, insert a row, verify retrieval."""
        await create_test_table(conn)

        event_id = uuid.uuid4()
        event_time = datetime.now(tz=UTC)

        await conn.execute(
            """
            INSERT INTO test_integration.anomaly_events
                (event_id, event_time, source_id, feature_vector,
                 anomaly_score, is_anomaly, model_version)
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

        row = await conn.fetchrow(
            "SELECT * FROM test_integration.anomaly_events WHERE event_id = $1",
            event_id,
        )

        assert row is not None
        assert row["source_id"] == "test-sensor"
        assert abs(row["anomaly_score"] - 0.85) < 1e-6
        assert row["is_anomaly"] is True
        assert row["model_version"] == "v1"

    @pytest.mark.asyncio
    async def test_anomaly_score_constraint_rejects_above_1(self, conn) -> None:
        """CHECK constraint must reject scores > 1.0."""
        import asyncpg

        await create_test_table(conn)

        with pytest.raises((asyncpg.CheckViolationError, Exception)):
            await conn.execute(
                """
                INSERT INTO test_integration.anomaly_events
                    (event_id, event_time, source_id, feature_vector,
                     anomaly_score, is_anomaly, model_version)
                VALUES (gen_random_uuid(), NOW(), 'src',
                        '[]'::jsonb, 1.5, false, 'v1')
                """
            )

    @pytest.mark.asyncio
    async def test_anomaly_score_constraint_rejects_below_0(self, conn) -> None:
        """CHECK constraint must reject scores < 0.0."""
        import asyncpg

        await create_test_table(conn)

        with pytest.raises((asyncpg.CheckViolationError, Exception)):
            await conn.execute(
                """
                INSERT INTO test_integration.anomaly_events
                    (event_id, event_time, source_id, feature_vector,
                     anomaly_score, is_anomaly, model_version)
                VALUES (gen_random_uuid(), NOW(), 'src',
                        '[]'::jsonb, -0.1, false, 'v1')
                """
            )

    @pytest.mark.asyncio
    async def test_anomaly_score_boundary_values_accepted(self, conn) -> None:
        """Scores of exactly 0.0 and 1.0 must be accepted."""
        await create_test_table(conn)

        for score in (0.0, 1.0):
            await conn.execute(
                """
                INSERT INTO test_integration.anomaly_events
                    (event_id, event_time, source_id, feature_vector,
                     anomaly_score, is_anomaly, model_version)
                VALUES (gen_random_uuid(), NOW(), $1,
                        '[]'::jsonb, $2, false, 'v1')
                """,
                f"boundary-test-{score}",
                score,
            )

        count = await conn.fetchval(
            "SELECT COUNT(*) FROM test_integration.anomaly_events "
            "WHERE source_id LIKE 'boundary-test-%'"
        )
        assert count == 2

    @pytest.mark.asyncio
    async def test_hypertable_chunk_is_created(self, conn) -> None:
        """Verify TimescaleDB created at least one chunk for the hypertable."""
        await create_test_table(conn)

        # Insert a row to trigger chunk creation
        await conn.execute(
            """
            INSERT INTO test_integration.anomaly_events
                (event_id, event_time, source_id, feature_vector,
                 anomaly_score, is_anomaly, model_version)
            VALUES (gen_random_uuid(), NOW(), 'chunk-test',
                    '[]'::jsonb, 0.5, false, 'v1')
            """
        )

        chunk_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM timescaledb_information.chunks
            WHERE hypertable_schema = 'test_integration'
              AND hypertable_name   = 'anomaly_events'
            """
        )
        assert chunk_count >= 1, "Expected at least one chunk to be created"

    @pytest.mark.asyncio
    async def test_batch_insert_100_events(self, conn) -> None:
        """Batch insert 100 events and verify all are retrievable."""
        await create_test_table(conn)

        batch_source = f"batch-{uuid.uuid4().hex[:8]}"
        now = datetime.now(tz=UTC)

        records = [
            (
                uuid.uuid4(),
                now,
                batch_source,
                json.dumps([float(i), float(i * 2)]),
                round(i / 100, 2),
                i > 70,
                "v1",
            )
            for i in range(100)
        ]

        await conn.executemany(
            """
            INSERT INTO test_integration.anomaly_events
                (event_id, event_time, source_id, feature_vector,
                 anomaly_score, is_anomaly, model_version)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            records,
        )

        count = await conn.fetchval(
            "SELECT COUNT(*) FROM test_integration.anomaly_events WHERE source_id = $1",
            batch_source,
        )
        assert count == 100

    @pytest.mark.asyncio
    async def test_is_anomaly_filter_works(self, conn) -> None:
        """Query by is_anomaly=true returns only flagged events."""
        await create_test_table(conn)

        filter_source = f"filter-{uuid.uuid4().hex[:8]}"
        now = datetime.now(tz=UTC)

        # Insert 3 normal + 2 anomaly events
        for _i, is_anomaly in enumerate([False, False, False, True, True]):
            await conn.execute(
                """
                INSERT INTO test_integration.anomaly_events
                    (event_id, event_time, source_id, feature_vector,
                     anomaly_score, is_anomaly, model_version)
                VALUES (gen_random_uuid(), $1, $2, '[]'::jsonb, $3, $4, 'v1')
                """,
                now,
                filter_source,
                0.9 if is_anomaly else 0.2,
                is_anomaly,
            )

        anomaly_count = await conn.fetchval(
            "SELECT COUNT(*) FROM test_integration.anomaly_events "
            "WHERE source_id = $1 AND is_anomaly = true",
            filter_source,
        )
        assert anomaly_count == 2

    @pytest.mark.asyncio
    async def test_feature_vector_jsonb_query(self, conn) -> None:
        """JSONB feature vector is stored and retrievable as valid JSON."""
        await create_test_table(conn)

        event_id = uuid.uuid4()
        feature_data = {"packet_length": 1024.5, "flow_duration": 300.0, "flag_count": 5}

        await conn.execute(
            """
            INSERT INTO test_integration.anomaly_events
                (event_id, event_time, source_id, feature_vector,
                 anomaly_score, is_anomaly, model_version)
            VALUES ($1, NOW(), 'jsonb-test', $2::jsonb, 0.6, false, 'v1')
            """,
            event_id,
            json.dumps(feature_data),
        )

        raw = await conn.fetchval(
            "SELECT feature_vector FROM test_integration.anomaly_events WHERE event_id = $1",
            event_id,
        )

        retrieved = json.loads(raw) if isinstance(raw, str) else raw
        assert retrieved["packet_length"] == 1024.5
        assert retrieved["flag_count"] == 5
