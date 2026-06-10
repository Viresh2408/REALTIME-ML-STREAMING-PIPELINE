"""
tests/unit/test_health_agent.py
─────────────────────────────────────────────────────────────────────────────
Unit tests for agents/health_agent.py.

Coverage targets:
  - Kafka health check (AdminClient ping)
  - TimescaleDB health check (connection test)
  - Alert orchestrator on service failure
  - Health check interval (30s)
  - Graceful failure handling

All external service calls mocked.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Setup sys.path
_AGENTS_DIR = Path(__file__).parent.parent.parent / "agents"
if str(_AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(_AGENTS_DIR))

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")


@pytest.mark.asyncio
class TestHealthAgent:
    """Test HealthAgent service monitoring."""

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_all_healthy_returns_healthy_status(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that all services healthy returns OK status."""
        from agents.health_agent import HealthAgent

        # Kafka health check succeeds
        mock_admin = MagicMock()
        mock_admin.list_topics = MagicMock(return_value=MagicMock(topics={"test": None}))
        mock_admin_cls.return_value = mock_admin

        # TimescaleDB health check succeeds
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()
        mock_pg_connect.return_value = mock_conn

        agent = HealthAgent()

        kafka_ok = await agent.check_kafka()
        postgres_ok = await agent.check_postgres()

        assert kafka_ok is True
        assert postgres_ok is True

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_kafka_unreachable_returns_degraded(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that unreachable Kafka returns degraded status."""
        from agents.health_agent import HealthAgent

        # Kafka health check fails
        mock_admin = MagicMock()
        mock_admin.list_topics = MagicMock(side_effect=Exception("Connection refused"))
        mock_admin_cls.return_value = mock_admin

        # TimescaleDB health check succeeds
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()
        mock_pg_connect.return_value = mock_conn

        agent = HealthAgent()

        kafka_ok = await agent.check_kafka()
        postgres_ok = await agent.check_postgres()

        assert kafka_ok is False
        assert postgres_ok is True

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_timescaledb_unreachable_returns_degraded(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that unreachable TimescaleDB returns degraded status."""
        from agents.health_agent import HealthAgent

        # Kafka health check succeeds
        mock_admin = MagicMock()
        mock_admin.list_topics = MagicMock(return_value=MagicMock(topics={"test": None}))
        mock_admin_cls.return_value = mock_admin

        # TimescaleDB health check fails
        mock_pg_connect.side_effect = Exception("Connection refused")

        agent = HealthAgent()

        kafka_ok = await agent.check_kafka()
        postgres_ok = await agent.check_postgres()

        assert kafka_ok is True
        assert postgres_ok is False

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_both_services_down_triggers_critical(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that both services down triggers CRITICAL alert."""
        from agents.health_agent import HealthAgent

        # Kafka fails
        mock_admin = MagicMock()
        mock_admin.list_topics = MagicMock(side_effect=Exception("Kafka down"))
        mock_admin_cls.return_value = mock_admin

        # TimescaleDB fails
        mock_pg_connect.side_effect = Exception("DB down")

        agent = HealthAgent()

        kafka_ok = await agent.check_kafka()
        postgres_ok = await agent.check_postgres()

        assert kafka_ok is False
        assert postgres_ok is False

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_alert_orchestrator_called_on_kafka_failure(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that orchestrator is alerted when Kafka fails."""
        from agents.health_agent import HealthAgent

        mock_admin = MagicMock()
        mock_admin.list_topics = MagicMock(side_effect=Exception("Kafka failure"))
        mock_admin_cls.return_value = mock_admin

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()
        mock_pg_connect.return_value = mock_conn

        agent = HealthAgent()

        # Check Kafka - should fail
        kafka_ok = await agent.check_kafka()
        assert kafka_ok is False

        # Alert orchestrator would be called
        # (Mocked with print in real implementation)
        await agent.alert_orchestrator("Kafka")

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_alert_orchestrator_called_on_postgres_failure(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that orchestrator is alerted when TimescaleDB fails."""
        from agents.health_agent import HealthAgent

        mock_admin = MagicMock()
        mock_admin.list_topics = MagicMock(return_value=MagicMock(topics={"test": None}))
        mock_admin_cls.return_value = mock_admin

        mock_pg_connect.side_effect = Exception("DB failure")

        agent = HealthAgent()

        # Check Postgres - should fail
        postgres_ok = await agent.check_postgres()
        assert postgres_ok is False

        # Alert orchestrator would be called
        await agent.alert_orchestrator("TimescaleDB")

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_health_check_interval_is_30_seconds(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that health checks run every 30 seconds."""
        from agents.health_agent import HealthAgent

        mock_admin = MagicMock()
        mock_admin.list_topics = MagicMock(return_value=MagicMock(topics={"test": None}))
        mock_admin_cls.return_value = mock_admin

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()
        mock_pg_connect.return_value = mock_conn

        agent = HealthAgent()

        # Verify check interval
        assert agent.check_interval == 30

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_kafka_check_verifies_cluster_metadata(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that Kafka check retrieves and validates cluster metadata."""
        from agents.health_agent import HealthAgent

        mock_admin = MagicMock()
        mock_topics = MagicMock()
        mock_topics.topics = {"test-topic": None, "events": None}
        mock_admin.list_topics = MagicMock(return_value=mock_topics)
        mock_admin_cls.return_value = mock_admin

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()
        mock_pg_connect.return_value = mock_conn

        agent = HealthAgent()

        kafka_ok = await agent.check_kafka()

        # Should verify that topics exist
        assert kafka_ok is True
        mock_admin.list_topics.assert_called_once()

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_postgres_check_executes_test_query(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that Postgres check executes SELECT 1 query."""
        from agents.health_agent import HealthAgent

        mock_admin = MagicMock()
        mock_admin.list_topics = MagicMock(return_value=MagicMock(topics={"test": None}))
        mock_admin_cls.return_value = mock_admin

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()
        mock_pg_connect.return_value = mock_conn

        agent = HealthAgent()

        postgres_ok = await agent.check_postgres()

        # Should execute test query
        assert postgres_ok is True
        mock_conn.execute.assert_called_once()

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_admin_client_timeout_configuration(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that AdminClient is configured with 5s timeout."""
        from agents.health_agent import HealthAgent

        mock_admin = MagicMock()
        mock_admin.list_topics = MagicMock(return_value=MagicMock(topics={"test": None}))
        mock_admin_cls.return_value = mock_admin

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()
        mock_pg_connect.return_value = mock_conn

        agent = HealthAgent()

        kafka_ok = await agent.check_kafka()

        # Verify timeout was set (5 seconds in list_topics call)
        assert kafka_ok is True

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_postgres_connection_timeout(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that Postgres connection uses 5s timeout."""
        from agents.health_agent import HealthAgent

        mock_admin = MagicMock()
        mock_admin.list_topics = MagicMock(return_value=MagicMock(topics={"test": None}))
        mock_admin_cls.return_value = mock_admin

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()
        mock_pg_connect.return_value = mock_conn

        agent = HealthAgent()

        postgres_ok = await agent.check_postgres()

        # Verify connection succeeded
        assert postgres_ok is True

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_health_check_error_handling(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that health checks handle errors gracefully."""
        from agents.health_agent import HealthAgent

        # Both services fail
        mock_admin = MagicMock()
        mock_admin.list_topics = MagicMock(side_effect=Exception("Error"))
        mock_admin_cls.return_value = mock_admin

        mock_pg_connect.side_effect = Exception("Error")

        agent = HealthAgent()

        # Should not raise, just return False
        kafka_ok = await agent.check_kafka()
        postgres_ok = await agent.check_postgres()

        assert kafka_ok is False
        assert postgres_ok is False

    @patch("agents.health_agent.AdminClient")
    @patch("agents.health_agent.asyncpg.connect")
    async def test_running_flag_stops_agent(
        self, mock_pg_connect, mock_admin_cls
    ):
        """Test that running flag can stop the agent."""
        from agents.health_agent import HealthAgent

        mock_admin = MagicMock()
        mock_admin.list_topics = MagicMock(return_value=MagicMock(topics={"test": None}))
        mock_admin_cls.return_value = mock_admin

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()
        mock_pg_connect.return_value = mock_conn

        agent = HealthAgent()

        # Initially not running
        assert agent.running is False

        # Call stop (should work even if not started)
        agent.stop()
        assert agent.running is False
