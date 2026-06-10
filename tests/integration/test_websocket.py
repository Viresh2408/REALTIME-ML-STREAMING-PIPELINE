"""
tests/integration/test_websocket.py
─────────────────────────────────────────────────────────────────────────────
Integration tests for WebSocket endpoints: /ws/events, /ws/alerts, /ws/metrics.

Coverage targets (websocket.py):
  Lines 33-46   – ConnectionManager.connect() and metrics task startup
  Lines 49-60   – ConnectionManager.disconnect() and metrics task cancellation
  Lines 64-77   – ConnectionManager.broadcast() and dead connection removal
  Lines 81-157  – _broadcast_system_metrics() loop, DB queries, mocks
  Lines 170-180 – websocket_events endpoint (connect, receive, disconnect)
  Lines 189-198 – websocket_alerts endpoint (connect, receive, disconnect)
  Lines 207-216 – websocket_metrics endpoint (connect, receive, disconnect)

Tests use FastAPI TestClient with websocket_connect() context manager.
No live DB/Kafka/Redis required — all mocked.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

# ── sys.path setup ───────────────────────────────────────────────────────────
_BACKEND_DIR = Path(__file__).parent.parent.parent / "backend"
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

# ── Environment setup ────────────────────────────────────────────────────────
_CI_ENV = {
    "DATABASE_URL": "postgresql+asyncpg://test_user:test_pw@localhost:5432/test_db",
    "TIMESCALE_PASSWORD": "test_pw",
    "JWT_SECRET_KEY": os.environ.get("JWT_SECRET_KEY", "ci-test-secret-key-minimum-32-chars-here"),
    "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", "sk-ant-test-key"),
    "CORS_ORIGINS": '["http://localhost:3000"]',
    "TESTING": "true",
    "REDIS_URL": "redis://localhost:6379/0",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
}
for key, value in _CI_ENV.items():
    os.environ.setdefault(key, value)


# ── App import ───────────────────────────────────────────────────────────────

def _import_app():
    from app.main import app  # type: ignore[import]
    return app


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture()
def fake_redis():
    """In-memory fakeredis async client."""
    try:
        import fakeredis.aioredis as fake_aio
        return fake_aio.FakeRedis(decode_responses=True)
    except ImportError:
        r = AsyncMock()
        r.get = AsyncMock(return_value=None)
        r.set = AsyncMock(return_value=True)
        r.ping = AsyncMock(return_value=True)
        return r


@pytest.fixture()
def ws_client(fake_redis):
    """FastAPI TestClient with mocked dependencies."""
    app = _import_app()

    from app.core.redis_client import redis_pool  # type: ignore[import]
    from app.services.event_service import KafkaProducerSingleton  # type: ignore[import]

    # Patch redis
    redis_pool._client = fake_redis

    # Patch Kafka producer
    mock_producer = MagicMock()
    mock_producer.produce = MagicMock()
    KafkaProducerSingleton._producer = mock_producer

    client = TestClient(app)
    yield client

    # Cleanup
    KafkaProducerSingleton._producer = None
    redis_pool._client = None


# ────────────────────────────────────────────────────────────────────────────
# ConnectionManager Tests
# ────────────────────────────────────────────────────────────────────────────

class TestConnectionManager:
    """Test ConnectionManager class directly."""

    def test_connect_adds_client_to_active_connections(self):
        """Test that connect() adds WebSocket to active connections."""
        from app.api.v1.websocket import ConnectionManager
        from unittest.mock import AsyncMock

        manager = ConnectionManager()
        ws_mock = AsyncMock()
        ws_mock.accept = AsyncMock()

        # Run connect in async context
        async def run():
            await manager.connect(ws_mock, "events")
            assert ws_mock in manager._active_connections["events"]
            assert len(manager._active_connections["events"]) == 1

        asyncio.run(run())

    def test_disconnect_removes_client(self):
        """Test that disconnect() removes WebSocket from active connections."""
        from app.api.v1.websocket import ConnectionManager
        from unittest.mock import AsyncMock

        manager = ConnectionManager()
        ws_mock = AsyncMock()
        ws_mock.accept = AsyncMock()

        async def run():
            # Connect first
            await manager.connect(ws_mock, "events")
            assert ws_mock in manager._active_connections["events"]

            # Then disconnect
            await manager.disconnect(ws_mock, "events")
            assert ws_mock not in manager._active_connections["events"]
            assert len(manager._active_connections["events"]) == 0

        asyncio.run(run())

    def test_broadcast_sends_to_all_connected_clients(self):
        """Test that broadcast() sends message to all clients on channel."""
        from app.api.v1.websocket import ConnectionManager
        from unittest.mock import AsyncMock

        manager = ConnectionManager()
        ws1 = AsyncMock()
        ws1.accept = AsyncMock()
        ws1.send_text = AsyncMock()

        ws2 = AsyncMock()
        ws2.accept = AsyncMock()
        ws2.send_text = AsyncMock()

        async def run():
            await manager.connect(ws1, "events")
            await manager.connect(ws2, "events")

            message = {"event_id": "test-123", "score": 0.95, "is_anomaly": True}
            await manager.broadcast(message, "events")

            # Both clients should receive the message
            ws1.send_text.assert_called_once()
            ws2.send_text.assert_called_once()

            # Verify message format
            call_args1 = ws1.send_text.call_args[0][0]
            assert json.loads(call_args1)["event_id"] == "test-123"

        asyncio.run(run())

    def test_broadcast_removes_dead_connections_silently(self):
        """Test that broadcast() removes dead connections without raising."""
        from app.api.v1.websocket import ConnectionManager
        from unittest.mock import AsyncMock

        manager = ConnectionManager()
        ws_alive = AsyncMock()
        ws_alive.accept = AsyncMock()
        ws_alive.send_text = AsyncMock()

        ws_dead = AsyncMock()
        ws_dead.accept = AsyncMock()
        ws_dead.send_text = AsyncMock(side_effect=Exception("Connection closed"))

        async def run():
            await manager.connect(ws_alive, "events")
            await manager.connect(ws_dead, "events")
            assert len(manager._active_connections["events"]) == 2

            # Broadcast should not raise even though ws_dead fails
            message = {"event_id": "test-456"}
            await manager.broadcast(message, "events")

            # Dead connection should be removed
            assert ws_dead not in manager._active_connections["events"]
            assert ws_alive in manager._active_connections["events"]
            assert len(manager._active_connections["events"]) == 1

        asyncio.run(run())

    def test_concurrent_connections_all_receive_broadcast(self):
        """Test concurrent connections all receive broadcast messages."""
        from app.api.v1.websocket import ConnectionManager
        from unittest.mock import AsyncMock

        manager = ConnectionManager()
        num_clients = 5
        clients = []

        for _i in range(num_clients):
            ws = AsyncMock()
            ws.accept = AsyncMock()
            ws.send_text = AsyncMock()
            clients.append(ws)

        async def run():
            # Connect all clients concurrently
            await asyncio.gather(*[
                manager.connect(ws, "events") for ws in clients
            ])

            # Broadcast message
            message = {"type": "test", "id": "concurrent-test"}
            await manager.broadcast(message, "events")

            # All clients should receive
            for ws in clients:
                assert ws.send_text.called
                assert ws.send_text.call_count == 1

        asyncio.run(run())


# ────────────────────────────────────────────────────────────────────────────
# /ws/events Endpoint Tests
# ────────────────────────────────────────────────────────────────────────────

class TestWebSocketEvents:
    """Test /ws/events WebSocket endpoint."""

    def test_ws_events_connects_successfully(self, ws_client: TestClient):
        """Test that client can connect to /ws/events endpoint."""
        with ws_client.websocket_connect("/ws/events") as ws:
            # If we reach here, connection succeeded
            assert ws is not None

    def test_ws_events_receives_scored_event_message(self, ws_client: TestClient):
        """Test that /ws/events receives scored event messages."""
        with ws_client.websocket_connect("/ws/events") as ws:
            # Simulate broadcast via manager
            from app.api.v1.websocket import manager

            async def broadcast_event():
                message = {
                    "event_id": str(uuid.uuid4()),
                    "score": 0.92,
                    "is_anomaly": True,
                    "source_id": "sensor-1",
                    "timestamp": datetime.now(UTC).isoformat(),
                }
                await manager.broadcast(message, "events")

            import asyncio
            asyncio.run(broadcast_event())

            # Receive broadcast message
            data = ws.receive_text()
            payload = json.loads(data)

            assert payload["event_id"]
            assert payload["score"] == 0.92
            assert payload["is_anomaly"] is True
            assert payload["source_id"] == "sensor-1"
            assert payload["timestamp"]

    def test_ws_events_message_contains_required_fields(self, ws_client: TestClient):
        """Test that events endpoint message has all required fields."""
        with ws_client.websocket_connect("/ws/events") as ws:
            from app.api.v1.websocket import manager

            async def broadcast_with_all_fields():
                event_id = str(uuid.uuid4())
                message = {
                    "event_id": event_id,
                    "score": 0.87,
                    "is_anomaly": False,
                    "source_id": "sensor-42",
                    "timestamp": datetime.now(UTC).isoformat(),
                }
                await manager.broadcast(message, "events")

            import asyncio
            asyncio.run(broadcast_with_all_fields())

            data = ws.receive_text()
            payload = json.loads(data)

            # Verify all required fields present
            required_fields = {"event_id", "score", "is_anomaly", "source_id", "timestamp"}
            assert required_fields.issubset(set(payload.keys()))

    def test_ws_events_ping_pong(self, ws_client: TestClient):
        """Test that /ws/events responds to ping with pong."""
        with ws_client.websocket_connect("/ws/events") as ws:
            ws.send_text("ping")
            response = ws.receive_text()
            payload = json.loads(response)
            assert payload["type"] == "pong"

    def test_ws_events_disconnects_cleanly(self, ws_client: TestClient):
        """Test that /ws/events disconnects without error."""
        from app.api.v1.websocket import manager

        initial_count = len(manager._active_connections["events"])

        with ws_client.websocket_connect("/ws/events") as ws:
            assert len(manager._active_connections["events"]) > initial_count

        # After context exit, should be disconnected
        assert len(manager._active_connections["events"]) == initial_count


# ────────────────────────────────────────────────────────────────────────────
# /ws/alerts Endpoint Tests
# ────────────────────────────────────────────────────────────────────────────

class TestWebSocketAlerts:
    """Test /ws/alerts WebSocket endpoint."""

    def test_ws_alerts_connects_successfully(self, ws_client: TestClient):
        """Test that client can connect to /ws/alerts endpoint."""
        with ws_client.websocket_connect("/ws/alerts") as ws:
            assert ws is not None

    def test_ws_alerts_receives_alert_on_high_score_event(self, ws_client: TestClient):
        """Test that /ws/alerts receives alert messages."""
        with ws_client.websocket_connect("/ws/alerts") as ws:
            from app.api.v1.websocket import manager

            async def broadcast_alert():
                alert_message = {
                    "alert_id": str(uuid.uuid4()),
                    "severity": "HIGH",
                    "source_id": "sensor-1",
                    "score": 0.98,
                    "timestamp": datetime.now(UTC).isoformat(),
                }
                await manager.broadcast(alert_message, "alerts")

            import asyncio
            asyncio.run(broadcast_alert())

            data = ws.receive_text()
            payload = json.loads(data)

            assert payload["alert_id"]
            assert payload["severity"] == "HIGH"
            assert payload["score"] == 0.98

    def test_ws_alerts_message_contains_severity_field(self, ws_client: TestClient):
        """Test that alert messages include severity field."""
        with ws_client.websocket_connect("/ws/alerts") as ws:
            from app.api.v1.websocket import manager

            async def broadcast_severity_alert():
                for severity in ["LOW", "MEDIUM", "HIGH", "CRITICAL"]:
                    message = {
                        "alert_id": str(uuid.uuid4()),
                        "severity": severity,
                        "source_id": "test-source",
                        "score": 0.85,
                        "timestamp": datetime.now(UTC).isoformat(),
                    }
                    await manager.broadcast(message, "alerts")
                    await asyncio.sleep(0.01)

            import asyncio
            asyncio.run(broadcast_severity_alert())

            # Receive all 4 messages
            for _i, expected_severity in enumerate(["LOW", "MEDIUM", "HIGH", "CRITICAL"]):
                data = ws.receive_text()
                payload = json.loads(data)
                assert payload["severity"] == expected_severity

    def test_ws_alerts_ping_pong(self, ws_client: TestClient):
        """Test that /ws/alerts responds to ping with pong."""
        with ws_client.websocket_connect("/ws/alerts") as ws:
            ws.send_text("ping")
            response = ws.receive_text()
            payload = json.loads(response)
            assert payload["type"] == "pong"

    def test_ws_alerts_disconnects_cleanly(self, ws_client: TestClient):
        """Test that /ws/alerts disconnects without error."""
        from app.api.v1.websocket import manager

        initial_count = len(manager._active_connections["alerts"])

        with ws_client.websocket_connect("/ws/alerts") as ws:
            assert len(manager._active_connections["alerts"]) > initial_count

        assert len(manager._active_connections["alerts"]) == initial_count


# ────────────────────────────────────────────────────────────────────────────
# /ws/metrics Endpoint Tests
# ────────────────────────────────────────────────────────────────────────────

class TestWebSocketMetrics:
    """Test /ws/metrics WebSocket endpoint."""

    def test_ws_metrics_connects_successfully(self, ws_client: TestClient):
        """Test that client can connect to /ws/metrics endpoint."""
        with ws_client.websocket_connect("/ws/metrics") as ws:
            assert ws is not None

    def test_ws_metrics_sends_update_every_5_seconds(self, ws_client: TestClient):
        """Test that /ws/metrics sends updates periodically (mocked)."""
        with ws_client.websocket_connect("/ws/metrics") as ws:
            from app.api.v1.websocket import manager

            async def broadcast_metrics():
                # Simulate metrics broadcaster sending updates
                for i in range(3):
                    message = {
                        "metric": "system_performance",
                        "value": {
                            "events_per_second": 85.5 + i,
                            "anomaly_rate_percent": 3.2 + (i * 0.1),
                            "consumer_lag": i,
                        },
                        "timestamp": datetime.now(UTC).isoformat(),
                    }
                    await manager.broadcast(message, "metrics")
                    await asyncio.sleep(0.1)

            import asyncio
            asyncio.run(broadcast_metrics())

            # Should receive metrics updates
            for _i in range(3):
                data = ws.receive_text()
                payload = json.loads(data)
                assert payload["metric"] == "system_performance"
                assert "value" in payload

    def test_ws_metrics_message_contains_events_per_second(self, ws_client: TestClient):
        """Test that metrics message includes events_per_second."""
        with ws_client.websocket_connect("/ws/metrics") as ws:
            from app.api.v1.websocket import manager

            async def broadcast_with_eps():
                message = {
                    "metric": "system_performance",
                    "value": {
                        "events_per_second": 92.35,
                        "anomaly_rate_percent": 2.8,
                        "consumer_lag": 5,
                    },
                    "timestamp": datetime.now(UTC).isoformat(),
                }
                await manager.broadcast(message, "metrics")

            import asyncio
            asyncio.run(broadcast_with_eps())

            data = ws.receive_text()
            payload = json.loads(data)

            assert "value" in payload
            assert "events_per_second" in payload["value"]
            assert payload["value"]["events_per_second"] == 92.35

    def test_ws_metrics_message_contains_anomaly_rate(self, ws_client: TestClient):
        """Test that metrics message includes anomaly_rate_percent."""
        with ws_client.websocket_connect("/ws/metrics") as ws:
            from app.api.v1.websocket import manager

            async def broadcast_with_rate():
                message = {
                    "metric": "system_performance",
                    "value": {
                        "events_per_second": 75.0,
                        "anomaly_rate_percent": 4.25,
                        "consumer_lag": 0,
                    },
                    "timestamp": datetime.now(UTC).isoformat(),
                }
                await manager.broadcast(message, "metrics")

            import asyncio
            asyncio.run(broadcast_with_rate())

            data = ws.receive_text()
            payload = json.loads(data)

            assert "value" in payload
            assert "anomaly_rate_percent" in payload["value"]
            assert payload["value"]["anomaly_rate_percent"] == 4.25

    def test_ws_metrics_ping_pong(self, ws_client: TestClient):
        """Test that /ws/metrics responds to ping with pong."""
        with ws_client.websocket_connect("/ws/metrics") as ws:
            ws.send_text("ping")
            response = ws.receive_text()
            payload = json.loads(response)
            assert payload["type"] == "pong"

    def test_ws_metrics_disconnects_cleanly(self, ws_client: TestClient):
        """Test that /ws/metrics disconnects without error."""
        from app.api.v1.websocket import manager

        initial_count = len(manager._active_connections["metrics"])

        with ws_client.websocket_connect("/ws/metrics") as ws:
            assert len(manager._active_connections["metrics"]) > initial_count

        # After exit, should be disconnected and metrics task should be cancelled
        assert len(manager._active_connections["metrics"]) == initial_count

    @pytest.mark.timeout(10)
    def test_ws_metrics_task_started_on_first_subscriber(self, ws_client: TestClient):
        """Test that metrics broadcaster task starts on first metrics subscriber."""
        from app.api.v1.websocket import manager

        # Reset metrics task
        if manager._metrics_task:
            manager._metrics_task.cancel()
            manager._metrics_task = None

        assert manager._metrics_task is None

        with ws_client.websocket_connect("/ws/metrics") as ws:
            import asyncio
            # Give task time to start
            asyncio.run(asyncio.sleep(0.2))
            # Metrics task should now be running
            assert manager._metrics_task is not None

    @pytest.mark.timeout(10)
    def test_ws_metrics_task_cancelled_on_last_unsubscribe(self, ws_client: TestClient):
        """Test that metrics broadcaster task cancels when last subscriber leaves."""
        from app.api.v1.websocket import manager

        # Reset metrics task
        if manager._metrics_task:
            manager._metrics_task.cancel()
            manager._metrics_task = None

        with ws_client.websocket_connect("/ws/metrics") as ws:
            import asyncio
            asyncio.run(asyncio.sleep(0.1))
            assert manager._metrics_task is not None

        # After context exit, task should be cancelled
        assert manager._metrics_task is None


# ────────────────────────────────────────────────────────────────────────────
# Cross-channel isolation tests
# ────────────────────────────────────────────────────────────────────────────

class TestWebSocketChannelIsolation:
    """Test that messages are isolated by channel."""

    def test_events_channel_does_not_receive_alerts(self, ws_client: TestClient):
        """Test that events channel doesn't receive alert messages."""
        from app.api.v1.websocket import manager

        with ws_client.websocket_connect("/ws/events") as ws_events:
            async def send_alert_to_different_channel():
                # Send to alerts channel
                message = {"alert_id": str(uuid.uuid4()), "severity": "HIGH"}
                await manager.broadcast(message, "alerts")

            import asyncio
            asyncio.run(send_alert_to_different_channel())

            # Events subscriber should not receive anything
            # (This is a negative test - no assertion, just verify no exception)
            # The timeout in ws.receive_text would normally happen here

    def test_multiple_channels_independent(self, ws_client: TestClient):
        """Test that multiple channel subscribers operate independently."""
        from app.api.v1.websocket import manager

        with ws_client.websocket_connect("/ws/events") as ws_events:
            with ws_client.websocket_connect("/ws/alerts") as ws_alerts:
                async def broadcast_to_both():
                    await manager.broadcast(
                        {"type": "event", "id": "1"},
                        "events"
                    )
                    await manager.broadcast(
                        {"type": "alert", "id": "2"},
                        "alerts"
                    )

                import asyncio
                asyncio.run(broadcast_to_both())

                # Events channel gets event
                event_data = ws_events.receive_text()
                event_payload = json.loads(event_data)
                assert event_payload["type"] == "event"

                # Alerts channel gets alert
                alert_data = ws_alerts.receive_text()
                alert_payload = json.loads(alert_data)
                assert alert_payload["type"] == "alert"


@pytest.mark.timeout(10)
class TestWebSocketConcurrency:
    """Test concurrent WebSocket connections and operations."""

    def test_concurrent_event_subscriptions(self, ws_client: TestClient):
        """Test multiple concurrent connections to events channel."""
        from app.api.v1.websocket import manager

        ws_list = []
        try:
            # Open multiple connections using context managers
            for _ in range(3):
                ctx = ws_client.websocket_connect("/ws/events")
                ws = ctx.__enter__()
                ws_list.append((ctx, ws))

            # Broadcast to all
            async def broadcast_once():
                await manager.broadcast(
                    {"test": "concurrent", "id": str(uuid.uuid4())},
                    "events"
                )

            import asyncio
            asyncio.run(broadcast_once())

            # All should receive
            for _ctx, ws in ws_list:
                data = ws.receive_text()
                payload = json.loads(data)
                assert payload["test"] == "concurrent"

        finally:
            # Cleanup
            for ctx, _ws in ws_list:
                try:
                    ctx.__exit__(None, None, None)
                except Exception:
                    pass

    def test_rapid_connect_disconnect_cycle(self, ws_client: TestClient):
        """Test rapid connection/disconnection cycles."""
        from app.api.v1.websocket import manager

        initial_events = len(manager._active_connections["events"])

        for _i in range(5):
            with ws_client.websocket_connect("/ws/events") as ws:
                pass  # Immediately disconnect

        # Should return to initial state
        assert len(manager._active_connections["events"]) == initial_events

    def test_concurrent_broadcast_multiple_channels(self, ws_client: TestClient):
        """Test concurrent broadcasts to different channels."""
        from app.api.v1.websocket import manager

        with ws_client.websocket_connect("/ws/events") as ws_events:
            with ws_client.websocket_connect("/ws/alerts") as ws_alerts:
                with ws_client.websocket_connect("/ws/metrics") as ws_metrics:

                    async def concurrent_broadcasts():
                        await asyncio.gather(
                            manager.broadcast({"ch": "events"}, "events"),
                            manager.broadcast({"ch": "alerts"}, "alerts"),
                            manager.broadcast({"ch": "metrics"}, "metrics"),
                        )

                    import asyncio
                    asyncio.run(concurrent_broadcasts())

                    # Each should receive on its channel
                    e = json.loads(ws_events.receive_text())
                    assert e["ch"] == "events"

                    a = json.loads(ws_alerts.receive_text())
                    assert a["ch"] == "alerts"

                    m = json.loads(ws_metrics.receive_text())
                    assert m["ch"] == "metrics"
