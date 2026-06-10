"""
tests/unit/test_orchestrator.py
─────────────────────────────────────────────────────────────────────────────
Unit tests for agents/orchestrator.py (LangGraph workflow).

Coverage targets: All graph nodes and routing logic.
No external dependencies — all nodes mocked.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Setup sys.path for imports
_BACKEND_DIR = Path(__file__).parent.parent.parent / "backend"
_AGENTS_DIR = Path(__file__).parent.parent.parent / "agents"
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))
if str(_AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(_AGENTS_DIR))

# Set required env vars before importing agents
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
os.environ.setdefault("JWT_SECRET_KEY", "test-key-minimum-32-characters-long")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test")


class TestOrchestratorGraph:
    """Test the orchestrator graph compilation and routing."""

    def test_graph_compiles_without_error(self):
        """Test that the orchestrator graph compiles successfully."""
        from agents.orchestrator import build_graph

        graph = build_graph()
        assert graph is not None
        # Verify the graph has a runnable interface
        assert hasattr(graph, "invoke") or hasattr(graph, "stream")

    def test_low_score_routes_to_end_not_alert(self):
        """Test that LOW severity routes to retrain_check, not escalate."""
        from agents.orchestrator import score_router

        state = {
            "event_id": "event-1",
            "severity": "LOW",
            "anomaly_score": 0.3,
            "retry_count": 0,
            "escalated": False,
            "agent_results": {},
            "error": None,
        }

        result = score_router(state)
        assert result == "end"

    def test_medium_score_routes_to_notify_node(self):
        """Test that MEDIUM severity routes to notify_node."""
        from agents.orchestrator import score_router

        state = {
            "event_id": "event-2",
            "severity": "MEDIUM",
            "anomaly_score": 0.65,
            "retry_count": 0,
            "escalated": False,
            "agent_results": {},
            "error": None,
        }

        result = score_router(state)
        assert result == "notify_node"

    def test_critical_score_routes_to_escalate_node(self):
        """Test that HIGH/CRITICAL severity routes to escalate_node."""
        from agents.orchestrator import score_router

        for severity in ["HIGH", "CRITICAL"]:
            state = {
                "event_id": f"event-{severity}",
                "severity": severity,
                "anomaly_score": 0.95,
                "retry_count": 0,
                "escalated": False,
                "agent_results": {},
                "error": None,
            }

            result = score_router(state)
            assert result == "escalate_node"

    def test_ingest_node_resets_retry_count(self):
        """Test that ingest node resets retry count."""
        from agents.orchestrator import ingest_node

        state = {
            "event_id": "event-1",
            "severity": "MEDIUM",
            "anomaly_score": 0.5,
            "retry_count": 3,  # Already failed twice
            "escalated": False,
            "agent_results": {},
            "error": None,
        }

        result = ingest_node(state)
        assert result["retry_count"] == 0
        assert "ingest" in result["agent_results"]
        assert result["agent_results"]["ingest"] == "success"

    def test_inference_node_updates_results(self):
        """Test that inference node updates agent_results."""
        from agents.orchestrator import inference_node

        state = {
            "event_id": "event-1",
            "severity": "MEDIUM",
            "anomaly_score": 0.5,
            "retry_count": 0,
            "escalated": False,
            "agent_results": {"ingest": "success"},
            "error": None,
        }

        result = inference_node(state)
        assert "inference" in result["agent_results"]
        assert result["agent_results"]["inference"] == "success"

    def test_write_node_chains_results(self):
        """Test that write node chains agent results."""
        from agents.orchestrator import write_node

        state = {
            "event_id": "event-1",
            "severity": "MEDIUM",
            "anomaly_score": 0.5,
            "retry_count": 0,
            "escalated": False,
            "agent_results": {"ingest": "success", "inference": "success"},
            "error": None,
        }

        result = write_node(state)
        assert "write" in result["agent_results"]
        assert result["agent_results"]["ingest"] == "success"
        assert result["agent_results"]["inference"] == "success"

    def test_alert_node_produces_routing_decision(self):
        """Test that alert node updates results for routing."""
        from agents.orchestrator import alert_node

        state = {
            "event_id": "event-1",
            "severity": "HIGH",
            "anomaly_score": 0.8,
            "retry_count": 0,
            "escalated": False,
            "agent_results": {"ingest": "success", "inference": "success", "write": "success"},
            "error": None,
        }

        result = alert_node(state)
        assert "alert" in result["agent_results"]

    def test_escalate_node_sets_escalated_flag(self):
        """Test that escalate_node sets escalated flag."""
        from agents.orchestrator import escalate_node

        state = {
            "event_id": "event-1",
            "severity": "CRITICAL",
            "anomaly_score": 0.95,
            "retry_count": 0,
            "escalated": False,
            "agent_results": {},
            "error": None,
        }

        result = escalate_node(state)
        assert result["escalated"] is True
        assert "escalate" in result["agent_results"]

    def test_notify_node_updates_results(self):
        """Test that notify_node updates results."""
        from agents.orchestrator import notify_node

        state = {
            "event_id": "event-1",
            "severity": "MEDIUM",
            "anomaly_score": 0.65,
            "retry_count": 0,
            "escalated": False,
            "agent_results": {},
            "error": None,
        }

        result = notify_node(state)
        assert "notify" in result["agent_results"]

    def test_retrain_check_node_evaluates_metrics(self):
        """Test that retrain_check_node evaluates retraining decision."""
        from agents.orchestrator import retrain_check_node

        state = {
            "event_id": "event-1",
            "severity": "MEDIUM",
            "anomaly_score": 0.65,
            "retry_count": 0,
            "escalated": False,
            "agent_results": {"alert": "success"},
            "error": None,
        }

        result = retrain_check_node(state)
        assert "retrain_check" in result["agent_results"]

    def test_error_node_handles_failures(self):
        """Test that error_node handles failures gracefully."""
        from agents.orchestrator import error_node

        state = {
            "event_id": "event-1",
            "severity": "MEDIUM",
            "anomaly_score": 0.65,
            "retry_count": 3,
            "escalated": False,
            "agent_results": {"error_handled": False},
            "error": "Database connection failed",
        }

        result = error_node(state)
        assert result["agent_results"]["error_handled"] is True

    def test_state_carries_event_id_through_graph(self):
        """Test that event_id is preserved through the entire graph."""
        from agents.orchestrator import build_graph

        initial_state = {
            "event_id": "test-event-12345",
            "severity": "LOW",
            "anomaly_score": 0.3,
            "retry_count": 0,
            "escalated": False,
            "agent_results": {},
            "error": None,
        }

        graph = build_graph()
        # Invoke graph with test state
        result = graph.invoke(initial_state)

        # Verify event_id is preserved
        assert result["event_id"] == "test-event-12345"

    def test_state_carries_severity_through_routing(self):
        """Test that severity is used for routing decisions."""
        from agents.orchestrator import build_graph

        for severity in ["LOW", "MEDIUM", "HIGH", "CRITICAL"]:
            state = {
                "event_id": f"event-{severity}",
                "severity": severity,
                "anomaly_score": 0.5 if severity == "LOW" else 0.8,
                "retry_count": 0,
                "escalated": False,
                "agent_results": {},
                "error": None,
            }

            graph = build_graph()
            result = graph.invoke(state)
            # Verify severity is preserved
            assert result["severity"] == severity

    def test_multiple_node_execution_accumulates_results(self):
        """Test that running through multiple nodes accumulates results."""
        from agents.orchestrator import ingest_node, inference_node, write_node

        state = {
            "event_id": "event-1",
            "severity": "MEDIUM",
            "anomaly_score": 0.5,
            "retry_count": 0,
            "escalated": False,
            "agent_results": {},
            "error": None,
        }

        # Run through multiple nodes
        state = {**state, **ingest_node(state)}
        state = {**state, **inference_node(state)}
        state = {**state, **write_node(state)}

        # Verify all results are accumulated
        assert "ingest" in state["agent_results"]
        assert "inference" in state["agent_results"]
        assert "write" in state["agent_results"]
        assert state["retry_count"] == 0  # Reset by ingest
