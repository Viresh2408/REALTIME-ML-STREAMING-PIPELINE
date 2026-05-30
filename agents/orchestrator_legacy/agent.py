"""
Orchestrator Agent — Supervisor node in the LangGraph DAG
Architecture: Section 5 — always-on heartbeat, route decisions,
              health checks, fallback coordination
"""

from __future__ import annotations

from typing import Any, Literal

import structlog
from langgraph.graph import END, StateGraph

from agents.shared.state import AgentState
from agents.shared.tools import get_system_health

logger = structlog.get_logger(__name__)


def build_orchestrator_graph() -> Any:
    """
    Build the LangGraph supervisor graph.

    Nodes:
      - orchestrator: decides routing
      - producer: triggers event generation
      - ml_inference: processes events
      - alert: fires on threshold breach
      - END: terminal state

    Edges include conditional routing and retry cycles.
    """
    from agents.alert.agent import alert_node
    from agents.ml_inference.agent import ml_inference_node
    from agents.producer.agent import producer_node

    workflow = StateGraph(AgentState)

    # Register nodes
    workflow.add_node("orchestrator", orchestrator_node)
    workflow.add_node("producer", producer_node)
    workflow.add_node("ml_inference", ml_inference_node)
    workflow.add_node("alert", alert_node)

    # Entry point
    workflow.set_entry_point("orchestrator")

    # Conditional routing from orchestrator
    workflow.add_conditional_edges(
        "orchestrator",
        route_decision,
        {
            "produce": "producer",
            "infer": "ml_inference",
            "alert": "alert",
            "end": END,
        },
    )

    # After producer → back to orchestrator
    workflow.add_edge("producer", "orchestrator")
    # After inference → back to orchestrator (may trigger alert)
    workflow.add_edge("ml_inference", "orchestrator")
    # After alert → end
    workflow.add_edge("alert", END)

    return workflow.compile()


async def orchestrator_node(state: AgentState) -> AgentState:
    """
    Supervisor node: evaluates system state and emits routing decisions.
    Uses Claude claude-sonnet-4-20250514 for anomaly explanation and triage.
    """
    logger.info("Orchestrator tick", iteration=state.get("iteration", 0))

    health = await get_system_health()
    state["system_health"] = health
    state["iteration"] = state.get("iteration", 0) + 1

    # Check if last scored event exceeded threshold
    if state.get("anomaly_score", 0.0) > state.get("threshold", 0.7):
        state["next_action"] = "alert"
    elif state.get("needs_inference"):
        state["next_action"] = "infer"
    else:
        state["next_action"] = "produce"

    return state


def route_decision(state: AgentState) -> Literal["produce", "infer", "alert", "end"]:
    """Edge routing function based on orchestrator decision."""
    action = state.get("next_action", "end")
    if action not in {"produce", "infer", "alert", "end"}:
        return "end"
    return action  # type: ignore[return-value]


# Build the compiled graph (singleton)
orchestrator_graph = build_orchestrator_graph()
