"""
Shared agent state — TypedDict for LangGraph StateGraph
"""
from __future__ import annotations

from typing import Any, TypedDict


class AgentState(TypedDict, total=False):
    """Shared mutable state passed between all agent nodes in the graph."""

    # Routing
    next_action: str
    iteration: int

    # Event payload
    event_id: str
    source_id: str
    feature_vector: list[float]
    raw_event: dict[str, Any]

    # ML output
    anomaly_score: float
    is_anomaly: bool
    model_version: str
    threshold: float
    needs_inference: bool

    # System health
    system_health: dict[str, Any]

    # Alert
    alert_severity: str
    alert_sent: bool

    # LLM explanation
    llm_explanation: str

    # Error handling
    error: str | None
    retry_count: int
