from typing import TypedDict, Optional, Dict, Any

class AgentState(TypedDict):
    """
    Central state object for the LangGraph Orchestrator.
    Carries context, results, and routing flags across all nodes.
    """
    event_id: str
    anomaly_score: float
    severity: str
    retry_count: int
    escalated: bool
    agent_results: Dict[str, Any]
    error: Optional[str]
