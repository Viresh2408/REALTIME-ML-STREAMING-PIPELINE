import os
import sys

# Ensure project root is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agents.env_loader import load_env

load_env()

from typing import Literal

from langgraph.graph import END, START, StateGraph

from agents.state import AgentState


def ingest_node(state: AgentState) -> dict:
    return {
        "agent_results": {**state.get("agent_results", {}), "ingest": "success"},
        "retry_count": 0,
    }


def inference_node(state: AgentState) -> dict:
    return {"agent_results": {**state.get("agent_results", {}), "inference": "success"}}


def write_node(state: AgentState) -> dict:
    return {"agent_results": {**state.get("agent_results", {}), "write": "success"}}


def alert_node(state: AgentState) -> dict:
    return {"agent_results": {**state.get("agent_results", {}), "alert": "success"}}


def notify_node(state: AgentState) -> dict:
    return {"agent_results": {**state.get("agent_results", {}), "notify": "success"}}


def escalate_node(state: AgentState) -> dict:
    return {
        "escalated": True,
        "agent_results": {**state.get("agent_results", {}), "escalate": "success"},
    }


def retrain_check_node(state: AgentState) -> dict:
    return {"agent_results": {**state.get("agent_results", {}), "retrain_check": "success"}}


def error_node(state: AgentState) -> dict:
    return {"agent_results": {**state.get("agent_results", {}), "error_handled": True}}


def score_router(state: AgentState) -> Literal["end", "notify_node", "escalate_node"]:
    """Conditional routing based on severity."""
    severity = state.get("severity", "LOW")
    if severity == "LOW":
        return "end"
    elif severity == "MEDIUM":
        return "notify_node"
    else:  # HIGH or CRITICAL
        return "escalate_node"


def build_graph():
    """Builds and compiles the Orchestrator DAG."""
    workflow = StateGraph(AgentState)

    # Add all nodes
    workflow.add_node("ingest", ingest_node)
    workflow.add_node("inference", inference_node)
    workflow.add_node("write", write_node)
    workflow.add_node("alert", alert_node)
    workflow.add_node("notify_node", notify_node)
    workflow.add_node("escalate_node", escalate_node)
    workflow.add_node("retrain_check", retrain_check_node)
    workflow.add_node("error", error_node)

    # Define primary edges
    workflow.add_edge(START, "ingest")
    workflow.add_edge("ingest", "inference")
    workflow.add_edge("inference", "write")
    workflow.add_edge("write", "alert")

    # Define conditional routing from alert
    workflow.add_conditional_edges(
        "alert",
        score_router,
        {"end": "retrain_check", "notify_node": "notify_node", "escalate_node": "escalate_node"},
    )

    workflow.add_edge("notify_node", "retrain_check")
    workflow.add_edge("escalate_node", "retrain_check")
    workflow.add_edge("retrain_check", END)

    # Compile graph
    return workflow.compile()


graph = build_graph()

if __name__ == "__main__":
    import os
    import subprocess
    import sys
    import time

    print("Starting Orchestrator and all Agent subprocesses...")

    # List of agent modules to start
    agents = [
        "agents.inference_agent",
        "agents.writer_agent",
        "agents.alert_agent",
        "agents.notification_agent",
        "agents.dashboard_agent",
        "agents.health_agent",
        "agents.retraining_agent",
        "agents.producer_agent",  # Starts last as it generates data
    ]

    # Ensure subprocesses can import the agents, features, and models modules correctly
    env = os.environ.copy()
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ml_root = os.path.join(project_root, "ml")
    env["PYTHONPATH"] = f"{project_root}{os.pathsep}{ml_root}"

    processes = []
    try:
        for agent_module in agents:
            print(f"Starting {agent_module}...")
            p = subprocess.Popen([sys.executable, "-m", agent_module], env=env)
            processes.append(p)
            time.sleep(1)  # Give each agent a second to initialize

        print("All agents started. Press Ctrl+C to stop.")
        for p in processes:
            p.wait()
    except KeyboardInterrupt:
        print("\nStopping all agents...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.wait()
        print("All agents stopped.")
