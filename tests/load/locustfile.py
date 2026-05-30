"""
Locust Load Test — Real-Time Anomaly Detection API
locust 2.28 | techstack.docx Section 7 (Dev & CI)
Run: locust -f tests/load/locustfile.py --host=http://localhost:8000
"""
from __future__ import annotations

import random
import uuid
from typing import Any

from locust import HttpUser, between, task


def random_feature_vector(n_features: int = 10) -> list[float]:
    """Generate a random feature vector. Occasionally inject anomalies."""
    if random.random() < 0.05:  # 5% anomalies
        return [random.gauss(10, 2) for _ in range(n_features)]
    return [random.gauss(0, 1) for _ in range(n_features)]


def random_event_payload() -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "source_id": f"source-{random.randint(1, 20)}",
        "feature_vector": random_feature_vector(),
        "timestamp": "2024-01-01T00:00:00Z",
    }


class AnomalyAPIUser(HttpUser):
    """
    Simulates a real API user hitting the anomaly detection endpoints.
    Wait time: 0.5–2s between tasks to simulate realistic traffic.
    """

    wait_time = between(0.5, 2.0)
    # Set auth header after login
    token: str = ""

    def on_start(self) -> None:
        """Login and store JWT for subsequent requests."""
        response = self.client.post(
            "/api/v1/auth/login",
            data={"username": "loadtest@example.com", "password": "loadtest123"},
            name="/api/v1/auth/login",
        )
        if response.status_code == 200:
            self.token = response.json().get("access_token", "")

    @property
    def auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    @task(weight=5)
    def ingest_event(self) -> None:
        """POST /api/v1/events — main hot path."""
        self.client.post(
            "/api/v1/events",
            json=random_event_payload(),
            headers=self.auth_headers,
            name="/api/v1/events [POST]",
        )

    @task(weight=3)
    def list_events(self) -> None:
        """GET /api/v1/events — list recent anomaly events."""
        self.client.get(
            "/api/v1/events?limit=50&offset=0",
            headers=self.auth_headers,
            name="/api/v1/events [GET]",
        )

    @task(weight=2)
    def get_anomaly_stats(self) -> None:
        """GET /api/v1/stats — dashboard statistics endpoint."""
        self.client.get(
            "/api/v1/stats",
            headers=self.auth_headers,
            name="/api/v1/stats",
        )

    @task(weight=1)
    def health_check(self) -> None:
        """GET /health — lightweight liveness check."""
        self.client.get("/health", name="/health")
