"""
ML Inference Agent
Architecture: Section 5 — Stateless worker, Kafka poll loop
Input: raw-events topic (6 partitions, 24h retention)
Output: anomaly score + label → scored-events topic
Models: IsolationForest (scikit-learn 1.5) + Autoencoder (PyTorch 2.3)
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import numpy as np
import structlog
from confluent_kafka import Consumer, KafkaException, Producer

from agents.shared.state import AgentState
from ml.inference.engine import InferenceEngine

logger = structlog.get_logger(__name__)


async def ml_inference_node(state: AgentState) -> AgentState:
    """
    LangGraph node: run ML inference on the current event's feature vector.
    Returns updated state with anomaly_score and is_anomaly fields.
    """
    feature_vector: list[float] = state.get("feature_vector", [])
    if not feature_vector:
        logger.warning("ml_inference_node called with empty feature vector")
        state["anomaly_score"] = 0.0
        state["is_anomaly"] = False
        return state

    engine = InferenceEngine.get_instance()
    score, model_version = await engine.predict(np.array(feature_vector))

    threshold = state.get("threshold", float(os.getenv("ANOMALY_SCORE_THRESHOLD", "0.7")))
    is_anomaly = score > threshold

    logger.info(
        "ML inference complete",
        event_id=state.get("event_id"),
        score=score,
        is_anomaly=is_anomaly,
        model_version=model_version,
    )

    state["anomaly_score"] = score
    state["is_anomaly"] = is_anomaly
    state["model_version"] = model_version
    state["needs_inference"] = False

    return state


class MLInferenceWorker:
    """
    Standalone Kafka consumer loop for the ML inference worker container.
    Polls raw-events, runs inference, publishes to scored-events.
    """

    def __init__(self) -> None:
        bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        group_id = os.environ.get("KAFKA_CONSUMER_GROUP_ID", "ml-inference-group")
        raw_topic = os.environ.get("KAFKA_RAW_EVENTS_TOPIC", "raw-events")
        scored_topic = os.environ.get("KAFKA_SCORED_EVENTS_TOPIC", "scored-events")

        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest"),
                "enable.auto.commit": False,
                "session.timeout.ms": 30000,
                "max.poll.interval.ms": 300000,
            }
        )
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.raw_topic = raw_topic
        self.scored_topic = scored_topic
        self.engine = InferenceEngine.get_instance()
        self._running = False

    async def run(self) -> None:
        """Main polling loop."""
        self.consumer.subscribe([self.raw_topic])
        self._running = True
        logger.info("ML inference worker started", topic=self.raw_topic)

        try:
            while self._running:
                msg = self.consumer.poll(timeout=0.1)
                if msg is None:
                    await asyncio.sleep(0.01)
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                val = msg.value()
                if val is None:
                    await asyncio.sleep(0.01)
                    continue
                event: dict[str, Any] = json.loads(val)
                await self._process(event)
                self.consumer.commit(message=msg)
                await asyncio.sleep(0.01)
        finally:
            self.consumer.close()

    async def _process(self, event: dict[str, Any]) -> None:
        """Process a single raw event and publish scored result."""
        features = np.array(event.get("feature_vector", []))
        score, model_version = await self.engine.predict(features)
        threshold = float(os.environ.get("ANOMALY_SCORE_THRESHOLD", "0.7"))

        scored = {
            **event,
            "anomaly_score": score,
            "is_anomaly": score > threshold,
            "model_version": model_version,
        }
        self.producer.produce(
            self.scored_topic,
            key=event.get("event_id", ""),
            value=json.dumps(scored),
        )
        self.producer.poll(0)

    def stop(self) -> None:
        self._running = False
