"""
Kafka producer manager — lifecycle wrapper for the FastAPI lifespan
"""

from __future__ import annotations

import structlog
from confluent_kafka import Producer

from app.core.config import settings

logger = structlog.get_logger(__name__)


class KafkaProducerManager:
    """Manages a shared Kafka producer across the FastAPI application lifecycle."""

    def __init__(self) -> None:
        self._producer: Producer | None = None

    async def start(self) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 5,
                "compression.type": "lz4",
            }
        )
        logger.info("Kafka producer initialised", servers=settings.KAFKA_BOOTSTRAP_SERVERS)

    async def stop(self) -> None:
        if self._producer:
            self._producer.flush(timeout=10)
            logger.info("Kafka producer flushed and closed")
            self._producer = None

    @property
    def producer(self) -> Producer:
        if self._producer is None:
            raise RuntimeError("Kafka producer is not initialised. Call start() first.")
        return self._producer


kafka_producer_manager = KafkaProducerManager()
