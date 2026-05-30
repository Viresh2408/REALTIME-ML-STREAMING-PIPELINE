import asyncio
import os

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


class AsyncKafkaProducer:
    def __init__(self):
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

        # Producer configuration with custom batching and ack rules
        producer_conf = {
            "bootstrap.servers": bootstrap_servers,
            "batch.size": 65536,
            "linger.ms": 5,
            "acks": "all"
        }
        self.producer = Producer(producer_conf)

        # Schema registry client
        schema_registry_conf = {"url": schema_registry_url}
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Initialize serializers lazily to avoid blocking on startup
        self.raw_event_serializer = None
        self.alert_serializer = None

    def _get_raw_event_serializer(self):
        if self.raw_event_serializer is None:
            schema_str = self.schema_registry_client.get_latest_version("raw-events-value").schema.schema_str
            self.raw_event_serializer = AvroSerializer(
                self.schema_registry_client,
                schema_str
            )
        return self.raw_event_serializer

    def _get_alert_serializer(self):
        if self.alert_serializer is None:
            schema_str = self.schema_registry_client.get_latest_version("alerts-value").schema.schema_str
            self.alert_serializer = AvroSerializer(
                self.schema_registry_client,
                schema_str
            )
        return self.alert_serializer

    def _delivery_report(self, err, msg):
        """Callback for tracking message delivery."""
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    async def produce_raw_event(self, event_data: dict):
        """Async method to produce a RawEvent."""
        serializer = self._get_raw_event_serializer()

        loop = asyncio.get_event_loop()

        def _produce():
            self.producer.produce(
                topic="raw-events",
                key=str(event_data.get("event_id")),
                value=serializer(event_data, SerializationContext("raw-events-value", MessageField.VALUE)),
                on_delivery=self._delivery_report
            )
            self.producer.poll(0)

        await loop.run_in_executor(None, _produce)

    async def produce_alert(self, alert_data: dict):
        """Async method to produce an AlertEvent."""
        serializer = self._get_alert_serializer()

        loop = asyncio.get_event_loop()

        def _produce():
            self.producer.produce(
                topic="alerts",
                key=str(alert_data.get("alert_id")),
                value=serializer(alert_data, SerializationContext("alerts-value", MessageField.VALUE)),
                on_delivery=self._delivery_report
            )
            self.producer.poll(0)

        await loop.run_in_executor(None, _produce)

    def flush(self):
        """Wait for any outstanding messages to be delivered."""
        self.producer.flush()
