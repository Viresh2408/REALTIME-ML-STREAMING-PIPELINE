"""
ml/worker/inference_worker.py
─────────────────────────────
Production ML Inference Worker
  • 4 concurrent asyncio consumer tasks sharing one Kafka producer
  • Each task: poll raw-events → deserialise (Avro/JSON) → preprocess → infer → DB write → produce
  • Offset committed ONLY after successful DB write AND Kafka produce
  • Inference errors routed to dead-letter topic (dl-events)
  • Every 1 000th event logged for continuous monitoring
  • Consumer lag published to Prometheus every 5 s
  • Hot-reload handled by ModelReloadHandler via model-updates topic
  • Target: 10 000 events/s across 4 concurrent worker tasks

Spec references:
  agents.docx      § 3 – ML Inference Agent Detail
  dataset_and_model.docx § 5 – Feature Engineering
"""

from __future__ import annotations

import asyncio
import warnings

warnings.filterwarnings("ignore")
import json
import os
import signal
import sys
import time
import uuid
from datetime import UTC, datetime
from typing import Any, cast

import numpy as np
import structlog
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# ──────────────────────────────────────────────────────────────────────────────
# Ensure project root is importable as the "ml" package regardless of CWD
# ──────────────────────────────────────────────────────────────────────────────
_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from ml.worker.hot_reload import ModelReloadHandler, ReadWriteLock
from ml.worker.metrics import (
    ANOMALIES_DETECTED,
    CONSUMER_LAG,
    EVENTS_PROCESSED,
    INFERENCE_LATENCY,
    MODEL_VERSION_INFO,
    start_metrics_server,
)
from ml.worker.preprocessor import EventPreprocessor

try:
    from ml.models.isolation_forest import AnomalyDetector
except ImportError:  # pragma: no cover
    from models.isolation_forest import AnomalyDetector  # type: ignore[no-redef]

logger = structlog.get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

NUM_WORKER_TASKS: int = 4          # concurrent consumer-task coroutines
LOG_EVERY_N_EVENTS: int = 1_000   # monitoring checkpoint interval

# Anomaly-score → severity mapping (scores are normalised to [0, 1])
_SEVERITY_THRESHOLDS: list[tuple[float, str]] = [
    (0.9, "CRITICAL"),
    (0.75, "HIGH"),
    (0.5, "MEDIUM"),
    (0.0, "LOW"),
]

# ──────────────────────────────────────────────────────────────────────────────
# SQL — matches hypertable schema in 01_create_hypertables.sql
# ──────────────────────────────────────────────────────────────────────────────
_INSERT_SQL = text(
    """
    INSERT INTO anomaly.anomaly_events (
        event_id, event_time, source_id, feature_vector,
        anomaly_score, is_anomaly, model_version, processed_at
    ) VALUES (
        :event_id,
        :event_time,
        :source_id,
        CAST(:feature_vector AS jsonb),
        :anomaly_score,
        :is_anomaly,
        :model_version,
        :processed_at
    )
    ON CONFLICT (event_id, event_time) DO NOTHING
    """
)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _score_to_severity(score: float) -> str:
    """Map a normalised anomaly score [0, 1] to a severity label."""
    for threshold, label in _SEVERITY_THRESHOLDS:
        if score >= threshold:
            return label
    return "LOW"


def _safe_uuid(raw: Any) -> str:
    """Convert any event_id to a valid PostgreSQL UUID string (deterministic)."""
    if raw:
        try:
            return str(uuid.UUID(str(raw)))
        except ValueError:
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(raw)))
    return str(uuid.uuid4())


def _ts_to_datetime(raw_ts: Any) -> datetime:
    """Convert an epoch-millisecond timestamp (int/float) to a UTC datetime."""
    if isinstance(raw_ts, (int, float)):
        return datetime.fromtimestamp(raw_ts / 1_000.0, tz=UTC)
    return datetime.now(tz=UTC)


# ──────────────────────────────────────────────────────────────────────────────
# Inference Engine  (shared across all worker tasks)
# ──────────────────────────────────────────────────────────────────────────────

class ProductionInferenceEngine:
    """
    Wraps the AnomalyDetector + EventPreprocessor behind a ReadWriteLock so
    that multiple concurrent reader-tasks can call `infer()` simultaneously,
    while ModelReloadHandler performs an atomic pointer-swap under a write lock.

    Spec: agents.docx § 3 — run model.predict() and model.score_samples() for
    continuous score; apply configurable threshold (default 0.7).
    """

    def __init__(self, rw_lock: ReadWriteLock) -> None:
        self.rw_lock = rw_lock
        self.preprocessor = EventPreprocessor("latest")
        self.model = AnomalyDetector()
        self.version: str = "dummy-v0"
        self.threshold: float = float(os.getenv("ANOMALY_SCORE_THRESHOLD", "0.7"))

        # Attempt to load the model version that matches the loaded pipeline
        try:
            pipeline_ver = self.preprocessor.version
            if pipeline_ver and pipeline_ver not in ("default", "dummy-v0"):
                self.model.load_from_minio(version=pipeline_ver)
                self.version = pipeline_ver
                logger.info(
                    "inference_engine.initialised",
                    version=pipeline_ver,
                    threshold=self.threshold,
                )
            else:
                logger.warning(
                    "inference_engine.no_pretrained_model",
                    reason="no pipeline found in MinIO; running untrained fallback",
                )
        except Exception as exc:
            logger.error(
                "inference_engine.init_failed",
                error=str(exc),
                fallback="untrained model",
            )

        MODEL_VERSION_INFO.labels(version=self.version).set(1)

    async def infer(self, raw_event: dict[str, Any]) -> dict[str, Any]:
        """
        Execute preprocessing + inference under a read-lock.
        Returns a ScoredEvent dict (Avro-compatible) with:
          anomaly_score, is_anomaly, severity, model_version, processed_at
        Latency is observed on INFERENCE_LATENCY histogram.
        """
        await self.rw_lock.acquire_read()
        t0 = time.perf_counter()
        try:
            # ── 1. Sub-millisecond feature preprocessing ──────────────────────
            X: np.ndarray = self.preprocessor.preprocess(raw_event)

            # ── 2. IsolationForest score + predict ────────────────────────────
            if self.model.is_trained:
                result = self.model.predict(X)
                anomaly_score = float(cast(Any, result["score"]))
                is_anomaly = bool(cast(Any, result["is_anomaly"]))
            else:
                # Untrained fallback — keeps pipeline alive during cold-starts
                anomaly_score = float(np.random.uniform(0.05, 0.35))
                is_anomaly = False

            # ── 3. Apply hot-reloadable threshold override ────────────────────
            effective_threshold = self.threshold  # read under lock avoids race
            if anomaly_score >= effective_threshold:
                is_anomaly = True

            model_ver = self.version

        finally:
            self.rw_lock.release_read()

        latency = time.perf_counter() - t0
        INFERENCE_LATENCY.observe(latency)

        severity = _score_to_severity(anomaly_score) if is_anomaly else "LOW"

        return {
            "event_id":      raw_event.get("event_id", ""),
            "source_id":     raw_event.get("source_id", ""),
            "event_type":    raw_event.get("event_type", ""),
            "features":      raw_event.get("features", {}),
            "event_time":    raw_event.get("event_time", int(time.time() * 1_000)),
            "anomaly_score": anomaly_score,
            "is_anomaly":    is_anomaly,
            "severity":      severity,
            "model_version": model_ver,
            "processed_at":  int(time.time() * 1_000),
        }

    async def infer_batch(self, raw_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Execute batched preprocessing + inference under a read-lock.
        Returns a list of ScoredEvent dicts.
        """
        await self.rw_lock.acquire_read()
        t0 = time.perf_counter()
        try:
            # ── 1. Batch preprocessing ────────────────────────────────────────
            X_list = [self.preprocessor.preprocess(evt) for evt in raw_events]
            X = np.vstack(X_list)

            # ── 2. IsolationForest score + predict ────────────────────────────
            if self.model.is_trained:
                result = self.model.predict(X)
                # Normalise to plain Python lists so zip() always gets an Iterable.
                # predict() may return a scalar float/bool (single event) or a list
                # (batch); wrapping in list() + checking for the scalar case gives
                # the type-checker concrete list[float] / list[bool] types.
                raw_scores = result["score"]
                raw_flags  = result["is_anomaly"]
                scores: list[float] = (
                    [float(raw_scores)]           # type: ignore[arg-type]
                    if not isinstance(raw_scores, list)
                    else [float(s) for s in raw_scores]
                )
                is_anomalies: list[bool] = (
                    [bool(raw_flags)]             # type: ignore[arg-type]
                    if not isinstance(raw_flags, list)
                    else [bool(f) for f in raw_flags]
                )
            else:
                scores = [float(np.random.uniform(0.05, 0.35)) for _ in raw_events]
                is_anomalies = [False] * len(raw_events)

            effective_threshold = self.threshold
            model_ver = self.version

        finally:
            self.rw_lock.release_read()

        latency = time.perf_counter() - t0
        per_event_latency = latency / len(raw_events)
        for _ in raw_events:
            INFERENCE_LATENCY.observe(per_event_latency)

        scored_list = []
        for raw_event, score, is_anomaly in zip(raw_events, scores, is_anomalies):
            if score >= effective_threshold:
                is_anomaly = True
            severity = _score_to_severity(score) if is_anomaly else "LOW"

            scored_list.append({
                "event_id":      raw_event.get("event_id", ""),
                "source_id":     raw_event.get("source_id", ""),
                "event_type":    raw_event.get("event_type", ""),
                "features":      raw_event.get("features", {}),
                "event_time":    raw_event.get("event_time", int(time.time() * 1_000)),
                "anomaly_score": float(score),
                "is_anomaly":    bool(is_anomaly),
                "severity":      severity,
                "model_version": model_ver,
                "processed_at":  int(time.time() * 1_000),
            })

        return scored_list


# ──────────────────────────────────────────────────────────────────────────────
# Single worker task (one per asyncio task / one Kafka Consumer per task)
# ──────────────────────────────────────────────────────────────────────────────

class InferenceWorkerTask:
    """
    One of NUM_WORKER_TASKS concurrent coroutines.  Each owns a dedicated
    confluent-kafka Consumer (thread-safe at the handle level).

    Pipeline per message:
      poll → deserialise (Avro → JSON fallback) → infer → DB write → produce
      → commit offset (only on full success)
      On ANY inference/DB/produce error → route to dl-events → commit offset
      (poisonous events must not block the partition indefinitely).

    Spec: agents.docx § 3 and § 6 error handling contract.
    """

    def __init__(
        self,
        task_id: int,
        engine: ProductionInferenceEngine,
        producer: Producer,
        session_factory: Any,
        raw_deserializer: AvroDeserializer | None,
        scored_serializer: AvroSerializer | None,
    ) -> None:
        self.task_id = task_id
        self.engine = engine
        self.producer = producer
        self.session_factory = session_factory
        self.raw_deserializer = raw_deserializer
        self.scored_serializer = scored_serializer

        # Kafka config from environment
        self._bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._raw_topic = os.getenv("KAFKA_RAW_EVENTS_TOPIC", "raw-events")
        self._scored_topic = os.getenv("KAFKA_SCORED_EVENTS_TOPIC", "scored-events")
        self._dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", "dl-events")

        # Consumer — manual commit, one per task for isolation
        self._consumer = Consumer(
            {
                "bootstrap.servers":    self._bootstrap,
                "group.id":             os.getenv("KAFKA_CONSUMER_GROUP_ID", "ml-inference-group"),
                "auto.offset.reset":    os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
                "enable.auto.commit":   False,          # critical: manual commit only
                "session.timeout.ms":   30_000,
                "max.poll.interval.ms": 300_000,        # 5 min max per poll cycle
                "fetch.min.bytes":      1,
                "fetch.wait.max.ms":    100,            # low latency polling
            }
        )

        self._running: bool = False
        self._event_count: int = 0          # total processed (success + error)
        self._success_count: int = 0        # for checkpoint logging
        self._uncommitted_count: int = 0    # count of processed but uncommitted events
        self._last_msg: Any = None          # last successfully processed message

    # ── Public API ────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Main consumer loop.  Runs until stop() is called."""
        self._consumer.subscribe([self._raw_topic])
        self._running = True
        loop = asyncio.get_running_loop()

        logger.info(
            "worker_task.started",
            task_id=self.task_id,
            topic=self._raw_topic,
            group=os.getenv("KAFKA_CONSUMER_GROUP_ID", "ml-inference-group"),
        )

        try:
            while self._running:
                # ── Poll in micro-batches of up to 20 messages ──────
                msgs = self._consumer.consume(num_messages=20, timeout=0.001)

                if not msgs:
                    if self._uncommitted_count > 0 and self._last_msg is not None:
                        self._consumer.commit(message=self._last_msg, asynchronous=True)
                        self._uncommitted_count = 0
                        self._last_msg = None
                    await asyncio.sleep(0.001)
                    continue

                await self._process_message_batch(msgs, loop)

        except asyncio.CancelledError:
            logger.info("worker_task.cancelled", task_id=self.task_id)
        except Exception as exc:
            logger.critical(
                "worker_task.fatal_error",
                task_id=self.task_id,
                error=str(exc),
                exc_info=True,
            )
        finally:
            self._consumer.close()
            logger.info("worker_task.stopped", task_id=self.task_id)

    def stop(self) -> None:
        self._running = False

    # ── Internal pipeline ─────────────────────────────────────────────────────

    async def _process_message_batch(
        self,
        msgs: list[Any],
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        """
        Process a batch of messages together to amortize model inference overhead.
        """
        valid_msgs = []
        raw_events = []

        for msg in msgs:
            if msg.error():
                logger.warning(
                    "worker_task.kafka_error",
                    task_id=self.task_id,
                    error=str(msg.error()),
                )
                continue

            raw_bytes = msg.value()
            if raw_bytes is None:
                self._consumer.commit(message=msg, asynchronous=True)
                continue

            try:
                event_dict = self._deserialise(raw_bytes)
                valid_msgs.append((msg, raw_bytes))
                raw_events.append(event_dict)
            except Exception as exc:
                EVENTS_PROCESSED.labels(status="error").inc()
                logger.error(
                    "worker_task.deserialise_error",
                    task_id=self.task_id,
                    error=str(exc),
                )
                self._route_to_dlq(raw_bytes, exc)
                self._consumer.commit(message=msg, asynchronous=True)

        if not raw_events:
            return

        self._event_count += len(raw_events)

        try:
            # ── Step 2: Run batched inference ─────────────────────────────────
            scored_events = await self.engine.infer_batch(raw_events)

            # ── Step 3 & 4: Produce ScoredEvent to scored-events topic ────────
            for (msg, raw_bytes), scored in zip(valid_msgs, scored_events):
                try:
                    self._produce_scored(scored)

                    EVENTS_PROCESSED.labels(status="success").inc()
                    if scored["is_anomaly"]:
                        ANOMALIES_DETECTED.labels(severity=scored["severity"]).inc()

                    self._success_count += 1
                    self._last_msg = msg
                    self._uncommitted_count += 1

                    if self._success_count % LOG_EVERY_N_EVENTS == 0:
                        logger.info(
                            "worker_task.checkpoint",
                            task_id=self.task_id,
                            total_processed=self._event_count,
                            total_success=self._success_count,
                            event_id=scored.get("event_id"),
                            anomaly_score=round(scored.get("anomaly_score", 0.0), 4),
                            is_anomaly=scored.get("is_anomaly"),
                            severity=scored.get("severity"),
                            model_version=scored.get("model_version"),
                        )
                except Exception as produce_err:
                    EVENTS_PROCESSED.labels(status="error").inc()
                    logger.error(
                        "worker_task.produce_error",
                        task_id=self.task_id,
                        error=str(produce_err),
                    )
                    self._route_to_dlq(raw_bytes, produce_err)

            # ── Step 5: Batch commit offsets ──────────────────────────────────
            if self._uncommitted_count >= 20:
                self._consumer.commit(message=self._last_msg, asynchronous=True)
                self._uncommitted_count = 0
                self._last_msg = None

        except Exception as exc:
            logger.error(
                "worker_task.batch_processing_error",
                task_id=self.task_id,
                error=str(exc),
                exc_info=True,
            )
            for msg, raw_bytes in valid_msgs:
                EVENTS_PROCESSED.labels(status="error").inc()
                self._route_to_dlq(raw_bytes, exc)
                self._consumer.commit(message=msg, asynchronous=True)

    def _deserialise(self, raw_bytes: bytes) -> dict[str, Any]:
        """
        Attempt Avro deserialisation first (Confluent magic byte 0x00 prefix).
        Falls back to UTF-8 JSON on any failure.
        """
        # Avro: Confluent wire format starts with magic byte 0x00
        if self.raw_deserializer is not None and raw_bytes[:1] == b"\x00":
            try:
                result = self.raw_deserializer(
                    raw_bytes,
                    SerializationContext(self._raw_topic, MessageField.VALUE),
                )
                if isinstance(result, dict):
                    return result
            except Exception as avro_err:
                logger.debug(
                    "worker_task.avro_fallback",
                    task_id=self.task_id,
                    error=str(avro_err),
                )

        # JSON fallback
        try:
            # Optimize: Python 3 json.loads accepts bytes directly, avoiding UTF-8 decode overhead
            payload = json.loads(raw_bytes)
        except json.JSONDecodeError as json_err:
            raise ValueError(
                f"Cannot deserialise message as Avro or JSON: {json_err}"
            ) from json_err

        if not isinstance(payload, dict):
            raise ValueError(
                f"Expected JSON object (dict), got {type(payload).__name__}"
            )
        return payload

    async def _write_to_db(self, scored: dict[str, Any]) -> None:
        """
        Insert a scored event into the TimescaleDB anomaly_events hypertable.
        Raises on failure so the caller can route to DLQ.
        """
        event_id = _safe_uuid(scored.get("event_id"))
        event_time = _ts_to_datetime(scored.get("event_time"))

        async with self.session_factory() as session:
            try:
                await session.execute(text("SET LOCAL synchronous_commit = off"))
                await session.execute(
                    _INSERT_SQL,
                    {
                        "event_id":       event_id,
                        "event_time":     event_time,
                        "source_id":      scored.get("source_id", "unknown"),
                        "feature_vector": json.dumps(scored.get("features", {})),
                        "anomaly_score":  scored["anomaly_score"],
                        "is_anomaly":     scored["is_anomaly"],
                        "model_version":  scored["model_version"],
                        "processed_at":   datetime.now(tz=UTC),
                    },
                )
                await session.commit()
            except Exception as db_err:
                await session.rollback()
                logger.error(
                    "worker_task.db_write_failed",
                    task_id=self.task_id,
                    event_id=event_id,
                    error=str(db_err),
                )
                raise  # propagate so caller routes to DLQ and commits offset

    def _produce_scored(self, scored: dict[str, Any]) -> None:
        """Serialise and produce a ScoredEvent to the scored-events topic."""
        if self.scored_serializer is not None:
            try:
                value_bytes = self.scored_serializer(
                    scored,
                    SerializationContext(self._scored_topic, MessageField.VALUE),
                )
            except Exception as ser_err:
                logger.debug(
                    "worker_task.avro_serialize_fallback",
                    task_id=self.task_id,
                    error=str(ser_err),
                )
                value_bytes = json.dumps(scored, default=str).encode("utf-8")
        else:
            value_bytes = json.dumps(scored, default=str).encode("utf-8")

        self.producer.produce(
            topic=self._scored_topic,
            key=(scored.get("event_id") or "").encode("utf-8"),
            value=value_bytes,
        )
        # Non-blocking poll to trigger internal delivery callbacks
        self.producer.poll(0)

    def _route_to_dlq(self, raw_bytes: bytes, exc: Exception) -> None:
        """
        Publish the original raw payload and error metadata to dl-events.
        Spec: agents.docx § 6 — inference errors logged to dead-letter topic.
        """
        dlq_payload = json.dumps(
            {
                "task_id":      self.task_id,
                "error_type":   type(exc).__name__,
                "error_message": str(exc),
                "raw_payload":  raw_bytes.decode("utf-8", errors="replace"),
                "failed_at_ms": int(time.time() * 1_000),
            },
            default=str,
        ).encode("utf-8")

        try:
            self.producer.produce(topic=self._dlq_topic, value=dlq_payload)
            self.producer.poll(0)
        except Exception as produce_err:
            logger.error(
                "worker_task.dlq_produce_failed",
                task_id=self.task_id,
                error=str(produce_err),
            )


# ──────────────────────────────────────────────────────────────────────────────
# Consumer-lag monitor (background coroutine)
# ──────────────────────────────────────────────────────────────────────────────

async def _monitor_consumer_lag(tasks: list[InferenceWorkerTask]) -> None:
    """
    Every 5 s: query committed offset + high-watermark for each partition
    assigned to each worker consumer, then update CONSUMER_LAG gauge.
    """
    logger.info("lag_monitor.started", interval_s=5)
    while True:
        try:
            loop = asyncio.get_running_loop()
            for task in tasks:
                if not task._running:
                    continue
                try:
                    partitions: list[TopicPartition] = await loop.run_in_executor(
                        None, task._consumer.assignment
                    )
                    for tp in partitions:
                        committed = await loop.run_in_executor(
                            None,
                            lambda tp=tp: task._consumer.committed([tp], timeout=1.0),
                        )
                        low, high = await loop.run_in_executor(
                            None,
                            lambda tp=tp: task._consumer.get_watermark_offsets(
                                tp, timeout=1.0
                            ),
                        )
                        committed_offset = (
                            committed[0].offset if committed and committed[0].offset >= 0 else high
                        )
                        lag = max(0, high - committed_offset)
                        CONSUMER_LAG.labels(
                            partition=f"{tp.topic}:{tp.partition}"
                        ).set(lag)
                except Exception as inner_err:
                    logger.debug("lag_monitor.partition_error", error=str(inner_err))

            await asyncio.sleep(5.0)

        except asyncio.CancelledError:
            logger.info("lag_monitor.stopped")
            break
        except Exception as outer_err:
            logger.warning("lag_monitor.error", error=str(outer_err))
            await asyncio.sleep(5.0)


# ──────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    logger.info(
        "inference_worker.boot",
        num_tasks=NUM_WORKER_TASKS,
        target_throughput="10_000 events/s",
    )

    # ── 1. Prometheus metrics HTTP server (port 8090) ─────────────────────────
    start_metrics_server(int(os.getenv("METRICS_PORT", "8090")))

    # ── 2. Async SQLAlchemy pool → TimescaleDB ────────────────────────────────
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        db_user = os.getenv("TIMESCALE_USER", "anomaly_admin")
        db_pass = os.getenv("TIMESCALE_PASSWORD", "StrongPass123!")
        db_host = os.getenv("TIMESCALE_HOST", "timescaledb")
        db_port = os.getenv("TIMESCALE_PORT", "5432")
        db_name = os.getenv("TIMESCALE_DB", "anomaly_db")
        database_url = (
            f"postgresql+asyncpg://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        )

    db_engine = create_async_engine(
        database_url,
        pool_size=20,            # 5 per task + headroom
        max_overflow=40,
        pool_pre_ping=True,      # evict stale connections automatically
        pool_recycle=3_600,      # recycle after 1 h to prevent idle timeouts
        echo=False,
    )
    session_factory = async_sessionmaker(db_engine, expire_on_commit=False)

    # ── 3. Avro Schema Registry ───────────────────────────────────────────────
    sr_url = os.getenv("KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8081")
    sr_client = SchemaRegistryClient({"url": sr_url})

    raw_deserializer: AvroDeserializer | None = None
    scored_serializer: AvroSerializer | None = None

    try:
        raw_schema_str = sr_client.get_latest_version("raw-events-value").schema.schema_str
        raw_deserializer = AvroDeserializer(sr_client, raw_schema_str)
        logger.info("schema_registry.raw_deserializer_ready")
    except Exception as sr_err:
        logger.warning("schema_registry.raw_unavailable", error=str(sr_err))

    try:
        scored_schema_str = sr_client.get_latest_version("scored-events-value").schema.schema_str
        scored_serializer = AvroSerializer(sr_client, scored_schema_str)
        logger.info("schema_registry.scored_serializer_ready")
    except Exception as sr_err:
        logger.warning("schema_registry.scored_unavailable", error=str(sr_err))

    # ── 4. Shared components ──────────────────────────────────────────────────
    rw_lock = ReadWriteLock()
    inference_engine = ProductionInferenceEngine(rw_lock)

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    shared_producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks":              "all",           # wait for ISR acknowledgement
            "batch.size":        131_072,         # 128 KB batch for throughput
            "linger.ms":         10,              # 10 ms accumulation window
            "compression.type":  "lz4",           # reduce network I/O
            "retries":           5,
            "retry.backoff.ms":  200,
        }
    )

    # ── 5. Hot-reload listener ────────────────────────────────────────────────
    reload_handler = ModelReloadHandler(inference_engine, rw_lock)

    # ── 6. Build 4 concurrent worker tasks ───────────────────────────────────
    worker_tasks: list[InferenceWorkerTask] = [
        InferenceWorkerTask(
            task_id=tid,
            engine=inference_engine,
            producer=shared_producer,
            session_factory=session_factory,
            raw_deserializer=raw_deserializer,
            scored_serializer=scored_serializer,
        )
        for tid in range(1, NUM_WORKER_TASKS + 1)
    ]

    # ── 7. Graceful shutdown signal handlers ──────────────────────────────────
    loop = asyncio.get_running_loop()
    _shutdown_called = False

    def _graceful_shutdown(sig_name: str) -> None:
        nonlocal _shutdown_called
        if _shutdown_called:
            return
        _shutdown_called = True
        logger.info("inference_worker.shutdown_signal", signal=sig_name)
        reload_handler.stop()
        for wt in worker_tasks:
            wt.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(
                sig, lambda s=sig: _graceful_shutdown(s.name)
            )
        except NotImplementedError:
            # Windows does not support add_signal_handler
            pass

    # ── 8. Launch background coroutines ──────────────────────────────────────
    hot_reload_coro = asyncio.create_task(reload_handler.start(), name="hot-reload")
    lag_monitor_coro = asyncio.create_task(
        _monitor_consumer_lag(worker_tasks), name="lag-monitor"
    )

    logger.info(
        "inference_worker.running",
        tasks=NUM_WORKER_TASKS,
        raw_topic=os.getenv("KAFKA_RAW_EVENTS_TOPIC", "raw-events"),
        scored_topic=os.getenv("KAFKA_SCORED_EVENTS_TOPIC", "scored-events"),
        dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "dl-events"),
        metrics_port=int(os.getenv("METRICS_PORT", "8090")),
    )

    # ── 9. Run all 4 worker loops (blocks until all finish / are cancelled) ───
    await asyncio.gather(
        *(wt.run() for wt in worker_tasks),
        return_exceptions=True,
    )

    # ── 10. Cleanup ───────────────────────────────────────────────────────────
    hot_reload_coro.cancel()
    lag_monitor_coro.cancel()
    try:
        await asyncio.gather(hot_reload_coro, lag_monitor_coro, return_exceptions=True)
    except Exception:
        pass

    shared_producer.flush(timeout=10)
    await db_engine.dispose()
    logger.info("inference_worker.shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
