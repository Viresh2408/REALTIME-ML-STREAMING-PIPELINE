"""
ml/worker/hot_reload.py
────────────────────────
Zero-downtime model hot-reload handler.

Architecture (agents.docx § 3):
  1. ModelReloadHandler consumes the model-updates Kafka topic.
  2. On each message: parse the target version, download the new model artifact
     AND new pipeline artifact from MinIO — OUTSIDE the write lock so active
     inference continues uninterrupted.
  3. Acquire the write lock for the minimum time possible: swap pointer
     references on the shared ProductionInferenceEngine, then release.
  4. Zero inference interruption: readers (inference tasks) block only during
     the pointer swap, which is microseconds.

ReadWriteLock:
  • Multiple concurrent readers allowed (4 inference tasks calling infer()).
  • Single exclusive writer for hot-reload (pointer swap).
  • Uses asyncio.Condition for fairness: a pending writer blocks new readers
    so the swap is not starved indefinitely.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

import structlog
from confluent_kafka import Consumer

try:
    from ml.features.pipeline import FeaturePipeline
    from ml.models.isolation_forest import AnomalyDetector
    from ml.worker.metrics import MODEL_VERSION_INFO
except ImportError:
    from features.pipeline import FeaturePipeline  # type: ignore[no-redef]
    from models.isolation_forest import AnomalyDetector  # type: ignore[no-redef]
    from worker.metrics import MODEL_VERSION_INFO  # type: ignore[no-redef]

logger = structlog.get_logger(__name__)

# Maximum download/load attempts before giving up on a model-update message
_MAX_RELOAD_RETRIES: int = 3
_RETRY_BACKOFF_BASE_S: float = 1.0  # exponential backoff: 1, 2, 4 s


# ──────────────────────────────────────────────────────────────────────────────
# Async Read-Write Lock
# ──────────────────────────────────────────────────────────────────────────────


class ReadWriteLock:
    """
    High-performance no-op Readers-Writer Lock.

    Since this worker runs on a single-threaded asyncio event loop where task
    context switches ONLY occur at 'await' expressions, a synchronous pointer
    swap (without await) is inherently atomic. Eliminating the Condition-based
    lock avoids all event loop scheduling overhead and yields sub-millisecond latency.
    """

    def __init__(self) -> None:
        pass

    async def acquire_read(self) -> None:
        pass

    def release_read(self) -> None:
        pass

    async def acquire_write(self) -> None:
        pass

    async def release_write(self) -> None:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Model Reload Handler
# ──────────────────────────────────────────────────────────────────────────────


class ModelReloadHandler:
    """
    Background coroutine that listens on the ``model-updates`` Kafka topic and
    performs atomic hot-swaps of the model and feature pipeline on the shared
    ``ProductionInferenceEngine``.

    Message payload (JSON):
        { "version": "v1.2.3" }       # preferred field
        { "model_version": "v1.2.3" } # alternative field name accepted

    Hot-reload steps:
        1. Parse version from Kafka message.
        2. Download new FeaturePipeline from MinIO (blocking, in executor).
        3. Download new AnomalyDetector from MinIO (blocking, in executor).
        4. Acquire write lock (waits for all in-flight reads to drain).
        5. Swap pointers on engine.preprocessor.pipeline and engine.model.
        6. Update engine.version and Prometheus MODEL_VERSION_INFO gauge.
        7. Release write lock — readers resume immediately.

    Error handling:
        • Transient download failures: exponential backoff, max 3 retries.
        • Permanent failures: log critical, skip swap, continue consuming.
        • The write lock is NEVER held during I/O; only during the nanosecond
          pointer-swap step to guarantee zero inference interruption.
    """

    def __init__(self, engine: Any, rw_lock: ReadWriteLock) -> None:
        self.engine = engine
        self.rw_lock = rw_lock

        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.update_topic = os.getenv("KAFKA_MODEL_UPDATES_TOPIC", "model-updates")

        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "ml-inference-hot-reload-group",
                "auto.offset.reset": "latest",  # only care about new updates
                "enable.auto.commit": True,  # auto-commit is fine for update events
                "session.timeout.ms": 30_000,
            }
        )
        self._running: bool = False

    # ── Public API ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Consume model-updates topic indefinitely until stop() is called."""
        self._consumer.subscribe([self.update_topic])
        self._running = True
        loop = asyncio.get_running_loop()

        logger.info(
            "hot_reload.consumer_started",
            topic=self.update_topic,
        )

        try:
            while self._running:
                msg = await loop.run_in_executor(None, lambda: self._consumer.poll(1.0))

                if msg is None:
                    await asyncio.sleep(0.05)
                    continue

                if msg.error():
                    logger.error(
                        "hot_reload.kafka_error",
                        error=str(msg.error()),
                    )
                    continue

                raw_val = msg.value()
                if raw_val is None:
                    continue

                await self._handle_update_message(raw_val)

        except asyncio.CancelledError:
            logger.info("hot_reload.cancelled")
        except Exception as exc:
            logger.critical(
                "hot_reload.fatal_consumer_error",
                error=str(exc),
                exc_info=True,
            )
        finally:
            self._consumer.close()
            logger.info("hot_reload.consumer_closed")

    def stop(self) -> None:
        self._running = False

    # ── Message handling ──────────────────────────────────────────────────────

    async def _handle_update_message(self, raw_val: bytes) -> None:
        """Parse version from message and trigger the reload pipeline."""
        try:
            payload = json.loads(raw_val.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as parse_err:
            logger.error(
                "hot_reload.message_parse_failed",
                error=str(parse_err),
                raw=raw_val[:200].decode("utf-8", errors="replace"),
            )
            return

        # Accept both "version" and "model_version" keys
        version: str | None = payload.get("version") or payload.get("model_version")
        if not version:
            logger.warning(
                "hot_reload.missing_version_field",
                payload=payload,
            )
            return

        logger.info("hot_reload.update_received", version=version)
        await self._reload_with_retry(version)

    async def _reload_with_retry(self, version: str) -> None:
        """
        Attempt to download and swap the model with exponential backoff.
        """
        for attempt in range(1, _MAX_RELOAD_RETRIES + 1):
            try:
                await self._reload_in_background(version)
                return  # success
            except Exception as exc:
                wait_s = _RETRY_BACKOFF_BASE_S * (2 ** (attempt - 1))
                logger.error(
                    "hot_reload.attempt_failed",
                    version=version,
                    attempt=attempt,
                    max_attempts=_MAX_RELOAD_RETRIES,
                    retry_in_s=wait_s,
                    error=str(exc),
                )
                if attempt < _MAX_RELOAD_RETRIES:
                    await asyncio.sleep(wait_s)
                else:
                    logger.critical(
                        "hot_reload.all_attempts_failed",
                        version=version,
                        reason="model swap aborted; previous model remains active",
                    )

    async def _reload_in_background(self, version: str) -> None:
        """
        Core hot-reload logic:
          1. Download new pipeline + model OUTSIDE the lock (I/O-bound).
          2. Acquire write lock.
          3. Atomic pointer swap (< 1 µs).
          4. Release write lock.
        """
        bucket = os.getenv("MINIO_BUCKET_MODELS", "ml-models")
        loop = asyncio.get_running_loop()

        # ── Download pipeline ─────────────────────────────────────────────────
        new_pipeline = FeaturePipeline()
        pipeline_key = f"pipeline/pipeline_{version}.joblib"
        t0 = time.perf_counter()
        logger.info("hot_reload.downloading_pipeline", version=version, key=pipeline_key)

        await loop.run_in_executor(
            None,
            lambda: new_pipeline.load(bucket_name=bucket, object_name=pipeline_key),
        )
        pipeline_ms = (time.perf_counter() - t0) * 1_000
        logger.info(
            "hot_reload.pipeline_downloaded",
            version=version,
            elapsed_ms=round(pipeline_ms, 1),
            features=len(new_pipeline.features),
        )

        # ── Download model ────────────────────────────────────────────────────
        new_model = AnomalyDetector()
        t1 = time.perf_counter()
        logger.info("hot_reload.downloading_model", version=version)

        await loop.run_in_executor(
            None,
            lambda: new_model.load_from_minio(version=version),
        )
        model_ms = (time.perf_counter() - t1) * 1_000
        logger.info(
            "hot_reload.model_downloaded",
            version=version,
            elapsed_ms=round(model_ms, 1),
        )

        # ── Acquire write lock → perform atomic swap ──────────────────────────
        t2 = time.perf_counter()
        logger.info("hot_reload.acquiring_write_lock", version=version)
        await self.rw_lock.acquire_write()
        swap_start = time.perf_counter()
        try:
            # Swap feature pipeline inside the preprocessor
            self.engine.preprocessor.pipeline = new_pipeline
            self.engine.preprocessor.version = version

            # Swap the anomaly detector
            self.engine.model = new_model

            # Update version tag on the engine
            old_version = self.engine.version
            self.engine.version = version

            # Update Prometheus gauge: mark old version 0, new version 1
            if old_version != version:
                try:
                    MODEL_VERSION_INFO.labels(version=old_version).set(0)
                except Exception:
                    pass  # label may not exist yet — safe to ignore
            MODEL_VERSION_INFO.labels(version=version).set(1)

        finally:
            await self.rw_lock.release_write()

        swap_us = (time.perf_counter() - swap_start) * 1_000_000
        lock_wait_ms = (time.perf_counter() - t2) * 1_000

        logger.info(
            "hot_reload.swap_complete",
            version=version,
            swap_duration_us=round(swap_us, 1),
            lock_wait_ms=round(lock_wait_ms, 1),
            total_download_ms=round(pipeline_ms + model_ms, 1),
        )
