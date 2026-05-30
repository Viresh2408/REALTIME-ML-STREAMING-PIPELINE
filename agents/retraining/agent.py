"""
Model Retraining Agent
Architecture: Section 5 — Scheduled (Cron daily or drift signal)
Output: Updated model artifact saved to MinIO + MLflow
Uses: APScheduler 3.10, scikit-learn 1.5, MLflow 2.13, MinIO
"""
from __future__ import annotations

import os
from datetime import UTC, datetime

import numpy as np
import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = structlog.get_logger(__name__)


async def fetch_training_data_from_db(
    days: int = 30,
    min_samples: int = 1000,
) -> np.ndarray | None:
    """
    Query TimescaleDB for recent feature vectors to use as retraining data.
    Returns a numpy array or None if insufficient data.
    """
    try:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine(os.environ["DATABASE_URL"])
        query = text("""
            SELECT feature_vector
            FROM anomaly.anomaly_events
            WHERE event_time > NOW() - INTERVAL ':days days'
            ORDER BY event_time DESC
            LIMIT 100000
        """)

        async with engine.begin() as conn:
            result = await conn.execute(query, {"days": days})
            rows = result.fetchall()

        if len(rows) < min_samples:
            logger.warning(
                "Insufficient data for retraining",
                available=len(rows),
                minimum=min_samples,
            )
            return None

        # Each feature_vector is a JSON list stored as JSONB
        import json
        arrays = [np.array(json.loads(row[0]) if isinstance(row[0], str) else row[0]) for row in rows]
        return np.vstack(arrays)

    except Exception as exc:
        logger.error("Failed to fetch training data", error=str(exc))
        return None


async def retrain_and_publish(model_version: str | None = None) -> bool:
    """
    Full retraining pipeline:
    1. Fetch data from TimescaleDB
    2. Train IsolationForest with MLflow tracking
    3. Upload artifact to MinIO
    4. Signal ML worker to hot-reload
    5. Publish model-updates Kafka message
    """
    from agents.shared.tools import trigger_model_reload
    from ml.training.train_isolation_forest import train_isolation_forest

    if model_version is None:
        model_version = f"v{datetime.now(tz=UTC).strftime('%Y%m%d%H%M%S')}"

    logger.info("Retraining pipeline started", model_version=model_version)

    data = await fetch_training_data_from_db()
    if data is None:
        logger.warning("Retraining skipped — insufficient data")
        return False

    artifact_path = os.environ.get("MODEL_ARTIFACT_PATH", "/app/artifacts")
    metrics = train_isolation_forest(
        data=data,
        contamination=float(os.environ.get("CONTAMINATION", "0.1")),
        artifact_path=artifact_path,
        model_version=model_version,
    )

    logger.info("Retraining complete", version=model_version, **metrics)

    # Signal hot-reload
    success = await trigger_model_reload(model_version)
    if not success:
        logger.warning("Hot-reload signal failed — model will be picked up on next restart")

    return True


def create_retraining_scheduler() -> AsyncIOScheduler:
    """
    Build and return an APScheduler AsyncIOScheduler with the daily
    retraining cron job configured from environment variables.
    """
    scheduler = AsyncIOScheduler()
    hour = int(os.environ.get("RETRAINING_CRON_HOUR", "2"))
    minute = int(os.environ.get("RETRAINING_CRON_MINUTE", "0"))

    scheduler.add_job(
        retrain_and_publish,
        trigger=CronTrigger(hour=hour, minute=minute),
        id="daily_retraining",
        name="Daily IsolationForest Retraining",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    logger.info("Retraining scheduler configured", hour=hour, minute=minute)
    return scheduler
