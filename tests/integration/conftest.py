"""
Integration-test conftest — executed before any integration test file is collected.

Why this file exists
--------------------
The FastAPI backend is structured so that, inside the Docker container, the
working directory is ``backend/``, meaning Python can resolve bare imports like
``from app.api.v1.router import api_router`` directly.

When pytest runs from the **repo root** (as GitHub Actions does), the ``backend``
directory is on sys.path as a package (``backend.app.*``), but the bare ``app``
namespace does not exist.  All internal imports inside ``backend/app/**`` use the
bare form, so importing ``backend.app.main`` triggers:

    ImportError: No module named 'app'

The correct fix is to insert ``backend/`` at the front of ``sys.path`` so that
both ``import app`` and ``import backend.app`` resolve correctly during tests.
This matches how the app runs in production (Docker WORKDIR=backend/).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# ── Make bare `app.*` imports work when running from repo root ──────────────
_BACKEND_DIR = Path(__file__).parent.parent.parent / "backend"
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

# ── Set required environment variables for the FastAPI Settings object ───────
# Settings uses Pydantic v2 BaseSettings with mandatory fields (Field(...)).
# These must be in the environment before `from backend.app.main import app`
# is called, otherwise Settings() raises a ValidationError.
_CI_ENV = {
    "DATABASE_URL": "postgresql+asyncpg://test_user:test_pw@localhost:5432/test_db",
    "TIMESCALE_PASSWORD": "test_pw",
    "JWT_SECRET_KEY": os.environ.get("JWT_SECRET_KEY", "ci-test-secret-key-minimum-32-chars-here"),
    "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", "sk-ant-test-key"),
    "CORS_ORIGINS": '["http://localhost:3000", "http://localhost:8080"]',
    "TESTING": "true",
}
for key, value in _CI_ENV.items():
    os.environ.setdefault(key, value)


import pytest
from collections.abc import Generator

@pytest.fixture(scope="module")
def run_background_workers(
    timescaledb_container,
    redis_container,
    kafka_container,
) -> Generator[None, None, None]:
    """Start MLInferenceWorker, TimescaleDBWriterWorker, and AlertAgent in background threads."""
    import asyncio
    import threading
    import structlog

    logger = structlog.get_logger("background_workers_fixture")
    logger.info("Starting background integration test workers...")

    from agents.ml_inference.agent import MLInferenceWorker
    from agents.timescaledb_writer.agent import TimescaleDBWriterWorker
    from agents.alert_agent import AlertAgent

    writer = TimescaleDBWriterWorker()
    agent = AlertAgent()
    ml_worker = MLInferenceWorker()

    stop_event = threading.Event()

    def run_ml_worker():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def run_and_wait():
            await asyncio.sleep(1.0)
            for attempt in range(5):
                try:
                    task = loop.create_task(ml_worker.run())
                    while not stop_event.is_set() and not task.done():
                        await asyncio.sleep(0.1)
                    if task.done() and task.exception():
                        raise task.exception()
                    ml_worker.stop()
                    await task
                    break
                except Exception as e:
                    logger.warning(f"MLInferenceWorker run failed, retrying in 2s (attempt {attempt+1}/5): {e}")
                    if stop_event.is_set():
                        break
                    await asyncio.sleep(2.0)

        try:
            loop.run_until_complete(run_and_wait())
        finally:
            loop.close()

    def run_writer():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def run_and_wait():
            await asyncio.sleep(1.0)
            for attempt in range(5):
                try:
                    task = loop.create_task(writer.run())
                    while not stop_event.is_set() and not task.done():
                        await asyncio.sleep(0.1)
                    if task.done() and task.exception():
                        raise task.exception()
                    writer.stop()
                    await task
                    break
                except Exception as e:
                    logger.warning(f"TimescaleDBWriterWorker run failed, retrying in 2s (attempt {attempt+1}/5): {e}")
                    if stop_event.is_set():
                        break
                    await asyncio.sleep(2.0)

        try:
            loop.run_until_complete(run_and_wait())
        finally:
            loop.close()

    def run_agent():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def run_and_wait():
            await asyncio.sleep(1.0)
            for attempt in range(5):
                try:
                    task = loop.create_task(agent.run())
                    while not stop_event.is_set() and not task.done():
                        await asyncio.sleep(0.1)
                    if task.done() and task.exception():
                        raise task.exception()
                    agent.stop()
                    await task
                    break
                except Exception as e:
                    logger.warning(f"AlertAgent run failed, retrying in 2s (attempt {attempt+1}/5): {e}")
                    if stop_event.is_set():
                        break
                    await asyncio.sleep(2.0)

        try:
            loop.run_until_complete(run_and_wait())
        finally:
            loop.close()

    t_ml = threading.Thread(target=run_ml_worker, daemon=True, name="MLInferenceWorkerThread")
    t_writer = threading.Thread(target=run_writer, daemon=True, name="TimescaleDBWriterThread")
    t_agent = threading.Thread(target=run_agent, daemon=True, name="AlertAgentThread")

    t_ml.start()
    t_writer.start()
    t_agent.start()

    logger.info("All background integration test workers started.")

    yield

    logger.info("Stopping background integration test workers...")
    stop_event.set()
    t_ml.join(timeout=5)
    t_writer.join(timeout=5)
    t_agent.join(timeout=5)
    logger.info("All background integration test workers stopped.")

