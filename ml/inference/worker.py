"""
ML Inference Worker — container entry point
Starts: TimescaleDB writer + ML inference Kafka consumer loops
"""

from __future__ import annotations

import asyncio
import signal

import structlog

logger = structlog.get_logger(__name__)


async def main() -> None:
    from agents.dashboard.agent import DashboardBridgeWorker
    from agents.ml_inference.agent import MLInferenceWorker
    from agents.timescaledb_writer.agent import TimescaleDBWriterWorker

    inference_worker = MLInferenceWorker()
    writer_worker = TimescaleDBWriterWorker()
    dashboard_worker = DashboardBridgeWorker()

    loop = asyncio.get_running_loop()

    def _shutdown(sig: signal.Signals) -> None:
        logger.info("Shutdown signal received", signal=sig.name)
        inference_worker.stop()
        writer_worker.stop()
        dashboard_worker.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: _shutdown(s))

    logger.info("ML Inference Worker starting all consumer loops...")
    await asyncio.gather(
        inference_worker.run(),
        writer_worker.run(),
        dashboard_worker.run(),
        return_exceptions=True,
    )
    logger.info("All workers stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
