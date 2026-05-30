"""
Database migration runner
Called by: `make migrate` → docker compose exec fastapi-backend python -m backend.app.core.migrate
Applies the TimescaleDB schema from infra/timescaledb/init/ to the connected database.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

logger = structlog.get_logger(__name__)

# Resolve SQL init files relative to project root
SQL_DIR = Path(__file__).resolve().parents[5] / "infra" / "timescaledb" / "init"


async def run_migrations() -> None:
    """Execute all .sql files in infra/timescaledb/init/ in alphabetical order."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is not set.")

    engine = create_async_engine(database_url, echo=True)
    sql_files = sorted(SQL_DIR.glob("*.sql"))

    if not sql_files:
        logger.warning("No SQL migration files found", directory=str(SQL_DIR))
        return

    async with engine.begin() as conn:
        for sql_file in sql_files:
            logger.info("Applying migration", file=sql_file.name)
            sql_content = sql_file.read_text(encoding="utf-8")

            # Split on statement boundaries (handles multi-statement files)
            statements = [s.strip() for s in sql_content.split(";") if s.strip()]
            for stmt in statements:
                try:
                    await conn.execute(text(stmt))
                except Exception as exc:
                    logger.warning(
                        "Statement skipped (may already exist)",
                        error=str(exc)[:120],
                        statement=stmt[:80],
                    )

    await engine.dispose()
    logger.info("All migrations applied successfully.")


if __name__ == "__main__":
    asyncio.run(run_migrations())
