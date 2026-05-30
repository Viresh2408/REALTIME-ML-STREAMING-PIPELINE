"""
backend/database/__init__.py
Package exports for the database layer.
"""
from app.core.database import Base

try:
    from database.connection import (
        DatabaseSessionManager,
        close_raw_pool,
        db_manager,
        get_db_session,
        get_raw_connection,
        init_raw_pool,
    )
    from database.models import AnomalyEvent, HourlyAnomalyStat
except ModuleNotFoundError:
    from backend.database.connection import (
        DatabaseSessionManager,
        close_raw_pool,
        db_manager,
        get_db_session,
        get_raw_connection,
        init_raw_pool,
    )
    from backend.database.models import AnomalyEvent, HourlyAnomalyStat

__all__ = [
    # Shared declarative base (from app.core.database)
    "Base",
    # Connection / pool
    "DatabaseSessionManager",
    "db_manager",
    "get_db_session",
    "get_raw_connection",
    "init_raw_pool",
    "close_raw_pool",
    # Models
    "AnomalyEvent",
    "HourlyAnomalyStat",
]
