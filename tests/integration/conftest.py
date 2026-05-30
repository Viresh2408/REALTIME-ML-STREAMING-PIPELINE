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
    "TESTING": "true",
}
for key, value in _CI_ENV.items():
    os.environ.setdefault(key, value)
