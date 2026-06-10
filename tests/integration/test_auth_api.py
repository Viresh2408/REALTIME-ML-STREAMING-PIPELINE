"""
tests/integration/test_auth_api.py
─────────────────────────────────────────────────────────────────────────────
Integration tests for POST /api/v1/auth endpoints.

Coverage targets (auth.py):
  Lines 70-269 – login, refresh, revoke, role checks

All tests run WITHOUT a live database/Kafka/Redis:
  • Redis (token blacklist) → fakeredis
  • No DB interaction in auth endpoints
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from jose import jwt  # type: ignore[import]


# ── Auth helpers ──────────────────────────────────────────────────────────────

def _get_settings():
    from app.core.config import settings  # type: ignore[import]
    return settings


@asynccontextmanager
async def auth_client() -> AsyncGenerator[AsyncClient, None]:
    """
    Async test client wired to the FastAPI app with fakeredis replacing
    the real Redis blacklist store.

    We patch redis_pool.connect / redis_pool.disconnect as no-ops so the
    FastAPI lifespan does NOT overwrite _client with a real Redis connection.
    The fakeredis instance is injected directly into redis_pool._client before
    the AsyncClient (and therefore the lifespan) starts.
    """
    try:
        import fakeredis.aioredis as fake_aio
        _redis = fake_aio.FakeRedis(decode_responses=True)
    except ImportError:
        _redis = AsyncMock()
        _redis.get = AsyncMock(return_value=None)
        _redis.setex = AsyncMock(return_value=True)
        _redis.ping = AsyncMock(return_value=True)

    from app.main import app  # type: ignore[import]
    from app.core.redis_client import redis_pool  # type: ignore[import]
    from app.services.event_service import KafkaProducerSingleton  # type: ignore[import]

    KafkaProducerSingleton._producer = MagicMock()

    # Patch connect/disconnect so the lifespan leaves _client alone, then
    # inject our fakeredis instance directly.
    with (
        patch.object(redis_pool, "connect", AsyncMock()),
        patch.object(redis_pool, "disconnect", AsyncMock()),
    ):
        redis_pool._client = _redis
        try:
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                yield client, _redis
        finally:
            KafkaProducerSingleton._producer = None
            redis_pool._client = None



# ── Helper: perform a login and return token response JSON ────────────────────

async def _login(client: AsyncClient, username: str, password: str) -> dict:
    response = await client.post(
        "/api/v1/auth/token",
        data={"username": username, "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    return response


# ─────────────────────────────────────────────────────────────────────────────
# 1. test_login_returns_access_and_refresh_tokens
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_login_returns_access_and_refresh_tokens():
    """
    POST /api/v1/auth/token with valid credentials should return 200
    with both access_token and refresh_token, and token_type=bearer.
    Covers lines 172-199 of auth.py.
    """
    async with auth_client() as (client, _redis):
        response = await _login(client, "admin@example.com", "admin123")

    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert "refresh_token" in data
    assert data.get("token_type", "bearer").lower() == "bearer"
    # Decoded access token must carry correct role
    settings = _get_settings()
    payload = jwt.decode(data["access_token"], settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
    assert payload["role"] == "admin"
    assert payload["type"] == "access"


@pytest.mark.asyncio
async def test_login_returns_analyst_role():
    """
    POST /api/v1/auth/token for analyst@example.com returns role=analyst.
    """
    async with auth_client() as (client, _):
        response = await _login(client, "analyst@example.com", "analyst123")

    assert response.status_code == 200
    data = response.json()
    settings = _get_settings()
    payload = jwt.decode(data["access_token"], settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
    assert payload["role"] == "analyst"


@pytest.mark.asyncio
async def test_login_returns_viewer_role():
    """
    POST /api/v1/auth/token for viewer@example.com returns role=viewer.
    """
    async with auth_client() as (client, _):
        response = await _login(client, "viewer@example.com", "viewer123")

    assert response.status_code == 200
    data = response.json()
    assert data["role"] == "viewer"


# ─────────────────────────────────────────────────────────────────────────────
# 2. test_login_wrong_password_returns_401
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_login_wrong_password_returns_401():
    """
    POST /api/v1/auth/token with incorrect password must return 401.
    Covers lines 179-184 of auth.py.
    """
    async with auth_client() as (client, _):
        response = await _login(client, "admin@example.com", "wrongpassword")

    assert response.status_code == 401
    assert "incorrect" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_login_unknown_user_returns_401():
    """
    POST /api/v1/auth/token with unknown username must return 401.
    """
    async with auth_client() as (client, _):
        response = await _login(client, "nonexistent@example.com", "password")

    assert response.status_code == 401


# ─────────────────────────────────────────────────────────────────────────────
# 3. test_refresh_token_returns_new_access_token
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_refresh_token_returns_new_access_token():
    """
    POST /api/v1/auth/refresh with a valid refresh_token should return
    a new access_token.
    Covers lines 202-249 of auth.py.
    """
    async with auth_client() as (client, _):
        # First login to get refresh token
        login_resp = await _login(client, "admin@example.com", "admin123")
        assert login_resp.status_code == 200
        refresh_token = login_resp.json()["refresh_token"]

        # Use refresh token to get new access token
        refresh_resp = await client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": refresh_token},
        )

    assert refresh_resp.status_code == 200
    new_data = refresh_resp.json()
    assert "access_token" in new_data
    assert "refresh_token" in new_data

    # New access token must be valid
    settings = _get_settings()
    payload = jwt.decode(new_data["access_token"], settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
    assert payload["type"] == "access"
    assert payload["sub"] == "admin@example.com"


# ─────────────────────────────────────────────────────────────────────────────
# 4. test_expired_token_returns_401
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_expired_token_returns_401():
    """
    Accessing a protected endpoint with an expired JWT must return 401.
    Covers lines 120-146 of auth.py (JWTError branch).
    """
    settings = _get_settings()
    # Craft an already-expired token
    expired_payload = {
        "sub": "admin@example.com",
        "role": "admin",
        "type": "access",
        "jti": str(uuid.uuid4()),
        "iat": datetime.now(UTC) - timedelta(hours=2),
        "exp": datetime.now(UTC) - timedelta(hours=1),  # expired
    }
    expired_token = jwt.encode(
        expired_payload,
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )

    async with auth_client() as (client, _):
        response = await client.get(
            "/api/v1/alerts",
            headers={"Authorization": f"Bearer {expired_token}"},
        )

    assert response.status_code == 401


# ─────────────────────────────────────────────────────────────────────────────
# 5. test_revoke_token_invalidates_session
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_revoke_token_invalidates_session():
    """
    POST /api/v1/auth/revoke adds the JTI to the Redis blacklist.
    A subsequent request using the same token must return 401.
    Covers lines 252-272 of auth.py.
    """
    async with auth_client() as (client, redis):
        # Login
        login_resp = await _login(client, "analyst@example.com", "analyst123")
        assert login_resp.status_code == 200
        access_token = login_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {access_token}"}

        # Revoke
        revoke_resp = await client.post("/api/v1/auth/revoke", headers=headers)
        assert revoke_resp.status_code == 200
        assert "revoked" in revoke_resp.json()["detail"].lower()

        # The jti should now be in the fakeredis blacklist
        settings = _get_settings()
        payload = jwt.decode(access_token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        jti = payload["jti"]
        blacklisted = await redis.get(f"blacklist:{jti}")
        assert blacklisted is not None, "JTI must be in the Redis blacklist after revocation"

        # Re-using the revoked token must fail
        re_use_resp = await client.get("/api/v1/alerts", headers=headers)

    assert re_use_resp.status_code == 401


# ─────────────────────────────────────────────────────────────────────────────
# 6. test_admin_endpoint_blocked_for_viewer_role
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_admin_endpoint_blocked_for_viewer_role():
    """
    An endpoint guarded by check_admin (PATCH /api/v1/events/{id}/label)
    must return 403 when accessed with viewer role.
    Covers lines 150-168 of auth.py (check_role dependency).
    """
    from app.api.v1.auth import create_token  # type: ignore[import]

    viewer_token = create_token(
        "viewer@example.com", "viewer", "access", timedelta(hours=1)
    )
    event_id = uuid.uuid4()

    async with auth_client() as (client, _):
        response = await client.patch(
            f"/api/v1/events/{event_id}/label",
            json={"label": "TP", "analyst_id": "viewer@example.com"},
            headers={"Authorization": f"Bearer {viewer_token}"},
        )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_analyst_endpoint_blocked_for_viewer_on_alerts_acknowledge():
    """
    PATCH /api/v1/alerts/{id}/acknowledge guarded by check_analyst
    must return 403 for a viewer token.
    """
    from app.api.v1.auth import create_token  # type: ignore[import]

    viewer_token = create_token(
        "viewer@example.com", "viewer", "access", timedelta(hours=1)
    )
    alert_id = uuid.uuid4()

    async with auth_client() as (client, _):
        response = await client.patch(
            f"/api/v1/alerts/{alert_id}/acknowledge",
            json={"analyst_id": "viewer@example.com"},
            headers={"Authorization": f"Bearer {viewer_token}"},
        )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_refresh_with_invalid_token_returns_401():
    """
    POST /api/v1/auth/refresh with a garbage token must return 401.
    Covers lines 248-249 of auth.py (JWTError in refresh).
    """
    async with auth_client() as (client, _):
        response = await client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": "not.a.valid.jwt.token"},
        )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_refresh_with_access_token_instead_of_refresh_returns_401():
    """
    POST /api/v1/auth/refresh with an access token (type=access) must
    return 401 — the endpoint rejects non-refresh token types.
    Covers lines 218-219 of auth.py.
    """
    async with auth_client() as (client, _):
        login_resp = await _login(client, "admin@example.com", "admin123")
        access_token = login_resp.json()["access_token"]

        # Pass the access token where a refresh token is expected
        response = await client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": access_token},
        )

    assert response.status_code == 401
