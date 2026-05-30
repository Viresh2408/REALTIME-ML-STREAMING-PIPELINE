"""
Authentication endpoints
OAuth2 password flow + JWT role-based access control (RBAC).
Provides token issuance, refresh (using 7d refresh token), and revocation.
Predefined users:
- admin@example.com (role: admin)
- analyst@example.com (role: analyst)
- viewer@example.com (role: viewer)
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext

from app.core.config import settings
from app.core.redis_client import redis_pool
from app.schemas.auth import Token, TokenData, TokenRefreshRequest, UserRole

logger = structlog.get_logger(__name__)

router = APIRouter()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"{settings.API_V1_PREFIX}/auth/token")

# Predefined user database with hashed passwords
_PREDEFINED_USERS = {
    "admin@example.com": {
        "email": "admin@example.com",
        "hashed_password": pwd_context.hash("admin123"),
        "role": UserRole.ADMIN,
        "disabled": False,
    },
    "analyst@example.com": {
        "email": "analyst@example.com",
        "hashed_password": pwd_context.hash("analyst123"),
        "role": UserRole.ANALYST,
        "disabled": False,
    },
    "viewer@example.com": {
        "email": "viewer@example.com",
        "hashed_password": pwd_context.hash("viewer123"),
        "role": UserRole.VIEWER,
        "disabled": False,
    },
}


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_token(
    subject: str,
    role: str,
    token_type: str = "access",
    expires_delta: timedelta | None = None,
) -> str:
    """Create a signed JWT token."""
    to_encode = {
        "sub": subject,
        "role": role,
        "type": token_type,
        "jti": str(uuid.uuid4()),
        "iat": datetime.now(tz=UTC),
    }

    if expires_delta:
        expire = datetime.now(tz=UTC) + expires_delta
    elif token_type == "access":
        expire = datetime.now(tz=UTC) + timedelta(hours=1)
    else:
        # Refresh token: 7 days
        expire = datetime.now(tz=UTC) + timedelta(days=7)

    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


async def is_token_revoked(jti: str) -> bool:
    """Check if token ID is in Redis blacklist."""
    try:
        client = redis_pool.client
        val = await client.get(f"blacklist:{jti}")
        return val is not None
    except Exception as exc:
        logger.error("Redis blacklist check failed", error=str(exc))
        return False


async def blacklist_token(jti: str, expires_in_seconds: int) -> None:
    """Store token ID in Redis blacklist with expiration."""
    try:
        client = redis_pool.client
        await client.setex(f"blacklist:{jti}", expires_in_seconds, "1")
    except Exception as exc:
        logger.error("Redis blacklist set failed", error=str(exc))


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]) -> TokenData:
    """Dependency to retrieve the current user and validate token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials or token expired",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        sub: str | None = payload.get("sub")
        role: str | None = payload.get("role")
        token_type: str | None = payload.get("type")
        jti: str | None = payload.get("jti")

        if sub is None or role is None or token_type != "access" or jti is None:
            raise credentials_exception

        if await is_token_revoked(jti):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has been revoked",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return TokenData(sub=sub, role=UserRole(role))
    except JWTError:
        raise credentials_exception


# ── Role Checks ──────────────────────────────────────────────────────────────
def check_role(required_roles: list[UserRole]):
    """Utility to enforce specific roles on endpoints."""

    async def dependency(
        current_user: Annotated[TokenData, Depends(get_current_user)],
    ) -> TokenData:
        if current_user.role not in required_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to access this resource",
            )
        return current_user

    return dependency


check_admin = check_role([UserRole.ADMIN])
check_analyst = check_role([UserRole.ADMIN, UserRole.ANALYST])
check_viewer = check_role([UserRole.ADMIN, UserRole.ANALYST, UserRole.VIEWER])


# ── Auth Endpoints ───────────────────────────────────────────────────────────
@router.post("/token", response_model=Token)
async def login_oauth2(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]) -> Token:
    """
    OAuth2 password grant login.
    Returns access_token (1 hour validity) and refresh_token (7 days validity).
    """
    user = _PREDEFINED_USERS.get(form_data.username)
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if user["disabled"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is disabled",
        )

    access_token = create_token(user["email"], user["role"], "access", timedelta(hours=1))
    refresh_token = create_token(user["email"], user["role"], "refresh", timedelta(days=7))

    return Token(
        access_token=access_token,
        refresh_token=refresh_token,
        role=user["role"],
    )


@router.post("/refresh", response_model=Token)
async def refresh_tokens(payload: TokenRefreshRequest) -> Token:
    """Refresh JWT access token using a valid refresh token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate refresh token",
    )
    try:
        decoded = jwt.decode(
            payload.refresh_token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
        )
        sub: str | None = decoded.get("sub")
        role: str | None = decoded.get("role")
        token_type: str | None = decoded.get("type")
        jti: str | None = decoded.get("jti")

        if sub is None or role is None or token_type != "refresh" or jti is None:
            raise credentials_exception

        if await is_token_revoked(jti):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token has been revoked",
            )

        # Predefined check
        user = _PREDEFINED_USERS.get(sub)
        if not user or user["disabled"]:
            raise credentials_exception

        # Create new tokens
        access_token = create_token(user["email"], user["role"], "access", timedelta(hours=1))
        new_refresh_token = create_token(user["email"], user["role"], "refresh", timedelta(days=7))

        # Revoke the old refresh token
        exp: float | None = decoded.get("exp")
        if exp:
            remaining = int(exp - datetime.now(tz=UTC).timestamp())
            if remaining > 0:
                await blacklist_token(jti, remaining)

        return Token(
            access_token=access_token,
            refresh_token=new_refresh_token,
            role=user["role"],
        )
    except JWTError:
        raise credentials_exception


@router.post("/revoke", status_code=status.HTTP_200_OK)
async def revoke_token(current_token: Annotated[str, Depends(oauth2_scheme)]) -> dict[str, str]:
    """Logout / revoke the current active access token."""
    try:
        payload = jwt.decode(
            current_token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
        )
        jti: str | None = payload.get("jti")
        exp: float | None = payload.get("exp")

        if jti and exp:
            remaining = int(exp - datetime.now(tz=UTC).timestamp())
            if remaining > 0:
                await blacklist_token(jti, remaining)

        return {"detail": "Token successfully revoked"}
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid token provided for revocation",
        )
