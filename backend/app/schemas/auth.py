"""
Authentication schemas — Pydantic v2
"""
from __future__ import annotations

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field

class UserRole(str, Enum):
    ADMIN = "admin"
    ANALYST = "analyst"
    VIEWER = "viewer"

class Token(BaseModel):
    access_token: str = Field(..., description="JWT access token")
    refresh_token: Optional[str] = Field(default=None, description="JWT refresh token")
    token_type: str = Field(default="bearer")
    role: Optional[str] = Field(default=None, description="Assigned role: admin, analyst, or viewer")

class TokenData(BaseModel):
    sub: str = Field(..., description="Subject — typically user email")
    role: Optional[UserRole] = Field(default=UserRole.VIEWER, description="User role")

class LoginRequest(BaseModel):
    username: str = Field(..., description="Email address used as login username")
    password: str = Field(..., description="User password")

class TokenRefreshRequest(BaseModel):
    refresh_token: str = Field(..., description="Valid refresh token")

class UserOut(BaseModel):
    email: str = Field(..., description="User email address")
    role: UserRole = Field(..., description="User authorization role")
    disabled: bool = Field(default=False)
