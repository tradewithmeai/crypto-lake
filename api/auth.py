"""JWT token management and FastAPI authentication dependencies."""

import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Cookie, Depends, HTTPException, Request, status
from jose import JWTError, jwt
from loguru import logger

from api.database import get_user_by_api_key, get_user_by_username

ALGORITHM = "HS256"


def _get_or_create_secret(base_path: str) -> str:
    """Load JWT secret from file, or generate and save one."""
    secret_path = os.path.join(base_path, "jwt_secret.key")
    if os.path.exists(secret_path):
        with open(secret_path, "r") as f:
            return f.read().strip()
    secret = secrets.token_urlsafe(64)
    os.makedirs(os.path.dirname(secret_path), exist_ok=True)
    with open(secret_path, "w") as f:
        f.write(secret)
    logger.info(f"Generated new JWT secret at {secret_path}")
    return secret


def create_access_token(secret_key: str, data: dict, expires_hours: int = 24) -> str:
    """Create a JWT access token."""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(hours=expires_hours)
    to_encode["exp"] = expire
    return jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)


def decode_token(secret_key: str, token: str) -> Optional[dict]:
    """Decode and validate a JWT token. Returns payload or None."""
    try:
        return jwt.decode(token, secret_key, algorithms=[ALGORITHM])
    except JWTError:
        return None


async def get_current_user(request: Request) -> dict:
    """
    FastAPI dependency that authenticates the request.

    Checks in order:
    1. Authorization: Bearer <token> header
    2. access_token cookie
    3. X-API-Key header

    Returns the user dict or raises 401.
    """
    db_path = request.app.state.db_path
    secret_key = request.app.state.jwt_secret

    # 1. Check Authorization header
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        payload = decode_token(secret_key, token)
        if payload and "sub" in payload:
            user = get_user_by_username(db_path, payload["sub"])
            if user:
                return user

    # 2. Check cookie
    token = request.cookies.get("access_token")
    if token:
        payload = decode_token(secret_key, token)
        if payload and "sub" in payload:
            user = get_user_by_username(db_path, payload["sub"])
            if user:
                return user

    # 3. Check API key header
    api_key = request.headers.get("X-API-Key")
    if api_key:
        user = get_user_by_api_key(db_path, api_key)
        if user:
            return user

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Bearer"},
    )


def validate_ws_token(db_path: str, secret_key: str, token: Optional[str]) -> Optional[dict]:
    """
    Validate a token for WebSocket connections.
    Returns user dict or None.
    """
    if not token:
        return None

    # Try as JWT
    payload = decode_token(secret_key, token)
    if payload and "sub" in payload:
        user = get_user_by_username(db_path, payload["sub"])
        if user:
            return user

    # Try as API key
    user = get_user_by_api_key(db_path, token)
    if user:
        return user

    return None
