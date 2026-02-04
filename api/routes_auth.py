"""Authentication endpoints: register, login, logout, user info, Google Sign-In."""

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token
from loguru import logger
from pydantic import BaseModel

from api.auth import create_access_token, get_current_user
from api.database import create_user, get_or_create_google_user, get_user_by_username, verify_password

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/register")
async def register(body: RegisterRequest, request: Request, response: Response):
    """Register a new user account."""
    config = request.app.state.config
    api_config = config.get("api", {})

    if not api_config.get("allow_registration", True):
        raise HTTPException(status_code=403, detail="Registration is disabled")

    if len(body.username) < 3:
        raise HTTPException(status_code=400, detail="Username must be at least 3 characters")
    if len(body.password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    if "@" not in body.email:
        raise HTTPException(status_code=400, detail="Invalid email address")

    db_path = request.app.state.db_path

    try:
        user = create_user(db_path, body.username, body.email, body.password)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    # Auto-login after registration
    secret_key = request.app.state.jwt_secret
    expires_hours = api_config.get("jwt_expiry_hours", 24)
    token = create_access_token(secret_key, {"sub": user["username"]}, expires_hours)

    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=expires_hours * 3600,
    )

    return {
        "user": {
            "id": user["id"],
            "username": user["username"],
            "email": user["email"],
            "api_key": user["api_key"],
        },
        "token": token,
    }


@router.post("/login")
async def login(body: LoginRequest, request: Request, response: Response):
    """Login with username and password."""
    db_path = request.app.state.db_path
    api_config = request.app.state.config.get("api", {})

    user = get_user_by_username(db_path, body.username)
    if not user or not verify_password(body.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    secret_key = request.app.state.jwt_secret
    expires_hours = api_config.get("jwt_expiry_hours", 24)
    token = create_access_token(secret_key, {"sub": user["username"]}, expires_hours)

    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=expires_hours * 3600,
    )

    return {
        "user": {
            "id": user["id"],
            "username": user["username"],
            "email": user["email"],
            "api_key": user["api_key"],
        },
        "token": token,
    }


@router.post("/logout")
async def logout(response: Response, user: dict = Depends(get_current_user)):
    """Logout by clearing the auth cookie."""
    response.delete_cookie("access_token")
    return {"message": "Logged out"}


@router.get("/me")
async def get_me(user: dict = Depends(get_current_user)):
    """Get current user info and API key."""
    return {
        "id": user["id"],
        "username": user["username"],
        "email": user["email"],
        "api_key": user["api_key"],
    }


@router.get("/google-client-id")
async def get_google_client_id(request: Request):
    """Return the Google OAuth client ID for frontend initialization."""
    client_id = request.app.state.config.get("api", {}).get("google_client_id", "")
    if not client_id:
        raise HTTPException(status_code=404, detail="Google Sign-In not configured")
    return {"client_id": client_id}


class GoogleAuthRequest(BaseModel):
    credential: str


@router.post("/google")
async def google_auth(body: GoogleAuthRequest, request: Request, response: Response):
    """Authenticate with a Google ID token."""
    api_config = request.app.state.config.get("api", {})
    client_id = api_config.get("google_client_id", "")

    if not client_id:
        raise HTTPException(status_code=404, detail="Google Sign-In not configured")

    # Verify the Google ID token
    try:
        idinfo = google_id_token.verify_oauth2_token(
            body.credential,
            google_requests.Request(),
            client_id,
        )
    except ValueError as e:
        logger.warning(f"Invalid Google token: {e}")
        raise HTTPException(status_code=401, detail=f"Invalid Google token: {e}")
    except Exception as e:
        logger.error(f"Google token verification failed: {type(e).__name__}: {e}")
        raise HTTPException(status_code=401, detail=f"Google token verification failed: {e}")

    google_id = idinfo["sub"]
    email = idinfo.get("email", "")
    name = idinfo.get("name", "")

    if not email:
        raise HTTPException(status_code=400, detail="Google account has no email")

    try:
        db_path = request.app.state.db_path
        user = get_or_create_google_user(db_path, google_id, email, name)
    except Exception as e:
        logger.error(f"Failed to create/find Google user: {e}")
        raise HTTPException(status_code=500, detail=f"User creation failed: {e}")

    # Issue JWT
    secret_key = request.app.state.jwt_secret
    expires_hours = api_config.get("jwt_expiry_hours", 24)
    token = create_access_token(secret_key, {"sub": user["username"]}, expires_hours)

    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=expires_hours * 3600,
    )

    return {
        "user": {
            "id": user["id"],
            "username": user["username"],
            "email": user["email"],
            "api_key": user["api_key"],
        },
        "token": token,
    }
