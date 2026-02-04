"""SQLite user database for authentication and API key management."""

import secrets
import sqlite3
import threading
from datetime import datetime
from typing import Optional

import bcrypt

_db_lock = threading.Lock()


def _get_conn(db_path: str) -> sqlite3.Connection:
    """Create a new SQLite connection with row factory."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(db_path: str) -> None:
    """Create users table if it doesn't exist, and run migrations."""
    conn = _get_conn(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT,
                api_key TEXT UNIQUE NOT NULL,
                google_id TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        """)
        conn.commit()

        # Migration: add google_id column and make password_hash nullable
        columns = {row[1]: row for row in conn.execute("PRAGMA table_info(users)").fetchall()}
        needs_migration = "google_id" not in columns
        # Also check if password_hash is still NOT NULL (notnull flag is index 3)
        if not needs_migration and "password_hash" in columns:
            notnull = columns["password_hash"][3]
            if notnull:
                needs_migration = True

        if needs_migration:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT,
                    api_key TEXT UNIQUE NOT NULL,
                    google_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1
                )
            """)
            # Copy existing data (google_id will be NULL for existing users)
            existing_cols = list(columns.keys())
            if "google_id" in existing_cols:
                conn.execute("INSERT OR IGNORE INTO users_new SELECT * FROM users")
            else:
                cols = ", ".join(existing_cols)
                conn.execute(f"INSERT OR IGNORE INTO users_new ({cols}) SELECT {cols} FROM users")
            conn.execute("DROP TABLE users")
            conn.execute("ALTER TABLE users_new RENAME TO users")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_id ON users(google_id)")
            conn.commit()
    finally:
        conn.close()


def generate_api_key() -> str:
    """Generate a random API key."""
    return f"cl_{secrets.token_urlsafe(32)}"


def create_user(db_path: str, username: str, email: str, password: str) -> dict:
    """
    Create a new user with hashed password and generated API key.

    Returns the created user dict (without password_hash).
    Raises ValueError if username or email already exists.
    """
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    api_key = generate_api_key()

    conn = _get_conn(db_path)
    try:
        with _db_lock:
            try:
                conn.execute(
                    "INSERT INTO users (username, email, password_hash, api_key) VALUES (?, ?, ?, ?)",
                    (username, email, password_hash, api_key),
                )
                conn.commit()
            except sqlite3.IntegrityError as e:
                error_msg = str(e).lower()
                if "username" in error_msg:
                    raise ValueError("Username already exists")
                elif "email" in error_msg:
                    raise ValueError("Email already exists")
                else:
                    raise ValueError("User already exists")

        row = conn.execute(
            "SELECT id, username, email, api_key, created_at, is_active FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        return dict(row)
    finally:
        conn.close()


def get_user_by_username(db_path: str, username: str) -> Optional[dict]:
    """Look up a user by username. Returns full user dict including password_hash."""
    conn = _get_conn(db_path)
    try:
        row = conn.execute("SELECT * FROM users WHERE username = ? AND is_active = 1", (username,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_user_by_api_key(db_path: str, api_key: str) -> Optional[dict]:
    """Look up a user by API key. Returns user dict without password_hash."""
    conn = _get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT id, username, email, api_key, created_at, is_active FROM users WHERE api_key = ? AND is_active = 1",
            (api_key,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_user_by_google_id(db_path: str, google_id: str) -> Optional[dict]:
    """Look up a user by Google ID."""
    conn = _get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT id, username, email, api_key, google_id, created_at, is_active FROM users WHERE google_id = ? AND is_active = 1",
            (google_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_user_by_email(db_path: str, email: str) -> Optional[dict]:
    """Look up a user by email."""
    conn = _get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT id, username, email, api_key, google_id, created_at, is_active FROM users WHERE email = ? AND is_active = 1",
            (email,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_or_create_google_user(db_path: str, google_id: str, email: str, name: str) -> dict:
    """
    Find or create a user for Google Sign-In.

    - If a user with this google_id exists, return it.
    - If a user with this email exists (registered via password), link the google_id.
    - Otherwise, create a new user.
    """
    # Check by google_id first
    user = get_user_by_google_id(db_path, google_id)
    if user:
        return user

    conn = _get_conn(db_path)
    try:
        with _db_lock:
            # Check if email already exists (password-registered user)
            row = conn.execute(
                "SELECT id, username, email, api_key, google_id, created_at, is_active FROM users WHERE email = ? AND is_active = 1",
                (email,),
            ).fetchone()

            if row:
                # Link google_id to existing account
                conn.execute("UPDATE users SET google_id = ? WHERE id = ?", (google_id, row["id"]))
                conn.commit()
                updated = conn.execute(
                    "SELECT id, username, email, api_key, google_id, created_at, is_active FROM users WHERE id = ?",
                    (row["id"],),
                ).fetchone()
                return dict(updated)

            # Create new user (no password)
            username = name.replace(" ", "_").lower()[:32] if name else email.split("@")[0]
            # Ensure username uniqueness
            base_username = username
            suffix = 0
            while True:
                existing = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
                if not existing:
                    break
                suffix += 1
                username = f"{base_username}_{suffix}"

            api_key = generate_api_key()
            conn.execute(
                "INSERT INTO users (username, email, password_hash, api_key, google_id) VALUES (?, ?, NULL, ?, ?)",
                (username, email, api_key, google_id),
            )
            conn.commit()

        row = conn.execute(
            "SELECT id, username, email, api_key, google_id, created_at, is_active FROM users WHERE google_id = ?",
            (google_id,),
        ).fetchone()
        return dict(row)
    finally:
        conn.close()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plain password against a bcrypt hash."""
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))
