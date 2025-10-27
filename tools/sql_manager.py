"""
SQL Database Manager

Provides portable SQL database management across DuckDB, SQLite, and PostgreSQL.
Handles schema creation, view registration, and integrity verification.
"""

import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import duckdb
from loguru import logger
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError

# Type alias for database connections
DatabaseConnection = Union[Engine, duckdb.DuckDBPyConnection]


def get_connection_string(config: Dict) -> str:
    """
    Parse database configuration into SQLAlchemy connection string.

    Supports both embedded credentials and separate fields with fallback logic.

    Args:
        config: Database configuration dictionary

    Returns:
        SQLAlchemy connection string

    Examples:
        >>> get_connection_string({"url": "sqlite:///test.db"})
        'sqlite:///test.db'
        >>> get_connection_string({"type": "postgres", "host": "localhost", "database": "crypto"})
        'postgresql://localhost/crypto'
    """
    db_config = config.get("database", {})

    # Option 1: Full connection string with embedded credentials
    if "url" in db_config and db_config["url"]:
        return db_config["url"]

    # Option 2: Build from separate fields
    db_type = db_config.get("type", "duckdb")
    host = db_config.get("host", "localhost")
    port = db_config.get("port")
    user = db_config.get("user")
    password = db_config.get("password")
    database = db_config.get("database", "crypto_lake")

    # Construct connection string
    if db_type == "postgres" or db_type == "postgresql":
        # PostgreSQL format: postgresql://user:pass@host:port/database
        creds = f"{user}:{password}@" if user and password else ""
        port_str = f":{port}" if port else ""
        return f"postgresql://{creds}{host}{port_str}/{database}"
    elif db_type == "sqlite":
        # SQLite format: sqlite:///path/to/database.db
        return f"sqlite:///{database}"
    elif db_type == "duckdb":
        # DuckDB format: duckdb:///path/to/database.db or duckdb:///:memory:
        return f"duckdb:///{database}"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def sanitize_connection_string(conn_str: str) -> str:
    """
    Sanitize connection string for logging by hiding password.

    Args:
        conn_str: Connection string with potential credentials

    Returns:
        Sanitized string with password masked
    """
    # Replace password in format user:password@host with user:****@host
    return re.sub(r":([^:@]+)@", r":****@", conn_str)


def init_database(engine: str = "duckdb", config_path: str = "config.yml") -> DatabaseConnection:
    """
    Initialize database connection from configuration.

    Args:
        engine: Database engine ("duckdb", "sqlite", "postgres", or "auto")
        config_path: Path to config.yml file

    Returns:
        SQLAlchemy Engine for sqlite/postgres, DuckDB connection for duckdb

    Raises:
        ValueError: If engine type is unsupported or config is invalid
        FileNotFoundError: If config file doesn't exist
    """
    # Load configuration
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    import yaml
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Get connection string
    try:
        conn_str = get_connection_string(config)
    except Exception as e:
        logger.error(f"Failed to parse database config: {e}")
        raise

    # Auto-detect engine from connection string
    if engine == "auto":
        if conn_str.startswith("postgresql://"):
            engine = "postgres"
        elif conn_str.startswith("sqlite://"):
            engine = "sqlite"
        elif conn_str.startswith("duckdb://"):
            engine = "duckdb"
        else:
            raise ValueError(f"Cannot auto-detect engine from connection string: {conn_str}")

    # Create connection
    logger.info(f"Initializing {engine.upper()} database: {sanitize_connection_string(conn_str)}")

    if engine == "duckdb":
        # Use DuckDB native connection
        db_path = conn_str.replace("duckdb:///", "")
        conn = duckdb.connect(db_path)
        logger.info(f"Connected to DuckDB: {db_path}")
        return conn
    elif engine in ("sqlite", "postgres", "postgresql"):
        # Use SQLAlchemy engine
        try:
            db_config = config.get("database", {})
            pool_size = db_config.get("pool_size", 5)
            max_overflow = db_config.get("max_overflow", 10)
            pool_timeout = db_config.get("pool_timeout", 30)

            engine_instance = create_engine(
                conn_str,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                echo=False
            )

            # Test connection
            with engine_instance.connect() as conn:
                conn.execute(text("SELECT 1"))

            logger.info(f"Connected to {engine.upper()} database successfully")
            return engine_instance
        except Exception as e:
            logger.error(f"Failed to connect to {engine}: {e}")
            raise
    else:
        raise ValueError(f"Unsupported database engine: {engine}")


def apply_schema(engine: DatabaseConnection) -> bool:
    """
    Load and execute schema.sql to create tables and indexes.

    Handles dialect differences between DuckDB, SQLite, and PostgreSQL.
    Executes within a transaction with automatic rollback on error.

    Args:
        engine: Database connection (SQLAlchemy Engine or DuckDB connection)

    Returns:
        True if schema applied successfully, False otherwise
    """
    # Load schema.sql
    schema_path = Path(__file__).parent.parent / "sql" / "schema.sql"
    if not schema_path.exists():
        logger.error(f"Schema file not found: {schema_path}")
        return False

    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    # Detect engine type
    is_duckdb = isinstance(engine, duckdb.DuckDBPyConnection)
    is_sqlite = not is_duckdb and "sqlite" in str(engine.url).lower()
    is_postgres = not is_duckdb and "postgres" in str(engine.url).lower()

    # Apply dialect-specific transformations
    if is_sqlite:
        # SQLite uses REAL instead of DOUBLE PRECISION
        schema_sql = schema_sql.replace("DOUBLE PRECISION", "REAL")
        logger.debug("Applied SQLite dialect transformations (DOUBLE PRECISION → REAL)")
    elif is_postgres:
        # PostgreSQL can enable partitioning by uncommenting hints
        # For now, leave as-is (partitioning hints are in comments)
        logger.debug("Using PostgreSQL dialect (partitioning hints available in comments)")

    # Remove SQL comments to prevent parsing issues
    lines = schema_sql.split("\n")
    cleaned_lines = []
    for line in lines:
        # Remove inline comments (-- comment)
        comment_pos = line.find("--")
        if comment_pos >= 0:
            # Keep the line up to the comment (unless comment is at start)
            line_content = line[:comment_pos].rstrip()
            if line_content:
                cleaned_lines.append(line_content)
        else:
            cleaned_lines.append(line)

    schema_sql_clean = "\n".join(cleaned_lines)

    # Execute schema
    try:
        if is_duckdb:
            # DuckDB: Execute directly
            logger.info("Applying schema to DuckDB...")
            engine.execute(schema_sql_clean)
            logger.info("Schema applied successfully to DuckDB")
            return True
        else:
            # SQLAlchemy: Execute in transaction
            logger.info(f"Applying schema to {engine.url.drivername}...")
            with engine.begin() as conn:
                # Split by semicolon and execute each statement
                statements = [s.strip() for s in schema_sql_clean.split(";") if s.strip()]
                for i, stmt in enumerate(statements):
                    if stmt:
                        try:
                            conn.execute(text(stmt))
                            if "CREATE TABLE" in stmt.upper():
                                # Extract table name for logging
                                if "IF NOT EXISTS" in stmt.upper():
                                    table_name = stmt.split("IF NOT EXISTS")[1].split("(")[0].strip()
                                else:
                                    table_name = stmt.split("CREATE TABLE")[1].split("(")[0].strip()
                                logger.debug(f"Created table: {table_name}")
                            elif "CREATE INDEX" in stmt.upper():
                                # Extract index name for logging
                                if "IF NOT EXISTS" in stmt.upper():
                                    index_name = stmt.split("IF NOT EXISTS")[1].split("ON")[0].strip()
                                else:
                                    index_name = stmt.split("CREATE INDEX")[1].split("ON")[0].strip()
                                logger.debug(f"Created index: {index_name}")
                        except (OperationalError, ProgrammingError) as e:
                            # Skip errors for "already exists" (idempotent)
                            if "already exists" in str(e).lower():
                                logger.debug(f"Skipping existing object (statement {i+1})")
                            else:
                                raise

            logger.info(f"Schema applied successfully to {engine.url.drivername}")
            return True

    except Exception as e:
        logger.error(f"Failed to apply schema: {e}")
        return False


def register_views_if_supported(engine: DatabaseConnection, base_path: str) -> bool:
    """
    Register views from views.sql if engine supports CREATE VIEW.

    Args:
        engine: Database connection
        base_path: Base data lake path for @@BASE@@ substitution

    Returns:
        True if views registered, False if skipped or failed
    """
    # Load views.sql
    views_path = Path(__file__).parent.parent / "sql" / "views.sql"
    if not views_path.exists():
        logger.warning(f"Views file not found: {views_path}")
        return False

    with open(views_path, "r", encoding="utf-8") as f:
        views_sql = f.read()

    # Normalize base path for DuckDB compatibility
    base_norm = base_path.replace("\\", "/").rstrip("/")
    views_sql = views_sql.replace("@@BASE@@", base_norm)

    # Detect engine type
    is_duckdb = isinstance(engine, duckdb.DuckDBPyConnection)
    is_postgres = not is_duckdb and "postgres" in str(engine.url).lower()

    # Check if engine supports views
    if not is_duckdb and not is_postgres:
        logger.info(f"Engine {engine.url.drivername if not is_duckdb else 'DuckDB'} does not support views, skipping")
        return False

    # Execute views
    try:
        if is_duckdb:
            logger.info("Registering views in DuckDB...")
            engine.execute(views_sql)
            logger.info("Views registered successfully in DuckDB")
            return True
        else:
            logger.info(f"Registering views in {engine.url.drivername}...")
            with engine.begin() as conn:
                conn.execute(text(views_sql))
            logger.info(f"Views registered successfully in {engine.url.drivername}")
            return True

    except Exception as e:
        logger.error(f"Failed to register views: {e}")
        return False


def verify_integrity(engine: DatabaseConnection) -> Tuple[bool, List[str]]:
    """
    Verify database schema integrity by checking expected tables and indexes.

    Args:
        engine: Database connection

    Returns:
        Tuple of (all_present: bool, missing: List[str])
    """
    expected_tables = [
        "bars_1s",
        "bars_1m",
        "klines_1m",
        "compare_our_vs_kline_1m",
        "funding_oi_hourly",
        "macro_minute",
    ]

    expected_indexes = {
        "bars_1s": ["idx_bars_1s_symbol_ts", "idx_bars_1s_ts"],
        "bars_1m": ["idx_bars_1m_symbol_ts", "idx_bars_1m_ts"],
        "klines_1m": ["idx_klines_1m_symbol_ts", "idx_klines_1m_ts"],
        "compare_our_vs_kline_1m": ["idx_compare_symbol_ts", "idx_compare_abs_error"],
        "funding_oi_hourly": ["idx_funding_oi_symbol_ts", "idx_funding_oi_ts"],
        "macro_minute": ["idx_macro_minute_key_ts", "idx_macro_minute_ts"],
    }

    missing = []

    try:
        is_duckdb = isinstance(engine, duckdb.DuckDBPyConnection)

        if is_duckdb:
            # DuckDB: Query information_schema
            result = engine.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
            existing_tables = {row[0] for row in result}

            # Check tables
            for table in expected_tables:
                if table not in existing_tables:
                    missing.append(f"table:{table}")
                    logger.debug(f"Missing table: {table}")

            # DuckDB doesn't have named indexes in information_schema, skip index check
            logger.debug("Skipping index verification for DuckDB (not exposed in information_schema)")

        else:
            # SQLAlchemy: Use inspector
            inspector = inspect(engine)
            existing_tables = set(inspector.get_table_names())

            # Check tables
            for table in expected_tables:
                if table not in existing_tables:
                    missing.append(f"table:{table}")
                    logger.debug(f"Missing table: {table}")
                else:
                    # Check indexes for this table
                    try:
                        existing_indexes = {idx["name"] for idx in inspector.get_indexes(table)}
                        for expected_idx in expected_indexes.get(table, []):
                            if expected_idx not in existing_indexes:
                                missing.append(f"index:{table}.{expected_idx}")
                                logger.debug(f"Missing index: {expected_idx} on {table}")
                    except Exception as e:
                        logger.warning(f"Could not verify indexes for {table}: {e}")

        if not missing:
            logger.info("Database integrity verified: all tables and indexes present")
            return True, []
        else:
            logger.warning(f"Database integrity check found {len(missing)} missing objects")
            return False, missing

    except Exception as e:
        logger.error(f"Failed to verify database integrity: {e}")
        return False, [f"error:{str(e)}"]
