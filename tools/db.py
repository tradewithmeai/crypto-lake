"""
Centralized DuckDB connection and view management.

Handles @@BASE@@ placeholder resolution from config.yml for all SQL views.
Supports multi-engine architecture: DuckDB, SQLite, and PostgreSQL.
"""

import glob
import os
from typing import Dict, Optional, Union

import duckdb
from loguru import logger

try:
    from sqlalchemy.engine import Engine
    from tools.sql_manager import init_database, register_views_if_supported
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    Engine = None


def is_sqlalchemy_engine(conn) -> bool:
    """
    Check if connection is SQLAlchemy Engine vs DuckDB connection.

    Args:
        conn: Database connection object

    Returns:
        True if SQLAlchemy Engine, False if DuckDB connection
    """
    if Engine and isinstance(conn, Engine):
        return True
    return hasattr(conn, 'execute') and not isinstance(conn, duckdb.DuckDBPyConnection)


def normalise_base(base_path: str) -> str:
    """
    Normalise base path for DuckDB compatibility.

    Converts backslashes to forward slashes and strips trailing slashes.

    Args:
        base_path: Base data lake path

    Returns:
        Normalised path with forward slashes
    """
    return base_path.replace("\\", "/").rstrip("/")


def load_views_sql(base_path: str, sql_path: str = "sql/views.sql") -> str:
    """
    Load SQL views file and replace @@BASE@@ placeholder with actual base path.

    Args:
        base_path: Base data lake path from config
        sql_path: Path to views.sql file (relative to project root)

    Returns:
        SQL string with @@BASE@@ replaced by normalized base path
    """
    # Normalise path to forward slashes for DuckDB compatibility
    base_norm = normalise_base(base_path)

    # Handle both absolute and relative paths
    if not os.path.isabs(sql_path):
        # Assume relative to project root (one level up from tools/)
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sql_path = os.path.join(project_root, sql_path)

    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"Views SQL file not found: {sql_path}")

    # Read and replace placeholder
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    # Replace @@BASE@@ with actual path
    sql_resolved = sql.replace("@@BASE@@", base_norm)

    # Verify no placeholders remain
    if "@@BASE@@" in sql_resolved:
        logger.warning("@@BASE@@ placeholder still present after replacement")

    return sql_resolved


def _split_sql_statements(sql: str) -> list:
    """Split SQL text into individual statements, handling semicolons correctly."""
    statements = []
    current = []
    for line in sql.split("\n"):
        stripped = line.strip()
        # Skip pure comment lines
        if stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current))
            current = []
    # Catch any trailing statement without semicolon
    if current and any(l.strip() for l in current):
        statements.append("\n".join(current))
    return statements


def connect_and_register_views(
    base_path: str,
    sql_path: str = "sql/views.sql",
    database: str = ":memory:",
    config: Optional[Dict] = None,
) -> Union[duckdb.DuckDBPyConnection, "Engine"]:
    """
    Create database connection and register all views from views.sql.

    Supports multi-engine architecture:
    - If config contains database.url → use SQL manager (SQLAlchemy/DuckDB)
    - Otherwise → fallback to DuckDB in-memory

    Args:
        base_path: Base data lake path from config
        sql_path: Path to views.sql file
        database: DuckDB database path (default: in-memory)
        config: Optional configuration dictionary with database settings

    Returns:
        Database connection (SQLAlchemy Engine or DuckDB connection)
    """
    # Check for database configuration
    if config and "database" in config:
        db_config = config["database"]

        # Check if database.url or database.type is configured
        if db_config.get("url") or db_config.get("type"):
            if not SQLALCHEMY_AVAILABLE:
                logger.warning("Database URL configured but SQLAlchemy not available, falling back to DuckDB")
            else:
                logger.info("Database URL configured, using SQL engine")
                try:
                    # Initialize SQL engine from config
                    import yaml
                    import tempfile

                    # Create temporary config file for sql_manager
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                        yaml.dump(config, f)
                        temp_config_path = f.name

                    try:
                        engine = init_database("auto", temp_config_path)

                        # Register views if supported
                        register_views_if_supported(engine, base_path)

                        return engine
                    finally:
                        os.remove(temp_config_path)

                except Exception as e:
                    logger.error(f"Failed to initialize SQL engine: {e}")
                    logger.info("Falling back to DuckDB in-memory")

    # Fallback: Use DuckDB (backward compatibility)
    logger.info("No database.url configured, using DuckDB in-memory")
    # Normalise base path for DuckDB
    base_norm = normalise_base(base_path)

    # Load and resolve SQL
    sql = load_views_sql(base_path, sql_path)

    # Connect to DuckDB
    conn = duckdb.connect(database)

    # Verify parquet files exist before registering views
    test_pattern = f"{base_norm}/parquet/binance/**/*.parquet"
    matches = glob.glob(test_pattern, recursive=True)
    if matches:
        # Log first 3 matches for verification
        examples = (matches + ["", "", ""])[:3]
        logger.info(f"Parquet check OK. Example matches:\n  {examples[0]}\n  {examples[1]}\n  {examples[2]}")
    else:
        logger.warning(f"No parquet files matched {test_pattern}")

    # Execute view definitions individually so optional view failures
    # (klines, derivs, macro) don't prevent required views from registering.
    # Split on CREATE OR REPLACE VIEW and also handle PRAGMA statements.
    registered = []
    failed = []
    for statement in _split_sql_statements(sql):
        statement = statement.strip()
        if not statement:
            continue
        try:
            conn.execute(statement)
            # Extract view name for logging
            if "VIEW" in statement.upper():
                parts = statement.split()
                idx = next((i for i, p in enumerate(parts) if p.upper() == "VIEW"), -1)
                if idx >= 0 and idx + 1 < len(parts):
                    registered.append(parts[idx + 1])
        except Exception as e:
            # Extract view name from failed statement
            view_name = "unknown"
            if "VIEW" in statement.upper():
                parts = statement.split()
                idx = next((i for i, p in enumerate(parts) if p.upper() == "VIEW"), -1)
                if idx >= 0 and idx + 1 < len(parts):
                    view_name = parts[idx + 1]
            failed.append(view_name)
            logger.debug(f"View {view_name} skipped (missing data source): {e}")

    if registered:
        logger.info(f"Registered {len(registered)} views: {', '.join(registered)}")
    if failed:
        logger.warning(f"Skipped {len(failed)} optional views (missing data): {', '.join(failed)}")

    return conn


def get_connection_with_views(config: dict) -> Union[duckdb.DuckDBPyConnection, "Engine"]:
    """
    Convenience wrapper to get a database connection with views from config.

    Supports multi-engine architecture based on config.database settings.

    Args:
        config: Configuration dictionary with general.base_path and optional database settings

    Returns:
        Database connection (SQLAlchemy Engine or DuckDB connection) with views registered
    """
    base_path = config["general"]["base_path"]
    return connect_and_register_views(base_path, config=config)
