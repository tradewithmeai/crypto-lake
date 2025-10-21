"""
Centralized DuckDB connection and view management.

Handles @@BASE@@ placeholder resolution from config.yml for all SQL views.
"""

import os
from typing import Optional

import duckdb
from loguru import logger


def load_views_sql(base_path: str, sql_path: str = "sql/views.sql") -> str:
    """
    Load SQL views file and replace @@BASE@@ placeholder with actual base path.

    Args:
        base_path: Base data lake path from config
        sql_path: Path to views.sql file (relative to project root)

    Returns:
        SQL string with @@BASE@@ replaced by normalized base path
    """
    # Normalize path to forward slashes for DuckDB compatibility
    base_path_normalized = base_path.replace("\\", "/")

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
    sql_resolved = sql.replace("@@BASE@@", base_path_normalized)

    # Verify no placeholders remain
    if "@@BASE@@" in sql_resolved:
        logger.warning("@@BASE@@ placeholder still present after replacement")

    return sql_resolved


def connect_and_register_views(
    base_path: str,
    sql_path: str = "sql/views.sql",
    database: str = ":memory:",
) -> duckdb.DuckDBPyConnection:
    """
    Create DuckDB connection and register all views from views.sql.

    Args:
        base_path: Base data lake path from config
        sql_path: Path to views.sql file
        database: DuckDB database path (default: in-memory)

    Returns:
        DuckDB connection with views registered
    """
    # Load and resolve SQL
    sql = load_views_sql(base_path, sql_path)

    # Connect to DuckDB
    conn = duckdb.connect(database)

    # Execute all view definitions
    # views.sql contains multiple CREATE OR REPLACE VIEW statements
    try:
        conn.execute(sql)
        logger.debug(f"Successfully registered views from {sql_path}")
    except Exception as e:
        logger.error(f"Failed to register views: {e}")
        raise

    return conn


def get_connection_with_views(config: dict) -> duckdb.DuckDBPyConnection:
    """
    Convenience wrapper to get a DuckDB connection with views from config.

    Args:
        config: Configuration dictionary with general.base_path

    Returns:
        DuckDB connection with views registered
    """
    base_path = config["general"]["base_path"]
    return connect_and_register_views(base_path)
