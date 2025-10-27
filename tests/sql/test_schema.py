"""
Tests for SQL schema creation and management.

Tests schema.sql against temporary SQLite database to verify:
- Table creation
- Idempotency
- Integrity verification
- View registration
"""

import os
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

from tools.sql_manager import apply_schema, register_views_if_supported, verify_integrity


@pytest.fixture
def temp_sqlite_db():
    """
    Create temporary SQLite database for testing.

    Yields SQLAlchemy engine, cleans up after test.
    """
    # Create temporary database file
    db_fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(db_fd)  # Close file descriptor, engine will open it

    # Create SQLAlchemy engine
    engine = create_engine(f'sqlite:///{db_path}', echo=False)

    yield engine

    # Cleanup
    engine.dispose()
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture
def temp_config_dir(tmp_path):
    """
    Create temporary directory with base_path structure for testing.

    Creates parquet/binance directory structure.
    """
    base_path = tmp_path / "crypto_lake"
    parquet_dir = base_path / "parquet" / "binance"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    return str(base_path)


def test_apply_schema_creates_tables(temp_sqlite_db):
    """Test that apply_schema() creates all expected tables."""
    engine = temp_sqlite_db

    # Apply schema
    result = apply_schema(engine)
    assert result is True, "apply_schema should return True on success"

    # Verify tables exist
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())

    expected_tables = {
        "bars_1s",
        "bars_1m",
        "klines_1m",
        "compare_our_vs_kline_1m",
        "funding_oi_hourly",
        "macro_minute",
    }

    assert expected_tables.issubset(tables), f"Missing tables: {expected_tables - tables}"


def test_apply_schema_idempotent(temp_sqlite_db):
    """Test that apply_schema() can run multiple times without errors."""
    engine = temp_sqlite_db

    # Apply schema first time
    result1 = apply_schema(engine)
    assert result1 is True

    # Apply schema second time (should be idempotent)
    result2 = apply_schema(engine)
    assert result2 is True, "apply_schema should be idempotent"

    # Verify tables still exist
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    assert "bars_1s" in tables
    assert "bars_1m" in tables


def test_table_columns_correct(temp_sqlite_db):
    """Test that tables have correct column definitions."""
    engine = temp_sqlite_db

    apply_schema(engine)

    inspector = inspect(engine)

    # Test bars_1s columns
    bars_1s_columns = {col["name"]: col["type"] for col in inspector.get_columns("bars_1s")}
    assert "symbol" in bars_1s_columns
    assert "ts" in bars_1s_columns
    assert "open" in bars_1s_columns
    assert "high" in bars_1s_columns
    assert "low" in bars_1s_columns
    assert "close" in bars_1s_columns
    assert "volume_base" in bars_1s_columns
    assert "trade_count" in bars_1s_columns

    # Test macro_minute columns
    macro_columns = {col["name"]: col["type"] for col in inspector.get_columns("macro_minute")}
    assert "macro_key" in macro_columns
    assert "ts" in macro_columns
    assert "value" in macro_columns


def test_primary_keys_created(temp_sqlite_db):
    """Test that primary keys are created correctly."""
    engine = temp_sqlite_db

    apply_schema(engine)

    inspector = inspect(engine)

    # Test bars_1s primary key
    pk_bars_1s = inspector.get_pk_constraint("bars_1s")
    assert set(pk_bars_1s["constrained_columns"]) == {"symbol", "ts"}

    # Test macro_minute primary key
    pk_macro = inspector.get_pk_constraint("macro_minute")
    assert set(pk_macro["constrained_columns"]) == {"macro_key", "ts"}


def test_indexes_created(temp_sqlite_db):
    """Test that indexes are created correctly."""
    engine = temp_sqlite_db

    apply_schema(engine)

    inspector = inspect(engine)

    # Test bars_1s indexes
    indexes_bars_1s = inspector.get_indexes("bars_1s")
    index_names = {idx["name"] for idx in indexes_bars_1s}

    # SQLite should have at least one of these indexes
    assert "idx_bars_1s_symbol_ts" in index_names or "idx_bars_1s_ts" in index_names


def test_verify_integrity_pass(temp_sqlite_db):
    """Test that verify_integrity() returns True for complete schema."""
    engine = temp_sqlite_db

    # Apply schema
    apply_schema(engine)

    # Verify integrity
    all_present, missing = verify_integrity(engine)

    assert all_present is True, f"verify_integrity should return True, but found missing: {missing}"
    assert len(missing) == 0, f"No objects should be missing, but found: {missing}"


def test_verify_integrity_fail_missing_table(temp_sqlite_db):
    """Test that verify_integrity() detects missing tables."""
    engine = temp_sqlite_db

    # Apply schema
    apply_schema(engine)

    # Drop one table
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS bars_1m"))

    # Verify integrity
    all_present, missing = verify_integrity(engine)

    assert all_present is False, "verify_integrity should return False when tables are missing"
    assert any("bars_1m" in item for item in missing), f"bars_1m should be in missing list: {missing}"


def test_verify_integrity_empty_database(temp_sqlite_db):
    """Test verify_integrity on empty database."""
    engine = temp_sqlite_db

    # Don't apply schema, just verify
    all_present, missing = verify_integrity(engine)

    assert all_present is False
    assert len(missing) > 0, "Empty database should have missing tables"
    assert any("bars_1s" in item for item in missing)


def test_register_views_if_supported_skips_sqlite(temp_sqlite_db, temp_config_dir):
    """Test that register_views_if_supported() skips SQLite gracefully."""
    engine = temp_sqlite_db
    base_path = temp_config_dir

    # Apply schema first
    apply_schema(engine)

    # Try to register views (should skip for SQLite)
    result = register_views_if_supported(engine, base_path)

    # SQLite doesn't support views in our implementation, should return False
    assert result is False, "register_views should skip for SQLite"


def test_insert_and_query_bars_1s(temp_sqlite_db):
    """Test inserting and querying data in bars_1s table."""
    engine = temp_sqlite_db

    apply_schema(engine)

    # Insert test data
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bars_1s (symbol, ts, open, high, low, close, volume_base, trade_count)
            VALUES ('BTCUSDT', '2025-01-01 00:00:00', 50000, 50100, 49900, 50050, 100.5, 150)
        """))

    # Query data
    with engine.connect() as conn:
        result = conn.execute(text("SELECT symbol, open, close FROM bars_1s WHERE symbol = 'BTCUSDT'"))
        row = result.fetchone()

    assert row is not None
    assert row[0] == "BTCUSDT"
    assert row[1] == 50000
    assert row[2] == 50050


def test_insert_and_query_macro_minute(temp_sqlite_db):
    """Test inserting and querying data in macro_minute table."""
    engine = temp_sqlite_db

    apply_schema(engine)

    # Insert test data
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO macro_minute (macro_key, ts, value)
            VALUES ('SPY', '2025-01-01 00:00:00', 475.50)
        """))

    # Query data
    with engine.connect() as conn:
        result = conn.execute(text("SELECT macro_key, value FROM macro_minute WHERE macro_key = 'SPY'"))
        row = result.fetchone()

    assert row is not None
    assert row[0] == "SPY"
    assert row[1] == 475.50


def test_primary_key_constraint_enforced(temp_sqlite_db):
    """Test that primary key constraint prevents duplicates."""
    engine = temp_sqlite_db

    apply_schema(engine)

    # Insert first row
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bars_1s (symbol, ts, open, high, low, close, volume_base, trade_count)
            VALUES ('BTCUSDT', '2025-01-01 00:00:00', 50000, 50100, 49900, 50050, 100.5, 150)
        """))

    # Try to insert duplicate (should fail)
    with pytest.raises(Exception) as exc_info:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO bars_1s (symbol, ts, open, high, low, close, volume_base, trade_count)
                VALUES ('BTCUSDT', '2025-01-01 00:00:00', 51000, 51100, 50900, 51050, 200.5, 250)
            """))

    assert "UNIQUE constraint failed" in str(exc_info.value) or "PRIMARY KEY" in str(exc_info.value)


def test_apply_schema_handles_missing_file():
    """Test that apply_schema() handles missing schema.sql gracefully."""
    # Create engine pointing to non-existent schema
    db_fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(db_fd)

    engine = create_engine(f'sqlite:///{db_path}', echo=False)

    # Temporarily rename schema.sql
    schema_path = Path(__file__).parent.parent.parent / "sql" / "schema.sql"
    backup_path = schema_path.with_suffix(".sql.bak")

    schema_exists = schema_path.exists()

    if schema_exists:
        schema_path.rename(backup_path)

    try:
        result = apply_schema(engine)
        assert result is False, "apply_schema should return False when schema.sql is missing"
    finally:
        # Restore schema.sql
        if schema_exists:
            backup_path.rename(schema_path)

        engine.dispose()
        os.remove(db_path)
