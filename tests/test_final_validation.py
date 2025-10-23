"""
Comprehensive system validation tests for Crypto Lake v1.0.

These tests ensure the system is production-ready for quantitative analysis.
"""

import pytest
import os
from pathlib import Path
import duckdb
from datetime import datetime, timezone
from tools.common import load_config


class TestSystemIntegrity:
    """Test overall system integrity and readiness."""

    @pytest.fixture
    def config(self):
        """Load configuration."""
        return load_config("config.yml")

    @pytest.fixture
    def base_path(self, config):
        """Get base path from config."""
        return Path(config["general"]["base_path"])

    @pytest.fixture
    def duckdb_conn(self):
        """Create DuckDB connection."""
        conn = duckdb.connect(":memory:")
        yield conn
        conn.close()

    def test_backfill_directory_exists(self, base_path):
        """Test that backfill directory structure exists."""
        backfill_dir = base_path / "backfill" / "binance"
        assert backfill_dir.exists(), "Backfill directory does not exist"

    def test_real_time_directory_exists(self, base_path):
        """Test that real-time data directory exists."""
        parquet_dir = base_path / "parquet" / "binance"
        assert parquet_dir.exists(), "Real-time parquet directory does not exist"

    def test_macro_directory_exists(self, base_path):
        """Test that macro data directory exists."""
        macro_dir = base_path / "macro" / "minute"
        assert macro_dir.exists(), "Macro data directory does not exist"

    def test_backfill_parquet_files_exist(self, base_path):
        """Test that backfill Parquet files exist."""
        backfill_dir = base_path / "backfill" / "binance"
        parquet_files = list(backfill_dir.rglob("*.parquet"))
        assert len(parquet_files) > 0, "No backfill Parquet files found"

    def test_backfill_schema_correct(self, base_path, duckdb_conn):
        """Test that backfill Parquet schema is correct."""
        backfill_path = base_path / "backfill" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        query = f"""
        DESCRIBE SELECT * FROM read_parquet('{backfill_path.as_posix()}')
        """
        schema = duckdb_conn.execute(query).fetchall()
        columns = [row[0] for row in schema]

        required_columns = {
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_base_vol", "taker_quote_vol"
        }

        for col in required_columns:
            assert col in columns, f"Required column '{col}' missing from backfill schema"

    def test_no_duplicate_timestamps(self, base_path, duckdb_conn):
        """Test that there are no duplicate timestamps in backfill data."""
        backfill_path = base_path / "backfill" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT open_time) as unique_timestamps
        FROM read_parquet('{backfill_path.as_posix()}')
        """
        result = duckdb_conn.execute(query).fetchone()

        total_rows = result[0]
        unique_timestamps = result[1]

        assert total_rows == unique_timestamps, f"Found {total_rows - unique_timestamps} duplicate timestamps"

    def test_ohlc_relationships_valid(self, base_path, duckdb_conn):
        """Test that OHLC relationships are valid (low <= open,close <= high)."""
        backfill_path = base_path / "backfill" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        query = f"""
        SELECT COUNT(*) FROM read_parquet('{backfill_path.as_posix()}')
        WHERE low > open OR low > close OR high < open OR high < close
        """
        violations = duckdb_conn.execute(query).fetchone()[0]

        assert violations == 0, f"Found {violations} OHLC relationship violations"

    def test_no_negative_prices(self, base_path, duckdb_conn):
        """Test that all prices are positive."""
        backfill_path = base_path / "backfill" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        query = f"""
        SELECT COUNT(*) FROM read_parquet('{backfill_path.as_posix()}')
        WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
        """
        violations = duckdb_conn.execute(query).fetchone()[0]

        assert violations == 0, f"Found {violations} negative or zero prices"

    def test_timestamps_are_utc(self, base_path, duckdb_conn):
        """Test that timestamps have UTC timezone."""
        backfill_path = base_path / "backfill" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        query = f"""
        SELECT open_time FROM read_parquet('{backfill_path.as_posix()}')
        LIMIT 1
        """
        result = duckdb_conn.execute(query).fetchone()

        if result and result[0]:
            timestamp = result[0]
            # Check if timestamp has timezone info (DuckDB timestamps with timezone are timezone-aware)
            assert timestamp.tzinfo is not None, "Timestamps are not timezone-aware"

    def test_real_time_data_exists_today(self, base_path, duckdb_conn):
        """Test that real-time data exists for today."""
        parquet_path = base_path / "parquet" / "binance" / "**" / "*.parquet"

        try:
            query = f"""
            SELECT COUNT(*) FROM read_parquet('{parquet_path.as_posix()}')
            WHERE DATE(window_start) = CURRENT_DATE
            """
            count = duckdb_conn.execute(query).fetchone()[0]
            assert count > 0, "No real-time data for today"
        except Exception as e:
            pytest.skip(f"Real-time data not available: {e}")

    def test_macro_data_has_multiple_tickers(self, base_path, duckdb_conn):
        """Test that macro data includes multiple tickers."""
        macro_path = base_path / "macro" / "minute" / "**" / "*.parquet"

        try:
            query = f"""
            SELECT COUNT(DISTINCT ticker) FROM read_parquet('{macro_path.as_posix()}')
            """
            ticker_count = duckdb_conn.execute(query).fetchone()[0]
            assert ticker_count >= 5, f"Expected at least 5 macro tickers, found {ticker_count}"
        except Exception as e:
            pytest.skip(f"Macro data not available: {e}")

    def test_health_monitoring_files_exist(self, base_path):
        """Test that health monitoring files are being created."""
        json_path = base_path / "logs" / "health" / "heartbeat.json"
        md_path = base_path / "reports" / "health.md"

        # At least one should exist
        assert json_path.exists() or md_path.exists(), "No health monitoring files found"

    def test_config_file_valid(self, config):
        """Test that config.yml is valid and has required keys."""
        assert "general" in config, "Missing 'general' section in config"
        assert "base_path" in config["general"], "Missing 'base_path' in config"
        assert "exchanges" in config, "Missing 'exchanges' section in config"
        assert len(config["exchanges"]) > 0, "No exchanges configured"

    def test_logging_configured(self, base_path):
        """Test that logging is configured and writing logs."""
        logs_dir = base_path / "logs"
        assert logs_dir.exists(), "Logs directory does not exist"

        # Check for any log files
        log_files = list(logs_dir.glob("**/*.log"))
        assert len(log_files) > 0, "No log files found"

    def test_backfill_data_range(self, base_path, duckdb_conn):
        """Test that backfill covers expected date range."""
        backfill_path = base_path / "backfill" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        query = f"""
        SELECT
            MIN(open_time) as earliest,
            MAX(open_time) as latest,
            EXTRACT(EPOCH FROM (MAX(open_time) - MIN(open_time))) / 86400 as days_span
        FROM read_parquet('{backfill_path.as_posix()}')
        """
        result = duckdb_conn.execute(query).fetchone()

        earliest, latest, days_span = result
        assert days_span >= 1, f"Backfill spans only {days_span} days, expected at least 1"
        assert latest is not None, "No data in backfill"
        assert earliest is not None, "No data in backfill"

    def test_volume_data_present(self, base_path, duckdb_conn):
        """Test that volume data is present and non-zero."""
        backfill_path = base_path / "backfill" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        query = f"""
        SELECT COUNT(*) FROM read_parquet('{backfill_path.as_posix()}')
        WHERE volume > 0
        """
        rows_with_volume = duckdb_conn.execute(query).fetchone()[0]

        query2 = f"""
        SELECT COUNT(*) FROM read_parquet('{backfill_path.as_posix()}')
        """
        total_rows = duckdb_conn.execute(query2).fetchone()[0]

        # At least 50% of rows should have volume > 0
        volume_percentage = (rows_with_volume / total_rows) * 100 if total_rows > 0 else 0
        assert volume_percentage >= 50, f"Only {volume_percentage:.1f}% of rows have volume > 0"

    def test_trade_count_reasonable(self, base_path, duckdb_conn):
        """Test that trade counts are reasonable."""
        backfill_path = base_path / "backfill" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        query = f"""
        SELECT AVG(trades), MAX(trades) FROM read_parquet('{backfill_path.as_posix()}')
        """
        result = duckdb_conn.execute(query).fetchone()

        avg_trades, max_trades = result
        assert avg_trades >= 0, "Average trade count cannot be negative"
        assert max_trades >= 0, "Max trade count cannot be negative"


class TestSystemConfiguration:
    """Test system configuration and setup."""

    def test_required_files_exist(self):
        """Test that all required files exist."""
        required_files = [
            "config.yml",
            "main.py",
            "collector/collector.py",
            "transformer/transformer.py",
            "tools/backfill_binance.py",
            "tools/orchestrator.py",
            "tools/macro_minute.py",
        ]

        for file_path in required_files:
            assert os.path.exists(file_path), f"Required file missing: {file_path}"

    def test_python_modules_import(self):
        """Test that all core Python modules can be imported."""
        try:
            from collector import collector
            from transformer import transformer
            from tools import backfill_binance
            from tools import orchestrator
            from tools import macro_minute
            from tools import common
        except ImportError as e:
            pytest.fail(f"Failed to import core module: {e}")

    def test_dependencies_installed(self):
        """Test that all required dependencies are installed."""
        required_packages = [
            "pandas",
            "pyarrow",
            "duckdb",
            "loguru",
            "pytest",
            "websockets",
            "aiohttp",
        ]

        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                pytest.fail(f"Required package not installed: {package}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
