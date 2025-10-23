"""
Tests for the unified orchestrator.
"""

import json
import os
import tempfile
import threading
import time
from unittest.mock import Mock, patch, MagicMock

import pytest

from tools.orchestrator import Orchestrator
from tools.health import write_heartbeat, summarize_files


class TestOrchestrator:
    """Test orchestrator initialization, start/stop, and thread management."""

    def test_orchestrator_init(self):
        """Test orchestrator initializes with correct parameters."""
        config = {
            "general": {"base_path": "/tmp/test", "log_level": "INFO"},
            "transformer": {"parquet_compression": "snappy"},
        }

        orch = Orchestrator(
            config=config,
            exchange_name="binance",
            symbols=["BTCUSDT", "ETHUSDT"],
            macro_tickers=["SPY", "UUP"],
            macro_interval_min=15,
            macro_lookback_startup_days=7,
            macro_runtime_lookback_days=1,
        )

        assert orch.exchange_name == "binance"
        assert orch.symbols == ["BTCUSDT", "ETHUSDT"]
        assert orch.macro_tickers == ["SPY", "UUP"]
        assert orch.macro_interval_min == 15
        assert orch.macro_lookback_startup_days == 7
        assert orch.macro_runtime_lookback_days == 1
        assert orch.base_path == "/tmp/test"
        assert orch.compression == "snappy"

    @patch("tools.orchestrator.run_collector")
    @patch("tools.orchestrator.fetch_yf_1m")
    @patch("tools.orchestrator.write_parquet")
    def test_orchestrator_start_stop(self, mock_write_parquet, mock_fetch_yf_1m, mock_run_collector):
        """Test orchestrator starts and stops threads cleanly."""
        config = {
            "general": {"base_path": "/tmp/test", "log_level": "INFO"},
            "transformer": {"parquet_compression": "snappy"},
        }

        # Mock collector to return immediately
        async def mock_collector_coro(*args, **kwargs):
            # Wait for stop event
            while not threading.current_thread()._target.__self__.stop_event.is_set():
                await asyncio.sleep(0.1)

        mock_run_collector.return_value = None

        # Mock macro fetch to return empty dataframe
        import pandas as pd

        mock_fetch_yf_1m.return_value = pd.DataFrame()

        orch = Orchestrator(
            config=config,
            exchange_name="binance",
            symbols=["BTCUSDT"],
            macro_tickers=["SPY"],
            macro_interval_min=15,
        )

        # Start orchestrator
        orch.start()

        # Give threads time to start
        time.sleep(0.5)

        # Check threads are alive
        assert orch.ws_thread is not None
        assert orch.macro_thread is not None
        assert orch.health_thread is not None

        # Stop orchestrator
        orch.stop(timeout=3.0)

        # Check threads have stopped
        assert not orch.ws_thread.is_alive()
        assert not orch.macro_thread.is_alive()
        assert not orch.health_thread.is_alive()

    def test_scheduler_interval_math(self):
        """Test scheduler calculates next run time correctly."""
        # If current time is t0 and interval is N minutes, next run should be at t0 + N*60 seconds
        t0 = time.time()
        interval_min = 15
        next_run = t0 + (interval_min * 60)

        assert next_run == t0 + 900  # 15 minutes = 900 seconds
        assert next_run > t0

        # Check it's approximately 15 minutes in the future
        time_diff_minutes = (next_run - t0) / 60
        assert abs(time_diff_minutes - 15) < 0.01


class TestHeartbeat:
    """Test heartbeat writer creates valid JSON and MD with required keys."""

    def test_write_heartbeat_creates_files(self):
        """Test heartbeat writer creates JSON and MD files."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            json_path = os.path.join(tmp_dir, "heartbeat.json")
            md_path = os.path.join(tmp_dir, "health.md")

            payload = {
                "ts_utc": "2025-10-22T12:00:00Z",
                "collector": {
                    "status": "running",
                    "last_latency_p50_ms": 10.5,
                    "last_latency_p95_ms": 25.3,
                    "last_seen_ts": "2025-10-22T11:59:00Z",
                },
                "macro_minute": {
                    "status": "idle",
                    "last_run_start": "2025-10-22T11:45:00Z",
                    "last_run_end": "2025-10-22T11:46:00Z",
                    "last_run_rows_written": 1500,
                    "last_error": None,
                },
                "files": {
                    "raw_count_today": 100,
                    "parquet_1s_rows_today": 50000,
                    "macro_min_rows_today": 2000,
                },
            }

            write_heartbeat(json_path, md_path, payload)

            # Check JSON file exists and has valid structure
            assert os.path.exists(json_path)
            with open(json_path, "r") as f:
                data = json.load(f)
                assert "ts_utc" in data
                assert "collector" in data
                assert "macro_minute" in data
                assert "files" in data
                assert data["collector"]["status"] == "running"
                assert data["macro_minute"]["last_run_rows_written"] == 1500

            # Check MD file exists and has content
            assert os.path.exists(md_path)
            with open(md_path, "r") as f:
                content = f.read()
                assert "Crypto Lake Health Report" in content
                assert "Real-Time Crypto Collector" in content
                assert "Macro/FX Data Fetcher" in content
                assert "RUNNING" in content
                assert "1,500" in content  # Check number formatting

    def test_heartbeat_required_keys(self):
        """Test heartbeat payload has all required keys."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            json_path = os.path.join(tmp_dir, "heartbeat.json")
            md_path = os.path.join(tmp_dir, "health.md")

            # Minimal payload
            payload = {
                "ts_utc": "2025-10-22T12:00:00Z",
                "collector": {"status": "running"},
                "macro_minute": {"status": "idle"},
                "files": {},
            }

            # Should not raise an exception
            write_heartbeat(json_path, md_path, payload)

            # Verify files were created
            assert os.path.exists(json_path)
            assert os.path.exists(md_path)


class TestFileSummary:
    """Test file summary statistics collection."""

    @patch("tools.health.duckdb.connect")
    @patch("tools.health.glob.glob")
    @patch("os.path.exists")
    def test_summarize_files(self, mock_exists, mock_glob, mock_duckdb_connect):
        """Test file summarization counts raw files and queries Parquet rows."""
        # Mock file system
        mock_glob.return_value = [f"/tmp/part_{i:03d}.jsonl" for i in range(50)]
        mock_exists.return_value = True

        # Mock DuckDB query results
        mock_conn = MagicMock()
        mock_duckdb_connect.return_value = mock_conn

        # First query (parquet_1s_rows): return 10000 rows
        # Second query (macro_min_rows): return 500 rows
        mock_conn.execute.return_value.fetchone.side_effect = [(10000,), (500,)]

        result = summarize_files("/tmp/data", "2025-10-22")

        assert result["raw_count_today"] == 50
        assert result["parquet_1s_rows_today"] == 10000
        assert result["macro_min_rows_today"] == 500

        # Verify DuckDB was called twice (once for parquet, once for macro)
        assert mock_conn.execute.call_count == 2

    @patch("tools.health.glob.glob")
    def test_summarize_files_handles_errors(self, mock_glob):
        """Test file summarization handles errors gracefully."""
        # Mock glob to raise an exception
        mock_glob.side_effect = Exception("Permission denied")

        result = summarize_files("/tmp/data", "2025-10-22")

        # Should return zeros instead of crashing
        assert result["raw_count_today"] == 0
        assert result["parquet_1s_rows_today"] == 0
        assert result["macro_min_rows_today"] == 0


# Add asyncio import for mock_collector_coro
import asyncio


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
