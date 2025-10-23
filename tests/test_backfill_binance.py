"""
Tests for Binance backfill module.
"""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from tools.backfill_binance import BinanceBackfiller, backfill_binance


@pytest.fixture
def temp_dir():
    """Create temporary directory for test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_klines_response():
    """Create mock klines response from Binance API."""
    # Generate 3 days of 1-minute candles (3 * 24 * 60 = 4320 candles)
    start_time = datetime(2025, 10, 20, 0, 0, 0, tzinfo=timezone.utc)
    klines = []

    for i in range(4320):
        open_time = start_time + timedelta(minutes=i)
        close_time = open_time + timedelta(minutes=1)

        klines.append([
            int(open_time.timestamp() * 1000),  # open_time
            "100.0",  # open
            "101.0",  # high
            "99.0",   # low
            "100.5",  # close
            "1000.0", # volume
            int(close_time.timestamp() * 1000),  # close_time
            "100500.0",  # quote_volume
            50,       # trades
            "500.0",  # taker_base_vol
            "50250.0", # taker_quote_vol
            "0"       # ignore
        ])

    return klines


class TestBinanceBackfiller:
    """Tests for BinanceBackfiller class."""

    def test_init(self, temp_dir):
        """Test backfiller initialization."""
        backfiller = BinanceBackfiller(temp_dir, interval="1m")

        assert backfiller.base_dir == temp_dir
        assert backfiller.interval == "1m"
        assert backfiller.session is not None

    def test_klines_to_dataframe(self, temp_dir, mock_klines_response):
        """Test conversion of klines to DataFrame."""
        backfiller = BinanceBackfiller(temp_dir)

        # Take first 100 klines
        df = backfiller._klines_to_dataframe(mock_klines_response[:100])

        # Verify DataFrame structure
        assert len(df) == 100
        assert "open_time" in df.columns
        assert "close_time" in df.columns
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
        assert "trades" in df.columns

        # Verify data types
        assert df["open_time"].dtype == "datetime64[ns, UTC]"
        assert df["close_time"].dtype == "datetime64[ns, UTC]"
        assert df["open"].dtype == "float64"
        assert df["trades"].dtype == "int64"

        # Verify "ignore" column is dropped
        assert "ignore" not in df.columns

    def test_timestamp_normalization(self, temp_dir, mock_klines_response):
        """Test that timestamps are properly normalized to UTC."""
        backfiller = BinanceBackfiller(temp_dir)
        df = backfiller._klines_to_dataframe(mock_klines_response[:10])

        # Verify all timestamps are timezone-aware UTC
        assert df["open_time"].dt.tz is not None
        assert str(df["open_time"].dt.tz) == "UTC"

    @patch("tools.backfill_binance.BinanceBackfiller._fetch_klines")
    def test_deduplication(self, mock_fetch, temp_dir, mock_klines_response):
        """Test that existing timestamps are not overwritten."""
        backfiller = BinanceBackfiller(temp_dir)

        # Mock API response
        mock_fetch.return_value = mock_klines_response[:1440]  # 1 day

        # First backfill
        date1 = datetime(2025, 10, 20, tzinfo=timezone.utc)
        rows1 = backfiller.backfill_symbol("SOLUSDT", lookback_days=1, end_date=date1)
        assert rows1 == 1440

        # Second backfill (same date) - should detect existing data
        rows2 = backfiller.backfill_symbol("SOLUSDT", lookback_days=1, end_date=date1)
        assert rows2 == 0  # No new rows written

    @patch("tools.backfill_binance.BinanceBackfiller._fetch_klines")
    def test_parquet_file_creation(self, mock_fetch, temp_dir, mock_klines_response):
        """Test that Parquet files are created with correct partitioning."""
        backfiller = BinanceBackfiller(temp_dir)

        # Mock API response for 2025-10-20
        mock_fetch.return_value = mock_klines_response[:1440]  # 1 day

        date = datetime(2025, 10, 20, tzinfo=timezone.utc)
        backfiller.backfill_symbol("SOLUSDT", lookback_days=1, end_date=date)

        # Verify directory structure
        expected_path = temp_dir / "SOLUSDT" / "year=2025" / "month=10" / "day=20" / "data.parquet"
        assert expected_path.exists()

        # Verify Parquet file can be read
        df = pd.read_parquet(expected_path)
        assert len(df) > 0
        assert "open_time" in df.columns

    @patch("tools.backfill_binance.BinanceBackfiller._fetch_klines")
    def test_rate_limit_handling(self, mock_fetch, temp_dir):
        """Test graceful handling of rate limit errors."""
        backfiller = BinanceBackfiller(temp_dir)

        # Mock rate limit response
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "1"}

        # Mock successful response after retry
        mock_klines = [[
            1697760000000, "100.0", "101.0", "99.0", "100.5", "1000.0",
            1697760060000, "100500.0", 50, "500.0", "50250.0", "0"
        ]]

        with patch.object(backfiller.session, "get") as mock_get:
            # First call returns rate limit, second succeeds
            mock_get.side_effect = [
                mock_response,
                Mock(status_code=200, json=lambda: mock_klines)
            ]

            result = backfiller._fetch_klines("SOLUSDT", 1697760000000, 1697763600000)

            # Verify retry occurred
            assert mock_get.call_count == 2
            assert result == mock_klines

    @patch("tools.backfill_binance.BinanceBackfiller._fetch_klines")
    def test_data_schema_validation(self, mock_fetch, temp_dir, mock_klines_response):
        """Test that output data matches expected schema."""
        backfiller = BinanceBackfiller(temp_dir)
        mock_fetch.return_value = mock_klines_response[:100]

        date = datetime(2025, 10, 20, tzinfo=timezone.utc)
        backfiller.backfill_symbol("SOLUSDT", lookback_days=1, end_date=date)

        # Read back and verify schema
        parquet_path = temp_dir / "SOLUSDT" / "year=2025" / "month=10" / "day=20" / "data.parquet"
        df = pd.read_parquet(parquet_path)

        expected_columns = {
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_base_vol", "taker_quote_vol"
        }
        assert set(df.columns) == expected_columns

    @patch("tools.backfill_binance.BinanceBackfiller._fetch_klines")
    def test_api_error_handling(self, mock_fetch, temp_dir):
        """Test handling of API errors."""
        backfiller = BinanceBackfiller(temp_dir)

        # Mock API error response
        mock_fetch.return_value = None

        date = datetime(2025, 10, 20, tzinfo=timezone.utc)
        rows = backfiller.backfill_symbol("SOLUSDT", lookback_days=1, end_date=date)

        # Should return 0 rows on error
        assert rows == 0


class TestBackfillFunction:
    """Tests for backfill_binance() function."""

    @patch("tools.backfill_binance.BinanceBackfiller.backfill_symbol")
    def test_multiple_symbols(self, mock_backfill, temp_dir):
        """Test backfilling multiple symbols."""
        mock_backfill.return_value = 1000

        symbols = ["SOLUSDT", "BTCUSDT", "ETHUSDT"]
        results = backfill_binance(symbols, lookback_days=7, base_dir=temp_dir)

        # Verify all symbols were processed
        assert len(results) == 3
        assert all(symbol in results for symbol in symbols)
        assert all(rows == 1000 for rows in results.values())

    @patch("tools.backfill_binance.BinanceBackfiller.backfill_symbol")
    def test_error_handling_multiple_symbols(self, mock_backfill, temp_dir):
        """Test error handling when backfilling multiple symbols."""
        # Mock: first symbol succeeds, second fails, third succeeds
        mock_backfill.side_effect = [1000, Exception("API error"), 1500]

        symbols = ["SOLUSDT", "BTCUSDT", "ETHUSDT"]
        results = backfill_binance(symbols, lookback_days=7, base_dir=temp_dir)

        # Verify partial success
        assert results["SOLUSDT"] == 1000
        assert results["BTCUSDT"] == 0  # Error results in 0 rows
        assert results["ETHUSDT"] == 1500

    @patch("tools.backfill_binance.BinanceBackfiller.backfill_symbol")
    def test_summary_output(self, mock_backfill, temp_dir, capsys):
        """Test that summary information is logged."""
        mock_backfill.side_effect = [1000, 2000, 1500]

        symbols = ["SOLUSDT", "BTCUSDT", "ETHUSDT"]
        results = backfill_binance(symbols, lookback_days=7, base_dir=temp_dir)

        # Verify total rows
        total = sum(results.values())
        assert total == 4500


@pytest.fixture
def mock_session():
    """Mock requests session."""
    with patch("tools.backfill_binance.requests.Session") as mock:
        session = Mock()
        mock.return_value = session
        yield session


def test_exponential_backoff(temp_dir, mock_session):
    """Test exponential backoff on request failures."""
    backfiller = BinanceBackfiller(temp_dir)

    # Mock failing requests
    mock_session.get.side_effect = [
        Exception("Connection error"),
        Exception("Connection error"),
        Mock(status_code=200, json=lambda: [])
    ]

    with patch("time.sleep") as mock_sleep:
        result = backfiller._fetch_klines("SOLUSDT", 1697760000000, 1697763600000, max_retries=3)

        # Verify exponential backoff occurred
        assert mock_sleep.call_count == 2
        # First retry: 2^0 = 1 second
        # Second retry: 2^1 = 2 seconds
        assert mock_sleep.call_args_list[0][0][0] == 1
        assert mock_sleep.call_args_list[1][0][0] == 2
