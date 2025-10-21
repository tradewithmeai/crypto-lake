"""Tests for macro 1-minute data collection."""

import os
from datetime import datetime, timezone

import pandas as pd
import pytest

from tools.macro_minute import fetch_yf_1m, write_parquet, _read_existing_data


def test_fetch_yf_1m_utc_normalization():
    """Test that fetched data is timezone-aware UTC."""
    # Note: This test requires internet connection and yfinance API availability
    # Using a small lookback to minimize data transfer
    df = fetch_yf_1m("SPY", lookback_days=1)

    if df.empty:
        pytest.skip("No data returned from yfinance (market might be closed)")

    # Verify required columns exist
    required_cols = ['ts', 'open', 'high', 'low', 'close', 'volume', 'ticker']
    assert all(col in df.columns for col in required_cols), f"Missing required columns. Got: {list(df.columns)}"

    # Verify ts is timezone-aware UTC
    assert df['ts'].dt.tz is not None, "Timestamp must be timezone-aware"
    assert str(df['ts'].dt.tz) == "UTC", "Timestamp must be in UTC timezone"

    # Verify data types
    assert df['open'].dtype == 'float64'
    assert df['high'].dtype == 'float64'
    assert df['low'].dtype == 'float64'
    assert df['close'].dtype == 'float64'
    assert df['volume'].dtype == 'int64'
    assert df['ticker'].dtype == 'object'

    # Verify ticker value
    assert all(df['ticker'] == 'SPY')


def test_dedup_logic(tmp_path):
    """Test that deduplication works correctly."""
    # Create test data with overlapping timestamps
    # Use recent date so it won't be filtered by lookback window
    base_ts = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    # First batch
    df1 = pd.DataFrame({
        'ts': pd.date_range(start=base_ts, periods=5, freq='1min', tz='UTC'),
        'open': [100.0, 101.0, 102.0, 103.0, 104.0],
        'high': [100.5, 101.5, 102.5, 103.5, 104.5],
        'low': [99.5, 100.5, 101.5, 102.5, 103.5],
        'close': [100.2, 101.2, 102.2, 103.2, 104.2],
        'volume': [1000, 1100, 1200, 1300, 1400],
        'ticker': ['TEST'] * 5
    })

    # Write first batch
    write_parquet(df1, str(tmp_path), compression='snappy')

    # Second batch overlaps with last 2 rows of first batch
    df2 = pd.DataFrame({
        'ts': pd.date_range(start=base_ts + pd.Timedelta(minutes=3), periods=5, freq='1min', tz='UTC'),
        'open': [103.0, 104.0, 105.0, 106.0, 107.0],  # Overlaps with rows 3, 4 from df1
        'high': [103.5, 104.5, 105.5, 106.5, 107.5],
        'low': [102.5, 103.5, 104.5, 105.5, 106.5],
        'close': [103.3, 104.3, 105.3, 106.3, 107.3],  # Different close values (simulating updates)
        'volume': [1333, 1444, 1500, 1600, 1700],  # Different volumes
        'ticker': ['TEST'] * 5
    })

    # Write second batch
    write_parquet(df2, str(tmp_path), compression='snappy')

    # Read all data back
    existing = _read_existing_data(str(tmp_path), 'TEST', lookback_days=8)

    # Should have 8 unique timestamps (5 from df1 + 3 new from df2)
    # Overlapping timestamps (rows 3, 4 from df1) should be replaced by df2 values
    assert len(existing) == 8, f"Expected 8 unique rows, got {len(existing)}"

    # Verify no duplicate timestamps
    assert existing['ts'].is_unique, "Timestamps should be unique after dedup"

    # Verify timestamps are sorted
    assert existing['ts'].is_monotonic_increasing, "Timestamps should be sorted"


def test_parquet_schema(tmp_path):
    """Test that written Parquet files have correct schema."""
    base_ts = datetime(2025, 5, 10, 14, 30, 0, tzinfo=timezone.utc)

    df = pd.DataFrame({
        'ts': pd.date_range(start=base_ts, periods=3, freq='1min', tz='UTC'),
        'open': [200.0, 201.0, 202.0],
        'high': [200.5, 201.5, 202.5],
        'low': [199.5, 200.5, 201.5],
        'close': [200.2, 201.2, 202.2],
        'volume': [5000, 5100, 5200],
        'ticker': ['SPY'] * 3
    })

    write_parquet(df, str(tmp_path), compression='snappy')

    # Read back and verify
    pattern = os.path.join(str(tmp_path), "macro", "minute", "SPY", "**", "*.parquet")
    import glob
    files = glob.glob(pattern, recursive=True)

    assert len(files) > 0, "No Parquet files were written"

    # Read first file
    df_read = pd.read_parquet(files[0])

    # Verify columns
    expected_cols = ['ts', 'open', 'high', 'low', 'close', 'volume', 'ticker']
    assert all(col in df_read.columns for col in expected_cols)

    # Verify UTC timezone
    df_read['ts'] = pd.to_datetime(df_read['ts'], utc=True)
    assert df_read['ts'].dt.tz is not None
    assert str(df_read['ts'].dt.tz) == "UTC"
