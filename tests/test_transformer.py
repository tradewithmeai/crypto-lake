import os
from datetime import datetime, timezone

import duckdb
import pandas as pd

from transformer.transformer import _aggregate_bars_1s

def test_aggregate_bars_1s_basic():
    # Construct a small trade+quote dataframe
    t0 = int(datetime(2025, 5, 5, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    rows = [
        {"symbol": "SUIUSDT", "ts_event": t0 + 0, "price": 1.0, "qty": 1.0, "stream": "trade"},
        {"symbol": "SUIUSDT", "ts_event": t0 + 500, "price": 1.2, "qty": 2.0, "stream": "trade"},
        {"symbol": "SUIUSDT", "ts_event": t0 + 900, "price": 1.1, "qty": 1.0, "stream": "trade"},
        {"symbol": "SUIUSDT", "ts_event": t0 + 1200, "bid": 1.05, "ask": 1.15, "stream": "bookTicker"},
    ]
    df = pd.DataFrame(rows)
    out = _aggregate_bars_1s(df, "SUIUSDT", second=1)
    assert not out.empty

    # Verify window_start field exists and is timezone-aware UTC
    assert "window_start" in out.columns, "Output must have 'window_start' column"
    assert out["window_start"].dt.tz is not None, "window_start must be timezone-aware"
    assert str(out["window_start"].dt.tz) == "UTC", "window_start must be in UTC timezone"

    r0 = out.iloc[0]
    assert abs(r0["open"] - 1.0) < 1e-9
    assert abs(r0["high"] - 1.2) < 1e-9
    assert abs(r0["low"] - 1.0) < 1e-9
    assert abs(r0["close"] - 1.1) < 1e-9
    # Check vwap within range
    assert r0["vwap"] >= r0["low"] and r0["vwap"] <= r0["high"]
    # Quotes propagated
    assert "bid" in out.columns and "ask" in out.columns

def test_quotes_only_no_trade_gaps():
    """Test that quotes-only periods (no trades) properly forward-fill OHLC and propagate quotes."""
    t0 = int(datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

    # First second: one trade to establish price
    # Next 3 seconds: quotes only, no trades
    rows = [
        # Second 0: trade establishes close=100.0
        {"symbol": "BTCUSDT", "ts_event": t0 + 100, "price": 100.0, "qty": 1.0, "stream": "trade"},

        # Second 1: quotes only
        {"symbol": "BTCUSDT", "ts_event": t0 + 1200, "bid": 99.5, "ask": 100.5, "stream": "bookTicker"},

        # Second 2: quotes only
        {"symbol": "BTCUSDT", "ts_event": t0 + 2300, "bid": 99.6, "ask": 100.6, "stream": "bookTicker"},

        # Second 3: quotes only
        {"symbol": "BTCUSDT", "ts_event": t0 + 3400, "bid": 99.7, "ask": 100.7, "stream": "bookTicker"},
    ]

    df = pd.DataFrame(rows)
    out = _aggregate_bars_1s(df, "BTCUSDT", second=1)

    # Verify window_start field exists and is timezone-aware UTC
    assert "window_start" in out.columns, "Output must have 'window_start' column"
    assert out["window_start"].dt.tz is not None, "window_start must be timezone-aware"
    assert str(out["window_start"].dt.tz) == "UTC", "window_start must be in UTC timezone"

    # Should have 4 rows (seconds 0-3)
    assert len(out) == 4

    # Second 0: has trade
    r0 = out.iloc[0]
    assert abs(r0["close"] - 100.0) < 1e-9
    assert r0["volume_base"] > 0

    # Second 1: no trades, should have FFILLED OHLC from second 0
    r1 = out.iloc[1]
    assert abs(r1["open"] - 100.0) < 1e-9  # FFILLED from close
    assert abs(r1["high"] - 100.0) < 1e-9
    assert abs(r1["low"] - 100.0) < 1e-9
    assert abs(r1["close"] - 100.0) < 1e-9
    assert r1["volume_base"] == 0  # No trades
    assert abs(r1["bid"] - 99.5) < 1e-9
    assert abs(r1["ask"] - 100.5) < 1e-9

    # Second 2: no trades, FFILLED OHLC, updated quotes
    r2 = out.iloc[2]
    assert abs(r2["close"] - 100.0) < 1e-9
    assert r2["volume_base"] == 0
    assert abs(r2["bid"] - 99.6) < 1e-9
    assert abs(r2["ask"] - 100.6) < 1e-9

    # Second 3: no trades, FFILLED OHLC, updated quotes
    r3 = out.iloc[3]
    assert abs(r3["close"] - 100.0) < 1e-9
    assert r3["volume_base"] == 0
    assert abs(r3["bid"] - 99.7) < 1e-9
    assert abs(r3["ask"] - 100.7) < 1e-9
