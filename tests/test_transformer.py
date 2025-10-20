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
    r0 = out.iloc[0]
    assert abs(r0["open"] - 1.0) < 1e-9
    assert abs(r0["high"] - 1.2) < 1e-9
    assert abs(r0["low"] - 1.0) < 1e-9
    assert abs(r0["close"] - 1.1) < 1e-9
    # Check vwap within range
    assert r0["vwap"] >= r0["low"] and r0["vwap"] <= r0["high"]
    # Quotes propagated
    assert "bid" in out.columns and "ask" in out.columns
