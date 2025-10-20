import json
import os
import time
from datetime import datetime, timezone

import duckdb
import pandas as pd

from collector.collector import parse_event, RotatingJSONLWriter
from transformer.transformer import transform_symbol_day
from tools.common import load_config, ensure_dir

def test_parse_event_trade_and_quote():
    trade_msg = {
        "stream": "adausdt@trade",
        "data": {"e": "trade", "E": 1699999999000, "s": "ADAUSDT", "p": "0.2500", "q": "10", "m": False},
    }
    quote_msg = {
        "stream": "adausdt@bookTicker",
        "data": {"e": "bookTicker", "E": 1699999999500, "s": "ADAUSDT", "b": "0.2498", "a": "0.2502"},
    }

    t = parse_event(trade_msg)
    q = parse_event(quote_msg)

    assert t["symbol"] == "ADAUSDT"
    assert t["stream"] == "trade"
    assert abs(t["price"] - 0.25) < 1e-9
    assert abs(t["qty"] - 10) < 1e-9
    assert t["side"] == "buy"

    assert q["symbol"] == "ADAUSDT"
    assert q["stream"] == "bookTicker"
    assert abs(q["bid"] - 0.2498) < 1e-9
    assert abs(q["ask"] - 0.2502) < 1e-9

def test_rotating_writer(tmp_path):
    base = tmp_path / "raw" / "binance"
    os.makedirs(base, exist_ok=True)
    w = RotatingJSONLWriter(base_dir=str(base), symbol="ADAUSDT", interval_sec=1)

    # Write two lines across two seconds to trigger rotation
    now = time.time()
    w.write_obj({"a": 1}, now_epoch=now)
    w.write_obj({"a": 2}, now_epoch=now + 1.1)
    w.close()

    # Ensure two parts created
    date_str = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%d")
    d = base / "ADAUSDT" / date_str
    files = list(sorted([f for f in os.listdir(d) if f.endswith(".jsonl")]))
    assert len(files) >= 2

def _write_mock_raw(tmp_base: str, exchange: str, symbol: str, date: str):
    """Create a small raw dataset with trades and quotes spanning 3 seconds."""
    out_dir = os.path.join(tmp_base, "raw", exchange, symbol, date)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "part_001.jsonl")
    t0 = int(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    rows = [
        {"symbol": symbol, "ts_event": t0 + 0, "ts_recv": t0 + 1, "price": 100.0, "qty": 1.0, "side": "buy", "bid": None, "ask": None, "stream": "trade"},
        {"symbol": symbol, "ts_event": t0 + 500, "ts_recv": t0 + 501, "price": 101.0, "qty": 2.0, "side": "sell", "bid": None, "ask": None, "stream": "trade"},
        {"symbol": symbol, "ts_event": t0 + 900, "ts_recv": t0 + 901, "price": 102.0, "qty": 1.5, "side": "buy", "bid": None, "ask": None, "stream": "trade"},
        {"symbol": symbol, "ts_event": t0 + 1200, "ts_recv": t0 + 1201, "price": None, "qty": None, "side": None, "bid": 99.5, "ask": 100.5, "stream": "bookTicker"},
        {"symbol": symbol, "ts_event": t0 + 1500, "ts_recv": t0 + 1501, "price": 101.0, "qty": 0.5, "side": "sell", "bid": None, "ask": None, "stream": "trade"},
        {"symbol": symbol, "ts_event": t0 + 2200, "ts_recv": t0 + 2201, "price": None, "qty": None, "side": None, "bid": 99.0, "ask": 101.0, "stream": "bookTicker"},
    ]
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")

def test_transformer_integration(tmp_path):
    # Create temp config pointing to tmp_path
    cfg_path = tmp_path / "config.yml"
    cfg_text = f"""
general:
  timezone: "UTC"
  log_level: "INFO"
  base_path: "{str(tmp_path).replace(chr(92), chr(92)*2)}"
exchanges:
  - name: "binance"
    symbols: ["ADAUSDT"]
    rest_url: "https://api.binance.com/api/v3"
    wss_url: "wss://stream.binance.com:9443/ws"
collector:
  write_interval_sec: 60
  reconnect_backoff: 10
  output_format: "jsonl"
transformer:
  resample_interval_sec: 1
  parquet_compression: "snappy"
"""
    cfg_path.write_text(cfg_text, encoding="utf-8")
    config = load_config(str(cfg_path))

    # Write mock raw
    date = "2025-01-01"
    _write_mock_raw(str(tmp_path), "binance", "ADAUSDT", date)

    # Run transform
    out_root = transform_symbol_day(config, "binance", "ADAUSDT", date, resample_interval_sec=1)
    assert out_root is not None

    # Read Parquet partitions with DuckDB
    y, m, d = date.split("-")
    pattern = os.path.join(out_root, f"year={int(y)}", f"month={int(m)}", f"day={int(d)}", "*.parquet")
    con = duckdb.connect()
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{pattern.replace(chr(92), chr(92)*2)}') ORDER BY window_start").fetch_df()
    finally:
        con.close()

    # Expect rows covering from t0 to last event second (>= 3 seconds)
    assert not df.empty

    # Check OHLC on first second
    first_row = df.iloc[0]
    assert abs(first_row["open"] - 100.0) < 1e-9
    assert abs(first_row["high"] - 102.0) < 1e-9
    assert abs(first_row["low"] - 100.0) < 1e-9
    assert abs(first_row["close"] - 102.0) < 1e-9

    # Volumes
    assert first_row["volume_base"] > 0
    assert first_row["volume_quote"] > 0
