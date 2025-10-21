import glob
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from tools.common import (
    ensure_dir,
    get_exchange_config,
    get_parquet_symbol_root,
    get_raw_symbol_day_dir,
    setup_logging,
    to_utc_dt,
)

def _load_jsonl_files(file_paths: List[str]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for fp in sorted(file_paths):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                # Stream-append to a list of records
                records = [json.loads(line) for line in f if line.strip()]
                if records:
                    frames.append(pd.DataFrame.from_records(records))
        except Exception as e:
            logger.error(f"Failed to read {fp}: {e}")
    if not frames:
        return pd.DataFrame(columns=["symbol", "ts_event", "ts_recv", "price", "qty", "side", "bid", "ask", "stream", "trade_id"])
    return pd.concat(frames, ignore_index=True)

def _aggregate_bars_1s(df: pd.DataFrame, symbol: str, second: int = 1) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    # Timestamps
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts_event"], unit="ms", utc=True)
    df["sec"] = df["ts"].dt.floor(f"{second}s")

    # Trade-based OHLCV
    trades = df[df["stream"] == "trade"].copy()
    quotes = df[df["stream"] == "bookTicker"].copy()

    # Sort by timestamp for deterministic first/last aggregation
    if not trades.empty:
        trades = trades.sort_values("ts")
    if not quotes.empty:
        quotes = quotes.sort_values("ts")

    bars = []
    if not trades.empty:
        trades["price"] = pd.to_numeric(trades["price"])
        trades["qty"] = pd.to_numeric(trades["qty"])
        trades["pq"] = trades["price"] * trades["qty"]

        agg = trades.groupby("sec").agg(
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            volume_base=("qty", "sum"),
            volume_quote=("pq", "sum"),
            trade_count=("price", "count"),
        )
        agg["vwap"] = (agg["volume_quote"] / agg["volume_base"]).where(agg["volume_base"] > 0, agg["close"])
        bars.append(agg)

    if bars:
        bars_df = bars[0]
    else:
        # No trades present, create empty bar frame
        bars_df = pd.DataFrame(columns=["open", "high", "low", "close", "volume_base", "volume_quote", "trade_count", "vwap"])

    # Quote snapshots (last bid/ask per second)
    if not quotes.empty:
        quotes["bid"] = pd.to_numeric(quotes["bid"])
        quotes["ask"] = pd.to_numeric(quotes["ask"])
        # Already sorted above for determinism
        q_agg = quotes.groupby("sec").agg(bid=("bid", "last"), ask=("ask", "last"))
        bars_df = bars_df.join(q_agg, how="outer")

    # Spread
    if "bid" in bars_df.columns and "ask" in bars_df.columns:
        bars_df["spread"] = (bars_df["ask"] - bars_df["bid"]).where((bars_df["ask"].notna()) & (bars_df["bid"].notna()))

    # Reindex to fill gaps
    if not df.empty:
        full_index = pd.date_range(df["sec"].min(), df["sec"].max(), freq=f"{second}s", tz=timezone.utc)
        bars_df = bars_df.reindex(full_index)

    # Forward-fill close into open/high/low/close for missing rows, zero-fill volumes
    bars_df["close"] = bars_df["close"].ffill()
    for col in ("open", "high", "low"):
        bars_df[col] = bars_df[col].fillna(bars_df["close"])
    for col in ("volume_base", "volume_quote", "trade_count"):
        if col in bars_df.columns:
            bars_df[col] = bars_df[col].fillna(0)
    if "vwap" in bars_df.columns:
        bars_df["vwap"] = bars_df["vwap"].fillna(bars_df["close"])
    if "bid" in bars_df.columns:
        bars_df["bid"] = bars_df["bid"].ffill()
    if "ask" in bars_df.columns:
        bars_df["ask"] = bars_df["ask"].ffill()
    if "spread" in bars_df.columns:
        bars_df["spread"] = bars_df["spread"].ffill()

    # Finalize schema
    out = bars_df.reset_index().rename(columns={"index": "window_start"})

    # --- NEW: Ensure timestamps are normalised to UTC ---
    if out["window_start"].dt.tz is not None:
        out["window_start"] = out["window_start"].dt.tz_convert("UTC")
    else:
        out["window_start"] = out["window_start"].dt.tz_localize("UTC")
    # ----------------------------------------------------

    out["symbol"] = symbol
    # Ensure types
    numerics = ["open", "high", "low", "close", "volume_base", "volume_quote", "trade_count", "vwap", "bid", "ask", "spread"]
    for c in numerics:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out

def _write_parquet_partitioned(df: pd.DataFrame, root: str, compression: str) -> None:
    if df.empty:
        return
    # Add partitions
    df = df.copy()
    df["year"] = df["window_start"].dt.year
    df["month"] = df["window_start"].dt.month
    df["day"] = df["window_start"].dt.day

    # Partition by year/month/day under symbol root
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(table, root_path=root, partition_cols=["year", "month", "day"], compression=compression)

def transform_symbol_day(
    config: Dict[str, Any],
    exchange_name: str,
    symbol: str,
    date: str,
    resample_interval_sec: int,
) -> Optional[str]:
    """
    Transform a single symbol/day from raw JSONL to Parquet 1-second bars.
    Returns path to symbol root where Parquet partitions were written, or None.
    """
    raw_dir = get_raw_symbol_day_dir(config, exchange_name, symbol, date)
    files = sorted(glob.glob(os.path.join(raw_dir, "*.jsonl")))
    if not files:
        logger.warning(f"No raw files for {symbol} on {date}")
        return None

    df = _load_jsonl_files(files)
    if df.empty:
        logger.warning(f"No events parsed for {symbol} on {date}")
        return None

    out = _aggregate_bars_1s(df, symbol, second=resample_interval_sec)
    if out.empty:
        logger.warning(f"No output bars for {symbol} on {date}")
        return None

    # Ensure datetime type is timezone-aware UTC
    out["window_start"] = pd.to_datetime(out["window_start"], utc=True)

    root = get_parquet_symbol_root(config, exchange_name, symbol)
    ensure_dir(root)
    compression = str(config["transformer"].get("parquet_compression", "snappy"))
    _write_parquet_partitioned(out, root, compression)
    logger.info(f"Wrote Parquet partitions for {symbol} at {root}")
    return root

def run_transformer(
    config: Dict[str, Any],
    exchange_name: str = "binance",
    date: Optional[str] = None,
    symbols: Optional[List[str]] = None,
) -> None:
    """
    Run transformer for the provided date (default: today's UTC date) and symbol list from config if not provided.
    """
    setup_logging("transformer", config)
    ex = get_exchange_config(config, exchange_name)
    if not symbols:
        symbols = ex.get("symbols", [])

    if not date:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    interval = int(config["transformer"].get("resample_interval_sec", 1))
    for sym in symbols:
        try:
            transform_symbol_day(config, exchange_name, sym, date, interval)
        except Exception as e:
            logger.exception(f"Transformer failed for {sym} {date}: {e}")
