import glob
import hashlib
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from tools.common import (
    ensure_dir,
    get_parquet_symbol_root,
    setup_logging,
)

def _read_partitions_to_df(symbol_root: str, date: str) -> pd.DataFrame:
    year, month, day = date.split("-")
    pattern = os.path.join(symbol_root, f"year={int(year)}", f"month={int(month)}", f"day={int(day)}", "*.parquet")
    # Use DuckDB to efficiently read all partitions
    con = duckdb.connect()
    try:
        q = f"SELECT * FROM read_parquet('{pattern.replace(chr(92), chr(92)*2)}')"
        df = con.execute(q).fetch_df()
    finally:
        con.close()
    # Ensure sorted
    if not df.empty:
        df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
        df = df.sort_values("window_start").drop_duplicates(subset=["window_start"])
    return df

def _hash_dataframe(df: pd.DataFrame) -> str:
    # Compute a stable hash over values
    # Convert to Arrow and then to bytes
    table = pa.Table.from_pandas(df, preserve_index=False)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    data = sink.getvalue().to_pybytes()
    return hashlib.sha256(data).hexdigest()

def _validate_continuity(df: pd.DataFrame) -> Dict[str, Any]:
    result = {"missing": 0, "duplicates": 0}
    if df.empty:
        return result
    s = df["window_start"].sort_values()
    diffs = (s.diff().dt.total_seconds().fillna(1)).astype(int)
    # Count gaps where step != 1s
    gaps = (diffs > 1).sum()
    result["missing"] = int(gaps)
    # Duplicates already dropped
    result["duplicates"] = 0
    return result

def run_compactor(config: Dict[str, Any], exchange_name: str = "binance", date: Optional[str] = None, symbols: Optional[List[str]] = None) -> None:
    setup_logging("compactor", config)
    if not date:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    ex_symbols = symbols or next((ex["symbols"] for ex in config["exchanges"] if ex["name"].lower() == exchange_name.lower()), [])
    for sym in ex_symbols:
        try:
            symbol_root = get_parquet_symbol_root(config, exchange_name, sym)
            df = _read_partitions_to_df(symbol_root, date)
            if df.empty:
                logger.warning(f"No Parquet partitions found for {sym} on {date}")
                continue

            # Select and order columns
            cols_order = [
                "symbol",
                "window_start",
                "open",
                "high",
                "low",
                "close",
                "volume_base",
                "volume_quote",
                "trade_count",
                "vwap",
                "bid",
                "ask",
                "spread",
                "year",
                "month",
                "day",
            ]
            df = df[[c for c in cols_order if c in df.columns]].sort_values("window_start")

            # Validate
            v = _validate_continuity(df)
            h = _hash_dataframe(df)
            num_rows = len(df)
            logger.info(f"Compactor {sym} {date}: timezone=UTC, rows={num_rows}, missing={v['missing']}, duplicates={v['duplicates']}, sha256={h}")

            # Write daily file
            daily_name = f"{date}.parquet"
            out_path = os.path.join(symbol_root, daily_name)
            ensure_dir(symbol_root)
            table = pa.Table.from_pandas(df.drop(columns=["year", "month", "day"], errors="ignore"), preserve_index=False)
            pq.write_table(table, out_path, compression="snappy")
            logger.info(f"Wrote compacted daily file: {out_path}")

            # Write metadata sidecar
            import json
            meta = {
                "timezone": "UTC",
                "date": date,
                "symbol": sym,
                "exchange": exchange_name,
                "rows": num_rows,
                "missing_seconds": v["missing"],
                "duplicates": v["duplicates"],
                "sha256": h,
            }
            meta_path = os.path.join(symbol_root, f"{date}.meta.json")
            with open(meta_path, "w", encoding="utf-8") as mf:
                json.dump(meta, mf, indent=2)
            logger.info(f"Wrote metadata sidecar: {meta_path}")
        except Exception as e:
            logger.exception(f"Compactor failed for {sym} {date}: {e}")
