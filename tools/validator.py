import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd
from loguru import logger

from tools.common import (
    ensure_dir,
    get_parquet_symbol_root,
    setup_logging,
)

EXPECTED_COLUMNS = [
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
]

def _read_daily(symbol_root: str, date: str) -> pd.DataFrame:
    daily_path = os.path.join(symbol_root, f"{date}.parquet")
    if not os.path.exists(daily_path):
        # Fallback: read partitioned
        y, m, d = date.split("-")
        pattern = os.path.join(symbol_root, f"year={int(y)}", f"month={int(m)}", f"day={int(d)}", "*.parquet")
    else:
        pattern = daily_path
    con = duckdb.connect()
    try:
        q = f"SELECT * FROM read_parquet('{pattern.replace(chr(92), chr(92)*2)}')"
        df = con.execute(q).fetch_df()
    finally:
        con.close()
    if not df.empty:
        df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
        df = df.sort_values("window_start")
    return df

def _check_schema(df: pd.DataFrame) -> List[str]:
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    return missing

def _find_missing_seconds(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    s = df["window_start"]
    diffs = s.diff().dt.total_seconds().fillna(1)
    return int((diffs > 1).sum())

def _find_duplicates(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    return int(df.duplicated(subset=["window_start"]).sum())

def _write_report(logs_dir: str, exchange: str, symbol: str, date: str, report: Dict[str, Any]) -> str:
    out_dir = os.path.join(logs_dir, "validation")
    ensure_dir(out_dir)
    path = os.path.join(out_dir, f"{exchange}_{symbol}_{date}.txt")
    lines = [f"{k}: {v}" for k, v in report.items()]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path

def run_validator(config: Dict[str, Any], exchange_name: str = "binance", date: Optional[str] = None, symbols: Optional[List[str]] = None) -> None:
    setup_logging("validation", config)
    if not date:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logs_dir = os.path.join(config["general"]["base_path"], "logs")

    ex_symbols = symbols or next((ex["symbols"] for ex in config["exchanges"] if ex["name"].lower() == exchange_name.lower()), [])
    for sym in ex_symbols:
        try:
            symbol_root = get_parquet_symbol_root(config, exchange_name, sym)
            df = _read_daily(symbol_root, date)
            report = {
                "exchange": exchange_name,
                "symbol": sym,
                "date": date,
                "rows": int(len(df)),
                "schema_missing": _check_schema(df),
                "missing_seconds": _find_missing_seconds(df),
                "duplicates": _find_duplicates(df),
            }
            path = _write_report(logs_dir, exchange_name, sym, date, report)
            logger.info(f"Validation report written: {path}")
        except Exception as e:
            logger.exception(f"Validation failed for {sym} {date}: {e}")
