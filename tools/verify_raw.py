#!/usr/bin/env python
"""
verify_raw.py — Compare locally aggregated 1s->1m bars against Binance 1m klines.

Usage (CMD):
  venv\Scripts\activate
  python tools\verify_raw.py ^
    --base D:\CryptoDataLake ^
    --symbols SOLUSDT,SUIUSDT,ADAUSDT ^
    --start 2025-10-21T00:00:00Z ^
    --end   2025-10-21T14:40:00Z ^
    --report reports\verify_2025-10-21.md ^
    --tolerance_ticks 0.001 ^
    --tolerance_vol_bps 50

Notes
- Requires: duckdb, pandas, requests
- We interpret tolerance_ticks as an *absolute* price tolerance (e.g., 0.01 means ±1 cent).
- Volume tolerance is in basis points (bps) of reference volume (e.g., 50 = 0.5%).
- Start/End must be UTC ISO (ending with 'Z' or include offset).
"""

import os
import sys
import time
import math
import json
import argparse
import datetime as dt
from typing import List, Dict, Tuple

import duckdb
import pandas as pd

try:
    import requests
except ImportError:
    print("Missing dependency: requests. Install with: pip install requests")
    sys.exit(1)


def parse_args():
    ap = argparse.ArgumentParser(description="Verify locally aggregated 1m bars vs Binance 1m klines.")
    ap.add_argument("--base", required=True, help="Base path to data lake (e.g., D:\\CryptoDataLake)")
    ap.add_argument("--exchange", default="binance", help="Exchange folder name (default: binance)")
    ap.add_argument("--symbols", required=True, help="CSV list, e.g. SOLUSDT,SUIUSDT,ADAUSDT")
    ap.add_argument("--start", required=True, help="Start ISO8601 UTC, e.g. 2025-10-21T00:00:00Z")
    ap.add_argument("--end", required=True, help="End ISO8601 UTC, e.g. 2025-10-21T14:40:00Z")
    ap.add_argument("--report", required=True, help="Output Markdown report path")
    ap.add_argument("--tolerance_ticks", type=float, default=0.001, help="Absolute price tolerance for OHLC (default: 0.001)")
    ap.add_argument("--tolerance_vol_bps", type=float, default=50.0, help="Volume tolerance in bps (default: 50 = 0.5%)")
    ap.add_argument("--binance_url", default="https://api.binance.com/api/v3/klines", help="Klines endpoint")
    ap.add_argument("--limit", type=int, default=1000, help="Max klines per request (Binance limit 1000)")
    return ap.parse_args()


def to_ms(ts: dt.datetime) -> int:
    # Ensure UTC and convert to milliseconds
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    else:
        ts = ts.astimezone(dt.timezone.utc)
    return int(ts.timestamp() * 1000)


def parse_iso_utc(s: str) -> dt.datetime:
    # Accept 'Z' or offset; normalize to aware UTC
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return dt.datetime.fromisoformat(s).astimezone(dt.timezone.utc)


def fetch_binance_klines(symbol: str, start: dt.datetime, end: dt.datetime, url: str, limit: int = 1000) -> pd.DataFrame:
    """
    Fetch 1m klines between [start, end) by paginating.
    Returns DataFrame with columns: ts (UTC-aware), open, high, low, close, volume_base
    """
    cols = ["open_time","open","high","low","close","volume","close_time",
            "quote_asset_volume","number_of_trades","taker_buy_base","taker_buy_quote","ignore"]

    start_ms = to_ms(start)
    end_ms   = to_ms(end)
    out = []

    while start_ms < end_ms:
        params = {
            "symbol": symbol.upper(),
            "interval": "1m",
            "startTime": start_ms,
            "endTime": min(end_ms, start_ms + limit * 60_000),  # 1m per candle
            "limit": limit
        }
        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            raise RuntimeError(f"Binance API error {r.status_code}: {r.text}")
        data = r.json()
        if not data:
            break
        for row in data:
            row_dict = dict(zip(cols, row))
            out.append(row_dict)
        # advance: next minute after the last candle open_time
        last_open = data[-1][0]
        start_ms = last_open + 60_000
        time.sleep(0.2)  # be gentle

    if not out:
        return pd.DataFrame(columns=["ts","open","high","low","close","volume_base"])

    df = pd.DataFrame(out)
    df["ts"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.rename(columns={"volume": "volume_base"})
    for c in ["open","high","low","close","volume_base"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df[["ts","open","high","low","close","volume_base"]].dropna()
    return df


def aggregate_local_1m(glob_path: str, start: dt.datetime, end: dt.datetime) -> pd.DataFrame:
    """
    Read local 1s parquet and aggregate to 1m with DuckDB.
    Assumes timestamp column is window_start (TIMESTAMP WITH TIME ZONE).
    """
    con = duckdb.connect()
    start_iso = start.isoformat()
    end_iso   = end.isoformat()
    q = f"""
    WITH base AS (
      SELECT window_start, open, high, low, close, volume_base
      FROM read_parquet('{glob_path}')
      WHERE window_start >= TIMESTAMP '{start_iso}'
        AND window_start <  TIMESTAMP '{end_iso}'
    ),
    b AS (
      SELECT
        date_trunc('minute', window_start) AS ts,
        first(open)  AS open,
        max(high)    AS high,
        min(low)     AS low,
        last(close)  AS close,
        sum(volume_base) AS volume_base
      FROM base
      GROUP BY ts
    )
    SELECT ts, open, high, low, close, volume_base
    FROM b
    ORDER BY ts;
    """
    df = con.execute(q).fetchdf()
    # DuckDB returns timezone-aware timestamps; ensure dtype is datetime64[ns, UTC]
    if not pd.api.types.is_datetime64_any_dtype(df["ts"]):
        df["ts"] = pd.to_datetime(df["ts"], utc=True)

    # de-dup in case of any accidental duplicates
    df = df.drop_duplicates(subset=["ts"]).reset_index(drop=True)
    return df


def compare_frames(our: pd.DataFrame, ref: pd.DataFrame,
                   tol_ticks: float, tol_vol_bps: float) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """
    Align on ts and compute diffs. Return (mismatches_df, summary_metrics).
    Volume tolerance: relative bps vs ref volume.
    """
    # Inner join on minute timestamp
    m = pd.merge(our, ref, on="ts", suffixes=("_our","_ref"), how="inner")

    if m.empty:
        return m, {
            "aligned_minutes": 0,
            "mismatch_rows": 0,
            "pct_ohlc_mismatch": 0.0,
            "pct_vol_mismatch": 0.0
        }

    # OHLC diffs
    for c in ["open","high","low","close"]:
        m[f"diff_{c}"] = (m[f"{c}_our"] - m[f"{c}_ref"]).abs()

    # Volume diffs in bps
    m["vol_diff_abs"] = (m["volume_base_our"] - m["volume_base_ref"]).abs()
    m["vol_diff_bps"] = (m["vol_diff_abs"] / m["volume_base_ref"].replace(0, pd.NA)) * 10_000
    m["vol_diff_bps"] = m["vol_diff_bps"].fillna(0.0)

    # Flags
    m["ohlc_flag"] = (
        (m["diff_open"] > tol_ticks) |
        (m["diff_high"] > tol_ticks) |
        (m["diff_low"]  > tol_ticks) |
        (m["diff_close"]> tol_ticks)
    )

    m["vol_flag"] = (m["vol_diff_bps"] > tol_vol_bps)

    mismatches = m[(m["ohlc_flag"]) | (m["vol_flag"])].copy()

    aligned = len(m)
    mism = len(mismatches)
    pct_ohlc = float((m["ohlc_flag"].sum() / aligned)*100.0) if aligned else 0.0
    pct_vol  = float((m["vol_flag"].sum()  / aligned)*100.0) if aligned else 0.0

    summary = {
        "aligned_minutes": aligned,
        "mismatch_rows": mism,
        "pct_ohlc_mismatch": pct_ohlc,
        "pct_vol_mismatch": pct_vol,
        "max_abs_ohlc_diff": float(max(
            m["diff_open"].max(), m["diff_high"].max(),
            m["diff_low"].max(),  m["diff_close"].max()
        )) if aligned else 0.0,
        "max_vol_diff_bps": float(m["vol_diff_bps"].max()) if aligned else 0.0
    }
    return mismatches, summary


def ensure_report_dir(path: str):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)


def write_markdown(report_path: str, run_meta: Dict, per_symbol: Dict[str, Dict], head_rows: int = 25):
    ensure_report_dir(report_path)
    lines = []
    lines.append(f"# Verification Report — {dt.datetime.now(dt.timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"**Window:** {run_meta['start']} → {run_meta['end']} (UTC)")
    lines.append(f"**Tolerance:** ±{run_meta['tol_ticks']} price units; Volume ≤ {run_meta['tol_vol_bps']} bps")
    lines.append("")
    lines.append("| Symbol | Aligned Minutes | Mismatch Rows | %OHLC Mismatch | %Vol Mismatch | Max | Max Vol bps |")
    lines.append("|-------:|----------------:|--------------:|---------------:|--------------:|----:|------------:|")

    for sym, res in per_symbol.items():
        s = res["summary"]
        lines.append(
            f"| {sym} | {s['aligned_minutes']} | {s['mismatch_rows']} | "
            f"{s['pct_ohlc_mismatch']:.2f}% | {s['pct_vol_mismatch']:.2f}% | "
            f"{s['max_abs_ohlc_diff']:.6g} | {s['max_vol_diff_bps']:.1f} |"
        )

    lines.append("")
    for sym, res in per_symbol.items():
        mism = res["mismatches"]
        lines.append(f"## {sym}")
        if mism.empty:
            lines.append("_No mismatches within tolerances._")
            lines.append("")
            continue

        lines.append(f"_Showing up to {head_rows} mismatching rows:_")
        lines.append("")
        # Small, readable table
        cols = [
            "ts",
            "open_our","open_ref","diff_open",
            "high_our","high_ref","diff_high",
            "low_our","low_ref","diff_low",
            "close_our","close_ref","diff_close",
            "volume_base_our","volume_base_ref","vol_diff_bps"
        ]
        show = mism[cols].copy()
        # limit size
        show = show.head(head_rows)
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("|" + "|".join(["---"]*len(cols)) + "|")
        for _, row in show.iterrows():
            vals = [row[c] if not isinstance(row[c], pd.Timestamp) else row[c].isoformat() for c in cols]
            lines.append("| " + " | ".join([str(v) for v in vals]) + " |")
        lines.append("")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    args = parse_args()
    start = parse_iso_utc(args.start)
    end   = parse_iso_utc(args.end)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    run_meta = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "tol_ticks": args.tolerance_ticks,
        "tol_vol_bps": args.tolerance_vol_bps
    }

    print(f"[verify] Window: {run_meta['start']} -> {run_meta['end']} UTC")
    print(f"[verify] Symbols: {symbols}")
    print(f"[verify] Tolerances: ±{args.tolerance_ticks} price units, volume ≤ {args.tolerance_vol_bps} bps")

    per_symbol = {}

    for sym in symbols:
        print(f"\n[verify] {sym}: aggregating local 1s -> 1m ...")
        glob_path = os.path.join(args.base, "parquet", args.exchange, sym, "**", "*.parquet")
        our_1m = aggregate_local_1m(glob_path, start, end)
        print(f"[verify] {sym}: local 1m rows = {len(our_1m)}")

        print(f"[verify] {sym}: fetching Binance 1m klines ...")
        ref_1m = fetch_binance_klines(sym, start, end, url=args.binance_url, limit=args.limit)
        print(f"[verify] {sym}: Binance 1m rows = {len(ref_1m)}")

        if our_1m.empty or ref_1m.empty:
            mismatches = pd.DataFrame()
            summary = {
                "aligned_minutes": 0,
                "mismatch_rows": 0,
                "pct_ohlc_mismatch": 0.0,
                "pct_vol_mismatch": 0.0,
                "max_abs_ohlc_diff": 0.0,
                "max_vol_diff_bps": 0.0
            }
        else:
            mismatches, summary = compare_frames(our_1m, ref_1m, args.tolerance_ticks, args.tolerance_vol_bps)

        per_symbol[sym] = {
            "mismatches": mismatches,
            "summary": summary
        }

        # Console summary line
        s = summary
        print(f"[verify] {sym}: aligned={s['aligned_minutes']} mismatches={s['mismatch_rows']} "
              f"ohlc%={s['pct_ohlc_mismatch']:.2f} vol%={s['pct_vol_mismatch']:.2f} "
              f"max_ohlc_diff={s['max_abs_ohlc_diff']:.6g} max_vol_bps={s['max_vol_diff_bps']:.1f}")

    # Write Markdown report
    print(f"\n[verify] Writing report -> {args.report}")
    write_markdown(args.report, run_meta, per_symbol)
    print("[verify] Done.")


if __name__ == "__main__":
    main()
