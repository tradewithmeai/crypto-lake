"""
Data slice export tool using DuckDB views.

Exports timeboxed datasets for analysis in Parquet or CSV format.

Usage:
    python -m tools.slice --config config.yml \
        --symbols SOLUSDT,SUIUSDT \
        --start 2025-10-21T00:00:00Z \
        --end   2025-10-21T23:59:00Z \
        --tf 1m \
        --source bars \
        --out data/extracts/sol_1m_2025-10-21.parquet \
        --format parquet
"""

import argparse
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd
from loguru import logger

from tools.common import ensure_dir, load_config, setup_logging
from tools.db import connect_and_register_views


def build_slice_query(
    symbols: List[str],
    start: str,
    end: str,
    tf: str,
    source: str,
) -> tuple[str, List[str]]:
    """
    Build SQL query for data slice.

    Returns:
        (sql_query, columns_to_export)
    """
    # Determine source view
    if tf == "1s":
        if source != "bars":
            raise ValueError("1s timeframe only available for bars source")
        view = "bars_1s"
        columns = ["symbol", "ts", "open", "high", "low", "close",
                  "volume_base", "volume_quote", "trade_count", "vwap",
                  "bid", "ask", "spread"]
    elif tf == "1m":
        if source == "bars":
            view = "bars_1m"
            columns = ["symbol", "ts", "open", "high", "low", "close",
                      "volume_base", "volume_quote", "trade_count", "vwap",
                      "bid", "ask", "spread"]
        elif source == "klines":
            view = "klines_1m"
            columns = ["symbol", "ts", "open", "high", "low", "close",
                      "volume_base", "volume_quote", "trade_count",
                      "taker_buy_base", "taker_buy_quote"]
        else:
            raise ValueError(f"Unknown source: {source}")
    else:
        raise ValueError(f"Unsupported timeframe: {tf} (use 1s or 1m)")

    # Build WHERE clause
    symbol_list = ", ".join([f"'{s}'" for s in symbols])

    query = f"""
    SELECT {', '.join(columns)}
    FROM {view}
    WHERE symbol IN ({symbol_list})
      AND ts >= TIMESTAMP '{start}'
      AND ts < TIMESTAMP '{end}'
    ORDER BY symbol, ts
    """

    return query, columns


def export_slice(
    config: Dict[str, Any],
    symbols: List[str],
    start: str,
    end: str,
    tf: str,
    source: str,
    out: str,
    format: str,
) -> None:
    """
    Export data slice to Parquet or CSV.

    Args:
        config: Configuration dict
        symbols: List of symbols to export
        start: Start timestamp (ISO format with Z)
        end: End timestamp (ISO format with Z)
        tf: Timeframe (1s or 1m)
        source: Data source (bars or klines)
        out: Output file path
        format: Output format (parquet or csv)
    """
    setup_logging("slice", config)

    base_path = config["general"]["base_path"]

    # Ensure output directory exists
    out_dir = os.path.dirname(out)
    if out_dir:
        ensure_dir(out_dir)

    logger.info(f"Exporting slice: symbols={symbols}, tf={tf}, source={source}")
    logger.info(f"Time range: {start} to {end}")

    # Connect to DuckDB and register views
    conn = connect_and_register_views(base_path)
    logger.info("Views loaded successfully")

    try:

        # Build query
        query, columns = build_slice_query(symbols, start, end, tf, source)
        logger.debug(f"Query: {query}")

        # Execute query
        result = conn.execute(query)
        df = result.fetchdf()

        if df.empty:
            logger.warning("No data returned for query")
            logger.warning(f"Verify data exists for symbols {symbols} in range {start} to {end}")
            return

        # Ensure timestamp is UTC timezone-aware
        if "ts" in df.columns:
            if df["ts"].dt.tz is None:
                df["ts"] = pd.to_datetime(df["ts"], utc=True)
            else:
                df["ts"] = df["ts"].dt.tz_convert("UTC")

        # Export
        if format == "parquet":
            df.to_parquet(out, index=False, compression="snappy")
        elif format == "csv":
            df.to_csv(out, index=False)
        else:
            raise ValueError(f"Unsupported format: {format}")

        logger.info(f"✓ Exported {len(df):,} rows to {out}")
        logger.info(f"  Columns: {list(df.columns)}")
        logger.info(f"  Time range: {df['ts'].min()} to {df['ts'].max()}")

    except Exception as e:
        logger.exception(f"Failed to export slice: {e}")
        raise
    finally:
        conn.close()


def main():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Export data slices for analysis")
    parser.add_argument("--config", type=str, default="config.yml", help="Path to config.yml")
    parser.add_argument("--symbols", type=str, required=True,
                       help="Comma-separated list of symbols (e.g., SOLUSDT,SUIUSDT)")
    parser.add_argument("--start", type=str, required=True,
                       help="Start timestamp (ISO format, e.g., 2025-10-21T00:00:00Z)")
    parser.add_argument("--end", type=str, required=True,
                       help="End timestamp (ISO format, e.g., 2025-10-21T23:59:00Z)")
    parser.add_argument("--tf", type=str, default="1m", choices=["1s", "1m"],
                       help="Timeframe (1s or 1m)")
    parser.add_argument("--source", type=str, default="bars", choices=["bars", "klines"],
                       help="Data source (bars or klines)")
    parser.add_argument("--out", type=str, required=True,
                       help="Output file path")
    parser.add_argument("--format", type=str, default="parquet", choices=["parquet", "csv"],
                       help="Output format (parquet or csv)")

    args = parser.parse_args()

    # Load config
    config = load_config(args.config)

    # Parse symbols
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    if not symbols:
        print("Error: No symbols provided")
        return

    # Export slice
    export_slice(
        config=config,
        symbols=symbols,
        start=args.start,
        end=args.end,
        tf=args.tf,
        source=args.source,
        out=args.out,
        format=args.format,
    )


if __name__ == "__main__":
    main()
