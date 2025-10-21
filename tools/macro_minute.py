"""
Macro 1-minute data collector using yfinance.

Fetches 1-minute OHLCV data for macro tickers (SPY, UUP, ES=F, etc.),
normalizes to UTC, and writes to partitioned Parquet files.

Usage:
    python -m tools.macro_minute --config config.yml --tickers SPY,UUP,ES=F --lookback_days 7
"""

import argparse
import glob
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf
from loguru import logger

from tools.common import ensure_dir, load_config, setup_logging


def fetch_yf_1m(ticker: str, lookback_days: int = 7) -> pd.DataFrame:
    """
    Fetch 1-minute OHLCV data from yfinance for the specified ticker.

    Args:
        ticker: Ticker symbol (e.g., "SPY", "UUP", "ES=F")
        lookback_days: Number of days to look back (max 7 for 1-minute data)

    Returns:
        DataFrame with columns: ts (UTC timezone-aware), open, high, low, close, volume, ticker
    """
    try:
        logger.info(f"Fetching 1-minute data for {ticker}, lookback={lookback_days} days")

        # yfinance download with 1-minute interval
        # Note: yfinance limits 1m data to 7 days max
        if lookback_days > 7:
            logger.warning(f"yfinance limits 1m data to 7 days, capping lookback_days to 7")
            lookback_days = 7

        # Download data
        data = yf.download(
            tickers=ticker,
            period=f"{lookback_days}d",
            interval="1m",
            progress=False,
            auto_adjust=False,  # Keep raw prices
            prepost=False,  # Regular trading hours only
        )

        if data.empty:
            logger.warning(f"No data returned for {ticker}")
            return pd.DataFrame()

        # Reset index to get timestamp as column
        df = data.reset_index()

        # Normalize column names (yfinance can return multi-index or single-index)
        if isinstance(df.columns, pd.MultiIndex):
            # Multi-ticker case (shouldn't happen with single ticker, but handle it)
            df.columns = ['_'.join(col).strip('_') if col[1] else col[0] for col in df.columns.values]

        # Standardize column names: remove ticker suffix and convert to lowercase
        # yfinance adds ticker suffix like 'Close_SPY' -> we want just 'close'
        new_columns = []
        for col in df.columns:
            # Strip ticker suffix if present (e.g., '_SPY')
            col_clean = col.split('_')[0] if '_' in col else col
            new_columns.append(col_clean.lower())

        df.columns = new_columns

        # Select and rename columns
        column_map = {
            'datetime': 'ts',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',  # Use regular Close, skip adj close
            'volume': 'volume'
        }

        # Select only the columns we need
        selected = []
        for old_col in df.columns:
            if old_col in column_map:
                selected.append(old_col)

        if not selected:
            logger.error(f"No matching columns found for {ticker}. Available: {list(df.columns)}")
            return pd.DataFrame()

        df = df[selected].copy()
        df = df.rename(columns=column_map)

        # Ensure we have required columns
        required = ['ts', 'open', 'high', 'low', 'close', 'volume']
        missing = [c for c in required if c not in df.columns]
        if missing:
            logger.error(f"Missing required columns for {ticker}: {missing}")
            return pd.DataFrame()

        # Select only required columns
        df = df[required].copy()

        # Normalize timestamp to UTC
        # yfinance returns timezone-aware timestamps, but they might be in exchange local time
        if df['ts'].dt.tz is None:
            # If no timezone, assume UTC
            df['ts'] = pd.to_datetime(df['ts'], utc=True)
        else:
            # Convert to UTC
            df['ts'] = df['ts'].dt.tz_convert('UTC')

        # Add ticker column
        df['ticker'] = ticker

        # Ensure correct dtypes
        df['open'] = df['open'].astype('float64')
        df['high'] = df['high'].astype('float64')
        df['low'] = df['low'].astype('float64')
        df['close'] = df['close'].astype('float64')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype('int64')

        # Drop any rows with NaN prices
        df = df.dropna(subset=['open', 'high', 'low', 'close'])

        # Sort by timestamp
        df = df.sort_values('ts').reset_index(drop=True)

        logger.info(f"Fetched {len(df)} rows for {ticker} from {df['ts'].min()} to {df['ts'].max()}")

        return df

    except Exception as e:
        logger.exception(f"Failed to fetch data for {ticker}: {e}")
        return pd.DataFrame()


def _read_existing_data(base_path: str, ticker: str, lookback_days: int = 8) -> pd.DataFrame:
    """
    Read existing Parquet data for the ticker to enable deduplication.

    Args:
        base_path: Base path for macro data
        ticker: Ticker symbol
        lookback_days: How many days back to read

    Returns:
        DataFrame with existing data, or empty DataFrame if no data exists
    """
    try:
        ticker_path = os.path.join(base_path, "macro", "minute", ticker)
        if not os.path.exists(ticker_path):
            return pd.DataFrame()

        # Find all Parquet files in the last N days
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)

        pattern = os.path.join(ticker_path, "**", "*.parquet")
        files = glob.glob(pattern, recursive=True)

        if not files:
            return pd.DataFrame()

        # Read all files
        frames = []
        for f in files:
            try:
                df_existing = pd.read_parquet(f)
                if not df_existing.empty and 'ts' in df_existing.columns:
                    # Ensure ts is datetime
                    df_existing['ts'] = pd.to_datetime(df_existing['ts'], utc=True)
                    # Filter to lookback window
                    df_existing = df_existing[df_existing['ts'] >= cutoff_date]
                    if not df_existing.empty:
                        frames.append(df_existing)
            except Exception as e:
                logger.warning(f"Failed to read {f}: {e}")

        if not frames:
            return pd.DataFrame()

        existing = pd.concat(frames, ignore_index=True)
        existing = existing.sort_values('ts').drop_duplicates(subset=['ts'], keep='last')

        logger.info(f"Read {len(existing)} existing rows for {ticker}")
        return existing

    except Exception as e:
        logger.exception(f"Failed to read existing data for {ticker}: {e}")
        return pd.DataFrame()


def write_parquet(df: pd.DataFrame, base_path: str, compression: str = "snappy") -> None:
    """
    Write DataFrame to partitioned Parquet files.

    Args:
        df: DataFrame with ts, open, high, low, close, volume, ticker columns
        base_path: Base path for data lake
        compression: Parquet compression codec
    """
    if df.empty:
        logger.warning("DataFrame is empty, nothing to write")
        return

    try:
        ticker = df['ticker'].iloc[0]

        # Add partition columns
        df = df.copy()
        df['year'] = df['ts'].dt.year
        df['month'] = df['ts'].dt.month
        df['day'] = df['ts'].dt.day

        # Output path: {base_path}/macro/minute/{ticker}/
        root = os.path.join(base_path, "macro", "minute", ticker)
        ensure_dir(root)

        # Convert to Arrow table
        table = pa.Table.from_pandas(df, preserve_index=False)

        # Write partitioned by year/month/day
        pq.write_to_dataset(
            table,
            root_path=root,
            partition_cols=["year", "month", "day"],
            compression=compression,
        )

        logger.info(f"Wrote {len(df)} rows for {ticker} to {root}")

    except Exception as e:
        logger.exception(f"Failed to write Parquet: {e}")


def run_macro_minute(
    config: Dict[str, Any],
    tickers: List[str],
    lookback_days: int = 7,
) -> None:
    """
    Main function to fetch and store macro 1-minute data.

    Args:
        config: Configuration dictionary
        tickers: List of ticker symbols
        lookback_days: Number of days to look back
    """
    setup_logging("macro_minute", config)

    base_path = config["general"]["base_path"]
    compression = config.get("transformer", {}).get("parquet_compression", "snappy")

    for ticker in tickers:
        try:
            logger.info(f"Processing ticker: {ticker}")

            # Fetch new data
            df_new = fetch_yf_1m(ticker, lookback_days=lookback_days)
            if df_new.empty:
                logger.warning(f"No new data for {ticker}, skipping")
                continue

            # Read existing data for deduplication
            df_existing = _read_existing_data(base_path, ticker, lookback_days=8)

            # Deduplicate
            if not df_existing.empty:
                # Combine new and existing
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                # Keep latest on collision (based on ts)
                df_combined = df_combined.sort_values('ts').drop_duplicates(subset=['ts'], keep='last')
                # Only write the new data (timestamps not in existing)
                existing_ts = set(df_existing['ts'])
                df_to_write = df_combined[~df_combined['ts'].isin(existing_ts)]

                if df_to_write.empty:
                    logger.info(f"No new unique data for {ticker} after deduplication")
                    continue

                logger.info(f"After dedup: {len(df_to_write)} new rows for {ticker}")
            else:
                df_to_write = df_new

            # Write to Parquet
            write_parquet(df_to_write, base_path, compression=compression)

        except Exception as e:
            logger.exception(f"Failed to process ticker {ticker}: {e}")


def main():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Fetch 1-minute macro data from yfinance")
    parser.add_argument("--config", type=str, default="config.yml", help="Path to config.yml")
    parser.add_argument("--tickers", type=str, required=True, help="Comma-separated list of tickers (e.g., SPY,UUP,ES=F)")
    parser.add_argument("--lookback_days", type=int, default=7, help="Number of days to look back (max 7)")

    args = parser.parse_args()

    # Load config
    config = load_config(args.config)

    # Parse tickers
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]

    if not tickers:
        print("Error: No tickers provided")
        return

    # Run collector
    run_macro_minute(config, tickers, lookback_days=args.lookback_days)


if __name__ == "__main__":
    main()
