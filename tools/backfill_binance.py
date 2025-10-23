"""
Binance historical data backfill module.

Fetches historical 1-minute OHLCV data from Binance REST API and writes
partitioned Parquet files for efficient querying.
"""

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger


class BinanceBackfiller:
    """Handles backfilling historical OHLCV data from Binance REST API."""

    BASE_URL = "https://api.binance.com"
    KLINES_ENDPOINT = "/api/v3/klines"

    # Binance limits: 1000 klines per request, weight=1
    MAX_KLINES_PER_REQUEST = 1000
    RATE_LIMIT_WEIGHT = 1200  # per minute
    REQUESTS_PER_MINUTE = 1200 // RATE_LIMIT_WEIGHT

    def __init__(self, base_dir: Path, interval: str = "1m"):
        """
        Initialize backfiller.

        Args:
            base_dir: Base directory for backfill data (e.g., D:/CryptoDataLake/backfill/binance)
            interval: Kline interval (default: 1m)
        """
        self.base_dir = Path(base_dir)
        self.interval = interval
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "crypto-lake/1.0"})

    def _get_existing_timestamps(self, symbol: str, date: datetime) -> set:
        """
        Get existing timestamps for a symbol on a specific date.

        Args:
            symbol: Trading pair symbol (e.g., SOLUSDT)
            date: Date to check

        Returns:
            Set of existing timestamps (as pandas Timestamp objects)
        """
        year = date.year
        month = date.month
        day = date.day

        parquet_path = (
            self.base_dir / symbol / f"year={year}" / f"month={month:02d}" / f"day={day:02d}"
        )

        if not parquet_path.exists():
            return set()

        try:
            # Read all parquet files for this date
            df = pd.read_parquet(parquet_path)
            if "open_time" in df.columns:
                return set(df["open_time"])
            return set()
        except Exception as e:
            logger.warning(f"Failed to read existing data for {symbol} on {date.date()}: {e}")
            return set()

    def _fetch_klines(
        self,
        symbol: str,
        start_time: int,
        end_time: int,
        max_retries: int = 5
    ) -> Optional[List]:
        """
        Fetch klines from Binance API with retry logic.

        Args:
            symbol: Trading pair symbol
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            max_retries: Maximum number of retry attempts

        Returns:
            List of klines or None if failed
        """
        params = {
            "symbol": symbol,
            "interval": self.interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": self.MAX_KLINES_PER_REQUEST,
        }

        for attempt in range(max_retries):
            try:
                response = self.session.get(
                    f"{self.BASE_URL}{self.KLINES_ENDPOINT}",
                    params=params,
                    timeout=30,
                )

                # Handle rate limits
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                data = response.json()

                # Handle API errors
                if isinstance(data, dict) and "code" in data:
                    logger.error(f"Binance API error: {data}")
                    return None

                return data

            except requests.exceptions.RequestException as e:
                wait_time = min(2 ** attempt, 60)  # Exponential backoff, max 60s
                logger.warning(
                    f"Request failed (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)

        logger.error(f"Failed to fetch klines for {symbol} after {max_retries} attempts")
        return None

    def _klines_to_dataframe(self, klines: List) -> pd.DataFrame:
        """
        Convert Binance klines to pandas DataFrame.

        Args:
            klines: List of kline arrays from Binance API

        Returns:
            DataFrame with standardized columns
        """
        df = pd.DataFrame(
            klines,
            columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "trades",
                "taker_base_vol", "taker_quote_vol", "ignore"
            ]
        )

        # Convert timestamp columns to datetime
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

        # Convert numeric columns
        numeric_cols = [
            "open", "high", "low", "close", "volume",
            "quote_volume", "taker_base_vol", "taker_quote_vol"
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["trades"] = pd.to_numeric(df["trades"], errors="coerce").astype("int64")

        # Drop the ignore column
        df = df.drop(columns=["ignore"])

        return df

    def _write_parquet(self, df: pd.DataFrame, symbol: str):
        """
        Write DataFrame to partitioned Parquet files.

        Args:
            df: DataFrame with kline data
            symbol: Trading pair symbol
        """
        if df.empty:
            logger.warning(f"No data to write for {symbol}")
            return

        # Add partition columns
        df["year"] = df["open_time"].dt.year
        df["month"] = df["open_time"].dt.month
        df["day"] = df["open_time"].dt.day

        # Group by date and write each partition
        for (year, month, day), group in df.groupby(["year", "month", "day"]):
            partition_dir = (
                self.base_dir / symbol / f"year={year}" / f"month={month:02d}" / f"day={day:02d}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Drop partition columns from data
            data_df = group.drop(columns=["year", "month", "day"])

            # Check for existing data and merge
            existing_file = partition_dir / "data.parquet"
            if existing_file.exists():
                try:
                    existing_df = pd.read_parquet(existing_file)
                    # Combine and deduplicate
                    combined_df = pd.concat([existing_df, data_df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=["open_time"], keep="last")
                    combined_df = combined_df.sort_values("open_time")
                    data_df = combined_df
                except Exception as e:
                    logger.warning(f"Failed to merge with existing data: {e}")

            # Write to Parquet (reset index to avoid __index_level_0__ column)
            data_df = data_df.reset_index(drop=True)
            table = pa.Table.from_pandas(data_df, preserve_index=False)
            pq.write_table(table, existing_file, compression="snappy")

            logger.debug(
                f"Wrote {len(data_df)} rows to {symbol}/year={year}/month={month:02d}/day={day:02d}"
            )

    def backfill_symbol(
        self,
        symbol: str,
        lookback_days: int,
        end_date: Optional[datetime] = None
    ) -> int:
        """
        Backfill historical data for a single symbol.

        Args:
            symbol: Trading pair symbol (e.g., SOLUSDT)
            lookback_days: Number of days to backfill
            end_date: End date (defaults to now)

        Returns:
            Total number of rows written
        """
        if end_date is None:
            end_date = datetime.now(timezone.utc)

        start_date = end_date - timedelta(days=lookback_days)

        logger.info(
            f"Starting backfill for {symbol}: {start_date.date()} to {end_date.date()} "
            f"({lookback_days} days)"
        )

        total_rows = 0
        current_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        while current_date < end_date:
            # Fetch one day at a time
            day_start = int(current_date.timestamp() * 1000)
            day_end = int((current_date + timedelta(days=1)).timestamp() * 1000)

            logger.info(f"Fetching {symbol} for {current_date.date()}")

            # Check for existing data
            existing_timestamps = self._get_existing_timestamps(symbol, current_date)
            if existing_timestamps:
                logger.debug(
                    f"Found {len(existing_timestamps)} existing timestamps for {symbol} "
                    f"on {current_date.date()}"
                )

            # Fetch klines
            klines = self._fetch_klines(symbol, day_start, day_end)

            if klines is None:
                logger.error(f"Failed to fetch data for {symbol} on {current_date.date()}")
                current_date += timedelta(days=1)
                continue

            if not klines:
                logger.warning(f"No data returned for {symbol} on {current_date.date()}")
                current_date += timedelta(days=1)
                continue

            # Convert to DataFrame
            df = self._klines_to_dataframe(klines)

            # Filter out existing timestamps
            if existing_timestamps:
                df = df[~df["open_time"].isin(existing_timestamps)]

            # Write to Parquet
            if not df.empty:
                self._write_parquet(df, symbol)
                total_rows += len(df)
                logger.info(
                    f"Wrote {len(df)} new rows for {symbol} on {current_date.date()} "
                    f"(total: {total_rows})"
                )
            else:
                logger.debug(f"No new data for {symbol} on {current_date.date()}")

            # Move to next day
            current_date += timedelta(days=1)

            # Rate limit: sleep to avoid hitting limits
            time.sleep(0.1)  # 10 requests per second max

        logger.info(f"Backfill complete for {symbol}: {total_rows} total rows written")
        return total_rows


def backfill_binance(
    symbols: List[str],
    lookback_days: int,
    base_dir: Path,
    interval: str = "1m"
) -> dict:
    """
    Backfill historical data for multiple symbols.

    Args:
        symbols: List of trading pair symbols
        lookback_days: Number of days to backfill
        base_dir: Base directory for backfill data
        interval: Kline interval (default: 1m)

    Returns:
        Dictionary mapping symbols to row counts
    """
    backfiller = BinanceBackfiller(base_dir, interval)
    results = {}

    for symbol in symbols:
        try:
            rows = backfiller.backfill_symbol(symbol, lookback_days)
            results[symbol] = rows
        except Exception as e:
            logger.error(f"Failed to backfill {symbol}: {e}")
            results[symbol] = 0

    # Summary
    total_rows = sum(results.values())
    logger.info(f"Backfill complete: {total_rows} total rows across {len(symbols)} symbols")
    for symbol, rows in results.items():
        logger.info(f"  {symbol}: {rows} rows")

    return results
