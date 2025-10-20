import asyncio
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from tools.common import (
    ensure_dir,
    get_backfill_symbol_root,
    get_exchange_config,
    setup_logging,
)

BINANCE_LIMIT = 1000  # per klines request

async def _fetch_with_retry(session: aiohttp.ClientSession, url: str, params: Dict[str, Any], retries: int = 5, backoff: float = 1.5) -> List[Any]:
    attempt = 0
    while True:
        try:
            async with session.get(url, params=params, timeout=30) as resp:
                if resp.status == 200:
                    return await resp.json()
                text = await resp.text()
                raise RuntimeError(f"HTTP {resp.status}: {text}")
        except Exception as e:
            attempt += 1
            if attempt > retries:
                raise
            sleep_for = backoff ** attempt
            logger.warning(f"REST error ({e}); retrying in {sleep_for:.1f}s")
            await asyncio.sleep(sleep_for)

def _klines_to_df(rows: List[List[Any]], symbol: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    # Binance kline spec (open time, open, high, low, close, volume, close time, quote volume, trades, taker buy base, taker buy quote, ignore)
    cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume_base",
        "close_time",
        "volume_quote",
        "trade_count",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ]
    df = pd.DataFrame(rows, columns=cols)
    for c in ("open", "high", "low", "close", "volume_base", "volume_quote"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["window_start"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["symbol"] = symbol
    # Approximate vwap as quote/base
    df["vwap"] = (df["volume_quote"] / df["volume_base"]).where(df["volume_base"] > 0, df["close"])
    # Placeholders for 1m-level fields that 1s has
    df["bid"] = pd.NA
    df["ask"] = pd.NA
    df["spread"] = pd.NA
    return df[["symbol", "window_start", "open", "high", "low", "close", "volume_base", "volume_quote", "trade_count", "vwap", "bid", "ask", "spread"]]

async def _backfill_symbol(session: aiohttp.ClientSession, base_url: str, symbol: str, start_ts_ms: int, end_ts_ms: int, out_root: str) -> None:
    ensure_dir(out_root)
    params = {"symbol": symbol, "interval": "1m", "limit": BINANCE_LIMIT}
    current = start_ts_ms
    files_written = 0
    while current < end_ts_ms:
        params["startTime"] = current
        params["endTime"] = min(current + BINANCE_LIMIT * 60_000, end_ts_ms - 1)
        rows = await _fetch_with_retry(session, f"{base_url}/klines", params)
        if not rows:
            current = params["endTime"] + 1
            continue

        df = _klines_to_df(rows, symbol)
        if df.empty:
            current = params["endTime"] + 1
            continue

        # Partition by date
        df["year"] = df["window_start"].dt.year
        df["month"] = df["window_start"].dt.month
        df["day"] = df["window_start"].dt.day
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(table, root_path=out_root, partition_cols=["year", "month", "day"], compression="snappy")
        files_written += 1

        # Advance by number of rows * 1 minute
        last_close = int(df["window_start"].max().timestamp() * 1000) + 60_000
        current = max(current + len(rows) * 60_000, last_close)

    logger.info(f"Backfill complete for {symbol}, files_written={files_written}")

async def _run_backfill_async(config: Dict[str, Any], exchange_name: str, days: int, symbols: Optional[List[str]]) -> None:
    setup_logging("backfill", config)
    ex = get_exchange_config(config, exchange_name)
    base_url = ex["rest_url"]

    if not symbols:
        symbols = ex.get("symbols", [])

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    start_ts_ms = int(start.timestamp() * 1000)
    end_ts_ms = int(end.timestamp() * 1000)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for sym in symbols:
            out_root = get_backfill_symbol_root(config, exchange_name, sym)
            tasks.append(asyncio.create_task(_backfill_symbol(session, base_url, sym, start_ts_ms, end_ts_ms, out_root)))
        await asyncio.gather(*tasks)

def run_backfill(config: Dict[str, Any], exchange_name: str = "binance", days: int = 90, symbols: Optional[List[str]] = None) -> None:
    asyncio.run(_run_backfill_async(config, exchange_name, days, symbols))
