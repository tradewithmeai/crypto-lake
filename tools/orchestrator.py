"""
Unified orchestrator for running both Binance WebSocket collector and macro/FX data fetcher.

This module provides a single long-running process that:
1. Runs the Binance WebSocket collector continuously
2. Fetches macro/FX 1-minute data on a schedule (e.g., every 15 minutes)
3. Performs one-time backfill on startup (7 days of historical data)
4. Exposes health metrics via JSON and Markdown files
5. Handles clean shutdown on Ctrl+C or SIGTERM
"""

import asyncio
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from loguru import logger

from collector.collector import run_collector
from tools.macro_minute import fetch_yf_1m, write_parquet, _read_existing_data
from tools.health import write_heartbeat, summarize_files
from transformer.transformer import run_transformer


class Orchestrator:
    """
    Orchestrator that manages both real-time crypto collection and scheduled macro data fetching.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        exchange_name: str = "binance",
        symbols: Optional[List[str]] = None,
        macro_tickers: Optional[List[str]] = None,
        macro_interval_min: int = 15,
        macro_lookback_startup_days: int = 7,
        macro_runtime_lookback_days: int = 1,
        transform_interval_min: int = 60,
    ):
        """
        Initialize orchestrator with configuration.

        Args:
            config: Configuration dictionary from config.yml
            exchange_name: Exchange name (default: binance)
            symbols: List of crypto symbols to collect (overrides config)
            macro_tickers: List of macro tickers to fetch (e.g., SPY, UUP, ES=F)
            macro_interval_min: Minutes between macro data fetches
            macro_lookback_startup_days: Days to backfill on startup
            macro_runtime_lookback_days: Days to fetch on each scheduled run
            transform_interval_min: Minutes between transformer runs (0 to disable)
        """
        self.config = config
        self.exchange_name = exchange_name
        self.symbols = symbols
        self.macro_tickers = macro_tickers or []
        self.macro_interval_min = macro_interval_min
        self.macro_lookback_startup_days = macro_lookback_startup_days
        self.macro_runtime_lookback_days = macro_runtime_lookback_days
        self.transform_interval_min = transform_interval_min

        self.base_path = config["general"]["base_path"]
        self.compression = config.get("transformer", {}).get("parquet_compression", "snappy")

        # Threading control
        self.stop_event = threading.Event()
        self.ws_thread: Optional[threading.Thread] = None
        self.macro_thread: Optional[threading.Thread] = None
        self.transform_thread: Optional[threading.Thread] = None
        self.health_thread: Optional[threading.Thread] = None

        # Shared state for health monitoring
        self.health_data = {
            "collector": {
                "status": "stopped",
                "last_latency_p50_ms": 0.0,
                "last_latency_p95_ms": 0.0,
                "last_seen_ts": None,
            },
            "macro_minute": {
                "status": "idle",
                "last_run_start": None,
                "last_run_end": None,
                "last_run_rows_written": 0,
                "last_error": None,
            },
            "transformer": {
                "status": "idle",
                "last_run_start": None,
                "last_run_end": None,
                "last_error": None,
            },
        }
        self.health_lock = threading.Lock()

        logger.info(
            f"Orchestrator initialized: exchange={exchange_name}, "
            f"macro_tickers={macro_tickers}, macro_interval={macro_interval_min}min, "
            f"transform_interval={transform_interval_min}min"
        )

    def start(self):
        """
        Start all orchestrator components:
        - WebSocket collector thread
        - Macro data fetcher thread
        - Health monitoring thread
        """
        logger.info("Starting orchestrator...")

        # Start WebSocket collector thread
        self.ws_thread = threading.Thread(target=self._run_ws_collector, daemon=False, name="ws-collector")
        self.ws_thread.start()
        logger.info("Started WebSocket collector thread")

        # Start macro data fetcher thread
        if self.macro_tickers:
            self.macro_thread = threading.Thread(target=self._run_macro_loop, daemon=False, name="macro-fetcher")
            self.macro_thread.start()
            logger.info("Started macro data fetcher thread")
        else:
            logger.warning("No macro tickers configured, macro fetcher disabled")

        # Start transformer thread
        if self.transform_interval_min > 0:
            self.transform_thread = threading.Thread(target=self._run_transform_loop, daemon=False, name="transformer")
            self.transform_thread.start()
            logger.info("Started transformer thread")
        else:
            logger.info("Transformer disabled (transform_interval_min=0)")

        # Start health monitoring thread
        self.health_thread = threading.Thread(target=self._run_health_monitor, daemon=False, name="health-monitor")
        self.health_thread.start()
        logger.info("Started health monitoring thread")

        logger.info("Orchestrator started successfully")

    def stop(self, timeout: float = 10.0):
        """
        Stop all orchestrator components gracefully.

        Args:
            timeout: Maximum seconds to wait for threads to stop
        """
        logger.info("Stopping orchestrator...")

        # Signal all threads to stop
        self.stop_event.set()

        # Wait for threads to join
        threads = [
            ("WebSocket collector", self.ws_thread),
            ("Macro fetcher", self.macro_thread),
            ("Transformer", self.transform_thread),
            ("Health monitor", self.health_thread),
        ]

        for name, thread in threads:
            if thread and thread.is_alive():
                logger.info(f"Waiting for {name} thread to stop...")
                thread.join(timeout=timeout)
                if thread.is_alive():
                    logger.warning(f"{name} thread did not stop within {timeout}s timeout")
                else:
                    logger.info(f"{name} thread stopped")

        # Write final heartbeat
        try:
            with self.health_lock:
                self.health_data["collector"]["status"] = "stopped"
                self.health_data["macro_minute"]["status"] = "stopped"
                self.health_data["transformer"]["status"] = "stopped"
            self._write_health_metrics()
            logger.info("Wrote final heartbeat")
        except Exception as e:
            logger.exception(f"Failed to write final heartbeat: {e}")

        logger.info("Orchestrator stopped")

    def _run_ws_collector(self):
        """
        Run WebSocket collector in a separate thread with its own event loop.
        """
        try:
            with self.health_lock:
                self.health_data["collector"]["status"] = "running"
                self.health_data["collector"]["last_seen_ts"] = datetime.now(timezone.utc).isoformat()

            logger.info("Starting WebSocket collector...")

            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Run collector until stop event is set
            async def run_with_stop_check():
                # Start collector as a task
                collector_task = asyncio.create_task(
                    run_collector(self.config, exchange_name=self.exchange_name, symbols=self.symbols)
                )

                # Poll stop event periodically
                while not self.stop_event.is_set():
                    if collector_task.done():
                        # Collector exited, check for exception
                        try:
                            await collector_task
                        except Exception as e:
                            logger.exception(f"WebSocket collector exited with error: {e}")
                            with self.health_lock:
                                self.health_data["collector"]["status"] = "error"
                        return

                    await asyncio.sleep(1)

                # Stop requested, cancel collector task
                logger.info("Stop requested, cancelling WebSocket collector...")
                collector_task.cancel()
                try:
                    await collector_task
                except asyncio.CancelledError:
                    logger.info("WebSocket collector cancelled")

            loop.run_until_complete(run_with_stop_check())
            loop.close()

        except Exception as e:
            logger.exception(f"WebSocket collector thread failed: {e}")
            with self.health_lock:
                self.health_data["collector"]["status"] = "error"
        finally:
            with self.health_lock:
                self.health_data["collector"]["status"] = "stopped"
            logger.info("WebSocket collector thread exiting")

    def _run_macro_loop(self):
        """
        Run macro data fetcher loop:
        1. One-time backfill on startup (7 days)
        2. Then periodic fetches every N minutes (1 day lookback)
        """
        try:
            logger.info("Starting macro data fetcher loop...")

            # One-time startup backfill
            logger.info(f"Performing startup backfill: {self.macro_lookback_startup_days} days")
            self._fetch_macro_data(lookback_days=self.macro_lookback_startup_days, is_startup=True)

            # Calculate next run time
            next_run = time.time() + (self.macro_interval_min * 60)

            # Periodic fetch loop
            while not self.stop_event.is_set():
                now = time.time()

                if now >= next_run:
                    # Time for next fetch
                    logger.info("Starting scheduled macro data fetch")
                    self._fetch_macro_data(lookback_days=self.macro_runtime_lookback_days, is_startup=False)
                    next_run = time.time() + (self.macro_interval_min * 60)
                    logger.info(f"Next macro fetch scheduled in {self.macro_interval_min} minutes")

                # Sleep for a short interval to check stop event frequently
                time.sleep(10)

        except Exception as e:
            logger.exception(f"Macro fetcher loop failed: {e}")
            with self.health_lock:
                self.health_data["macro_minute"]["status"] = "error"
                self.health_data["macro_minute"]["last_error"] = str(e)
        finally:
            with self.health_lock:
                self.health_data["macro_minute"]["status"] = "stopped"
            logger.info("Macro fetcher thread exiting")

    def _fetch_macro_data(self, lookback_days: int, is_startup: bool):
        """
        Fetch macro data for all configured tickers.

        Args:
            lookback_days: Number of days to look back
            is_startup: True if this is the startup backfill
        """
        start_time = datetime.now(timezone.utc)

        with self.health_lock:
            self.health_data["macro_minute"]["status"] = "running"
            self.health_data["macro_minute"]["last_run_start"] = start_time.isoformat()
            self.health_data["macro_minute"]["last_error"] = None

        total_rows = 0

        try:
            for ticker in self.macro_tickers:
                if self.stop_event.is_set():
                    logger.info("Stop requested, aborting macro fetch")
                    break

                try:
                    logger.info(f"Fetching {ticker} (lookback={lookback_days} days)")

                    # Fetch new data
                    df_new = fetch_yf_1m(ticker, lookback_days=lookback_days)
                    if df_new.empty:
                        logger.warning(f"No new data for {ticker}")
                        continue

                    # Read existing data for deduplication
                    df_existing = _read_existing_data(self.base_path, ticker, lookback_days=lookback_days + 1)

                    # Deduplicate
                    if not df_existing.empty:
                        import pandas as pd

                        # Combine new and existing
                        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                        # Keep latest on collision (based on ts)
                        df_combined = df_combined.sort_values("ts").drop_duplicates(subset=["ts"], keep="last")
                        # Only write the new data (timestamps not in existing)
                        existing_ts = set(df_existing["ts"])
                        df_to_write = df_combined[~df_combined["ts"].isin(existing_ts)]

                        if df_to_write.empty:
                            logger.info(f"No new unique data for {ticker} after deduplication")
                            continue

                        logger.info(f"After dedup: {len(df_to_write)} new rows for {ticker}")
                    else:
                        df_to_write = df_new

                    # Write to Parquet
                    write_parquet(df_to_write, self.base_path, compression=self.compression)
                    total_rows += len(df_to_write)

                except Exception as e:
                    logger.exception(f"Failed to fetch {ticker}: {e}")
                    with self.health_lock:
                        self.health_data["macro_minute"]["last_error"] = f"{ticker}: {str(e)}"

        except Exception as e:
            logger.exception(f"Macro fetch failed: {e}")
            with self.health_lock:
                self.health_data["macro_minute"]["last_error"] = str(e)
        finally:
            end_time = datetime.now(timezone.utc)
            with self.health_lock:
                self.health_data["macro_minute"]["status"] = "idle"
                self.health_data["macro_minute"]["last_run_end"] = end_time.isoformat()
                self.health_data["macro_minute"]["last_run_rows_written"] = total_rows

            duration = (end_time - start_time).total_seconds()
            logger.info(f"Macro fetch completed: {total_rows} rows written in {duration:.1f}s")

    def _run_transform_loop(self):
        """
        Run transformer loop: process raw data to Parquet every N minutes.
        """
        try:
            logger.info("Starting transformer loop...")

            # Wait a bit before first transform to allow some data to accumulate
            logger.info(f"Waiting {self.transform_interval_min} minutes before first transform")
            for _ in range(self.transform_interval_min * 60):
                if self.stop_event.is_set():
                    return
                time.sleep(1)

            # Periodic transform loop
            while not self.stop_event.is_set():
                logger.info("Starting scheduled transformer run")
                self._run_transformer()

                # Calculate next run time
                logger.info(f"Next transformer run scheduled in {self.transform_interval_min} minutes")
                for _ in range(self.transform_interval_min * 60):
                    if self.stop_event.is_set():
                        return
                    time.sleep(1)

        except Exception as e:
            logger.exception(f"Transformer loop failed: {e}")
            with self.health_lock:
                self.health_data["transformer"]["status"] = "error"
                self.health_data["transformer"]["last_error"] = str(e)
        finally:
            with self.health_lock:
                self.health_data["transformer"]["status"] = "stopped"
            logger.info("Transformer thread exiting")

    def _run_transformer(self):
        """
        Run transformer for today's date.
        """
        start_time = datetime.now(timezone.utc)
        today = start_time.strftime("%Y-%m-%d")

        with self.health_lock:
            self.health_data["transformer"]["status"] = "running"
            self.health_data["transformer"]["last_run_start"] = start_time.isoformat()
            self.health_data["transformer"]["last_error"] = None

        try:
            logger.info(f"Running transformer for date: {today}")
            run_transformer(
                config=self.config,
                exchange_name=self.exchange_name,
                date=today,
                symbols=self.symbols,
            )
            logger.info(f"Transformer completed for {today}")

        except Exception as e:
            logger.exception(f"Transformer failed: {e}")
            with self.health_lock:
                self.health_data["transformer"]["last_error"] = str(e)
        finally:
            end_time = datetime.now(timezone.utc)
            with self.health_lock:
                self.health_data["transformer"]["status"] = "idle"
                self.health_data["transformer"]["last_run_end"] = end_time.isoformat()

            duration = (end_time - start_time).total_seconds()
            logger.info(f"Transformer run completed in {duration:.1f}s")

    def _run_health_monitor(self):
        """
        Health monitoring loop: writes metrics every 60 seconds.
        """
        try:
            logger.info("Starting health monitoring loop...")

            while not self.stop_event.is_set():
                try:
                    self._write_health_metrics()
                except Exception as e:
                    logger.exception(f"Failed to write health metrics: {e}")

                # Sleep for 60 seconds, checking stop event periodically
                for _ in range(60):
                    if self.stop_event.is_set():
                        break
                    time.sleep(1)

        except Exception as e:
            logger.exception(f"Health monitoring loop failed: {e}")
        finally:
            logger.info("Health monitoring thread exiting")

    def _write_health_metrics(self):
        """
        Collect and write health metrics to JSON and Markdown files.
        """
        now = datetime.now(timezone.utc)

        # Collect file statistics
        try:
            file_stats = summarize_files(self.base_path, now.strftime("%Y-%m-%d"))
        except Exception as e:
            logger.warning(f"Failed to collect file statistics: {e}")
            file_stats = {"raw_count_today": 0, "parquet_1s_rows_today": 0, "macro_min_rows_today": 0}

        # Build health payload
        with self.health_lock:
            payload = {
                "ts_utc": now.isoformat(),
                "collector": self.health_data["collector"].copy(),
                "macro_minute": self.health_data["macro_minute"].copy(),
                "files": file_stats,
            }

        # Write to JSON and Markdown
        json_path = f"{self.base_path}/logs/health/heartbeat.json"
        md_path = f"{self.base_path}/reports/health.md"

        write_heartbeat(json_path, md_path, payload)
