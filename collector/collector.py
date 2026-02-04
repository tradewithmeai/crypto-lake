import asyncio
import json
import os
import random
import signal
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import websockets
from loguru import logger

# Initialise quiet-by-default logging for production/containerised environments
from tools.logging_setup import setup_logging
setup_logging()

from tools.common import (
    ensure_dir,
    ensure_parent_dir,
    get_exchange_config,
    get_local_date_str_utc,
    get_raw_base_dir,
)


class NetworkTimeoutWarning(UserWarning):
    """
    Custom warning for network timeout and connection issues.

    Emitted when the collector encounters transient network failures
    and enters exponential backoff retry logic. This provides a searchable
    token for monitoring and alerting in production deployments.
    """
    pass

@dataclass
class RotatingJSONLWriter:
    base_dir: str
    symbol: str
    interval_sec: int = 60
    current_date: str = field(default_factory=get_local_date_str_utc)
    part_index: int = 0
    next_rotation_epoch: float = field(default_factory=lambda: 0.0)
    fp: Optional[Any] = None

    def _resolve_dir(self, date_str: str) -> str:
        dirpath = os.path.join(self.base_dir, self.symbol, date_str)
        ensure_dir(dirpath)
        return dirpath

    def _next_part_index(self, dirpath: str) -> int:
        # Determine next part number by scanning directory once per day rollover
        try:
            files = [f for f in os.listdir(dirpath) if f.lower().endswith(".jsonl")]
            max_part = 0
            for f in files:
                # Expect pattern part_XXX.jsonl
                name = os.path.splitext(f)[0]
                if name.startswith("part_"):
                    try:
                        idx = int(name.split("_")[1])
                        if idx > max_part:
                            max_part = idx
                    except Exception:
                        continue
            return max_part + 1
        except FileNotFoundError:
            return 1

    def _open_new_file(self, now_epoch: float) -> None:
        date_str = get_local_date_str_utc(epoch=now_epoch)
        if date_str != self.current_date:
            self.current_date = date_str
            self.part_index = 0  # will be incremented to 1 below

        dirpath = self._resolve_dir(self.current_date)
        if self.part_index == 0:
            self.part_index = self._next_part_index(dirpath)

        filename = f"part_{self.part_index:03d}.jsonl"
        path = os.path.join(dirpath, filename)
        ensure_parent_dir(path)
        if self.fp:
            try:
                self.fp.flush()
                self.fp.close()
            except Exception:
                pass
        self.fp = open(path, "a", encoding="utf-8")
        # Set next rotation boundary
        window = int(now_epoch // self.interval_sec) * self.interval_sec
        self.next_rotation_epoch = float(window + self.interval_sec)
        logger.info(f"[Writer] Opened {path}; next rotation at {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(self.next_rotation_epoch))} UTC")

    def _rotate_if_needed(self, now_epoch: float) -> None:
        if self.fp is None:
            self._open_new_file(now_epoch)
            return
        if now_epoch >= self.next_rotation_epoch:
            self.part_index += 1
            self._open_new_file(now_epoch)

    def write_obj(self, obj: Dict[str, Any], now_epoch: Optional[float] = None) -> None:
        t = time.time() if now_epoch is None else now_epoch
        self._rotate_if_needed(t)
        if self.fp:
            line = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
            self.fp.write(line + "\n")

    def close(self) -> None:
        if self.fp:
            try:
                self.fp.flush()
                self.fp.close()
            except Exception:
                pass
            self.fp = None

def parse_event(message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse Binance combined-stream or ws-subscription message into a normalized record.
    Returns a dict with keys: symbol, ts_event, ts_recv, price, qty, side, bid, ask, stream.
    """
    try:
        # Combined stream payload includes {"stream": "...", "data": {...}}
        stream = message.get("stream")
        data = message.get("data", message)

        # Derive type
        if stream:
            if "@trade" in stream:
                typ = "trade"
            elif "@bookTicker" in stream:
                typ = "bookTicker"
            else:
                typ = data.get("e", "unknown")
        else:
            etype = str(data.get("e", "")).lower()
            if "trade" in etype:
                typ = "trade"
            elif "bookticker" in etype:
                typ = "bookTicker"
            else:
                typ = etype or "unknown"

        symbol = str(data.get("s", "")).upper()
        ts_event = int(data.get("E") or data.get("T") or int(time.time() * 1000))
        ts_recv = int(time.time() * 1000)

        price = None
        qty = None
        side = None
        bid = None
        ask = None
        trade_id = None

        if typ == "trade":
            # Trade payload: p (price), q (qty), m (is buyer maker), t (trade ID)
            p = data.get("p")
            q = data.get("q")
            t = data.get("t")
            if p is not None:
                price = float(p)
            if q is not None:
                qty = float(q)
            if t is not None:
                trade_id = int(t)
            mflag = data.get("m")
            side = "sell" if bool(mflag) else "buy"
        elif typ == "bookTicker":
            # Best bid/ask payload: b (bid price), a (ask price)
            b = data.get("b")
            a = data.get("a")
            if b is not None:
                bid = float(b)
            if a is not None:
                ask = float(a)

        record = {
            "symbol": symbol,
            "ts_event": ts_event,
            "ts_recv": ts_recv,
            "price": price,
            "qty": qty,
            "side": side,
            "bid": bid,
            "ask": ask,
            "stream": typ,
            "trade_id": trade_id,
        }
        return record
    except Exception as e:
        logger.exception(f"Failed to parse event message: {e}")
        return None

def build_combined_stream_url(wss_url: str, symbols: list[str]) -> str:
    """
    Build Binance combined stream URL for trade and bookTicker per symbol.
    Example: wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@bookTicker
    """
    # Ensure base combined endpoint
    base = wss_url.replace("/ws", "/stream?streams=").rstrip("/")
    topics: list[str] = []
    for s in symbols:
        ls = s.lower()
        topics.append(f"{ls}@trade")
        topics.append(f"{ls}@bookTicker")
    return base + "/".join(topics)

async def _consume_ws(adapter, writers: Dict[str, RotatingJSONLWriter], stop_event: asyncio.Event, event_bus=None) -> None:
    url = adapter.build_ws_url()
    async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=10, max_queue=2000) as ws:
        logger.info(f"Connected to {url}")

        # Send subscription message if the exchange requires it
        sub_msg = adapter.build_subscribe_message()
        if sub_msg is not None:
            if isinstance(sub_msg, list):
                for msg in sub_msg:
                    await ws.send(json.dumps(msg))
                    logger.info(f"Sent subscription: {msg.get('params', {}).get('channel', 'unknown')}")
            else:
                await ws.send(json.dumps(sub_msg))
                logger.info(f"Sent subscription message to {adapter.exchange_name}")

        # Latency tracking for operational visibility
        latency_window = deque(maxlen=1000)
        last_summary_time = time.time()
        summary_interval = 60

        while not stop_event.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=60)
            except asyncio.TimeoutError:
                continue
            except websockets.ConnectionClosed:
                logger.warning(f"WebSocket connection closed by {adapter.exchange_name} server.")
                raise
            except Exception as e:
                logger.error(f"WebSocket receive error: {e}")
                raise

            try:
                msg = json.loads(raw)
            except Exception:
                logger.warning("Received non-JSON message; skipping.")
                continue

            rec = adapter.parse_event(msg)
            if not rec or not rec.get("symbol"):
                continue
            sym = rec["symbol"]
            writer = writers.get(sym)
            if writer:
                writer.write_obj(rec)

            # Publish to event bus for API WebSocket clients
            if event_bus is not None:
                event_bus.publish(f"{rec.get('stream', 'unknown')}:{sym}", rec)
                event_bus.publish("all", rec)

            # Track latency with rolling statistics
            try:
                latency_ms = rec["ts_recv"] - rec["ts_event"]
                latency_window.append(latency_ms)

                now = time.time()
                if now - last_summary_time >= summary_interval and len(latency_window) > 0:
                    sorted_latencies = sorted(latency_window)
                    p50 = sorted_latencies[len(sorted_latencies) // 2]
                    p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)]
                    max_lat = sorted_latencies[-1]

                    logger.info(f"[{adapter.exchange_name}] Latency (last {len(latency_window)} msgs): p50={p50}ms, p95={p95}ms, max={max_lat}ms")

                    if p95 > 2000 or max_lat > 5000:
                        logger.warning(f"[{adapter.exchange_name}] High latency: p95={p95}ms, max={max_lat}ms")

                    last_summary_time = now
            except Exception:
                pass

async def run_collector(config: Dict[str, Any], exchange_name: str = "binance", symbols: Optional[list[str]] = None, event_bus=None) -> None:
    """
    Run the streaming collector with auto-reconnect and graceful shutdown (Ctrl+C).

    Features:
    - Exponential backoff with jitter on connection failures
    - Configurable max backoff (default 5 minutes)
    - Handles network errors, timeouts, and unexpected disconnections
    - Tracks connection state and gap duration for monitoring
    - Graceful shutdown on SIGINT/SIGTERM
    """
    setup_logging()
    from collector.exchanges import get_adapter

    ex_conf = get_exchange_config(config, exchange_name)

    if symbols is None or not symbols:
        symbols = ex_conf.get("symbols", [])
    logger.info(f"Starting collector for exchange={exchange_name} symbols={symbols}")

    # Create exchange adapter
    adapter = get_adapter(exchange_name, ex_conf["wss_url"], symbols)

    raw_root = get_raw_base_dir(config, exchange_name)
    writers: Dict[str, RotatingJSONLWriter] = {}
    for s in adapter.get_writer_symbols():
        writers[s] = RotatingJSONLWriter(base_dir=raw_root, symbol=s, interval_sec=int(config["collector"]["write_interval_sec"]))

    stop_event = asyncio.Event()

    def _graceful_shutdown(*_args):
        logger.warning("Shutdown signal received. Stopping collector...")
        stop_event.set()

    # Register signal handlers (may be limited on Windows/asyncio or when running in threads)
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _graceful_shutdown)
    except (NotImplementedError, ValueError, RuntimeError):
        # Windows may not support add_signal_handler for SIGINT in Proactor
        # ValueError/RuntimeError raised when not in main thread (orchestrator runs collector in thread)
        pass

    # Configurable backoff settings
    initial_backoff = int(config["collector"].get("reconnect_backoff", 10)) or 5
    max_backoff = int(config["collector"].get("max_reconnect_backoff", 300)) or 300
    jitter_factor = float(config["collector"].get("reconnect_jitter", 0.5))
    backoff = initial_backoff

    # Connection state tracking
    last_connected_time: Optional[float] = None
    disconnect_time: Optional[float] = None
    reconnect_count = 0

    try:
        while not stop_event.is_set():
            try:
                # Log reconnection with gap duration if this is a reconnect
                if disconnect_time is not None:
                    gap_seconds = time.time() - disconnect_time
                    reconnect_count += 1
                    logger.info(f"Reconnecting to WebSocket (attempt #{reconnect_count}, gap: {gap_seconds:.1f}s)")

                last_connected_time = time.time()
                await _consume_ws(adapter, writers, stop_event, event_bus=event_bus)

                # If consume returns without exception and no stop requested, reconnect
                if not stop_event.is_set():
                    disconnect_time = time.time()
                    logger.warning("WebSocket consume ended unexpectedly; will reconnect.")
                    logger.warning("NetworkTimeoutWarning")
                    jitter = random.uniform(0, jitter_factor)
                    await asyncio.sleep(backoff + jitter)
                    backoff = min(max_backoff, backoff * 2)

            except (asyncio.TimeoutError, websockets.ConnectionClosed, ConnectionError, OSError) as e:
                # Transient network failures - includes OSError for "Network unreachable" etc.
                disconnect_time = time.time() if disconnect_time is None else disconnect_time
                logger.warning(f"Network timeout/connection issue: {type(e).__name__}: {e}")
                logger.warning("NetworkTimeoutWarning")
                # Exponential backoff with jitter to prevent thundering herd
                jitter = random.uniform(0, jitter_factor)
                sleep_time = backoff + jitter
                logger.info(f"Retrying in {sleep_time:.1f}s (backoff: {backoff}s, max: {max_backoff}s)")
                await asyncio.sleep(sleep_time)
                backoff = min(max_backoff, backoff * 2)

            except Exception as e:
                # Unexpected errors - log full details and reconnect
                disconnect_time = time.time() if disconnect_time is None else disconnect_time
                logger.error(f"Collector error: {type(e).__name__}: {e}")
                logger.info(f"Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, max(5, backoff * 2))

            else:
                # Successful connection established; reset backoff
                if last_connected_time:
                    logger.info(f"Connection stable, resetting backoff to {initial_backoff}s")
                backoff = initial_backoff
                disconnect_time = None

    finally:
        for w in writers.values():
            w.close()
        logger.info(f"Collector stopped cleanly. Total reconnects during session: {reconnect_count}")
