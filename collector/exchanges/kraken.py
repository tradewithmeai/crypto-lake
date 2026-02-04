"""Kraken v2 WebSocket adapter."""

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from loguru import logger

from collector.exchanges.base import ExchangeAdapter


class KrakenAdapter(ExchangeAdapter):
    """Adapter for Kraken WebSocket v2 API (trade + ticker channels)."""

    def __init__(self, wss_url: str, symbols: List[str]):
        super().__init__(wss_url, symbols, "kraken")

    def build_ws_url(self) -> str:
        return self.wss_url

    def build_subscribe_message(self) -> Optional[Dict[str, Any]]:
        # Kraken v2 requires separate subscriptions per channel
        # We return the trade subscription; ticker is sent separately in a list
        return [
            {
                "method": "subscribe",
                "params": {
                    "channel": "trade",
                    "symbol": list(self.symbols),
                },
            },
            {
                "method": "subscribe",
                "params": {
                    "channel": "ticker",
                    "symbol": list(self.symbols),
                },
            },
        ]

    def parse_event(self, raw_msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            # Kraken v2 sends different message types
            channel = raw_msg.get("channel", "")
            msg_type = raw_msg.get("type", "")

            # Skip system/subscription messages
            if channel in ("status", "heartbeat") or msg_type in ("subscribe", "unsubscribe"):
                return None

            # Subscription acknowledgement
            if msg_type == "subscriptionStatus":
                return None

            ts_recv = int(time.time() * 1000)
            data_list = raw_msg.get("data", [])

            if not data_list:
                return None

            if channel == "trade":
                # Kraken v2 trade: data is a list of trade objects
                # Each: {symbol, price, qty, side, timestamp, ...}
                # We return one record per trade (take the last/most recent)
                for trade in data_list:
                    symbol = trade.get("symbol", "")
                    ts_str = trade.get("timestamp", "")
                    try:
                        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        ts_event = int(dt.timestamp() * 1000)
                    except Exception:
                        ts_event = ts_recv

                    price_val = trade.get("price")
                    qty_val = trade.get("qty")

                    return {
                        "exchange": self.exchange_name,
                        "symbol": symbol,
                        "ts_event": ts_event,
                        "ts_recv": ts_recv,
                        "price": float(price_val) if price_val is not None else None,
                        "qty": float(qty_val) if qty_val is not None else None,
                        "side": trade.get("side", "unknown"),
                        "bid": None,
                        "ask": None,
                        "stream": "trade",
                        "trade_id": trade.get("trade_id"),
                    }

            if channel == "ticker":
                # Kraken v2 ticker: data is a list with one ticker object
                for tick in data_list:
                    symbol = tick.get("symbol", "")
                    best_bid = tick.get("bid")
                    best_ask = tick.get("ask")

                    return {
                        "exchange": self.exchange_name,
                        "symbol": symbol,
                        "ts_event": ts_recv,
                        "ts_recv": ts_recv,
                        "price": None,
                        "qty": None,
                        "side": None,
                        "bid": float(best_bid) if best_bid is not None else None,
                        "ask": float(best_ask) if best_ask is not None else None,
                        "stream": "bookTicker",
                        "trade_id": None,
                    }

            return None
        except Exception as e:
            logger.exception(f"Kraken parse error: {e}")
            return None
