"""Coinbase WebSocket adapter."""

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from loguru import logger

from collector.exchanges.base import ExchangeAdapter


def _parse_iso_to_epoch_ms(iso_str: str) -> int:
    """Parse ISO 8601 timestamp to epoch milliseconds."""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(time.time() * 1000)


class CoinbaseAdapter(ExchangeAdapter):
    """Adapter for Coinbase Advanced Trade WebSocket feed."""

    def __init__(self, wss_url: str, symbols: List[str]):
        super().__init__(wss_url, symbols, "coinbase")

    def build_ws_url(self) -> str:
        return self.wss_url

    def build_subscribe_message(self) -> Optional[Dict[str, Any]]:
        return {
            "type": "subscribe",
            "product_ids": list(self.symbols),
            "channels": ["ticker", "matches"],
        }

    def parse_event(self, raw_msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            msg_type = raw_msg.get("type", "")
            ts_recv = int(time.time() * 1000)

            if msg_type in ("subscriptions", "heartbeat", "error"):
                return None

            if msg_type in ("match", "last_match"):
                product = raw_msg.get("product_id", "")
                ts_str = raw_msg.get("time", "")
                ts_event = _parse_iso_to_epoch_ms(ts_str) if ts_str else ts_recv

                price_str = raw_msg.get("price")
                size_str = raw_msg.get("size")

                return {
                    "exchange": self.exchange_name,
                    "symbol": product,
                    "ts_event": ts_event,
                    "ts_recv": ts_recv,
                    "price": float(price_str) if price_str else None,
                    "qty": float(size_str) if size_str else None,
                    "side": raw_msg.get("side", "unknown"),
                    "bid": None,
                    "ask": None,
                    "stream": "trade",
                    "trade_id": raw_msg.get("trade_id"),
                }

            if msg_type == "ticker":
                product = raw_msg.get("product_id", "")
                ts_str = raw_msg.get("time", "")
                ts_event = _parse_iso_to_epoch_ms(ts_str) if ts_str else ts_recv

                best_bid = raw_msg.get("best_bid")
                best_ask = raw_msg.get("best_ask")

                return {
                    "exchange": self.exchange_name,
                    "symbol": product,
                    "ts_event": ts_event,
                    "ts_recv": ts_recv,
                    "price": None,
                    "qty": None,
                    "side": None,
                    "bid": float(best_bid) if best_bid else None,
                    "ask": float(best_ask) if best_ask else None,
                    "stream": "bookTicker",
                    "trade_id": None,
                }

            return None
        except Exception as e:
            logger.exception(f"Coinbase parse error: {e}")
            return None
