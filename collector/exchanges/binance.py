"""Binance WebSocket adapter."""

import time
from typing import Any, Dict, List, Optional

from loguru import logger

from collector.exchanges.base import ExchangeAdapter


class BinanceAdapter(ExchangeAdapter):
    """Adapter for Binance combined WebSocket streams (trade + bookTicker)."""

    def __init__(self, wss_url: str, symbols: List[str]):
        super().__init__(wss_url, symbols, "binance")

    def build_ws_url(self) -> str:
        base = self.wss_url.replace("/ws", "/stream?streams=").rstrip("/")
        topics = []
        for s in self.symbols:
            ls = s.lower()
            topics.append(f"{ls}@trade")
            topics.append(f"{ls}@bookTicker")
        return base + "/".join(topics)

    def build_subscribe_message(self) -> Optional[Dict[str, Any]]:
        return None  # Binance uses URL-based subscription

    def parse_event(self, raw_msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            stream = raw_msg.get("stream")
            data = raw_msg.get("data", raw_msg)

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
                b = data.get("b")
                a = data.get("a")
                if b is not None:
                    bid = float(b)
                if a is not None:
                    ask = float(a)

            return {
                "exchange": self.exchange_name,
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
        except Exception as e:
            logger.exception(f"Binance parse error: {e}")
            return None
