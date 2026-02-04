"""Abstract base class for exchange WebSocket adapters."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class ExchangeAdapter(ABC):
    """
    Base class for exchange-specific WebSocket adapters.

    Each adapter handles:
    - WebSocket URL construction
    - Subscription message (if needed after connect)
    - Raw message parsing into normalized records
    """

    def __init__(self, wss_url: str, symbols: List[str], exchange_name: str):
        self.wss_url = wss_url
        self.symbols = symbols
        self.exchange_name = exchange_name

    @abstractmethod
    def build_ws_url(self) -> str:
        """Return the WebSocket URL to connect to."""
        ...

    @abstractmethod
    def build_subscribe_message(self) -> Optional[Dict[str, Any]]:
        """Return JSON to send after connect, or None if subscription is URL-based."""
        ...

    @abstractmethod
    def parse_event(self, raw_msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse exchange-specific message into normalized record.

        Must return dict with keys:
            exchange, symbol, ts_event, ts_recv, price, qty, side,
            bid, ask, stream, trade_id

        Returns None if message should be skipped (e.g. heartbeat, subscription ack).
        """
        ...

    def get_writer_symbols(self) -> List[str]:
        """Return symbols as they should appear in file paths and writer keys."""
        return list(self.symbols)
