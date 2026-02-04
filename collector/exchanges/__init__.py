"""Exchange adapter factory."""

from typing import List

from collector.exchanges.base import ExchangeAdapter
from collector.exchanges.binance import BinanceAdapter
from collector.exchanges.coinbase import CoinbaseAdapter
from collector.exchanges.kraken import KrakenAdapter


def get_adapter(exchange_name: str, wss_url: str, symbols: List[str]) -> ExchangeAdapter:
    """Create the appropriate exchange adapter."""
    adapters = {
        "binance": BinanceAdapter,
        "coinbase": CoinbaseAdapter,
        "kraken": KrakenAdapter,
    }
    cls = adapters.get(exchange_name.lower())
    if cls is None:
        raise ValueError(f"Unsupported exchange: {exchange_name}. Supported: {list(adapters.keys())}")
    return cls(wss_url, symbols)
