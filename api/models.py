"""Pydantic models for API request/response schemas."""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class BarRecord(BaseModel):
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume_base: float
    volume_quote: float
    trade_count: int
    vwap: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    spread: Optional[float] = None


class MacroRecord(BaseModel):
    ticker: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class HealthResponse(BaseModel):
    status: str
    collector: dict
    macro_minute: dict
    transformer: dict
    api: dict


class SymbolsResponse(BaseModel):
    exchange: str
    symbols: List[str]


class MacroTickersResponse(BaseModel):
    tickers: List[str]
