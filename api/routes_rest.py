"""REST API endpoints for querying historical bar and macro data via DuckDB."""

import threading
from datetime import datetime
from typing import List, Optional

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from loguru import logger

from api.auth import get_current_user

from tools.db import connect_and_register_views

router = APIRouter(prefix="/api/v1")

# DuckDB connection management (thread-safe via lock)
_db_lock = threading.Lock()
_db_conn: Optional[duckdb.DuckDBPyConnection] = None


def _get_db(config: dict) -> duckdb.DuckDBPyConnection:
    """Get or create the shared DuckDB connection with views registered."""
    global _db_conn
    with _db_lock:
        if _db_conn is None:
            base_path = config["general"]["base_path"]
            _db_conn = connect_and_register_views(base_path, config=config)
        return _db_conn


def _refresh_db(config: dict) -> duckdb.DuckDBPyConnection:
    """Force-refresh the DuckDB connection (picks up new parquet files)."""
    global _db_conn
    with _db_lock:
        if _db_conn is not None:
            try:
                _db_conn.close()
            except Exception:
                pass
        _db_conn = None
    return _get_db(config)


# Valid timeframes mapped to DuckDB view names
_TF_VIEWS = {
    "1s": "bars_1s",
    "1m": "bars_1m",
    "5m": "bars_5m",
    "15m": "bars_15m",
    "1h": "bars_1h",
}


@router.get("/symbols")
async def get_symbols(request: Request, user: dict = Depends(get_current_user)):
    """List all available crypto symbols grouped by exchange."""
    config = request.app.state.config
    exchanges = {}
    for ex in config.get("exchanges", []):
        exchanges[ex["name"]] = sorted(ex.get("symbols", []))
    return {"exchanges": exchanges}


@router.get("/bars/{symbol}")
async def get_bars(
    request: Request,
    symbol: str,
    tf: str = Query("1m", pattern="^(1s|1m|5m|15m|1h)$"),
    start: Optional[str] = Query(None, description="Start timestamp ISO format"),
    end: Optional[str] = Query(None, description="End timestamp ISO format"),
    limit: int = Query(1000, ge=1, le=50000),
    user: dict = Depends(get_current_user),
):
    """Query historical OHLCV bars for a symbol."""
    config = request.app.state.config
    view = _TF_VIEWS[tf]

    try:
        conn = _get_db(config)

        sql = f"SELECT * FROM {view} WHERE symbol = ?"
        params: list = [symbol.upper()]

        if start:
            sql += " AND ts >= CAST(? AS TIMESTAMP)"
            params.append(start)
        if end:
            sql += " AND ts < CAST(? AS TIMESTAMP)"
            params.append(end)

        sql += f" ORDER BY ts DESC LIMIT {limit}"

        result = conn.execute(sql, params).fetchdf()
        records = result.to_dict(orient="records")

        # Convert timestamps to ISO strings
        for rec in records:
            for k, v in rec.items():
                if hasattr(v, "isoformat"):
                    rec[k] = v.isoformat()

        return {"symbol": symbol.upper(), "timeframe": tf, "count": len(records), "data": records}

    except Exception as e:
        logger.error(f"Error querying bars: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/bars/{symbol}/latest")
async def get_latest_bars(
    request: Request,
    symbol: str,
    tf: str = Query("1m", pattern="^(1s|1m|5m|15m|1h)$"),
    limit: int = Query(60, ge=1, le=10000),
    user: dict = Depends(get_current_user),
):
    """Get the latest N bars for a symbol."""
    config = request.app.state.config
    view = _TF_VIEWS[tf]

    try:
        conn = _get_db(config)

        sql = f"SELECT * FROM {view} WHERE symbol = ? ORDER BY ts DESC LIMIT {limit}"
        result = conn.execute(sql, [symbol.upper()]).fetchdf()
        records = result.to_dict(orient="records")

        for rec in records:
            for k, v in rec.items():
                if hasattr(v, "isoformat"):
                    rec[k] = v.isoformat()

        return {"symbol": symbol.upper(), "timeframe": tf, "count": len(records), "data": records}

    except Exception as e:
        logger.error(f"Error querying latest bars: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/macro")
async def get_macro_tickers(request: Request, user: dict = Depends(get_current_user)):
    """List available macro tickers from config."""
    config = request.app.state.config
    tickers = config.get("macro_minute", {}).get("tickers", [])
    return {"tickers": sorted(tickers)}


@router.get("/macro/{ticker}")
async def get_macro_bars(
    request: Request,
    ticker: str,
    start: Optional[str] = Query(None, description="Start timestamp ISO format"),
    end: Optional[str] = Query(None, description="End timestamp ISO format"),
    limit: int = Query(1000, ge=1, le=50000),
    user: dict = Depends(get_current_user),
):
    """Query historical macro minute bars for a ticker."""
    config = request.app.state.config

    try:
        conn = _get_db(config)

        sql = "SELECT * FROM macro_minute WHERE ticker = ?"
        params: list = [ticker.upper()]

        if start:
            sql += " AND ts >= CAST(? AS TIMESTAMP)"
            params.append(start)
        if end:
            sql += " AND ts < CAST(? AS TIMESTAMP)"
            params.append(end)

        sql += f" ORDER BY ts DESC LIMIT {limit}"

        result = conn.execute(sql, params).fetchdf()
        records = result.to_dict(orient="records")

        for rec in records:
            for k, v in rec.items():
                if hasattr(v, "isoformat"):
                    rec[k] = v.isoformat()

        return {"ticker": ticker.upper(), "count": len(records), "data": records}

    except Exception as e:
        logger.error(f"Error querying macro bars: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def get_health(request: Request, user: dict = Depends(get_current_user)):
    """Get system health status."""
    health_data = getattr(request.app.state, "health_data", {})
    event_bus = getattr(request.app.state, "event_bus", None)

    api_stats = {}
    if event_bus:
        api_stats = event_bus.stats

    return {
        "status": "running",
        "collector": health_data.get("collector", {}),
        "macro_minute": health_data.get("macro_minute", {}),
        "transformer": health_data.get("transformer", {}),
        "api": api_stats,
    }


@router.post("/refresh")
async def refresh_db(request: Request, user: dict = Depends(get_current_user)):
    """Force-refresh the DuckDB connection to pick up new parquet files."""
    config = request.app.state.config
    _refresh_db(config)
    return {"status": "refreshed"}
