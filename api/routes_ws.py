"""WebSocket endpoint for streaming real-time trade and bookTicker events."""

import asyncio
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from loguru import logger

from api.auth import validate_ws_token
from api.event_bus import EventBus

router = APIRouter()


@router.websocket("/api/v1/ws/stream")
async def stream(
    ws: WebSocket,
    symbols: str = "*",
    types: str = "all",
    token: Optional[str] = None,
):
    """
    Stream real-time events to WebSocket clients.

    Query params:
        symbols: Comma-separated symbol list or '*' for all (default: '*')
        types: Comma-separated types: 'trade', 'book', or 'all' (default: 'all')
        token: JWT or API key for authentication

    Each message is a JSON object with the same schema as the raw collector events:
        {symbol, ts_event, ts_recv, price, qty, side, bid, ask, stream, trade_id}
    """
    # Authenticate before accepting
    db_path = ws.app.state.db_path
    secret_key = ws.app.state.jwt_secret

    # Also check cookie
    if not token:
        token = ws.cookies.get("access_token")

    user = validate_ws_token(db_path, secret_key, token)
    if not user:
        await ws.close(code=4001, reason="Authentication required")
        return

    await ws.accept()

    bus: EventBus = ws.app.state.event_bus
    if bus is None:
        await ws.send_json({"error": "Event bus not available"})
        await ws.close()
        return

    # Determine channels to subscribe to
    channels = []
    if symbols == "*" or types == "all":
        channels = ["all"]
    else:
        symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        type_list = [t.strip() for t in types.split(",") if t.strip()]
        for sym in symbol_list:
            for typ in type_list:
                channels.append(f"{typ}:{sym}")

    # Subscribe to all channels, merging into a single queue
    # We subscribe to each channel but merge events into one output stream
    subscriptions = [(ch, bus.subscribe(ch)) for ch in channels]

    client_info = f"symbols={symbols}, types={types}"
    logger.info(f"WebSocket client connected: {client_info}")

    try:
        while True:
            # Check all subscription queues for events
            sent = False
            for _ch, q in subscriptions:
                try:
                    event = q.get_nowait()
                    await ws.send_json(event)
                    sent = True
                except asyncio.QueueEmpty:
                    pass

            if not sent:
                # No events available, brief sleep to avoid busy loop
                await asyncio.sleep(0.01)

    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected: {client_info}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        # Clean up all subscriptions
        for ch, q in subscriptions:
            bus.unsubscribe(ch, q)
