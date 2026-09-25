"""Subscribe to live ticks from the Deriv WebSocket API.

Protocol: send {"ticks": "<symbol>", "subscribe": 1} per symbol, then receive
{"msg_type": "tick", "tick": {...}} messages. See https://developers.deriv.com.

Deriv's tick.id is the *subscription* id: it is the same on every tick of a
stream. Ticks are therefore keyed on (symbol, epoch) instead.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import websockets

log = logging.getLogger(__name__)

TickHandler = Callable[[dict[str, Any]], Awaitable[None] | None]


class DerivAPIError(RuntimeError):
    pass


def parse_tick(message: dict[str, Any]) -> dict[str, Any] | None:
    """Turn a Deriv tick message into a raw_ticks record (without ingested_at).

    Returns None for non-tick messages. Raises DerivAPIError on API errors, so a
    bad symbol or a changed API is visible instead of producing silence.
    """
    if "error" in message:
        error = message["error"]
        raise DerivAPIError(f"{error.get('code')}: {error.get('message')}")
    if message.get("msg_type") != "tick":
        return None
    tick = message["tick"]
    return {
        "tick_id": f"{tick['symbol']}:{tick['epoch']}",
        "subscription_id": str(tick["id"]),
        "symbol": tick["symbol"],
        "quote": float(tick["quote"]),
        "bid": float(tick["bid"]) if tick.get("bid") is not None else None,
        "ask": float(tick["ask"]) if tick.get("ask") is not None else None,
        "pip_size": int(tick["pip_size"]),
        "epoch": int(tick["epoch"]),
        "quoted_at": datetime.fromtimestamp(tick["epoch"], UTC).isoformat(),
    }


async def stream_ticks(
    ws_url: str,
    symbols: tuple[str, ...],
    on_tick: TickHandler,
    duration_s: float | None = None,
    max_backoff_s: float = 60.0,
) -> None:
    """Stream ticks for `symbols` forever (or for `duration_s`), reconnecting with backoff."""
    deadline = time.monotonic() + duration_s if duration_s else None
    backoff = 1.0

    while deadline is None or time.monotonic() < deadline:
        try:
            async with websockets.connect(ws_url, ping_interval=20) as ws:
                for symbol in symbols:
                    await ws.send(json.dumps({"ticks": symbol, "subscribe": 1}))
                log.info("Subscribed to %s", ", ".join(symbols))
                backoff = 1.0

                while deadline is None or time.monotonic() < deadline:
                    timeout = None if deadline is None else max(deadline - time.monotonic(), 0)
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except TimeoutError:
                        return
                    record = parse_tick(json.loads(raw))
                    if record is not None:
                        result = on_tick(record)
                        if asyncio.iscoroutine(result):
                            await result
        except DerivAPIError:
            raise
        except (
            OSError,
            TimeoutError,
            websockets.InvalidHandshake,
            websockets.ConnectionClosed,
        ) as exc:
            log.warning("Deriv connection lost (%s); reconnecting in %.0fs", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff_s)
