"""
Mock FinnHub-style WebSocket server.

Streams simulated real-time trade updates for a set of popular US stocks,
mimicking the wire format of wss://ws.finnhub.io so client code written
against the real FinnHub API can be tested at any time (not just during
US market hours).

Protocol (matches FinnHub):

Client -> Server (subscribe):
    {"type": "subscribe", "symbol": "AAPL"}

Client -> Server (unsubscribe):
    {"type": "unsubscribe", "symbol": "AAPL"}

Server -> Client (trade update):
    {
        "type": "trade",
        "data": [
            {"s": "AAPL", "p": 191.23, "t": 1700000000000, "v": 25, "c": []}
        ]
    }

Usage:
    python server.py --host 0.0.0.0 --port 8765

Requires an API key query param or header, same as FinnHub
(wss://ws.finnhub.io?token=API_KEY), but the mock server does not validate
it - any (or no) token is accepted.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field

import websockets
from websockets.asyncio.server import ServerConnection, serve

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("finnhub_mock")


@dataclass
class SymbolState:
    """Tracks the simulated market state for a single symbol."""

    symbol: str
    price: float
    volatility: float = 0.002  # ~0.2% max move per tick
    min_volume: int = 1
    max_volume: int = 500

    def next_trade(self) -> dict:
        """Advance the price with a random walk and return a trade record."""
        pct_change = random.uniform(-self.volatility, self.volatility)
        self.price = max(0.01, round(self.price * (1 + pct_change), 2))
        volume = random.randint(self.min_volume, self.max_volume)
        return {
            "s": self.symbol,
            "p": self.price,
            "t": int(time.time() * 1000),
            "v": volume,
            "c": [],
        }


# Starting prices roughly reflect real-world magnitudes; exact values do not
# matter since this is a mock feed for development/testing.
DEFAULT_SYMBOLS: dict[str, float] = {
    "AAPL": 190.00,
    "MSFT": 425.00,
    "GOOGL": 165.00,
    "AMZN": 180.00,
    "NVDA": 120.00,
    "META": 500.00,
    "TSLA": 250.00,
    "BRK.B": 410.00,
    "JPM": 210.00,
    "NFLX": 650.00,
}


@dataclass
class ClientSession:
    """Per-connection subscription state."""

    connection: ServerConnection
    subscribed: set[str] = field(default_factory=set)


class MockFinnhubServer:
    def __init__(
        self,
        symbols: dict[str, float] | None = None,
        tick_interval: float = 1.0,
        max_trades_per_message: int = 3,
    ) -> None:
        self.symbol_states: dict[str, SymbolState] = {
            symbol: SymbolState(symbol=symbol, price=price)
            for symbol, price in (symbols or DEFAULT_SYMBOLS).items()
        }
        self.tick_interval = tick_interval
        self.max_trades_per_message = max_trades_per_message
        self.sessions: dict[ServerConnection, ClientSession] = {}

    async def handler(self, connection: ServerConnection) -> None:
        session = ClientSession(connection=connection)
        self.sessions[connection] = session
        peer = connection.remote_address
        logger.info("Client connected: %s", peer)
        try:
            async for raw_message in connection:
                await self._handle_message(session, raw_message)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self.sessions.pop(connection, None)
            logger.info("Client disconnected: %s", peer)

    async def _handle_message(self, session: ClientSession, raw_message: str | bytes) -> None:
        try:
            message = json.loads(raw_message)
        except (json.JSONDecodeError, TypeError):
            await self._send_error(session, "invalid JSON message")
            return

        msg_type = message.get("type")
        symbol = message.get("symbol")

        if msg_type == "subscribe":
            if not symbol or symbol not in self.symbol_states:
                await self._send_error(session, f"unknown symbol '{symbol}'")
                return
            session.subscribed.add(symbol)
            logger.info("Client %s subscribed to %s", session.connection.remote_address, symbol)
        elif msg_type == "unsubscribe":
            session.subscribed.discard(symbol)
            logger.info("Client %s unsubscribed from %s", session.connection.remote_address, symbol)
        else:
            await self._send_error(session, f"unsupported message type '{msg_type}'")

    async def _send_error(self, session: ClientSession, message: str) -> None:
        payload = {"type": "error", "msg": message}
        await session.connection.send(json.dumps(payload))

    async def broadcast_loop(self) -> None:
        """Continuously push simulated trades to subscribed clients."""
        while True:
            await asyncio.sleep(self.tick_interval)
            if not self.sessions:
                continue

            for session in list(self.sessions.values()):
                if not session.subscribed:
                    continue

                symbols_to_send = random.sample(
                    sorted(session.subscribed),
                    k=min(self.max_trades_per_message, len(session.subscribed)),
                )
                trades = [self.symbol_states[s].next_trade() for s in symbols_to_send]
                payload = {"type": "trade", "data": trades}
                try:
                    await session.connection.send(json.dumps(payload))
                except websockets.exceptions.ConnectionClosed:
                    continue


async def main() -> None:
    parser = argparse.ArgumentParser(description="Mock FinnHub WebSocket trades server")
    parser.add_argument("--host", default="localhost", help="Host to bind (default: localhost)")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind (default: 8765)")
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between simulated trade broadcasts (default: 1.0)",
    )
    args = parser.parse_args()

    server = MockFinnhubServer(tick_interval=args.interval)

    async with serve(server.handler, args.host, args.port) as ws_server:
        logger.info("Mock FinnHub WS server listening on ws://%s:%s", args.host, args.port)
        broadcast_task = asyncio.create_task(server.broadcast_loop())
        try:
            await ws_server.serve_forever()
        finally:
            broadcast_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
