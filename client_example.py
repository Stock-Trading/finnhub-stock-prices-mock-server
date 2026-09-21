"""
Simple test client for the mock FinnHub WebSocket server.

Connects, subscribes to a handful of symbols, and prints incoming trade
messages - mirroring how you would use the real wss://ws.finnhub.io feed.

Usage:
    python client_example.py --url ws://localhost:8765 --symbols AAPL MSFT TSLA
"""

from __future__ import annotations

import argparse
import asyncio
import json

from websockets.asyncio.client import connect


async def run(url: str, symbols: list[str]) -> None:
    async with connect(url) as ws:
        for symbol in symbols:
            await ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            print(f"Subscribed to {symbol}")

        async for raw_message in ws:
            message = json.loads(raw_message)
            if message.get("type") == "trade":
                for trade in message["data"]:
                    print(
                        f"{trade['s']:>6}  price={trade['p']:.2f}  "
                        f"volume={trade['v']}  t={trade['t']}"
                    )
            else:
                print(message)


def main() -> None:
    parser = argparse.ArgumentParser(description="Mock FinnHub WS test client")
    parser.add_argument("--url", default="ws://localhost:8765?token=mock")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["AAPL", "MSFT", "TSLA"],
        help="Symbols to subscribe to",
    )
    args = parser.parse_args()
    asyncio.run(run(args.url, args.symbols))


if __name__ == "__main__":
    main()
