# finnhub-stock-prices-mock-server

Used as a mock server for live stock prices API.

Because FinnHub server streams data only during US market hours, there was a need to develop a simple mock that can work any time.

Documentation of real server: https://finnhub.io/docs/api/websocket-trades

## Requirements

- Python 3.14
- `pip install -r requirements.txt`

## Running the server

```bash
python server.py --host localhost --port 8765 --interval 1.0
```

## Protocol

Mirrors the real FinnHub trades WebSocket API:

Subscribe:
```json
{"type": "subscribe", "symbol": "AAPL"}
```

Unsubscribe:
```json
{"type": "unsubscribe", "symbol": "AAPL"}
```

Trade update pushed to subscribed clients:
```json
{
  "type": "trade",
  "data": [
    {"s": "AAPL", "p": 191.23, "t": 1700000000000, "v": 25, "c": []}
  ]
}
```

Any connection query/token (e.g. `ws://localhost:8765?token=YOUR_KEY`) is accepted without validation.

## Symbols

10 popular US stocks are pre-loaded with a random-walk price simulator: `AAPL`, `MSFT`, `GOOGL`, `AMZN`, `NVDA`, `META`, `TSLA`, `BRK.B`, `JPM`, `NFLX`.

## Example client

```bash
python client_example.py --url ws://localhost:8765 --symbols AAPL MSFT TSLA
```
