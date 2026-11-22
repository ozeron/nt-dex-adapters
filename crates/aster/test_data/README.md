# Aster fixtures

Public payloads copied from an `nt-dex-capture` session.

- Captured: 2026-11-22, 11:04–11:34 UTC
- REST source: `https://fapi.asterdex.com`
- WebSocket source: `wss://fstream.asterdex.com/stream` (combined streams)

| File | Endpoint or stream | Notes |
| --- | --- | --- |
| `http_exchange_info.json` | `GET /fapi/v1/exchangeInfo` | Trimmed to six symbols: `BTCUSDT`, `SOLUSDT` (tick padded to `0.0100`), `YFIUSDT` (tick `1` at precision 3), `XAUUSD1` (USD1-quoted), `TONUSDT` (`SETTLING`), `MBLUSDT` (`PENDING_TRADING`). Re-indented. |
| `http_funding_info.json` | `GET /fapi/v1/fundingInfo` | Captured 2026-11-22 later the same day. Trimmed to four symbols: `BTCUSDT`, `SOLUSDT`, `SUSHIUSDT` (8 h), `SLXUSDT` (4 h). Re-indented. |
| `http_depth_solusdt.json` | `GET /fapi/v1/depth?symbol=SOLUSDT&limit=1000` | Trimmed to five levels per side. |
| `ws_depth.json` | `solusdt@depth@100ms` | Payload bytes as received; includes a zero-quantity level. |
| `http_trades.json` | `GET /fapi/v1/trades?symbol=BTCUSDT&limit=3` | Captured 2026-11-22 later the same day. Re-indented. |
| `ws_trade.json` | `btcusdt@trade` | Captured 2026-11-22 later the same day. Payload bytes as received. |
| `ws_agg_trade.json` | `solusdt@aggTrade` | Payload bytes as received. The adapter no longer subscribes to this stream; old captures still carry it. |
| `ws_book_ticker.json` | `solusdt@bookTicker` | Payload bytes as received. |
| `ws_mark_price.json` | `solusdt@markPrice@1s` | Payload bytes as received. |
