# Fixture capture

`nt-dex-capture` records a session of Aster's public futures feed to a JSONL file so parser and book-sync tests can replay it without the network.

```sh
cargo run -p nt-dex-capture -- --minutes 30
cargo run -p nt-dex-capture -- --symbols BTCUSDT,XAUUSD1 --minutes 5 --snapshot-secs 60
```

Defaults: `BTCUSDT,ETHUSDT,SOLUSDT`, 30 minutes, a depth snapshot every 300 seconds, output in `captures/aster/aster-<unix-seconds>.jsonl`. About 3 MB per minute for the default symbols. `captures/` is gitignored: keep full sessions out of Git.

## File format

One JSON object per line, in receive order:

| Field | Meaning |
| --- | --- |
| `ts_recv_ns` | Local receive time, Unix nanoseconds |
| `kind` | `ws`, `rest`, or `meta` |
| `source` | `stream` for WebSocket frames, the request path for REST, the event name for meta |
| `raw` | Payload text exactly as the venue sent it |

A session holds:

- `rest` `/fapi/v1/exchangeInfo` and `/fapi/v1/fundingInfo` at startup, for instrument mapping and funding intervals.
- `ws` combined-stream frames for `depth@100ms`, `trade`, `bookTicker`, and `markPrice@1s` per symbol. Sessions recorded before 2026-11-22 carry `aggTrade` in place of `trade`; replay accepts both.
- `rest` `/fapi/v1/depth?limit=1000` per symbol two seconds after each (re)connect and then on the snapshot interval. The first bootstraps the book; the later ones are checkpoints to compare the rebuilt book against.
- `rest` one-minute klines and 24h tickers at the end, as venue statistics for reconciling trade counts and volume. They are fetched 60 seconds after the stream closes, because the venue's REST klines trail its trade stream.
- `meta` markers: `session_start`, `ws_open`, `ws_ping`, `ws_close`, `ws_error`, `rest_error`, `session_end`.

The socket reconnects after a drop, so a session that saw one also contains a recovery example.

## Fixtures

Copy selected lines into `crates/<venue>/test_data/` with their endpoint, subscription, and UTC capture time. Preserve payload bytes. Only public feeds are captured; never add private payloads or credentials.
