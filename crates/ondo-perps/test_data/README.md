# Ondo Perps public fixtures

These fixtures contain selected messages recorded on 2026-11-23 UTC. `raw` preserves the original JSON text, including decimal spelling and timestamps. The outer JSONL record contains `ts_recv_ns` (local UTC Unix nanoseconds), `kind`, and `source`. For `rest`, `source` is the full GET URL and the record includes the HTTP `status` and selected response `headers`. For `ws` and `ws_send`, `source` is the WebSocket URL and `session` identifies the connection. `meta` records describe local observations such as an HTTP upgrade, client close, or the end of an idle probe.

All requests and subscriptions were anonymous. The capture used curl for HTTPS and Python websockets for the WebSocket client. The fixtures are excerpts, so a time gap between two included records does not imply a feed gap.

| File | Source and coverage |
| --- | --- |
| [markets.jsonl](markets.jsonl) | Full production `GET /v1/markets` and `GET /v1/perps/contracts` responses around 13:08 UTC. Includes US500 increments, USD quote, trading state, and disabled examples. |
| [trades-rest.jsonl](trades-rest.jsonl) | Two consecutive five-fill REST trade pages around 13:08 UTC and a 200-fill page that overlaps the WebSocket startup history. |
| [trades-ws.jsonl](trades-ws.jsonl) | Production trades from 13:06–13:11 UTC with omitted, zero, five, and 200-history requests, followed by the deliberate disconnect and reconnect. Includes the witness fills from the parallel `main-1` socket and depth startup on each side of the reconnect. |
| [depth.jsonl](depth.jsonl) | Default, zero-limit, three-level, and grouped depth on parallel sockets around 13:07 UTC. Includes an ungrouped ten-level REST and WebSocket pair with identical levels. |
| [other-ws.jsonl](other-ws.jsonl) | Production quote, mark, funding, and bar updates from around 13:08 UTC. Includes provisional and finalized one-minute candles. |
| [reference-rest.jsonl](reference-rest.jsonl) | Public TradingView symbol info and a short UDF bar window, the 401 from anonymous `/v1/perps/candles`, and production open interest and volume. |
| [transport.jsonl](transport.jsonl) | Production JSON ping and pong, idle observations, multi-market and invalid-market probes, the single-market kline constraint, and a sandbox trades acknowledgement. |

## Trades

Select records by `session` before interpreting WebSocket order. `trades-history` has a five-trade startup batch whose IDs also appear in the 200-fill REST page. `trades-reconnect` demonstrates the 100-trade response to a 200-trade startup request. `before-disconnect` and `after-reconnect` are a separate deliberate disconnect test. The `main-1` records in that file show the parallel socket's 17 trades during the measured gap.

## Full local captures

The complete wire logs are in gitignored `captures/ondo-perps/` in the worktree where verification ran. The files in this directory provide small inputs for future deterministic tests. [Scope and evidence](../../../docs/ondo-perps-checklist.md)

| Capture file | Evidence |
| --- | --- |
| `history-20261123T130836Z.jsonl` | 41 raw cursor pages spanning the entire 2026-11-22 UTC day for US500. |
| `udf-1795439473.jsonl` | Independent anonymous one-minute and five-minute UDF bars for the same date. The one-minute response is `symbol=US500USD.P&resolution=1&from=1795305600&to=1795392000`. |
| `public-20261123T130751Z.jsonl` | Six production channels for US500, with periodic REST trade, depth, and metadata checkpoints. |
| `probe-20261123T130651Z.jsonl`, `recovery-20261123T131059Z.jsonl` | Parallel subscription probes and the deliberate reconnect test. |
| `instrument-live-20261123T132051Z.jsonl` | Two-minute live record for US500, BTC, and NVDA, plus anonymous market and contracts responses and REST trade checkpoints. |
| `live_model_probe.rs`, `instrument-live-20261123T132051Z.model.json` | Local Rust model and cache probe and its summary. Run `captures/ondo-perps/live_model_probe captures/ondo-perps/instrument-live-20261123T132051Z.jsonl` from the repository root to reproduce the result on the saved capture. |
