# Ondo Perps market-data connector checklist

Scope the connector to public data for USD-settled perpetuals. Follow the [per-venue checklist](../AGENTS.md) and log evidence against the applicable [NautilusTrader checks](connector-checklist.jsonl). Apply each `skip_when` condition against the verified feed and keep checks pending where evidence is missing.

## Scope and capture

- [x] Register [`nt-dex-ondo-perps`](../crates/ondo-perps/Cargo.toml) in the workspace.
- [ ] Confirm anonymous access to `GET /v1/markets` and `wss://api.ondoperps.xyz/ws`. Verify capabilities, rate limits, subscription limits, and heartbeat behavior. Start with `US500-USD.P` and `tradesPerps` if live capture confirms an active market and replay coverage. [Markets](https://docs.ondoperps.xyz/markets) · [WebSocket connection](https://docs.ondoperps.xyz/api-reference/connection/connect) · [Trades channel](https://docs.ondoperps.xyz/api-reference/public-channels/subscribe:-perps-trades)
- [ ] Capture a few hours of raw public messages and save representative REST and WebSocket payloads in `crates/ondo-perps/test_data/` with UTC capture times and provenance. Include startup, any initial trade history, steady state, reconnect, and market pauses if observed.

## Instruments and parsing

- [ ] Map perpetual IDs, quote and settlement currencies, tick sizes, lot sizes, and market status from captured metadata. Preserve the `.P` market identity and exclude spot market entries. Check whether metadata or trading status changes outside underlying-market hours. [Market metadata](https://docs.ondoperps.xyz/api-reference/markets/get-markets)
- [ ] Parse captured messages with explicit receive timestamps. Verify timestamp units, trade IDs, aggressor side, exact decimal values, and the boundary between trade history and live updates. Verify mark and funding semantics before adding those channels. [Funding rates](https://docs.ondoperps.xyz/funding-rates)
- [ ] Determine whether `depthBooksPerps` sends full or grouped snapshots, whether updates carry sequence information, and how they correspond to REST depth. Use the verified semantics for replacing book state, emitting quotes and recovering after disconnects. [Depth channel](https://docs.ondoperps.xyz/api-reference/public-channels/subscribe:-perps-depth-book) · [REST depth](https://docs.ondoperps.xyz/api-reference/market-data/get-order-book-depth)

## Transport and data client

- [ ] Implement public subscriptions, ping/pong, reconnect, and resubscription. Check trade deduplication and book recovery against captured startup and reconnect behavior. Leave raw payload logging off by default.
- [ ] Add configuration, a data-only client, and its factory. Use Lighter as the NautilusTrader interface reference. Verify instrument requests, subscription lifecycle, and recovery through the normal client entry points.

## Catalog and qualification

- [ ] Stream supported events into the NautilusTrader catalog and read them back after promotion. Compare instrument IDs, timestamps, and values with the raw captures.
- [ ] Replay a full day and run a backtest. Check sequence gaps where sequences are available, inspect crossed books, and reconcile REST trade IDs and counts for the same market and UTC window. Include off-hours or session transitions in the evidence when observed. [REST trades](https://docs.ondoperps.xyz/api-reference/market-data/get-trades)
- [ ] Record replay results, coverage gaps, and evidence by NautilusTrader check ID. Run `scripts/gate.sh` before hand review.
