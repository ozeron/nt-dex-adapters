# Ondo Perps market-data connector checklist

Scope the connector to public data for USD-settled linear perpetuals. Follow the [per-venue checklist](../AGENTS.md) and log evidence against the applicable [NautilusTrader checks](connector-checklist.jsonl). Apply each `skip_when` condition against the connector's capabilities and keep checks pending where evidence is missing.

**2026-11-23 status.** Variational is ahead of Ondo in the [venue order](../README.md#milestones). Its local worktree records a short catalog and backtest smoke, with authoritative instrument increments, full-day replay, and venue reconciliation still open. This Ondo branch records venue verification, raw fixtures, and the initial scope. Connector implementation and catalog qualification depend on Variational qualification.

## Scope and capture

- [x] Register [`nt-dex-ondo-perps`](../crates/ondo-perps/Cargo.toml) in the workspace.
- [x] Verify anonymous production REST metadata and public WebSocket subscriptions, starting with live `US500-USD.P` trades. Record advertised limits, observed subscription behavior, and remaining gaps below.
- [x] Save raw trade responses spanning more than a UTC day and live WebSocket examples of startup, history, steady state, and reconnect. Include UTC receive stamps and provenance in the [fixture inventory](../crates/ondo-perps/test_data/README.md).
- [ ] Check metadata across session boundaries and capture market pauses if observed.

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

## Verified production protocol

Anonymous HTTPS `GET /v1/markets` returned 200 and anonymous `wss://api.ondoperps.xyz/ws` upgraded with 101. WebSocket commands and responses are JSON text. Public subscriptions accept explicit `markets` arrays. A single socket accepted six public channels for US500, and a single trades subscription accepted US500, BTC, and NVDA with three startup trades per market. An invalid market returned `type: "error"`. Kline accepted one market with `resolution: "1"` and returned an error for two markets. The maximum number of markets, subscriptions, and parallel sockets remains unverified. The sandbox REST metadata and WebSocket acknowledgement also worked anonymously. Its fees and disabled-market flags differed from production. Sandbox events and qualification are outside the initial scope. [Connect](https://docs.ondoperps.xyz/api-reference/connection/connect) · [Environments](https://docs.ondoperps.xyz/api-reference/integration_guide.md#sandbox-vs-production)

The venue documents a 32 KB maximum inbound WebSocket message, 25 requests/s with burst 50, and an idle timeout of 180 seconds. The published standard REST ceiling for endpoints without an override is 10 requests/s with burst 60, with variation by account and environment. These are documented ceilings. The capture used low-rate reads and did not exhaust the ceilings. REST metadata advertised `Cache-Control: public, max-age=30`, and edge-cached responses were observed. Anonymous default Python HTTP requests received edge 403s where plain curl requests returned 200. Test the eventual Rust transport directly and handle edge errors and HTTP 429 separately. [REST limits](https://docs.ondoperps.xyz/api-reference/spot-api-guide.md#10-rate-limits) · [Markets payloads](../crates/ondo-perps/test_data/markets.jsonl)

An application `{"op":"ping"}` received `type: "pong"`, including during a live session using a 25-second interval. A no-subscription socket closed after 180.685 seconds without a close frame. An active trades subscription stayed open through a 205-second observation with no client ping. Use periodic application pings and a bounded pong deadline. Unsubscribing with the same channel and parameters produced `type: "unsubscribed"` on the probe sockets. [Transport fixtures](../crates/ondo-perps/test_data/transport.jsonl) · [Ping](https://docs.ondoperps.xyz/api-reference/connection/ping)

### Instruments and session state

`result.perps.tradingPairs` contained 97 unique `.P` IDs with quote `USD`. The separate `result.spot.tradingPairs` contained 28 entries. All 97 perps reported `tradingState: "TRADING"` and 13 also had `disabled: true`. Use both fields for eligibility. Preserve `market` as the exchange symbol, including `.P`, and keep `underlyingMarket`, UI display names, and TradingView IDs out of exchange symbol resolution.

For `US500-USD.P`, `pair.base = US500`, `pair.quote = USD`, price tick `quoteIncrement = 0.01`, and size lot `baseIncrement = 0.001`. The production record had `tradingState: "TRADING"` and omitted `disabled`. `/v1/perps/contracts` reported `contractType: "linear"`, `disabled: false`, `isClosed: false`, and `reduceOnly: false`. The market documentation specifies USD settlement. Model this as base US500, quote and settlement USD, and retain the increments as exact decimals.

US500 trades and books were active before the U.S. cash open. REST history also contains trades for every UTC hour of Sunday 2026-11-22, including before the Sunday evening underlying session opens. `schedule` contains New York hours and holiday overrides. Use live venue state for status decisions. Metadata comparisons across the underlying's close, restricted states, and venue pauses are pending. [Metadata and contracts fixtures](../crates/ondo-perps/test_data/markets.jsonl) · [Markets](https://docs.ondoperps.xyz/markets) · [Extended hours](https://docs.ondoperps.xyz/weekend-trading)

### Live instrument and model test

An isolated Rust probe built against the workspace's NautilusTrader RC9 libraries validated a fresh two-minute production capture starting at 13:20:53 UTC on 2026-11-23. It constructed the 84 enabled linear USD perps, loaded them into NautilusTrader's instrument cache, and looked each one up by its canonical ID. The ID string round-trip preserved `.P`. Both market reads agreed on eligibility and increments for the enabled instruments. The contracts endpoint corroborated linear USD contracts and disabled flags for all 97 perp listings. The probe excluded 13 disabled perps and all 28 spot listings.

| Symbol | Price tick | Size lot | Live trades | Live quotes |
| --- | ---: | ---: | ---: | ---: |
| `US500-USD.P` | 0.01 | 0.001 | 109 | 733 |
| `BTC-USD.P` | 1 | 0.0001 | 56 | 695 |
| `NVDA-USD.P` | 0.01 | 0.01 | 10 | 917 |

All 175 `TradeTick`s and 2,345 `QuoteTick`s resolved through the cache and matched the published price and size grids with exact decimals. The quotes were two-sided and uncrossed. The probe parsed venue event times at nanosecond resolution, supplied local receive times separately, and saw acknowledgements for subscription and unsubscribe on both channels. Periodic REST checkpoints matched all 175 live fills field for field, with no extra REST fills inside each symbol's observed live trade time range. Offline negative controls rejected an off-grid price and an unlisted symbol.

The probe source, raw capture, and JSON summary are local under `captures/ondo-perps/`. The Ondo crate compiles with `cargo test --locked --offline -p nt-dex-ondo-perps` and currently has zero crate tests. The instrument request path, live data client, and catalog streaming remain pending. [Local capture inventory](../crates/ondo-perps/test_data/README.md#full-local-captures)

### Trades, startup, and replay inputs

- Omitting `numPastTrades` and setting it to zero both yielded an acknowledgement followed by live-only updates in the probe. Requesting five yielded an immediate five-trade history batch with oldest-first event times, then live updates. Requesting 200 yielded 100 historical trades. Both history and live arrive as `type: "update"` with a `data` array and identical trade fields. An array can contain several fills at the same event time. The wire has no history flag or sequence.
- Use the opaque trade `id` with instrument identity as the deduplication key. Captured IDs were 32-character lowercase hex strings, stable across REST, live WebSocket and startup history. Use `aggressor_side` as taker side and read price, size, and cost from their decimal strings. Equal event times or price/size combinations can represent distinct fills.
- Parse trade `time` as UTC RFC 3339 with up to nine fractional digits and preserve nanoseconds in `ts_event`. The envelope `timestamp` is the venue frame time. Pass local receipt Unix nanoseconds explicitly for `ts_init` at the parsing boundary. REST cursors are opaque. Pages are newest first, including page boundaries that split the same timestamp. Use the cursor and ID to preserve each fill.
- For the first live path, request zero history. Buffer live updates while querying REST on reconnect and reconcile until a known prior ID is found. Deduplicate the overlap by ID and surface a coverage gap if the anchor cannot be reached. In a deliberate 9.906-second disconnect, the 100-trade reconnect batch repeated all 12 observed trades from the old socket and included all 17 fills seen during the gap by a separate live socket. The replacement depth subscription also supplied a fresh snapshot.
- Between 13:07:55.533772744Z and 13:16:08.813630307Z inclusive on 2026-11-23, REST and the zero-history live WebSocket returned the same 302 fills. Every shared ID matched on time, price, size, cost, and aggressor. Both endpoints supplied the complete set observed in that comparison.
- The local raw cursor walk retrieved 41 pages of 1,000 unique fills from 2026-11-23 13:08:36Z back to 2026-11-21 22:20:59Z. For `[2026-11-22T00:00:00Z, 2026-11-23T00:00:00Z)`, the REST trades apportioned 14,677 fills to 1,158 active minutes. Their OHLC matched all 1,158 active one-minute TradingView bars, and base volume matched all 1,440 bars, including 282 zero-volume minutes. Total base volume was 1,538.786. This establishes a raw trade replay input and an independent bar comparison. Catalog promotion, readback, and backtest still require connector implementation.

[Trade fixtures](../crates/ondo-perps/test_data/README.md#trades) · [REST trades](https://docs.ondoperps.xyz/api-reference/market-data/get-trades) · [TradingView history](https://docs.ondoperps.xyz/api-reference/market-data/get-price-history-tradingview)

### Depth and quotes

`depthBooksPerps` sent an immediate snapshot and then repeated independent replacement snapshots. Every payload supplied sorted ask and bid arrays of decimal-string price/size pairs with a market and a snapshot `time`. There was no sequence or checksum. Replacing both sides retires prices omitted from a subsequent snapshot. The next valid frame after reconnect can rebuild the book within the selected depth.

The default and `limit: 0` matched in all 32 same-time comparisons. The probe observed 100 asks and 59–61 bids and did not establish an absolute venue cap. `limit: 3` matched the ungrouped top three levels on each side in all 32 comparisons. `depthLevels: "0.1"` and `"1"` grouped asks upward and bids downward and summed sizes within each bucket. Both groups matched the corresponding sums from ungrouped data at all 32 shared snapshot times for each increment.

REST `GET /v1/perps/depth?market=US500-USD.P&depth=10` supplied the same ungrouped level format. The paired fixture has identical 10 levels on each side, with the WebSocket snapshot 59,999,981 ns later by payload time. Different sample times can reflect different book states. REST and WebSocket expose no shared sequence to align them. The native `topOfBooksPerps` channel supplies one level per side for `QuoteTick`. When evaluating depth, keep grouping off for executable L2 prices, replace state atomically, and carry the subscribed depth as a coverage bound. [Depth fixtures](../crates/ondo-perps/test_data/depth.jsonl) · [Depth channel](https://docs.ondoperps.xyz/api-reference/public-channels/subscribe:-perps-depth-book) · [REST depth](https://docs.ondoperps.xyz/api-reference/market-data/get-order-book-depth)

## Capability matrix

| Data type | Anonymous venue evidence | Connector scope |
| --- | --- | --- |
| Instruments | `/v1/markets`, corroborated by `/v1/perps/contracts` | Production USD-settled linear perps. Start with US500. |
| Trades | `tradesPerps`, cursor-paginated `/v1/perps/trades` with up to 1,000 returned in a page | First end-to-end slice. Live `TradeTick`, then bounded historical requests and reconnect reconciliation. |
| Quotes | `topOfBooksPerps`, two-sided prices and sizes observed | Add native `QuoteTick` for the initial connector conformance run. Historical quotes are outside scope. |
| Depth | `depthBooksPerps` and `/v1/perps/depth` | Candidate follow-up after the first slice. Prove snapshot-based L2, freshness, and independent book comparison before enabling it. |
| Mark and oracle | `markPricesPerps` publishes `markPrice`, `oraclePrice`, `lastExternalPrice` and `lastUpdatedTime`. The docs describe an internally derived oracle during extended sessions. | Mark is a candidate. Verify oracle versus contracts `indexPrice` and status transitions before assigning a Nautilus index mapping. Preserve oracle precision. |
| Funding | `fundingRatesPerps` gives decimal `rate`, a forward `intervalEnds` and recent premium samples. `/v1/perps/contracts` separates last completed `fundingRate` and estimated `nextFundingRate`. | Live estimate is a candidate. Confirm the boundary and timestamp contract before publishing. No settled history is in scope. |
| Bars | `kLinePerps` emitted provisional and closed candles (`x`) with Unix seconds (`t`, `s`, `e`) and numeric OHLCV. A close can arrive after the bar's end. UDF history returned data for `US500USD.P` from `/v1/perps/symbol_info`. Anonymous `/v1/perps/candles` returned 401 `auth_missing`. | Use UDF bars as an independent trade aggregate. Bar projection is deferred. |
| Volume and open interest | Anonymous `/v1/perps/volume` and `/v1/perps/open_interest` returned snapshots. | Scope evidence only. |

The connector is a Rust data client for market data. Spot, accounts, private streams, execution, Python bindings, options, and DeFi pools are outside its scope. [Other WebSocket fixtures](../crates/ondo-perps/test_data/other-ws.jsonl) · [Reference REST fixtures](../crates/ondo-perps/test_data/reference-rest.jsonl) · [Mark mechanics](https://docs.ondoperps.xyz/weekend-trading) · [Funding mechanics](https://docs.ondoperps.xyz/funding-rates) · [Contracts](https://docs.ondoperps.xyz/api-reference/market-data/get-contracts)

## Evidence by NautilusTrader checklist ID

| ID | Status and evidence |
| --- | --- |
| `NT-P0.1`, `NT-P0.3`, `NT-P0.4`, `NT-P0-EXIT` | Scope recorded above. First path is REST instrument to a live US500 trade to streaming catalog, promotion, readback, and replay. Independent venue bar data is available. |
| `NT-P0.2` | Public/private boundary, production/sandbox differences, documented limits, and observed venue constraints recorded. Load ceilings and session transitions are open. |
| `NT-P0.5` | `cargo metadata --locked --offline --no-deps --format-version 1` discovers the Rust crate and its library test target. Connector and node wiring are pending. |
| `NT-P1.3`, `NT-P1.7`, `NT-TEST-FIXTURES` | Raw inputs and wire semantics recorded. Typed connector models and fixture-backed tests are pending. |
| `NT-P2.1`, `NT-P2.2`, `NT-P2.3`, `NT-DATA-TIMESTAMPS`, `TC-D01`, `TC-D03` | The live model probe loaded and resolved 84 definitions in the NautilusTrader cache and parsed live trades and quotes with event and receive times. The data-client requests and DataTester cases are pending. |
| `NT-P3.1`, `NT-P3.2`, `NT-P3.5`, `NT-WS-RECOVERY`, `TC-D20`, `TC-D30`, `TC-D31`, `TC-D70` | Venue probes exercised the inputs. Adapter event delivery, recovery, retirement, and test lifecycle are pending. |
| `NT-P3.4`, `NT-BOOK-SNAPSHOT`, `NT-BOOK-ORACLE`, `TC-D10`, `TC-D11`, `TC-D12`, `TC-D13`, `TC-D14` | Depth semantics have raw proof. Apply the respective no-book/no-depth predicates if the first conformance run has no book output. Revisit when book output is enabled. |
| `NT-P6.3`, `NT-P6.4`, `NT-P6.5`, `NT-P7.3`, `TC-D62`, `TC-D63`, `TC-D74`, `TC-D75` | Out of scope under the no-Python, no-options, and no-DeFi predicates. |
| `TC-D21`, `TC-D40`, `TC-D41`, `TC-D50`, `TC-D51`, `TC-D52`, `TC-D53`, `TC-D60`, `TC-D61` | The initial slice excludes historical quotes, bars, mark, index, funding, status, and close events. Check each unsupported-capability predicate against the implemented surface at conformance time. |
| `NT-P7.6`, `NT-P9.5`, `NT-P9-EXIT` | Pending. Record final capability-dependent skips, catalog reconciliation, full-day replay and backtest, and gate results after implementation. |

The remaining checks are pending or require a capability decision at conformance time. In particular, `TC-D20` and `TC-D73` have no documented skip condition.
