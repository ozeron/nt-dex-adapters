# Aster data reference

What the Aster feed puts in a recorded catalog, and what that means for research and backtests. Venue: Aster perpetuals (`https://fapi.asterdex.com`, `wss://fstream.asterdex.com`). Instrument ids look like `BTCUSDT-PERP.ASTER`.

Figures marked "measured" come from a 30-minute raw capture (2026-11-22, 11:04-11:34 UTC, BTCUSDT, ETHUSDT, SOLUSDT) or from public REST calls made on 2026-11-22. They describe that day, not a guarantee.

| Data | Venue source | Granularity |
| --- | --- | --- |
| Order book deltas | `<symbol>@depth@100ms` plus REST `/fapi/v1/depth` | L2 price levels |
| Quotes | `<symbol>@bookTicker` | Best bid and ask |
| Mark, index price | `<symbol>@markPrice@1s` | One per second |
| Funding rate (running) | `<symbol>@markPrice@1s` | One per second |
| Funding rate (settled) | REST `/fapi/v1/fundingRate`, on request only | One per payment |
| Trades | See [Trades](#trades) | |
| Instruments | REST `/fapi/v1/exchangeInfo`, `/fapi/v1/fundingInfo` | Hourly refresh |

## Order book depth

### Source

The book is built from depth diffs on `<symbol>@depth@100ms`, seeded by a REST snapshot `GET /fapi/v1/depth?limit=<n>`. Each diff lists price levels with their new absolute quantity; a quantity of zero deletes the level. The snapshot depth is `book_snapshot_depth` in the client config (default 1000). The venue accepts only certain values: 5 and 1000 were accepted on 2026-11-22; 4, 7, 200 and 2000 were rejected. The code does not validate the value.

### What this means for a simulation

- **Updates inside a window are not observable.** A diff is the venue's aggregate of a window of roughly 100 ms. Measured on BTCUSDT, diff `T` values are multiples of 50 ms and the gap between consecutive diffs is 100 ms (8,050 of 15,107), 150 ms (4,730), 50 ms (1,207) or longer; 70 gaps were 0. A level that changed and changed back inside one window leaves no trace, and the order of events inside a window is unknown. A diff that changes no level is not recorded.
- **It is L2 data.** Each row is a price, a side and a total quantity. Rows carry `order_id` 0 and there is no queue position, order count or per-order event. Fill models that need queue position have to assume one.
- **The snapshot covers at most `book_snapshot_depth` levels per side**; diffs cover the whole venue book. The adapter keeps no book of its own and does not truncate, so a diff for a level outside the snapshot is recorded as an ordinary update (or delete). Consequences:
  - Levels beyond the snapshot are not seeded. They exist in a rebuilt book only after a diff touches them. Far levels that rested untouched since the snapshot are missing.
  - A rebuilt book does not accumulate stale levels this way: a level is added by a diff and removed by the diff that deletes it. A delete for a level the rebuilt book never held does nothing.
  - Measured against the first snapshot of the capture, diffs reached outside it for 31 of 189,909 levels on BTCUSDT (up to 7.6% of price from the best price) and 5 of 206,755 on ETHUSDT (up to 34%). The snapshot spanned 56,020-65,564 for BTCUSDT (best bid 60,889.5) and 1,300-1,920 for ETHUSDT (best 1,710.75). SOLUSDT's snapshot held 805 bids, the whole bid side down to 0.5, and 1000 asks.
  - Treat depth beyond roughly the snapshot window as incomplete, and do not size an order against it.

### Sync rules

Update ids (`U`, `u`, `pu`) are venue-global: they are shared across symbols, so one symbol's consecutive diffs are not numerically adjacent. In the capture `pu` equalled the previous `u` for 15,107 of 15,107 consecutive BTCUSDT diffs. The adapter handles each book as follows:

1. The first diff queues a paced snapshot request. While waiting, the adapter validates the diffs and retains compact wire payloads, timestamps, and sequence IDs. It keeps the newest suffix within the 20,000-diff count limit and the configurable `book_buffer_bytes` and `session_book_buffer_bytes` budgets (4 MiB per book and 64 MiB per session by default).
2. When the snapshot (`lastUpdateId`) arrives, held diffs with `u <= lastUpdateId` are dropped. The snapshot must cover every evicted diff. The first remaining diff must have `pu <= lastUpdateId`, otherwise the adapter requests another snapshot. Only the updates applied to the book are expanded into Nautilus deltas.
3. After that, every diff must have `pu` equal to the previous diff's `u`. Otherwise the book has a gap: the diff is held, a new snapshot is requested, and nothing is emitted for that book until it arrives.
4. A WebSocket reconnect resets every book carried by that connection and requests new snapshots. A changed instrument definition resets that book too.

Until a book has its first snapshot the catalog has nothing for it. Snapshots are paced to the venue's request weight (`snapshot_weight_per_minute`, default 1800 of the 2400 per minute the venue's `exchangeInfo` lists). A 1000-level snapshot weighs 20, so one is requested every 0.67 s. The first pass of requests for 595 books takes about 6.6 minutes at that rate. Books that need a newer snapshot can take longer. A reconnect of a connection with many books is paced the same way.

The adapter logs queued frame counts, estimated ingress bytes, age of the queue head, pending book bytes, and snapshot jobs. The recorder also logs the Nautilus runner's sampled event count, estimated inline queue bytes, and the maximum socket-to-actor age of mark prices in each reporting window. Mark age includes processing before dispatch. The runner's byte estimate excludes owned book payloads.

### Recognising a resync in the catalog

A snapshot is one batch of deltas: an `OrderBookDelta` with action `Clear`, then one `Add` per level, all with flag `F_SNAPSHOT` (32); the last delta also has `F_LAST` (128). Every other batch is a diff: its deltas have flag 0 except the last, which has `F_LAST`. The first `Clear` for an instrument in a recording is the initial sync. Any later `Clear` is a resync after a gap, a reconnect, or an instrument change. Between the last diff before it and the snapshot, the book's changes are not in the catalog.

A resync starts a new book, so discard your rebuilt book at each `Clear`. Diffs held while waiting for the snapshot are published right after it, with `ts_init` raised to the snapshot's `ts_init`.

### The `sequence` field

For diffs `sequence` is the diff's final update id `u`. For a snapshot it is `lastUpdateId`, on every row of the batch. Because ids are venue-global, `sequence` increases within a symbol but is not consecutive, and a difference between two values means nothing. Do not use it to count missed updates.

## Quotes

`QuoteTick` from `<symbol>@bookTicker`: best bid and ask price and size only. Prices use the instrument's price precision, sizes its size precision. Quotes are independent of the book stream; they are not derived from the recorded book.

## Mark and index prices

`MarkPriceUpdate` and `IndexPriceUpdate` come from `<symbol>@markPrice@1s`, one pair per second per symbol (1,799 messages in 1,798 s for BTCUSDT).

**Precision is 8 decimals, not the instrument's.** `Price.precision` is 8 for every mark and index row. The venue sends 8 decimals on the wire but drops them from whole numbers (`"1666"` for YFIUSDT from `/fapi/v1/premiumIndex`, seen earlier on 2026-11-22; the 19:32 capture had none), so the adapter fixes the precision instead of reading it from the string. The Parquet catalog requires one precision per instrument file.

This differs from the Binance futures adapter in `nautilus-binance` 0.68.0, which builds both at the instrument's price precision (checked in its source; other built-in adapters were not checked). Rounding to the tick would lose information here. Measured on all 595 trading perpetuals at 19:32 UTC with the precision the adapter assigns each instrument (`pricePrecision`):

| Quantity | Value |
| --- | --- |
| Worst rounding error | 59 bps (ZILUSDT, precision 5), 42 bps (DYDXUSDT, 3), 28 bps (GALAUSDT, 5) |
| Symbols with an error over 10 bps | 5 of 595 (over 1 bp: 19) |
| Median rounding error | 0.008 bps |
| BTCUSDT, ETHUSDT, SOLUSDT | 0.006, 0.025, 0.003 bps |
| Median absolute mark-index difference | 5.9 bps |
| Symbols whose tick exceeds the mark-index difference | 18% |

An earlier measurement the same day gave up to 64 bps; the figures move with prices. The fixture and test in `crates/aster/tests/reference_prices.rs` check that all 595 symbols' values are stored without loss.

A value with more than 8 decimals is rounded half-even to 8 and logged at warn; none of the 1,190 values in the 19:32 REST capture had more. A mark price, index price or funding rate that does not parse is logged and only that value is missing for that second; the other two are still published.

## Funding

**Live rows are not payments.** The `FundingRateUpdate` rows the recorder writes come from the same `markPrice@1s` message, one per second per symbol. `rate` is the value of the stream's `r` field. It changes during a funding period (24 distinct values for SOLUSDT in 30 minutes; a constant 0.0001 for BTCUSDT), so it is the venue's running figure for the current period rather than a settled amount. The venue's documentation of `r` was not checked. Fields:

| Field | Value |
| --- | --- |
| `rate` | `r` as sent, a fraction per funding period |
| `interval` | Minutes between payments, from `/fapi/v1/fundingInfo` (`fundingIntervalHours` x 60); `None` if the symbol is not listed there |
| `next_funding_ns` | Venue field `T`, the next payment time; `None` if it is 0 |
| `ts_event` | Venue event time `E` |

Intervals vary by symbol: `fundingInfo` listed 105 symbols at 1 h, 3 at 2 h, 360 at 4 h and 333 at 8 h (801 entries, including symbols that are not trading). A backtest that charges every recorded row applies the rate once a second instead of once per interval: 3,600 times too much for a 1 h interval and 28,800 times for 8 h. Charge once per payment time. The relation between the last running value before a payment and the settled rate was not checked (BTCUSDT settled at 0.00005172 at 16:00 UTC while the stream showed 0.0001 earlier in that period), so use settled payments where accuracy matters.

**Settled payments** come from `request_funding_rates`, which reads `/fapi/v1/fundingRate` (pages of 1000, up to 20 pages). Each row has `ts_event` = the venue's `fundingTime`, `next_funding_ns` = `None`, and `interval` = the gap to the previous payment in minutes, rounded to the nearest minute. The first row of a request has no `interval` unless the previous payment is known. The venue has changed intervals over time, so today's interval does not describe past payments. The recorder does not make this request, so the catalog contains no settled payments; fetch them separately. Some `fundingTime` values are 1 ms late (1795334400001 for BTCUSDT).

## Trades

Trades come from the venue's individual trade stream `<symbol>@trade`: one `TradeTick` per fill, not per aggregate.

- `trade_id` is the venue's trade id, which is consecutive per symbol. A repeated id is dropped. A jump in ids is logged as a warning with the number of trades missed; the gap is not recorded in the catalog.
- `aggressor_side` comes from the venue's buyer-is-maker flag: buyer is maker means the aggressor sold.
- The stream also carries a type field (`X`, `MARKET` on every trade observed on 2026-11-22). It is not stored, and every trade is published whatever its value, as the built-in Binance client does. Whether liquidation or ADL fills appear with another value was not checked.
- In a 40-second comparison each individual trade matched one aggregate trade, and the aggregate usually arrived about 150 ms after it.
- `request_trades` reads `/fapi/v1/trades`, which returns only the venue's latest 1000 trades for a symbol. A requested time window is applied to those rows; older history is not reachable without an API key.
- Captures made before this change contain `aggTrade` frames. The feed still parses them, with the aggregate id as `trade_id`, so those captures replay; do not mix the two kinds in one catalog.

## Timestamps

| Data | `ts_event` | `ts_init` |
| --- | --- | --- |
| Book diff | Venue transaction time `T` | Local clock when the frame left the socket |
| Book snapshot | Snapshot's `T` | Local clock when the snapshot response was processed |
| Quote | `T` | Frame receipt |
| Mark, index, funding (stream) | `E` | Frame receipt |
| Funding (settled, requested) | `fundingTime` | Local clock when the response was parsed |
| Instrument | Load time | Load time |

`ts_init` is taken as the frame leaves the socket (`websocket/pool.rs`), before parsing or queueing, so a busy consumer does not delay it. It comes from this machine's clock; its offset from the venue's was not measured. Measured on BTCUSDT, receive time minus `T` was 35 ms at the median and 148 ms at the 99th percentile.

Diffs held while a snapshot was pending get `ts_init` of the snapshot, so for those rows `ts_init` is later than receipt, by up to the snapshot wait. For a backtest ordered by `ts_init`, those rows appear at the snapshot time. The catalog requires non-decreasing `ts_init`, which is why those rows are restamped.

## Instruments

Loaded from `GET /fapi/v1/exchangeInfo`. Only symbols with status `TRADING` and contract type `PERPETUAL` become instruments: 595 of 647 listed on 2026-11-22 (46 `SETTLING`, 6 `PENDING_TRADING`). Each is a `CryptoPerpetual` with id `<symbol>-PERP.ASTER`, linear, with base, quote and settlement currency from `baseAsset`, `quoteAsset` and `marginAsset`.

- **Precision** comes from `pricePrecision` and `quantityPrecision`, the precision of prices and sizes on the wire, not from `tickSize`, which is zero-padded or coarser than the precision on some symbols. `tickSize` and `stepSize` are rescaled to that precision, so `price_increment` can be coarser than the last decimal of the prices (YFIUSDT: tick `1`, precision 3, per the fixture in `crates/aster/test_data/`).
- Margin fractions come from `requiredMarginPercent` and `maintMarginPercent`. Venue fees are not read.
- Instruments are reloaded every hour (`instrument_refresh_interval_secs`, default 3600). A new instrument or a changed definition is published as an `Instrument` again, and a changed instrument's book is resynced. An instrument that is no longer a trading perpetual is published as `InstrumentStatus` with `NotAvailableForTrading` and removed from the client's cache. Whether the catalog stores those status rows was not verified; the recorder does not subscribe to them. What the feed does for a delisted symbol's streams was not verified.

## Known limits

- One collector, one network path. A gap in this collector is a gap in the data.
- The recorder keeps no raw frames. `debug_frames` logs frames at debug level, and the recorder's log level is info.
- No liquidations or open interest.
- Gaps are logged, not stored in the catalog. A book gap shows only as a later `Clear`; missed trades are counted in the log.
- `record --all` takes the instrument list at start. Instruments listed later are not subscribed.
- Catalog history recorded before a 24-hour soak is unqualified. The soak has not been run, and nothing has yet been reconciled against venue statistics over a full day.
- Book levels beyond the snapshot window are incomplete (see above).
