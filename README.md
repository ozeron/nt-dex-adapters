# nt-dex-adapters

Out-of-tree Rust market-data clients for NautilusTrader perpetual futures. Aster is the first venue. GRVT, edgeX, and Extended follow one at a time, each added as its own crate once the previous venue is qualified. Strategies live in a separate private repository that can depend on these crates.

| Path | Purpose |
| --- | --- |
| `crates/aster` | Aster adapter: instruments, parsers, book sync, live data client. The reference for later venue crates |
| `bins/node` | `nt-dex-node`: records the live Aster feed into a Parquet catalog, imports a capture into one, and summarises one |
| `tools/capture` | `nt-dex-capture`: records raw public feed sessions to JSONL for fixtures and replay tests |
| `scripts/gate.sh` | Format, lint, test, and audit the whole workspace |
| `AGENTS.md` | Per-venue build and qualification checklist |
| `docs/data.md` | What the recorded catalog contains and how to read it: book sync, prices, funding, timestamps |

## Dev loop

```sh
cargo check          # venue crate only (default-members)
cargo test           # venue crate only
bacon                # re-check on every save (`bacon clippy`, `bacon test`)

cargo run -p nt-dex-capture -- --minutes 30                 # record a raw session to captures/aster/
cargo test -p nt-dex-aster --test replay -- --nocapture     # replay it through the adapter, check against the venue
cargo run -p nt-dex-aster --example data_tester BTCUSDT     # live smoke test inside a LiveNode
cargo run -p nt-dex-node -- import-capture <capture> <dir>  # capture -> Parquet catalog
cargo run --release -p nt-dex-node -- record <dir> --minutes 5 BTCUSDT ETHUSDT  # live feed -> Parquet catalog
cargo run --release -p nt-dex-node -- record <dir> --all     # every trading instrument
cargo test -p nt-dex-aster --test live_client               # data client against a mock venue on real sockets
cargo run -p nt-dex-node -- summary <dir>                   # read the catalog back
scripts/gate.sh      # fmt + clippy + tests + cargo-deny for the whole workspace
```

Install the two helper tools with `brew install bacon cargo-deny`. Lints are set to `warn` in `Cargo.toml` so they never block the edit loop; the gate runs clippy with `-D warnings`. Use `cargo build --profile debugging` when a debugger needs full debug info; the default dev profile keeps only line tables.

The venue crates stay free of persistence dependencies so the inner loop is fast; only `bins/node` enables `nautilus-live`'s `streaming` feature. The first `--workspace` build compiles about 600 crates, optimised, and takes about five minutes; after that the 30-minute capture replays in about two seconds.

The replay test uses `$ASTER_CAPTURE` or the newest file in `captures/aster/` and passes without checking anything when there is none. It rebuilds each book with NautilusTrader's `OrderBook`, compares it with the venue's own depth snapshots taken during the session, and reconciles per-minute trade volume and counts against the venue's klines.

## Rust and NautilusTrader

The workspace pins the published NautilusTrader **2.0 RC9** Rust crates to `0.68.0` and selects Rust `1.99.1`. Add NautilusTrader dependencies through `[workspace.dependencies]`. The venue crates use the high-precision model, matching the default of the three built-in adapters. [RC9 release](https://github.com/nautechsystems/nautilus_trader/releases/tag/v2.0.0rc9)

## Milestones

1. **Aster capture and parser.** Done: capture tool, fixtures in `crates/aster/test_data/`, parsers, and book sync, validated by replay against a 30-minute session. The wire format follows Binance futures, with two differences that matter: update ids are venue-global, so only `pu == previous u` detects a gap, and precision comes from `pricePrecision`/`quantityPrecision` because `tickSize` is zero-padded or coarser on some symbols. [Aster futures API](https://docs.asterdex.com/for-developers/aster-api/api-documentation) · [Binance futures adapter](https://github.com/nautechsystems/nautilus_trader/tree/v2.0.0rc9/crates/adapters/binance/src/futures)
2. **Aster live client.** Built: data client, factory, and the `data_tester` example. Streams are spread over a pool of connections the way the Binance futures client does it (first fit, 200 streams each, up to 20 connections), with two differences the venue forces: a control message is kept under 3,000 bytes because the venue closes the connection above about 4 KB, and a stream the venue refuses is tried again. Book snapshots are paced to the venue's request weight, so subscribing to every book takes about seven minutes to sync. The client reloads instruments and funding intervals every hour and publishes what changed. Subscribing to mark, index, or funding publishes all three. Mark and index prices are stored at eight decimals, not instrument precision: rounding to the tick loses up to 64 bps on low-priced symbols and hides the basis on about one symbol in seven. Funding updates on the stream are the venue's running estimate with the interval and next funding time; `request_funding_rates` returns settled payments. `tests/live_client.rs` runs the client against a mock venue over real HTTP and WebSocket connections. Trades are published from the venue's individual `<symbol>@trade` stream, with the venue trade id as `TradeId`; ids are consecutive per symbol, so repeats are dropped and jumps are logged as missed trades. `request_trades` returns up to the venue's latest 1000 trades. What each recorded data type means, including book depth, is in `docs/data.md`. Still open: `request_bars`, `request_book_snapshot`, subscribing to instruments listed after start, and the 24-hour soak. [Lighter adapter](https://github.com/nautechsystems/nautilus_trader/tree/v2.0.0rc9/crates/adapters/lighter)
3. **Catalog node.** Importing a capture works, and `record` streams the live Aster feed through Feather staging (`<dir>.staging`) into the Parquet catalog: instruments, book deltas, trades, quotes, mark and index prices, and funding rates with interval and next funding time. `summary` reads all of them back and counts crossed books. Next, wire `bins/node` to the built-in Lighter, Hyperliquid, and Binance perpetuals data clients alongside Aster. Subscribe explicitly to the chosen instruments and event types. Use NautilusTrader streaming with a local Feather staging path and a Parquet catalog. Check that the catalog returns each subscribed venue's data after promotion, then use those events to exercise replay. [RC9 streaming guide](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/docs/how_to/stream_parquet_catalog.md)
4. **Next venues.** Repeat the `AGENTS.md` checklist for GRVT, edgeX, and Extended, one at a time.
