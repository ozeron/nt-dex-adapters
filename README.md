# nt-dex-adapters

Out-of-tree Rust market-data clients for NautilusTrader perpetual futures. Aster is the first venue. GRVT, edgeX, and Extended follow one at a time. Strategies live in a separate private repository that can depend on these crates.

| Path | Purpose |
| --- | --- |
| `crates/{aster,grvt,edgex,extended}` | Venue crate scaffolds |
| `bins/node` | Scaffold for a data-only NautilusTrader live node |
| `tools/capture` | Workflow for collecting public messages as parser fixtures |
| `AGENTS.md` | Per-venue build and qualification checklist |

## Rust and NautilusTrader

The workspace pins the published NautilusTrader **2.0 RC9** Rust crates to `0.68.0` and selects Rust `1.99.1`. Add NautilusTrader dependencies through `[workspace.dependencies]`. The venue crates use the high-precision model, matching the default of the three built-in adapters. [RC9 release](https://github.com/nautechsystems/nautilus_trader/releases/tag/v2.0.0rc9)

```sh
cargo metadata --locked --no-deps --format-version 1
cargo fmt --all --check
```

## First milestone

Wire `bins/node` to the built-in Lighter, Hyperliquid, and Binance perpetuals data clients. Subscribe explicitly to the chosen instruments and event types. Use NautilusTrader streaming with a local Feather staging path and a Parquet catalog. Check that the catalog returns each subscribed venue's data after promotion, then use those events to exercise replay. The node scaffold currently exits with a status message. [RC9 streaming guide](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/docs/how_to/stream_parquet_catalog.md)

For Aster, compare the public futures API with the RC9 Binance futures client before borrowing its REST or book logic. Verify instrument filters, snapshot and delta sequencing, and time units against captured messages. Use RC9 Lighter as a reference for Nautilus data client and factory wiring. [Aster futures API](https://docs.asterdex.com/for-developers/aster-api/api-documentation) · [Binance futures adapter](https://github.com/nautechsystems/nautilus_trader/tree/v2.0.0rc9/crates/adapters/binance/src/futures) · [Lighter adapter](https://github.com/nautechsystems/nautilus_trader/tree/v2.0.0rc9/crates/adapters/lighter)
