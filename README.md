# nt-dex-adapters

Out-of-tree Rust market-data clients for NautilusTrader perpetual futures. Aster is the first venue. GRVT, edgeX, and Extended follow one at a time, each added as its own crate once the previous venue is qualified. Strategies live in a separate private repository that can depend on these crates.

| Path | Purpose |
| --- | --- |
| `crates/aster` | Aster adapter (in progress); the reference for later venue crates |
| `bins/node` | Data-only NautilusTrader live node that streams to the catalog (stub) |
| `tools/capture` | Workflow for collecting public messages as parser fixtures |
| `scripts/gate.sh` | Format, lint, test, and audit the whole workspace |
| `AGENTS.md` | Per-venue build and qualification checklist |

## Dev loop

```sh
cargo check          # venue crate only (default-members)
cargo test           # venue crate only
bacon                # re-check on every save (`bacon clippy`, `bacon test`)
scripts/gate.sh      # fmt + clippy + tests + cargo-deny for the whole workspace
```

Install the two helper tools with `brew install bacon cargo-deny`. Lints are set to `warn` in `Cargo.toml` so they never block the edit loop; the gate runs clippy with `-D warnings`. Use `cargo build --profile debugging` when a debugger needs full debug info; the default dev profile keeps only line tables.

The venue crates stay free of persistence dependencies so the inner loop is fast; only `bins/node` enables `nautilus-live`'s `streaming` feature. The first `--workspace` build compiles about 600 crates.

## Rust and NautilusTrader

The workspace pins the published NautilusTrader **2.0 RC9** Rust crates to `0.68.0` and selects Rust `1.99.1`. Add NautilusTrader dependencies through `[workspace.dependencies]`. The venue crates use the high-precision model, matching the default of the three built-in adapters. [RC9 release](https://github.com/nautechsystems/nautilus_trader/releases/tag/v2.0.0rc9)

## Milestones

1. **Aster capture and parser.** Capture public feed messages, commit representative fixtures to `crates/aster/test_data/`, and build `parse.rs` against them. Compare the public futures API with the RC9 Binance futures client before borrowing its REST or book logic. Verify instrument filters, snapshot and delta sequencing, and time units against captured messages. [Aster futures API](https://docs.asterdex.com/for-developers/aster-api/api-documentation) · [Binance futures adapter](https://github.com/nautechsystems/nautilus_trader/tree/v2.0.0rc9/crates/adapters/binance/src/futures)
2. **Aster live client.** Add the WebSocket client, data client, and factory, with a per-venue data-tester example for live smoke tests. Use RC9 Lighter as the reference for Nautilus data client and factory wiring. [Lighter adapter](https://github.com/nautechsystems/nautilus_trader/tree/v2.0.0rc9/crates/adapters/lighter)
3. **Catalog node.** Wire `bins/node` to the built-in Lighter, Hyperliquid, and Binance perpetuals data clients alongside Aster. Subscribe explicitly to the chosen instruments and event types. Use NautilusTrader streaming with a local Feather staging path and a Parquet catalog. Check that the catalog returns each subscribed venue's data after promotion, then use those events to exercise replay. [RC9 streaming guide](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/docs/how_to/stream_parquet_catalog.md)
4. **Next venues.** Repeat the `AGENTS.md` checklist for GRVT, edgeX, and Extended, one at a time.
