# Adapter development

Build one public market-data adapter at a time. Start with Aster and hand-review it before using it as the reference for later venues. Follow the venue order in the README. Keep strategies in the separate private repository. Use NautilusTrader's streaming catalog for decoded market data.

## Per-venue checklist

1. **Capture.** Save a few hours of public feed messages. Copy representative wire payloads into `crates/<venue>/test_data/` with capture time, source, and channel. Include stream startup and recovery examples as available.
2. **Instruments.** Load symbols, tick sizes, and lot sizes from the instrument endpoint. Check the mapping against captured metadata.
3. **Parser.** Convert raw bytes into NautilusTrader book deltas, trades, and quotes in `parse.rs`. Keep network and clock access outside the parser. Test with captured fixtures and provide receive timestamps explicitly when the parser needs them.
4. **WebSocket.** Implement subscriptions, heartbeats, reconnects, and book resynchronization on sequence gaps. Provide an opt-in debug flag for dumping raw public frames and leave it off by default.
5. **Data client.** Add NautilusTrader configuration, a data client, and its factory. Keep the node data-only.
6. **Replay.** Read a day back from the catalog and run a backtest over it. Check sequence gaps and crossed books, and reconcile trade counts with venue statistics for the same instruments and time range before marking the venue complete.

Use `common`, `http`, `websocket`, `parse.rs`, `config.rs`, `data.rs`, and `factories.rs` as the adapter grows. Compare Aster behavior with the public docs and captures before reusing Binance futures parsing or synchronization. Use Lighter as a reference for NautilusTrader's client interfaces.

Keep `nautilus-*` pins in the root manifest. Verify an upgrade across the adapter crates and node together.

Keep Rust comments in the leading file header. Use `//!` for file documentation. `scripts/gate.sh` checks this rule before Clippy.
