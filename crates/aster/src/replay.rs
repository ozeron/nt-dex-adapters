//! Offline replay of `nt-dex-capture` session files.
//!
//! Plays the role of the live client: frames go to the feed, and when the
//! feed asks for a depth snapshot the next captured one for that symbol is
//! handed over. Later snapshots in the file are left unused.

use std::io::BufRead;

use ahash::AHashSet;
use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{data::Data, instruments::InstrumentAny};
use serde::Deserialize;
use ustr::Ustr;

use crate::{
    feed::{AsterFeed, FeedEvent},
    http::parse::{parse_exchange_info, parse_funding_intervals},
};

/// One line of a capture file.
#[derive(Debug, Deserialize)]
struct Record {
    ts_recv_ns: u64,
    kind: String,
    source: String,
    raw: String,
}

/// What a capture replayed into.
#[derive(Debug, Default)]
pub struct Replay {
    /// Every instrument in the captured `exchangeInfo`.
    pub instruments: Vec<InstrumentAny>,
    /// Market data in emission order, with non-decreasing `ts_init`.
    pub data: Vec<Data>,
    /// Frames the feed rejected, as `line number: reason`.
    pub rejected: Vec<String>,
    /// Symbols whose book was out of sequence when the capture ended, with
    /// no snapshot left to recover from. Their deltas stop at the gap.
    pub unsynced_books: Vec<String>,
}

/// Returns the `symbol` query parameter of a captured REST path.
fn query_symbol(source: &str) -> Option<&str> {
    let (_, query) = source.split_once("symbol=")?;
    query.split('&').next()
}

/// Replays a capture, returning its instruments and market data.
///
/// The receive timestamps in the file become `ts_init`.
///
/// # Errors
///
/// Returns an error if a line is not a capture record, or if the
/// `exchangeInfo` or a depth snapshot the feed needs does not parse.
/// Individual stream frames that fail are listed in [`Replay::rejected`].
pub fn replay_capture(reader: impl BufRead) -> anyhow::Result<Replay> {
    let mut replay = Replay::default();
    let mut feed = AsterFeed::default();
    let mut awaiting_snapshot: AHashSet<Ustr> = AHashSet::new();
    let mut events = Vec::new();
    let mut sockets_opened = 0_u32;
    let mut clock = UnixNanos::default();

    for (index, line) in reader.lines().enumerate() {
        let line_number = index.saturating_add(1);
        let record: Record = serde_json::from_str(&line?)
            .with_context(|| format!("line {line_number} is not a capture record"))?;
        // REST records are stamped on arrival but written slightly later.
        clock = clock.max(UnixNanos::from(record.ts_recv_ns));

        match (record.kind.as_str(), record.source.as_str()) {
            ("rest", source) if source.ends_with("/exchangeInfo") => {
                let parsed = parse_exchange_info(&record.raw, clock)?;
                for instrument in &parsed.instruments {
                    feed.add_instrument(instrument);
                }
                replay.instruments = parsed.instruments;
            }
            ("rest", source) if source.ends_with("/fundingInfo") => {
                feed.set_funding_intervals(&parse_funding_intervals(&record.raw)?);
            }
            ("meta", "ws_open") => {
                sockets_opened = sockets_opened.saturating_add(1);
                if sockets_opened > 1 {
                    feed.reset_books();
                    awaiting_snapshot.clear();
                }
            }
            ("ws", _) => {
                if let Err(error) = feed.handle_text(&record.raw, clock, &mut events) {
                    replay.rejected.push(format!("{line_number}: {error:#}"));
                }
            }
            ("rest", source) if source.contains("/depth?") => {
                let symbol = query_symbol(source).context("depth record without a symbol")?;
                if awaiting_snapshot.remove(&Ustr::from(symbol)) {
                    feed.handle_depth_snapshot(symbol, &record.raw, clock, &mut events)?;
                }
            }
            _ => {}
        }

        for event in events.drain(..) {
            match event {
                FeedEvent::SnapshotRequired(symbol) => {
                    awaiting_snapshot.insert(symbol);
                }
                FeedEvent::Data(data) => replay.data.push(data),
            }
        }
    }
    replay.unsynced_books = feed
        .books_awaiting_snapshot()
        .iter()
        .map(ToString::to_string)
        .collect();
    Ok(replay)
}
