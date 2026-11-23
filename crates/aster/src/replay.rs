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

#[derive(Debug, Deserialize)]
struct Record {
    ts_recv_ns: u64,
    kind: String,
    source: String,
    raw: String,
}

#[derive(Debug, Default)]
pub struct Replay {
    pub instruments: Vec<InstrumentAny>,
    pub data: Vec<Data>,
    pub rejected: Vec<String>,
    pub unsynced_books: Vec<String>,
}

fn query_symbol(source: &str) -> Option<&str> {
    let (_, query) = source.split_once("symbol=")?;
    query.split('&').next()
}

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
