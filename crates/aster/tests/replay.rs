//! Replays a raw capture from `nt-dex-capture` through the feed and checks
//! the result against the venue's own snapshots and statistics.
//!
//! The capture is `$ASTER_CAPTURE`, or the newest file in `captures/aster/`.
//! With neither present the test passes without checking anything.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::print_stderr,
    clippy::too_many_lines,
    reason = "test code"
)]

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs,
    path::PathBuf,
    str::FromStr,
};

use nautilus_core::UnixNanos;
use nautilus_model::{data::Data, enums::BookType, instruments::Instrument, orderbook::OrderBook};
use nt_dex_aster::{
    feed::{AsterFeed, FeedEvent},
    http::parse::parse_exchange_info,
};
use rust_decimal::Decimal;
use serde_json::Value;

const CHECK_DEPTH: usize = 100;
const MINUTE_MS: u64 = 60_000;
const KLINE_SETTLE_MS: u64 = 60_000;

fn capture_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("ASTER_CAPTURE") {
        return Some(PathBuf::from(path));
    }
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../captures/aster");
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .ok()?
        .filter_map(|entry| Some(entry.ok()?.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
        .collect();
    files.sort();
    files.pop()
}

struct Record {
    ts: UnixNanos,
    kind: String,
    source: String,
    raw: String,
}

fn read_capture(path: &PathBuf) -> Vec<Record> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| {
            let value: Value = serde_json::from_str(line).unwrap();
            Record {
                ts: UnixNanos::from(value["ts_recv_ns"].as_u64().unwrap()),
                kind: value["kind"].as_str().unwrap().to_owned(),
                source: value["source"].as_str().unwrap().to_owned(),
                raw: value["raw"].as_str().unwrap().to_owned(),
            }
        })
        .collect()
}

fn query_symbol(source: &str) -> String {
    let (_, query) = source.split_once("symbol=").unwrap();
    query.split('&').next().unwrap().to_owned()
}

fn dec(value: &Value) -> Decimal {
    Decimal::from_str(value.as_str().unwrap()).unwrap()
}

struct Checkpoint {
    last_update_id: u64,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
}

fn checkpoint(raw: &str) -> Checkpoint {
    let value: Value = serde_json::from_str(raw).unwrap();
    let levels = |side: &str| {
        value[side]
            .as_array()
            .unwrap()
            .iter()
            .take(CHECK_DEPTH)
            .map(|level| (dec(&level[0]), dec(&level[1])))
            .collect()
    };
    Checkpoint {
        last_update_id: value["lastUpdateId"].as_u64().unwrap(),
        bids: levels("bids"),
        asks: levels("asks"),
    }
}

fn mismatches(book: &OrderBook, checkpoint: &Checkpoint, touched: &HashSet<Decimal>) -> usize {
    let sides = [
        (book.bids_as_map(None), &checkpoint.bids, true),
        (book.asks_as_map(None), &checkpoint.asks, false),
    ];
    let mut count = 0;
    for (ours, theirs, is_bid) in sides {
        let Some(&(worst, _)) = theirs.last() else {
            continue;
        };
        let in_range = |price: Decimal| {
            if is_bid {
                price >= worst
            } else {
                price <= worst
            }
        };
        let theirs: HashMap<Decimal, Decimal> = theirs.iter().copied().collect();
        for (price, size) in &theirs {
            if !touched.contains(price) && ours.get(price) != Some(size) {
                count += 1;
            }
        }
        for (price, _) in &ours {
            if in_range(*price) && !touched.contains(price) && !theirs.contains_key(price) {
                count += 1;
            }
        }
    }
    count
}

#[derive(Default)]
struct SymbolStats {
    deltas: usize,
    trades: usize,
    quotes: usize,
    marks: usize,
    snapshots_applied: usize,
    checkpoints: usize,
    exact_checkpoints: usize,
    checkpoint_mismatches: usize,
    crossed: usize,
    minutes: BTreeMap<u64, (Decimal, u64)>,
}

#[test]
fn replay_capture_matches_venue() {
    let Some(path) = capture_path() else {
        eprintln!("no capture found; run `cargo run -p nt-dex-capture` to create one");
        return;
    };
    let records = read_capture(&path);
    eprintln!(
        "replaying {} records from {}",
        records.len(),
        path.display()
    );

    let exchange_info = records
        .iter()
        .find(|r| r.source.ends_with("exchangeInfo"))
        .unwrap();
    let parsed = parse_exchange_info(&exchange_info.raw, exchange_info.ts).unwrap();
    assert!(
        parsed.failed.is_empty(),
        "unparsed instruments: {:?}",
        parsed.failed
    );
    eprintln!(
        "instruments: {} loaded, {} skipped (not trading perpetuals)",
        parsed.instruments.len(),
        parsed.skipped.len()
    );

    let mut checkpoints: HashMap<String, VecDeque<Checkpoint>> = HashMap::new();
    let mut symbols = Vec::new();
    for record in records
        .iter()
        .filter(|r| r.kind == "rest" && r.source.contains("/depth?"))
    {
        let symbol = query_symbol(&record.source);
        if !symbols.contains(&symbol) {
            symbols.push(symbol.clone());
        }
        checkpoints
            .entry(symbol)
            .or_default()
            .push_back(checkpoint(&record.raw));
    }

    let mut feed = AsterFeed::new(&parsed.instruments);
    let mut books: HashMap<String, OrderBook> = HashMap::new();
    let mut by_instrument = HashMap::new();
    for symbol in &symbols {
        let instrument = parsed
            .instruments
            .iter()
            .find(|instrument| instrument.raw_symbol().as_str() == symbol)
            .unwrap_or_else(|| panic!("{symbol} missing from exchangeInfo"));
        books.insert(
            symbol.clone(),
            OrderBook::new(instrument.id(), BookType::L2_MBP),
        );
        by_instrument.insert(instrument.id(), symbol.clone());
    }

    let mut stats: HashMap<String, SymbolStats> = HashMap::new();
    let mut awaiting_snapshot: HashSet<String> = HashSet::new();
    let mut events = Vec::new();
    let mut sockets_opened = 0;
    let (mut first_ws_ms, mut last_ws_ms) = (u64::MAX, 0);

    for record in &records {
        match (record.kind.as_str(), record.source.as_str()) {
            ("meta", "ws_open") => {
                sockets_opened += 1;
                if sockets_opened > 1 {
                    feed.reset_books();
                }
            }
            ("ws", _) => {
                let ms = record.ts.as_u64() / 1_000_000;
                first_ws_ms = first_ws_ms.min(ms);
                last_ws_ms = last_ws_ms.max(ms);
                feed.handle_text(&record.raw, record.ts, &mut events)
                    .unwrap_or_else(|error| panic!("{error:#}\nframe: {}", record.raw));
                let is_agg = record.raw.contains("\"e\":\"aggTrade\"");
                if is_agg || record.raw.contains("\"e\":\"trade\"") {
                    let frame: Value = serde_json::from_str(&record.raw).unwrap();
                    let data = &frame["data"];
                    let minute = data["T"].as_u64().unwrap() / MINUTE_MS * MINUTE_MS;
                    let fills = if is_agg {
                        data["l"].as_u64().unwrap() - data["f"].as_u64().unwrap() + 1
                    } else {
                        1
                    };
                    stats
                        .entry(data["s"].as_str().unwrap().to_owned())
                        .or_default()
                        .minutes
                        .entry(minute)
                        .or_default()
                        .1 += fills;
                }
            }
            ("rest", source) if source.contains("/depth?") => {
                let symbol = query_symbol(source);
                if awaiting_snapshot.remove(&symbol) {
                    checkpoints.get_mut(&symbol).unwrap().pop_front();
                    feed.handle_depth_snapshot(&symbol, &record.raw, record.ts, &mut events)
                        .unwrap();
                    stats.entry(symbol).or_default().snapshots_applied += 1;
                }
            }
            _ => {}
        }

        for event in events.drain(..) {
            match event {
                FeedEvent::SnapshotRequired(symbol) => {
                    awaiting_snapshot.insert(symbol.to_string());
                }
                FeedEvent::Data(Data::BookDeltas(deltas)) => {
                    let symbol = &by_instrument[&deltas.instrument_id];
                    let book = books.get_mut(symbol).unwrap();
                    let stat = stats.entry(symbol.clone()).or_default();

                    let queue = checkpoints.get_mut(symbol).unwrap();
                    while queue
                        .front()
                        .is_some_and(|c| c.last_update_id < deltas.sequence)
                    {
                        let checkpoint = queue.pop_front().unwrap();
                        let touched: HashSet<Decimal> = deltas
                            .deltas
                            .iter()
                            .map(|d| d.order.price.as_decimal())
                            .collect();
                        stat.checkpoints += 1;
                        stat.checkpoint_mismatches += mismatches(book, &checkpoint, &touched);
                    }

                    book.apply_deltas(&deltas).unwrap();
                    stat.deltas += deltas.deltas.len();

                    while queue
                        .front()
                        .is_some_and(|c| c.last_update_id == deltas.sequence)
                    {
                        let checkpoint = queue.pop_front().unwrap();
                        stat.checkpoints += 1;
                        stat.exact_checkpoints += 1;
                        stat.checkpoint_mismatches +=
                            mismatches(book, &checkpoint, &HashSet::new());
                    }
                    if let (Some(bid), Some(ask)) = (book.best_bid_price(), book.best_ask_price())
                        && bid >= ask
                    {
                        stat.crossed += 1;
                    }
                }
                FeedEvent::Data(Data::Trade(trade)) => {
                    let stat = stats
                        .entry(by_instrument[&trade.instrument_id].clone())
                        .or_default();
                    stat.trades += 1;
                    let minute = trade.ts_event.as_u64() / 1_000_000 / MINUTE_MS * MINUTE_MS;
                    stat.minutes.entry(minute).or_default().0 += trade.size.as_decimal();
                }
                FeedEvent::Data(Data::Quote(quote)) => {
                    assert!(quote.bid_price < quote.ask_price, "crossed quote: {quote}");
                    stats
                        .entry(by_instrument[&quote.instrument_id].clone())
                        .or_default()
                        .quotes += 1;
                }
                FeedEvent::Data(Data::MarkPrice(mark)) => {
                    stats
                        .entry(by_instrument[&mark.instrument_id].clone())
                        .or_default()
                        .marks += 1;
                }
                FeedEvent::Data(_) => {}
            }
        }
    }

    let mut minutes_checked = 0;
    for record in records
        .iter()
        .filter(|r| r.kind == "rest" && r.source.contains("/klines?"))
    {
        let symbol = query_symbol(&record.source);
        let klines: Value = serde_json::from_str(&record.raw).unwrap();
        for kline in klines.as_array().unwrap() {
            let open = kline[0].as_u64().unwrap();
            let close = kline[6].as_u64().unwrap();
            let fetched_ms = record.ts.as_u64() / 1_000_000;
            if open < first_ws_ms || close >= last_ws_ms || close + KLINE_SETTLE_MS > fetched_ms {
                continue;
            }
            let ours = stats[&symbol]
                .minutes
                .get(&open)
                .copied()
                .unwrap_or_default();
            let theirs = (dec(&kline[5]), kline[8].as_u64().unwrap());
            assert_eq!(
                ours, theirs,
                "{symbol} minute {open}: (volume, trades) differ from kline"
            );
            minutes_checked += 1;
        }
    }

    for symbol in &symbols {
        let stat = &stats[symbol];
        eprintln!(
            "{symbol}: {} deltas, {} trades, {} quotes, {} marks, {} snapshots applied, \
             {} checkpoints ({} exact, {} mismatched levels), {} crossed, book live: {}",
            stat.deltas,
            stat.trades,
            stat.quotes,
            stat.marks,
            stat.snapshots_applied,
            stat.checkpoints,
            stat.exact_checkpoints,
            stat.checkpoint_mismatches,
            stat.crossed,
            feed.is_book_live(symbol),
        );
        assert!(feed.is_book_live(symbol), "{symbol} book never synced");
        assert!(
            stat.deltas > 0 && stat.quotes > 0 && stat.marks > 0,
            "{symbol} missing data"
        );
        assert_eq!(
            stat.snapshots_applied, sockets_opened,
            "{symbol} resynced unexpectedly"
        );
        assert!(
            stat.checkpoints > 0,
            "{symbol} has no depth checkpoint to verify the book against; \
             capture longer or lower --snapshot-secs"
        );
        assert_eq!(stat.crossed, 0, "{symbol} book crossed");
        assert_eq!(
            stat.checkpoint_mismatches, 0,
            "{symbol} book diverged from venue snapshot"
        );
    }
    eprintln!("{minutes_checked} symbol-minutes reconciled against klines");

    let total_trades: usize = stats.values().map(|stat| stat.trades).sum();
    assert!(total_trades > 0, "capture contains no trades");
    if last_ws_ms.saturating_sub(first_ws_ms) > 4 * MINUTE_MS {
        assert!(
            minutes_checked > 0,
            "capture has no settled klines to reconcile trades against"
        );
    }
}
