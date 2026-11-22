//! Catalog tooling for the DEX adapters.
//!
//! - `import-capture <capture.jsonl> <catalog-dir>` replays an
//!   `nt-dex-capture` session through the Aster adapter into a Parquet
//!   catalog.
//! - `record <catalog-dir> [--minutes N] [--rotate-secs N] (--all | SYMBOL...)` runs
//!   the Aster data client live and streams everything it publishes into a
//!   Parquet catalog.
//! - `summary <catalog-dir>` prints what a catalog holds.

#![expect(clippy::print_stdout, reason = "command-line tool reporting results")]

mod record;

use std::{collections::BTreeMap, fs::File, io::BufReader, path::Path, time::Duration};

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{IndexPriceUpdate, MarkPriceUpdate, OrderBookDelta},
    enums::BookType,
    identifiers::InstrumentId,
    instruments::Instrument,
    orderbook::OrderBook,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nt_dex_aster::common::F_LAST;
use nt_dex_aster::replay::replay_capture;

const USAGE: &str = "usage:
  nt-dex-node import-capture <capture.jsonl> <catalog-dir>
  nt-dex-node record <catalog-dir> [--minutes N] [--rotate-secs N] (--all | SYMBOL...)
  nt-dex-node summary <catalog-dir>";

fn open_catalog(dir: &str) -> ParquetDataCatalog {
    ParquetDataCatalog::new(Path::new(dir), None, None, None, None)
}

fn import_capture(capture: &str, catalog_dir: &str) -> anyhow::Result<()> {
    let file = File::open(capture).with_context(|| format!("cannot open {capture}"))?;
    let replay = replay_capture(BufReader::new(file))?;
    anyhow::ensure!(
        replay.rejected.is_empty(),
        "{} frames rejected, first: {:?}",
        replay.rejected.len(),
        replay.rejected.first()
    );

    anyhow::ensure!(
        replay.unsynced_books.is_empty(),
        "books out of sequence at the end of the capture, deltas would stop at a gap: {:?}",
        replay.unsynced_books
    );

    std::fs::create_dir_all(catalog_dir)?;
    let catalog = open_catalog(catalog_dir);
    let instrument_count = replay.instruments.len();
    catalog.write_instruments(replay.instruments)?;
    catalog.write_data_enum(&replay.data, None, None, None)?;
    println!(
        "imported {instrument_count} instruments and {} data events into {catalog_dir}",
        replay.data.len()
    );
    Ok(())
}

/// Per-instrument event counts and the time range they cover.
#[derive(Default)]
struct Tally {
    deltas: usize,
    trades: usize,
    quotes: usize,
    mark: usize,
    index: usize,
    funding: usize,
    first: Option<UnixNanos>,
    last: Option<UnixNanos>,
}

impl Tally {
    fn see(&mut self, ts_event: UnixNanos) {
        self.first = Some(self.first.map_or(ts_event, |first| first.min(ts_event)));
        self.last = Some(self.last.map_or(ts_event, |last| last.max(ts_event)));
    }
}

/// Rebuilds every book from the recorded deltas and counts the completed
/// updates that leave it crossed.
fn crossed_books(deltas: &[OrderBookDelta]) -> anyhow::Result<BTreeMap<InstrumentId, usize>> {
    let mut books: BTreeMap<InstrumentId, OrderBook> = BTreeMap::new();
    let mut crossed: BTreeMap<InstrumentId, usize> = BTreeMap::new();
    for delta in deltas {
        let book = books
            .entry(delta.instrument_id)
            .or_insert_with(|| OrderBook::new(delta.instrument_id, BookType::L2_MBP));
        book.apply_delta(delta)?;
        let count = crossed.entry(delta.instrument_id).or_default();
        if delta.flags & F_LAST != 0
            && let (Some(bid), Some(ask)) = (book.best_bid_price(), book.best_ask_price())
            && bid >= ask
        {
            *count = count.saturating_add(1);
        }
    }
    Ok(crossed)
}

fn summary(catalog_dir: &str) -> anyhow::Result<()> {
    let mut catalog = open_catalog(catalog_dir);
    let instruments = catalog.query_instruments(None)?;
    let deltas = catalog.order_book_deltas(None, None, None)?;
    let trades = catalog.trade_ticks(None, None, None)?;
    let quotes = catalog.quote_ticks(None, None, None)?;
    let mark = catalog.query::<MarkPriceUpdate>(None, None, None, None, None, true)?;
    let index = catalog.query::<IndexPriceUpdate>(None, None, None, None, None, true)?;
    let funding = catalog.funding_rates(None, None, None)?;

    println!("instruments: {}", instruments.len());
    println!("order book deltas: {}", deltas.len());
    println!("trades: {}", trades.len());
    println!("quotes: {}", quotes.len());
    println!("mark prices: {}", mark.len());
    println!("index prices: {}", index.len());
    println!("funding rates: {}", funding.len());

    let mut tallies: BTreeMap<InstrumentId, Tally> = BTreeMap::new();
    macro_rules! tally {
        ($events:expr, $field:ident) => {
            for event in &$events {
                let tally = tallies.entry(event.instrument_id).or_default();
                tally.$field = tally.$field.saturating_add(1);
                tally.see(event.ts_event);
            }
        };
    }
    tally!(deltas, deltas);
    tally!(trades, trades);
    tally!(quotes, quotes);
    tally!(mark, mark);
    tally!(index, index);
    tally!(funding, funding);

    let known: std::collections::BTreeSet<InstrumentId> =
        instruments.iter().map(Instrument::id).collect();
    for (id, tally) in &tallies {
        let range = match (tally.first, tally.last) {
            (Some(first), Some(last)) => format!("{} .. {}", first.to_rfc3339(), last.to_rfc3339()),
            _ => String::new(),
        };
        println!(
            "  {id}: {} deltas, {} trades, {} quotes, {} mark, {} index, {} funding, {range}{}",
            tally.deltas,
            tally.trades,
            tally.quotes,
            tally.mark,
            tally.index,
            tally.funding,
            if known.contains(id) {
                ""
            } else {
                " (no instrument definition!)"
            },
        );
    }

    let crossed: usize = crossed_books(&deltas)?.values().sum();
    println!("crossed book states: {crossed}");
    // Updates must come back in the order the venue issued them, or a
    // rebuilt book is wrong. A snapshot and the updates applied on top of
    // it share a receive time, so this also checks that ties keep order.
    let mut last_sequence: BTreeMap<InstrumentId, u64> = BTreeMap::new();
    let mut out_of_order = 0_usize;
    for delta in &deltas {
        let last = last_sequence.entry(delta.instrument_id).or_default();
        if delta.sequence < *last {
            out_of_order = out_of_order.saturating_add(1);
        }
        *last = delta.sequence;
    }
    println!("book deltas out of venue order: {out_of_order}");
    let without_book = tallies.values().filter(|tally| tally.deltas == 0).count();
    println!("instruments with data but no book deltas: {without_book}");

    let mut latest = BTreeMap::new();
    for update in &funding {
        latest.insert(update.instrument_id, update);
    }
    for (id, update) in latest {
        println!(
            "  {id}: funding rate {}, interval {}, next funding {}",
            update.rate,
            update
                .interval
                .map_or_else(|| "unknown".to_owned(), |mins| format!("{mins} min")),
            update
                .next_funding_ns
                .map_or_else(|| "unknown".to_owned(), |ts| ts.to_rfc3339()),
        );
    }
    Ok(())
}

/// Parses `<catalog-dir> [--minutes N] [--rotate-secs N] SYMBOL...`.
fn record_options(args: &[&str]) -> anyhow::Result<record::RecordOptions> {
    let mut args = args.iter().copied();
    let catalog_dir = args.next().context(USAGE)?.to_owned();
    let mut options = record::RecordOptions {
        catalog_dir,
        symbols: Vec::new(),
        all: false,
        duration: None,
        rotate: Duration::from_mins(15),
    };
    while let Some(arg) = args.next() {
        match arg {
            "--minutes" => {
                let minutes: u64 = args.next().context(USAGE)?.parse()?;
                options.duration = Some(Duration::from_secs(minutes.saturating_mul(60)));
            }
            "--all" => options.all = true,
            "--rotate-secs" => {
                let secs: u64 = args.next().context(USAGE)?.parse()?;
                anyhow::ensure!(secs > 0, "--rotate-secs must be positive");
                options.rotate = Duration::from_secs(secs);
            }
            flag if flag.starts_with("--") => anyhow::bail!("unknown option {flag}\n{USAGE}"),
            symbol => options.symbols.push(symbol.to_uppercase()),
        }
    }
    Ok(options)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["import-capture", capture, catalog_dir] => import_capture(capture, catalog_dir),
        ["summary", catalog_dir] => summary(catalog_dir),
        ["record", rest @ ..] => record::record(record_options(rest)?).await,
        _ => anyhow::bail!("{USAGE}"),
    }
}
