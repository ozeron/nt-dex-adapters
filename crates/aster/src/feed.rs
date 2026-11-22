//! The venue feed as a pure state machine: text frames and depth snapshots
//! in, Nautilus data out.
//!
//! The live data client and the capture replay tests both drive this type,
//! so what is validated offline is what runs online.

use ahash::AHashMap;
use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Data, OrderBookDeltas},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use ustr::Ustr;

use crate::{
    book::{Action, BookSync, DepthDiff},
    common,
    http::parse::parse_depth_snapshot,
    websocket::{
        messages::{AggTrade, BookTicker, DepthUpdate, Envelope, MarkPrice, Trade},
        parse::{
            InstrumentSpec, parse_agg_trade, parse_book_ticker, parse_depth_update,
            parse_mark_price_parts, parse_trade,
        },
    },
};

/// Something the feed produced while handling an input.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "data is the common case; boxing it would allocate per event"
)]
pub enum FeedEvent {
    /// Data to publish.
    Data(Data),
    /// The book for this venue symbol needs a REST depth snapshot, to be
    /// passed to [`AsterFeed::handle_depth_snapshot`].
    SnapshotRequired(Ustr),
}

#[derive(Debug)]
struct Market {
    spec: InstrumentSpec,
    book: BookSync,
    /// The latest `ts_init` given to this book's deltas. A snapshot is
    /// stamped when it is applied, which can be after a newer diff left the
    /// socket; later deltas are held to it so the book replays in order.
    book_ts_floor: UnixNanos,
    /// The last individual trade published. The venue numbers them
    /// consecutively per symbol.
    last_trade_id: Option<u64>,
    /// The last aggregate trade published, from the separate aggregate id
    /// space. Only old captures carry these.
    last_agg_trade_id: Option<u64>,
}

/// Checks a trade id against the last one published for its symbol and
/// records it. Returns `false` for a trade already published, for instance
/// by a stream that was briefly open on two connections. An id that skips
/// ahead is logged and added to `missed`, as happens across a reconnect.
fn track_trade_id(
    last: &mut Option<u64>,
    id: u64,
    symbol: &str,
    what: &str,
    missed: &mut u64,
) -> bool {
    match *last {
        Some(previous) if id <= previous => return false,
        Some(previous) if id > previous.saturating_add(1) => {
            let gap = id.saturating_sub(previous).saturating_sub(1);
            *missed = missed.saturating_add(gap);
            log::warn!("Aster missed {gap} {what} for {symbol} between ids {previous} and {id}");
        }
        _ => {}
    }
    *last = Some(id);
    true
}

fn market<'a>(
    markets: &'a mut AHashMap<Ustr, Market>,
    raw_symbol: &str,
) -> anyhow::Result<&'a mut Market> {
    markets
        .get_mut(&Ustr::from(raw_symbol))
        .with_context(|| format!("no instrument loaded for symbol {raw_symbol}"))
}

/// Parses market-stream frames and keeps each book in sequence.
#[derive(Debug, Default)]
pub struct AsterFeed {
    markets: AHashMap<Ustr, Market>,
    deltas: Vec<OrderBookDeltas>,
    missed_trades: u64,
}

impl AsterFeed {
    /// Creates a feed that knows the given instruments.
    #[must_use]
    pub fn new(instruments: &[InstrumentAny]) -> Self {
        let mut feed = Self::default();
        for instrument in instruments {
            feed.add_instrument(instrument);
        }
        feed
    }

    /// Registers an instrument, keeping existing book state if it is known.
    pub fn add_instrument(&mut self, instrument: &InstrumentAny) {
        let spec = InstrumentSpec {
            id: instrument.id(),
            price_precision: instrument.price_precision(),
            size_precision: instrument.size_precision(),
            funding_interval_mins: None,
        };
        self.markets
            .entry(instrument.raw_symbol().inner())
            .and_modify(|market| {
                market.spec.price_precision = spec.price_precision;
                market.spec.size_precision = spec.size_precision;
            })
            .or_insert_with(|| Market {
                spec,
                book: BookSync::new(),
                book_ts_floor: UnixNanos::default(),
                last_trade_id: None,
                last_agg_trade_id: None,
            });
    }

    /// Replaces every instrument's funding interval with the venue's current
    /// list. A symbol the list no longer has loses its interval.
    pub fn replace_funding_intervals(&mut self, intervals: &[(String, u16)]) {
        for market in self.markets.values_mut() {
            market.spec.funding_interval_mins = None;
        }
        self.set_funding_intervals(intervals);
    }

    /// Sets the minutes between funding payments for the listed venue
    /// symbols, as parsed from `fundingInfo`. Unknown symbols are ignored.
    pub fn set_funding_intervals(&mut self, intervals: &[(String, u16)]) {
        for (symbol, minutes) in intervals {
            if let Some(market) = self.markets.get_mut(&Ustr::from(symbol)) {
                market.spec.funding_interval_mins = Some(*minutes);
            }
        }
    }

    /// Trades known to be missing: the venue's ids skipped ahead, as they do
    /// across a reconnect. Counts individual and aggregate trades together.
    #[must_use]
    pub const fn missed_trades(&self) -> u64 {
        self.missed_trades
    }

    /// Returns the instrument id for a venue symbol, if it is known.
    #[must_use]
    pub fn instrument_id(&self, raw_symbol: &str) -> Option<InstrumentId> {
        self.markets
            .get(&Ustr::from(raw_symbol))
            .map(|market| market.spec.id)
    }

    /// Returns `true` when the symbol's book is in sequence.
    #[must_use]
    pub fn is_book_live(&self, raw_symbol: &str) -> bool {
        self.markets
            .get(&Ustr::from(raw_symbol))
            .is_some_and(|market| market.book.is_live())
    }

    /// Venue symbols whose book lost sequence and is still waiting for a
    /// snapshot. Their last published state is stale.
    #[must_use]
    pub fn books_awaiting_snapshot(&self) -> Vec<Ustr> {
        let mut symbols: Vec<Ustr> = self
            .markets
            .iter()
            .filter(|(_, market)| market.book.is_awaiting_snapshot())
            .map(|(symbol, _)| *symbol)
            .collect();
        symbols.sort_unstable();
        symbols
    }

    /// Forgets all book progress, as after a reconnect. Each book requests a
    /// fresh snapshot on its next diff.
    pub fn reset_books(&mut self) {
        for market in self.markets.values_mut() {
            market.book.reset();
        }
    }

    /// Forgets one book's progress, as when its subscription is dropped.
    pub fn reset_book(&mut self, raw_symbol: &str) {
        if let Some(market) = self.markets.get_mut(&Ustr::from(raw_symbol)) {
            market.book.reset();
        }
    }

    fn drain_deltas(&mut self, action: &Action, symbol: &str, out: &mut Vec<FeedEvent>) {
        out.extend(
            self.deltas
                .drain(..)
                .map(|deltas| FeedEvent::Data(Data::BookDeltas(Box::new(deltas)))),
        );
        if *action == Action::RequestSnapshot {
            out.push(FeedEvent::SnapshotRequired(Ustr::from(symbol)));
        }
    }

    /// Handles one text frame from the combined stream.
    ///
    /// Control replies and streams the adapter does not consume are ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if the frame is malformed or names an unknown
    /// symbol. The feed stays usable: one bad frame does not poison it.
    pub fn handle_text(
        &mut self,
        text: &str,
        ts_init: UnixNanos,
        out: &mut Vec<FeedEvent>,
    ) -> anyhow::Result<()> {
        let envelope: Envelope = serde_json::from_str(text).context("invalid stream frame")?;
        let (Some(stream), Some(data)) = (envelope.stream, envelope.data) else {
            return Ok(());
        };
        let payload = data.get();
        let channel = stream.split('@').nth(1).unwrap_or_default();

        match channel {
            "depth" => {
                let update: DepthUpdate = serde_json::from_str(payload).context("invalid depth")?;
                let market = market(&mut self.markets, update.symbol)?;
                let diff = DepthDiff {
                    final_update_id: update.final_update_id,
                    prev_final_update_id: update.prev_final_update_id,
                    deltas: parse_depth_update(
                        &update,
                        market.spec,
                        ts_init.max(market.book_ts_floor),
                    )?,
                };
                let action = market.book.on_diff(diff, &mut self.deltas);
                self.drain_deltas(&action, update.symbol, out);
            }
            "trade" => {
                let trade: Trade = serde_json::from_str(payload).context("invalid trade")?;
                let market = market(&mut self.markets, trade.symbol)?;
                let tick = parse_trade(&trade, market.spec, ts_init)?;
                if track_trade_id(
                    &mut market.last_trade_id,
                    trade.trade_id,
                    trade.symbol,
                    "trades",
                    &mut self.missed_trades,
                ) {
                    out.push(FeedEvent::Data(Data::Trade(tick)));
                }
            }
            "aggTrade" => {
                let trade: AggTrade = serde_json::from_str(payload).context("invalid aggTrade")?;
                let market = market(&mut self.markets, trade.symbol)?;
                let tick = parse_agg_trade(&trade, market.spec, ts_init)?;
                if track_trade_id(
                    &mut market.last_agg_trade_id,
                    trade.agg_trade_id,
                    trade.symbol,
                    "aggregate trades",
                    &mut self.missed_trades,
                ) {
                    out.push(FeedEvent::Data(Data::Trade(tick)));
                }
            }
            "bookTicker" => {
                let ticker: BookTicker =
                    serde_json::from_str(payload).context("invalid bookTicker")?;
                let spec = market(&mut self.markets, ticker.symbol)?.spec;
                out.push(FeedEvent::Data(Data::Quote(parse_book_ticker(
                    &ticker, spec, ts_init,
                )?)));
            }
            "markPrice" => {
                let message: MarkPrice =
                    serde_json::from_str(payload).context("invalid markPrice")?;
                let spec = market(&mut self.markets, message.symbol)?.spec;
                // One bad field does not cost the other two.
                let parts = parse_mark_price_parts(&message, spec, ts_init)?;
                for result in parts.into_results() {
                    match result {
                        Ok(data) => out.push(FeedEvent::Data(data)),
                        Err(error) => {
                            log::warn!(
                                "Aster {} markPrice field dropped: {error:#}",
                                message.symbol
                            );
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Handles a REST depth snapshot body for a venue symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the symbol is unknown or the body does not parse.
    pub fn handle_depth_snapshot(
        &mut self,
        raw_symbol: &str,
        body: &str,
        ts_init: UnixNanos,
        out: &mut Vec<FeedEvent>,
    ) -> anyhow::Result<()> {
        let market = market(&mut self.markets, raw_symbol)?;
        let spec = market.spec;
        let ts_init = ts_init.max(market.book_ts_floor);
        market.book_ts_floor = ts_init;
        let snapshot = parse_depth_snapshot(
            body,
            spec.id,
            spec.price_precision,
            spec.size_precision,
            ts_init,
        )?;
        let action =
            market
                .book
                .on_snapshot(snapshot.last_update_id, snapshot.deltas, &mut self.deltas);
        // Diffs buffered while waiting enter the system now, with the snapshot.
        // Restamping keeps `ts_init` non-decreasing, which the catalog requires.
        for deltas in &mut self.deltas {
            if deltas.ts_init < ts_init {
                deltas.ts_init = ts_init;
                for delta in &mut deltas.deltas {
                    delta.ts_init = ts_init;
                }
            }
        }
        self.drain_deltas(&action, raw_symbol, out);
        Ok(())
    }
}

/// Returns the venue symbol for an instrument id, as used in stream names.
#[must_use]
pub fn stream_symbol(instrument_id: &InstrumentId) -> String {
    common::raw_symbol(instrument_id).to_lowercase()
}
