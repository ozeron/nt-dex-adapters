//! Converts market-stream payloads into Nautilus data.
//!
//! Parsers are pure: the receive timestamp is passed in as `ts_init`.

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{
        BookOrder, Data, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate, OrderBookDelta,
        OrderBookDeltas, QuoteTick, TradeTick,
    },
    enums::{BookAction, OrderSide},
    identifiers::InstrumentId,
    types::{Price, Quantity},
};

use super::messages::{AggTrade, BookTicker, DepthUpdate, InlineDepthUpdate, MarkPrice, Trade};
use crate::common::{self, REFERENCE_PRICE_PRECISION, RawTrade};

/// The identity and precisions needed to parse an instrument's messages.
#[derive(Clone, Copy, Debug)]
pub struct InstrumentSpec {
    pub id: InstrumentId,
    pub price_precision: u8,
    pub size_precision: u8,
    /// Minutes between funding payments, when the venue's `fundingInfo`
    /// lists the symbol. It varies by symbol: 1, 2, 4, or 8 hours.
    pub funding_interval_mins: Option<u16>,
}

/// Parses a depth diff into deltas, or `None` when it changes no level.
///
/// A zero quantity deletes the level; any other quantity replaces it. The
/// sequence is the diff's final update id.
///
/// # Errors
///
/// Returns an error if a level or timestamp does not parse.
pub fn parse_depth_update(
    update: &DepthUpdate,
    spec: InstrumentSpec,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<OrderBookDeltas>> {
    build_depth_update(update, spec, ts_init)
}

pub(crate) fn parse_inline_depth_update(
    update: &InlineDepthUpdate<'_>,
    spec: InstrumentSpec,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<OrderBookDeltas>> {
    build_depth_update(update, spec, ts_init)
}

pub(crate) fn validate_inline_depth_update(
    update: &InlineDepthUpdate<'_>,
    spec: InstrumentSpec,
) -> anyhow::Result<()> {
    common::millis(update.transaction_time)?;
    visit_levels(update, spec, |_, _, _| {})
}

fn visit_levels<'a>(
    update: &DepthUpdate<'a, impl AsRef<[[&'a str; 2]]>>,
    spec: InstrumentSpec,
    mut visit: impl FnMut(OrderSide, Price, Quantity),
) -> anyhow::Result<()> {
    for (side, levels) in [
        (OrderSide::Buy, update.bids.as_ref()),
        (OrderSide::Sell, update.asks.as_ref()),
    ] {
        for [price, size] in levels {
            let quantity = common::exact_quantity(size, spec.size_precision)?;
            let price = common::exact_price(price, spec.price_precision)?;
            visit(side, price, quantity);
        }
    }
    Ok(())
}

fn build_depth_update<'a>(
    update: &DepthUpdate<'a, impl AsRef<[[&'a str; 2]]>>,
    spec: InstrumentSpec,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<OrderBookDeltas>> {
    let ts_event = common::millis(update.transaction_time)?;
    let sequence = update.final_update_id;

    let level_count = update
        .bids
        .as_ref()
        .len()
        .saturating_add(update.asks.as_ref().len());
    let mut deltas = Vec::with_capacity(level_count);
    visit_levels(update, spec, |side, price, size| {
        let action = if size.is_zero() {
            BookAction::Delete
        } else {
            BookAction::Update
        };
        let order = BookOrder::new(side, price, size, 0);
        deltas.push(OrderBookDelta::new(
            spec.id, action, order, 0, sequence, ts_event, ts_init,
        ));
    })?;
    let Some(last) = deltas.last_mut() else {
        return Ok(None);
    };
    last.flags |= common::F_LAST;
    Ok(Some(OrderBookDeltas::new(spec.id, deltas)))
}

/// Parses an aggregate trade.
///
/// # Errors
///
/// Returns an error if the price, quantity, or timestamp does not parse.
pub fn parse_agg_trade(
    trade: &AggTrade,
    spec: InstrumentSpec,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    let raw = RawTrade {
        id: trade.agg_trade_id,
        price: trade.price,
        quantity: trade.quantity,
        buyer_is_maker: trade.buyer_is_maker,
        time_ms: trade.trade_time,
    };
    trade_tick(&raw, spec, ts_init)
}

fn trade_tick(
    raw: &RawTrade,
    spec: InstrumentSpec,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    common::trade_tick(
        raw,
        spec.id,
        spec.price_precision,
        spec.size_precision,
        ts_init,
    )
}

/// Parses an individual trade. Its id is the venue's trade id.
///
/// # Errors
///
/// Returns an error if the price, quantity, or timestamp does not parse, or
/// if the size is zero.
pub fn parse_trade(
    trade: &Trade,
    spec: InstrumentSpec,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    let raw = RawTrade {
        id: trade.trade_id,
        price: trade.price,
        quantity: trade.quantity,
        buyer_is_maker: trade.buyer_is_maker,
        time_ms: trade.trade_time,
    };
    trade_tick(&raw, spec, ts_init)
}

/// Parses a best bid/ask update.
///
/// # Errors
///
/// Returns an error if a price, quantity, or timestamp does not parse.
pub fn parse_book_ticker(
    ticker: &BookTicker,
    spec: InstrumentSpec,
    ts_init: UnixNanos,
) -> anyhow::Result<QuoteTick> {
    Ok(QuoteTick::new(
        spec.id,
        common::exact_price(ticker.bid_price, spec.price_precision)?,
        common::exact_price(ticker.ask_price, spec.price_precision)?,
        common::exact_quantity(ticker.bid_quantity, spec.size_precision)?,
        common::exact_quantity(ticker.ask_quantity, spec.size_precision)?,
        common::millis(ticker.transaction_time)?,
        ts_init,
    ))
}

/// Mark price, index price, and funding rate from one `markPrice` message.
#[derive(Debug)]
pub struct MarkPriceData {
    pub mark: MarkPriceUpdate,
    pub index: IndexPriceUpdate,
    pub funding: FundingRateUpdate,
}

/// The three values of one `markPrice` message, each parsed on its own so a
/// bad field does not hide the others.
#[derive(Debug)]
pub struct MarkPriceParts {
    pub mark: anyhow::Result<MarkPriceUpdate>,
    pub index: anyhow::Result<IndexPriceUpdate>,
    pub funding: anyhow::Result<FundingRateUpdate>,
    /// The mark price had digits beyond eight decimals and was rounded.
    pub mark_rounded: bool,
    /// The index price had digits beyond eight decimals and was rounded.
    pub index_rounded: bool,
}

impl MarkPriceParts {
    /// The values as data to publish, in mark, index, funding order.
    pub fn into_results(self) -> [anyhow::Result<Data>; 3] {
        [
            self.mark.map(Data::MarkPrice),
            self.index.map(Data::IndexPrice),
            self.funding.map(Data::FundingRate),
        ]
    }
}

/// Parses a mark price message one field at a time.
///
/// Mark and index prices are kept at eight decimals, finer than the
/// instrument's tick. A value with more digits is rounded half-even, flagged
/// in the result and logged at warn: the flag is testable and the log reaches
/// callers that ignore it, while the parser stays free of clocks and sockets.
///
/// # Errors
///
/// Returns an error only if the event time overflows, since all three values
/// carry it. A price, rate, or next funding time that does not parse fails
/// just its own field.
pub fn parse_mark_price_parts(
    message: &MarkPrice,
    spec: InstrumentSpec,
    ts_init: UnixNanos,
) -> anyhow::Result<MarkPriceParts> {
    let ts_event = common::millis(message.event_time)?;
    let reference_price = |field: &str, value: &str| {
        let parsed = common::reference_price(value);
        if let Ok(reference) = &parsed
            && reference.rounded
        {
            log::warn!(
                "Aster {} {field} '{value}' has more than {REFERENCE_PRICE_PRECISION} decimals; rounded to {}",
                message.symbol,
                reference.price
            );
        }
        parsed
    };
    let mark = reference_price("mark price", message.mark_price);
    let index = reference_price("index price", message.index_price);
    let funding = || {
        let next_funding = (message.next_funding_time > 0)
            .then(|| common::millis(message.next_funding_time))
            .transpose()?;
        Ok(FundingRateUpdate::new(
            spec.id,
            common::decimal(message.funding_rate).context("invalid funding rate")?,
            spec.funding_interval_mins,
            next_funding,
            ts_event,
            ts_init,
        ))
    };
    Ok(MarkPriceParts {
        mark_rounded: mark.as_ref().is_ok_and(|reference| reference.rounded),
        index_rounded: index.as_ref().is_ok_and(|reference| reference.rounded),
        mark: mark
            .map(|reference| MarkPriceUpdate::new(spec.id, reference.price, ts_event, ts_init)),
        index: index
            .map(|reference| IndexPriceUpdate::new(spec.id, reference.price, ts_event, ts_init)),
        funding: funding(),
    })
}

/// Parses a mark price message.
///
/// See [`parse_mark_price_parts`] for the precision rule. This fails when any
/// one field does.
///
/// # Errors
///
/// Returns an error if a price, rate, or timestamp does not parse.
pub fn parse_mark_price(
    message: &MarkPrice,
    spec: InstrumentSpec,
    ts_init: UnixNanos,
) -> anyhow::Result<MarkPriceData> {
    let parts = parse_mark_price_parts(message, spec, ts_init)?;
    Ok(MarkPriceData {
        mark: parts.mark?,
        index: parts.index?,
        funding: parts.funding?,
    })
}
