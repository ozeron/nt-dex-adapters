//! Converts REST payloads into Nautilus instruments and book snapshots.

use std::str::FromStr;

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BookOrder, FundingRateUpdate, OrderBookDelta, OrderBookDeltas, TradeTick},
    enums::{BookAction, OrderSide},
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

use super::models::{
    DepthSnapshot, ExchangeInfo, FundingInfo, FundingRate, RecentTrade, SymbolFilter, SymbolInfo,
};
use crate::common::{self, RawTrade};

const STATUS_TRADING: &str = "TRADING";
const CONTRACT_PERPETUAL: &str = "PERPETUAL";

/// Instruments parsed from `exchangeInfo`, plus the symbols left out.
#[derive(Debug)]
pub struct ParsedInstruments {
    pub instruments: Vec<InstrumentAny>,
    /// Symbols that are not trading perpetuals (settling, pending, ...).
    pub skipped: Vec<String>,
    /// Trading perpetuals whose definition could not be mapped.
    pub failed: Vec<(String, anyhow::Error)>,
}

/// Parses an `exchangeInfo` body into perpetual instruments.
///
/// Symbols that are not `TRADING` perpetuals are listed in `skipped`. A
/// symbol that should load but has a malformed definition is listed in
/// `failed` rather than failing the whole load.
///
/// # Errors
///
/// Returns an error if `body` is not valid `exchangeInfo` JSON.
pub fn parse_exchange_info(body: &str, ts_init: UnixNanos) -> anyhow::Result<ParsedInstruments> {
    let info: ExchangeInfo = serde_json::from_str(body).context("invalid exchangeInfo")?;
    let mut parsed = ParsedInstruments {
        instruments: Vec::with_capacity(info.symbols.len()),
        skipped: Vec::new(),
        failed: Vec::new(),
    };
    for symbol in info.symbols {
        if symbol.status != STATUS_TRADING || symbol.contract_type != CONTRACT_PERPETUAL {
            parsed.skipped.push(symbol.symbol);
            continue;
        }
        match parse_instrument(&symbol, ts_init) {
            Ok(instrument) => parsed.instruments.push(instrument),
            Err(error) => parsed.failed.push((symbol.symbol, error)),
        }
    }
    Ok(parsed)
}

/// Parses a `fundingInfo` body into minutes between funding payments, by
/// venue symbol.
///
/// # Errors
///
/// Returns an error if `body` is not valid `fundingInfo` JSON.
pub fn parse_funding_intervals(body: &str) -> anyhow::Result<Vec<(String, u16)>> {
    let entries: Vec<FundingInfo> = serde_json::from_str(body).context("invalid fundingInfo")?;
    Ok(entries
        .into_iter()
        .filter_map(|entry| {
            let minutes = entry.funding_interval_hours.checked_mul(60)?;
            Some((entry.symbol, minutes))
        })
        .collect())
}

/// Parses `GET /fapi/v1/fundingRate` into settled funding rates, oldest
/// first.
///
/// A row's interval is the time since the payment before it, to the nearest
/// minute. The venue has changed intervals over time, so today's interval
/// does not describe past payments. `previous_ms` is the payment before the
/// first row when it is known; without it the first row has no interval.
///
/// # Errors
///
/// Returns an error if the body is not valid or a rate does not parse.
pub fn parse_funding_rates(
    body: &str,
    instrument_id: InstrumentId,
    previous_ms: Option<u64>,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<FundingRateUpdate>> {
    const MINUTE_MS: u64 = 60_000;
    let entries: Vec<FundingRate> = serde_json::from_str(body).context("invalid fundingRate")?;
    let mut previous_ms = previous_ms;
    entries
        .into_iter()
        .map(|entry| {
            let interval_mins = previous_ms
                .and_then(|previous| entry.funding_time.checked_sub(previous))
                .map(|elapsed| elapsed.saturating_add(MINUTE_MS / 2) / MINUTE_MS)
                .filter(|minutes| *minutes > 0)
                .and_then(|minutes| u16::try_from(minutes).ok());
            previous_ms = Some(entry.funding_time);
            Ok(FundingRateUpdate::new(
                instrument_id,
                common::decimal(&entry.funding_rate).context("invalid funding rate")?,
                interval_mins,
                None,
                common::millis(entry.funding_time)?,
                ts_init,
            ))
        })
        .collect()
}

/// Parses `GET /fapi/v1/trades` into trade ticks, oldest first.
///
/// # Errors
///
/// Returns an error if the body is not valid or a trade does not parse.
pub fn parse_trades(
    body: &str,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<TradeTick>> {
    let trades: Vec<RecentTrade> = serde_json::from_str(body).context("invalid trades")?;
    trades
        .iter()
        .map(|trade| {
            let raw = RawTrade {
                id: trade.id,
                price: trade.price,
                quantity: trade.qty,
                buyer_is_maker: trade.is_buyer_maker,
                time_ms: trade.time,
            };
            common::trade_tick(
                &raw,
                instrument_id,
                price_precision,
                size_precision,
                ts_init,
            )
        })
        .collect()
}

fn filter<'a>(symbol: &'a SymbolInfo, filter_type: &str) -> anyhow::Result<&'a SymbolFilter> {
    symbol
        .filters
        .iter()
        .find(|filter| filter.filter_type == filter_type)
        .with_context(|| format!("missing {filter_type} filter"))
}

fn required<'a>(value: Option<&'a String>, field: &str) -> anyhow::Result<&'a str> {
    value
        .map(String::as_str)
        .with_context(|| format!("missing {field}"))
}

/// Converts a percent string such as `"5.0000"` into the fraction `0.05`.
fn percent(value: Option<&String>) -> Option<Decimal> {
    let percent = Decimal::from_str(value?).ok()?;
    percent.checked_div(Decimal::ONE_HUNDRED)
}

/// Parses one trading perpetual.
///
/// Precision follows the venue's `pricePrecision` and `quantityPrecision`,
/// which is how prices and sizes are formatted on the wire. `tickSize` and
/// `stepSize` are rescaled to that precision, so a tick of `0.0100` on a
/// four-decimal symbol stays a 0.01 increment.
///
/// # Errors
///
/// Returns an error if a required filter or field is missing or malformed.
pub fn parse_instrument(symbol: &SymbolInfo, ts_init: UnixNanos) -> anyhow::Result<InstrumentAny> {
    let price_precision = symbol.price_precision;
    let size_precision = symbol.quantity_precision;

    let price_filter = filter(symbol, "PRICE_FILTER")?;
    let tick_size = common::price(
        required(price_filter.tick_size.as_ref(), "tickSize")?,
        price_precision,
    )?;
    let max_price = optional_price(price_filter.max_price.as_ref(), price_precision);
    let min_price = optional_price(price_filter.min_price.as_ref(), price_precision);

    let lot_filter = filter(symbol, "LOT_SIZE")?;
    let step_size = common::quantity(
        required(lot_filter.step_size.as_ref(), "stepSize")?,
        size_precision,
    )?;
    let max_quantity = optional_quantity(lot_filter.max_qty.as_ref(), size_precision);
    let min_quantity = optional_quantity(lot_filter.min_qty.as_ref(), size_precision);

    let quote_currency = Currency::get_or_create_crypto(&symbol.quote_asset);
    let min_notional = filter(symbol, "MIN_NOTIONAL")
        .ok()
        .and_then(|filter| filter.notional.as_ref())
        .and_then(|notional| Decimal::from_str(notional).ok())
        .and_then(|notional| Money::from_decimal(notional, quote_currency).ok());

    let instrument = CryptoPerpetual::builder()
        .instrument_id(common::instrument_id(&symbol.symbol))
        .raw_symbol(Symbol::new(&symbol.symbol))
        .base_currency(Currency::get_or_create_crypto(&symbol.base_asset))
        .quote_currency(quote_currency)
        .settlement_currency(Currency::get_or_create_crypto(&symbol.margin_asset))
        .is_inverse(false)
        .price_precision(price_precision)
        .size_precision(size_precision)
        .price_increment(tick_size)
        .size_increment(step_size)
        .lot_size(step_size)
        .maybe_max_quantity(max_quantity)
        .maybe_min_quantity(min_quantity)
        .maybe_min_notional(min_notional)
        .maybe_max_price(max_price)
        .maybe_min_price(min_price)
        .maybe_margin_init(percent(symbol.required_margin_percent.as_ref()))
        .maybe_margin_maint(percent(symbol.maint_margin_percent.as_ref()))
        .ts_event(ts_init)
        .ts_init(ts_init)
        .build()
        .with_context(|| format!("invalid instrument definition for {}", symbol.symbol))?;
    Ok(InstrumentAny::CryptoPerpetual(instrument))
}

fn optional_price(value: Option<&String>, precision: u8) -> Option<Price> {
    common::price(value?, precision).ok()
}

fn optional_quantity(value: Option<&String>, precision: u8) -> Option<Quantity> {
    common::quantity(value?, precision).ok()
}

/// A REST depth snapshot converted to book deltas.
#[derive(Debug)]
pub struct BookSnapshot {
    pub last_update_id: u64,
    /// A clear followed by one add per level, flagged as a snapshot.
    pub deltas: OrderBookDeltas,
}

/// Parses a `depth` body into a book-replacing set of deltas.
///
/// # Errors
///
/// Returns an error if the body is not valid depth JSON or a level does not
/// parse at the given precisions.
pub fn parse_depth_snapshot(
    body: &str,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<BookSnapshot> {
    let snapshot: DepthSnapshot = serde_json::from_str(body).context("invalid depth snapshot")?;
    let sequence = snapshot.last_update_id;
    let ts_event = common::millis(snapshot.transaction_time)?;
    let snapshot_flag = common::F_SNAPSHOT;

    let level_count = snapshot.bids.len().saturating_add(snapshot.asks.len());
    let mut deltas = Vec::with_capacity(level_count.saturating_add(1));
    deltas.push(OrderBookDelta::clear(
        instrument_id,
        sequence,
        ts_event,
        ts_init,
    ));

    let sides = [
        (OrderSide::Buy, &snapshot.bids),
        (OrderSide::Sell, &snapshot.asks),
    ];
    for (side, levels) in sides {
        for [price, size] in levels {
            let size = common::exact_quantity(size, size_precision)?;
            if size.is_zero() {
                continue;
            }
            let order = BookOrder::new(side, common::exact_price(price, price_precision)?, size, 0);
            deltas.push(OrderBookDelta::new(
                instrument_id,
                BookAction::Add,
                order,
                snapshot_flag,
                sequence,
                ts_event,
                ts_init,
            ));
        }
    }
    if let Some(last) = deltas.last_mut() {
        last.flags |= common::F_LAST;
    }
    Ok(BookSnapshot {
        last_update_id: sequence,
        deltas: OrderBookDeltas::new(instrument_id, deltas),
    })
}
