//! Venue constants, symbol mapping, and scalar parsing shared by the parsers.

use std::str::FromStr;

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::TradeTick,
    enums::{AggressorSide, RecordFlag},
    identifiers::{InstrumentId, Symbol, TradeId, Venue},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

pub const ASTER: &str = "ASTER";
pub const HTTP_URL: &str = "https://fapi.asterdex.com";
pub const WS_URL: &str = "wss://fstream.asterdex.com";
#[expect(clippy::as_conversions, reason = "fieldless enum to its u8 repr")]
pub const F_LAST: u8 = RecordFlag::F_LAST as u8;
#[expect(clippy::as_conversions, reason = "fieldless enum to its u8 repr")]
pub const F_SNAPSHOT: u8 = RecordFlag::F_SNAPSHOT as u8;
const PERP_SUFFIX: &str = "-PERP";

#[must_use]
pub fn venue() -> Venue {
    Venue::new(ASTER)
}

#[must_use]
pub fn instrument_id(raw_symbol: &str) -> InstrumentId {
    InstrumentId::new(Symbol::new(format!("{raw_symbol}{PERP_SUFFIX}")), venue())
}

#[must_use]
pub fn raw_symbol(instrument_id: &InstrumentId) -> &str {
    let symbol = instrument_id.symbol.as_str();
    symbol.strip_suffix(PERP_SUFFIX).unwrap_or(symbol)
}

pub fn millis(value: u64) -> anyhow::Result<UnixNanos> {
    value
        .checked_mul(1_000_000)
        .map(UnixNanos::from)
        .with_context(|| format!("timestamp {value} ms overflows nanoseconds"))
}

pub const REFERENCE_PRICE_PRECISION: u8 = 8;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReferencePrice {
    pub price: Price,
    pub rounded: bool,
}

pub fn reference_price(value: &str) -> anyhow::Result<ReferencePrice> {
    let parsed = decimal(value)?;
    let price = Price::from_decimal_dp(parsed, REFERENCE_PRICE_PRECISION).with_context(|| {
        format!("invalid price '{value}' at precision {REFERENCE_PRICE_PRECISION}")
    })?;
    Ok(ReferencePrice {
        price,
        rounded: parsed.normalize().scale() > u32::from(REFERENCE_PRICE_PRECISION),
    })
}

pub fn decimal(value: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str(value).with_context(|| format!("invalid decimal '{value}'"))
}

pub fn price(value: &str, precision: u8) -> anyhow::Result<Price> {
    Price::from_decimal_dp(decimal(value)?, precision)
        .with_context(|| format!("invalid price '{value}' at precision {precision}"))
}

pub fn quantity(value: &str, precision: u8) -> anyhow::Result<Quantity> {
    Quantity::from_decimal_dp(decimal(value)?, precision)
        .with_context(|| format!("invalid quantity '{value}' at precision {precision}"))
}

fn exact_decimal(value: &str, precision: u8) -> anyhow::Result<Decimal> {
    let parsed = decimal(value)?;
    anyhow::ensure!(
        parsed.normalize().scale() <= u32::from(precision),
        "'{value}' has more decimals than the instrument's precision {precision}"
    );
    Ok(parsed)
}

pub fn exact_price(value: &str, precision: u8) -> anyhow::Result<Price> {
    Price::from_decimal_dp(exact_decimal(value, precision)?, precision)
        .with_context(|| format!("invalid price '{value}' at precision {precision}"))
}

pub fn exact_quantity(value: &str, precision: u8) -> anyhow::Result<Quantity> {
    Quantity::from_decimal_dp(exact_decimal(value, precision)?, precision)
        .with_context(|| format!("invalid quantity '{value}' at precision {precision}"))
}

#[derive(Clone, Copy, Debug)]
pub struct RawTrade<'a> {
    pub id: u64,
    pub price: &'a str,
    pub quantity: &'a str,
    pub buyer_is_maker: bool,
    pub time_ms: u64,
}

pub fn trade_tick(
    trade: &RawTrade,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    let aggressor_side = if trade.buyer_is_maker {
        AggressorSide::Sell
    } else {
        AggressorSide::Buy
    };
    let size = exact_quantity(trade.quantity, size_precision)?;
    anyhow::ensure!(!size.is_zero(), "trade {} has zero size", trade.id);
    Ok(TradeTick::new(
        instrument_id,
        exact_price(trade.price, price_precision)?,
        size,
        aggressor_side,
        TradeId::new(itoa::Buffer::new().format(trade.id)),
        millis(trade.time_ms)?,
        ts_init,
    ))
}
