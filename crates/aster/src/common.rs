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

/// Nautilus venue code.
pub const ASTER: &str = "ASTER";
/// REST base URL for the futures API.
pub const HTTP_URL: &str = "https://fapi.asterdex.com";
/// WebSocket base URL for market streams.
pub const WS_URL: &str = "wss://fstream.asterdex.com";
/// Flag marking the last delta of an event.
#[expect(clippy::as_conversions, reason = "fieldless enum to its u8 repr")]
pub const F_LAST: u8 = RecordFlag::F_LAST as u8;
/// Flag marking a delta that belongs to a book snapshot.
#[expect(clippy::as_conversions, reason = "fieldless enum to its u8 repr")]
pub const F_SNAPSHOT: u8 = RecordFlag::F_SNAPSHOT as u8;
/// Suffix appended to venue symbols to form Nautilus perpetual symbols.
const PERP_SUFFIX: &str = "-PERP";

/// Returns the Aster venue identifier.
#[must_use]
pub fn venue() -> Venue {
    Venue::new(ASTER)
}

/// Maps a venue symbol such as `BTCUSDT` to `BTCUSDT-PERP.ASTER`.
#[must_use]
pub fn instrument_id(raw_symbol: &str) -> InstrumentId {
    InstrumentId::new(Symbol::new(format!("{raw_symbol}{PERP_SUFFIX}")), venue())
}

/// Maps `BTCUSDT-PERP.ASTER` back to the venue symbol `BTCUSDT`.
#[must_use]
pub fn raw_symbol(instrument_id: &InstrumentId) -> &str {
    let symbol = instrument_id.symbol.as_str();
    symbol.strip_suffix(PERP_SUFFIX).unwrap_or(symbol)
}

/// Converts a venue millisecond timestamp to nanoseconds.
///
/// # Errors
///
/// Returns an error if the value overflows the nanosecond range.
pub fn millis(value: u64) -> anyhow::Result<UnixNanos> {
    value
        .checked_mul(1_000_000)
        .map(UnixNanos::from)
        .with_context(|| format!("timestamp {value} ms overflows nanoseconds"))
}

/// Decimal places of mark and index prices.
///
/// The venue computes them finer than an instrument's tick and sends eight
/// decimals, though it drops them from whole numbers (`"1666"`). A fixed
/// precision keeps the full value and gives every update of an instrument
/// the same precision, which the Parquet catalog requires of one file.
pub const REFERENCE_PRICE_PRECISION: u8 = 8;

/// A mark or index price at [`REFERENCE_PRICE_PRECISION`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReferencePrice {
    pub price: Price,
    /// `true` when the venue sent significant digits beyond the precision and
    /// they were rounded half-even, so `price` differs from the wire value.
    pub rounded: bool,
}

/// Parses a mark or index price at [`REFERENCE_PRICE_PRECISION`].
///
/// A value with more digits is still published, rounded half-even, and
/// reported through [`ReferencePrice::rounded`] rather than dropped.
///
/// # Errors
///
/// Returns an error if `value` is not a decimal or is outside the range of
/// [`Price`].
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

/// Parses a venue decimal string.
///
/// # Errors
///
/// Returns an error if `value` is not a decimal number.
pub fn decimal(value: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str(value).with_context(|| format!("invalid decimal '{value}'"))
}

/// Parses a price at the instrument's precision.
///
/// # Errors
///
/// Returns an error if `value` is not a decimal or does not fit `precision`.
pub fn price(value: &str, precision: u8) -> anyhow::Result<Price> {
    Price::from_decimal_dp(decimal(value)?, precision)
        .with_context(|| format!("invalid price '{value}' at precision {precision}"))
}

/// Parses a quantity at the instrument's precision.
///
/// # Errors
///
/// Returns an error if `value` is not a non-negative decimal or does not fit
/// `precision`.
pub fn quantity(value: &str, precision: u8) -> anyhow::Result<Quantity> {
    Quantity::from_decimal_dp(decimal(value)?, precision)
        .with_context(|| format!("invalid quantity '{value}' at precision {precision}"))
}

/// Fails when `value` has more decimals than `precision` holds. Rounding a
/// book level or a trade would move it to another price or change its size,
/// and can turn a small quantity into zero, which deletes the level.
fn ensure_exact(value: &str, precision: u8) -> anyhow::Result<()> {
    anyhow::ensure!(
        decimal(value)?.normalize().scale() <= u32::from(precision),
        "'{value}' has more decimals than the instrument's precision {precision}"
    );
    Ok(())
}

/// Parses a market-data price, which must fit the instrument's precision.
///
/// # Errors
///
/// Returns an error if `value` is not a decimal or needs rounding.
pub fn exact_price(value: &str, precision: u8) -> anyhow::Result<Price> {
    ensure_exact(value, precision)?;
    price(value, precision)
}

/// Parses a market-data quantity, which must fit the instrument's precision.
///
/// # Errors
///
/// Returns an error if `value` is not a non-negative decimal or needs
/// rounding.
pub fn exact_quantity(value: &str, precision: u8) -> anyhow::Result<Quantity> {
    ensure_exact(value, precision)?;
    quantity(value, precision)
}

/// A trade as the venue reports it, whichever endpoint or stream it came from.
#[derive(Clone, Copy, Debug)]
pub struct RawTrade<'a> {
    /// The venue's id for the trade or aggregate trade.
    pub id: u64,
    pub price: &'a str,
    pub quantity: &'a str,
    /// `true` when the buyer was the maker, so the aggressor sold.
    pub buyer_is_maker: bool,
    /// Trade time, milliseconds.
    pub time_ms: u64,
}

/// Builds a [`TradeTick`] from a venue trade.
///
/// # Errors
///
/// Returns an error if the price, quantity, or time does not parse, or if the
/// size is zero.
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
    // `TradeTick::new` panics on a zero size.
    anyhow::ensure!(!size.is_zero(), "trade {} has zero size", trade.id);
    Ok(TradeTick::new(
        instrument_id,
        exact_price(trade.price, price_precision)?,
        size,
        aggressor_side,
        TradeId::new(trade.id.to_string()),
        millis(trade.time_ms)?,
        ts_init,
    ))
}
