//! Market-stream payloads. Field names follow the venue's one-letter keys.

use serde::Deserialize;
use serde_json::value::RawValue;

/// Combined-stream wrapper: `{"stream": "btcusdt@aggTrade", "data": {...}}`.
///
/// Control replies such as `{"result": null, "id": 1}` have neither field.
#[derive(Debug, Deserialize)]
pub struct Envelope<'a> {
    pub stream: Option<&'a str>,
    #[serde(borrow)]
    pub data: Option<&'a RawValue>,
}

/// `<symbol>@depth`: a diff of absolute level quantities.
#[derive(Debug, Deserialize)]
pub struct DepthUpdate<'a> {
    #[serde(rename = "T")]
    pub transaction_time: u64,
    #[serde(rename = "s")]
    pub symbol: &'a str,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub final_update_id: u64,
    #[serde(rename = "pu")]
    pub prev_final_update_id: u64,
    #[serde(rename = "b", borrow)]
    pub bids: Vec<[&'a str; 2]>,
    #[serde(rename = "a", borrow)]
    pub asks: Vec<[&'a str; 2]>,
}

/// `<symbol>@aggTrade`: fills of one taker order at one price.
#[derive(Debug, Deserialize)]
pub struct AggTrade<'a> {
    #[serde(rename = "a")]
    pub agg_trade_id: u64,
    #[serde(rename = "s")]
    pub symbol: &'a str,
    #[serde(rename = "p")]
    pub price: &'a str,
    #[serde(rename = "q")]
    pub quantity: &'a str,
    #[serde(rename = "f")]
    pub first_trade_id: u64,
    #[serde(rename = "l")]
    pub last_trade_id: u64,
    #[serde(rename = "T")]
    pub trade_time: u64,
    /// `true` when the buyer was the maker, so the aggressor sold.
    #[serde(rename = "m")]
    pub buyer_is_maker: bool,
}

/// `<symbol>@trade`: one fill.
///
/// The venue's `X` field (order type, `"MARKET"` in every sample) is not
/// read, as in the Binance futures client: every trade is published.
#[derive(Debug, Deserialize)]
pub struct Trade<'a> {
    /// Consecutive per symbol.
    #[serde(rename = "t")]
    pub trade_id: u64,
    #[serde(rename = "s")]
    pub symbol: &'a str,
    #[serde(rename = "p")]
    pub price: &'a str,
    #[serde(rename = "q")]
    pub quantity: &'a str,
    #[serde(rename = "T")]
    pub trade_time: u64,
    /// `true` when the buyer was the maker, so the aggressor sold.
    #[serde(rename = "m")]
    pub buyer_is_maker: bool,
}

/// `<symbol>@bookTicker`: best bid and ask.
#[derive(Debug, Deserialize)]
pub struct BookTicker<'a> {
    #[serde(rename = "s")]
    pub symbol: &'a str,
    #[serde(rename = "b")]
    pub bid_price: &'a str,
    #[serde(rename = "B")]
    pub bid_quantity: &'a str,
    #[serde(rename = "a")]
    pub ask_price: &'a str,
    #[serde(rename = "A")]
    pub ask_quantity: &'a str,
    #[serde(rename = "T")]
    pub transaction_time: u64,
}

/// `<symbol>@markPrice`: mark price, index price, and funding.
#[derive(Debug, Deserialize)]
pub struct MarkPrice<'a> {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: &'a str,
    #[serde(rename = "p")]
    pub mark_price: &'a str,
    #[serde(rename = "i")]
    pub index_price: &'a str,
    #[serde(rename = "r")]
    pub funding_rate: &'a str,
    #[serde(rename = "T")]
    pub next_funding_time: u64,
}
