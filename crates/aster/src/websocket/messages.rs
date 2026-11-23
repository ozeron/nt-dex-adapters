//! Market-stream payloads. Field names follow the venue's one-letter keys.

use anyhow::Context;
use serde::Deserialize;
use serde_json::value::RawValue;
use smallvec::SmallVec;

#[derive(Debug, Deserialize)]
pub struct Envelope<'a> {
    pub stream: Option<&'a str>,
    #[serde(borrow)]
    pub data: Option<&'a RawValue>,
}

impl<'a> Envelope<'a> {
    pub(crate) fn parse(text: &'a str) -> anyhow::Result<Self> {
        serde_json::from_str(text).context("invalid WebSocket frame")
    }
}

#[derive(Debug, Deserialize)]
pub struct DepthUpdate<'a, Levels = Vec<[&'a str; 2]>> {
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
    #[serde(rename = "b")]
    pub bids: Levels,
    #[serde(rename = "a")]
    pub asks: Levels,
}

pub(crate) type InlineDepthUpdate<'a> = DepthUpdate<'a, SmallVec<[[&'a str; 2]; 8]>>;

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
    #[serde(rename = "m")]
    pub buyer_is_maker: bool,
}

#[derive(Debug, Deserialize)]
pub struct Trade<'a> {
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
    #[serde(rename = "m")]
    pub buyer_is_maker: bool,
}

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
