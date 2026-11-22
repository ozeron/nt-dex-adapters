//! REST response models. Only the fields the adapter reads are declared.

use serde::Deserialize;

/// `GET /fapi/v1/exchangeInfo`.
#[derive(Debug, Deserialize)]
pub struct ExchangeInfo {
    pub symbols: Vec<SymbolInfo>,
}

/// One entry of `exchangeInfo.symbols`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolInfo {
    pub symbol: String,
    pub status: String,
    pub contract_type: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub margin_asset: String,
    pub price_precision: u8,
    pub quantity_precision: u8,
    /// Initial margin, in percent (`"5.0000"` is 5%).
    pub required_margin_percent: Option<String>,
    /// Maintenance margin, in percent.
    pub maint_margin_percent: Option<String>,
    pub filters: Vec<SymbolFilter>,
}

/// One entry of a symbol's `filters`; fields vary by `filterType`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolFilter {
    pub filter_type: String,
    pub tick_size: Option<String>,
    pub min_price: Option<String>,
    pub max_price: Option<String>,
    pub step_size: Option<String>,
    pub min_qty: Option<String>,
    pub max_qty: Option<String>,
    pub notional: Option<String>,
}

/// One entry of `GET /fapi/v1/fundingInfo`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FundingInfo {
    pub symbol: String,
    pub funding_interval_hours: u16,
}

/// One entry of `GET /fapi/v1/fundingRate`: a funding payment that settled.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FundingRate {
    pub funding_time: u64,
    pub funding_rate: String,
}

/// One entry of `GET /fapi/v1/trades`: a recent individual trade.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecentTrade<'a> {
    pub id: u64,
    pub price: &'a str,
    pub qty: &'a str,
    /// Trade time, milliseconds.
    pub time: u64,
    /// `true` when the buyer was the maker, so the aggressor sold.
    pub is_buyer_maker: bool,
}

/// `GET /fapi/v1/depth`.
#[derive(Debug, Deserialize)]
pub struct DepthSnapshot<'a> {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    /// Transaction time, milliseconds.
    #[serde(rename = "T")]
    pub transaction_time: u64,
    #[serde(borrow)]
    pub bids: Vec<[&'a str; 2]>,
    #[serde(borrow)]
    pub asks: Vec<[&'a str; 2]>,
}
