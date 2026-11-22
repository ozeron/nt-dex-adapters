//! REST client for the public market-data endpoints.

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_network::http::HttpClient;

use super::parse::{ParsedInstruments, parse_exchange_info, parse_funding_intervals};

/// Fetches public market data over REST.
#[derive(Clone, Debug)]
pub struct AsterHttpClient {
    base_url: String,
    inner: HttpClient,
}

impl AsterHttpClient {
    /// Creates a client for `base_url`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP transport cannot be built.
    pub fn new(base_url: &str, timeout_secs: u64) -> anyhow::Result<Self> {
        let inner = HttpClient::builder()
            .timeout_secs(timeout_secs)
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            inner,
        })
    }

    async fn get(&self, path: &str) -> anyhow::Result<String> {
        let url = format!("{}{path}", self.base_url);
        let response = self
            .inner
            .get(url, None, None, None, None)
            .await
            .with_context(|| format!("GET {path} failed"))?;
        let body = String::from_utf8_lossy(&response.body);
        anyhow::ensure!(
            response.status.is_success(),
            "GET {path} returned {:?}: {body}",
            response.status
        );
        Ok(body.into_owned())
    }

    /// Fetches and parses all instrument definitions.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the body is not valid.
    pub async fn request_instruments(
        &self,
        ts_init: UnixNanos,
    ) -> anyhow::Result<ParsedInstruments> {
        let body = self.get("/fapi/v1/exchangeInfo").await?;
        parse_exchange_info(&body, ts_init)
    }

    /// Fetches minutes between funding payments, by venue symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the body is not valid.
    pub async fn request_funding_intervals(&self) -> anyhow::Result<Vec<(String, u16)>> {
        parse_funding_intervals(&self.get("/fapi/v1/fundingInfo").await?)
    }

    /// Fetches a raw page of settled funding rates for a venue symbol,
    /// oldest first. Without `start_ms` the venue returns the latest rows.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn request_funding_rates(
        &self,
        raw_symbol: &str,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
        limit: usize,
    ) -> anyhow::Result<String> {
        let start = start_ms.map_or_else(String::new, |ms| format!("&startTime={ms}"));
        let end = end_ms.map_or_else(String::new, |ms| format!("&endTime={ms}"));
        let path = format!("/fapi/v1/fundingRate?symbol={raw_symbol}&limit={limit}{start}{end}");
        self.get(&path).await
    }

    /// Fetches the raw body of the most recent individual trades for a venue
    /// symbol, oldest first. The venue allows at most 1000.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn request_trades(&self, raw_symbol: &str, limit: usize) -> anyhow::Result<String> {
        self.get(&format!(
            "/fapi/v1/trades?symbol={raw_symbol}&limit={limit}"
        ))
        .await
    }

    /// Fetches a raw depth snapshot body for a venue symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn request_depth(&self, raw_symbol: &str, limit: u16) -> anyhow::Result<String> {
        self.get(&format!("/fapi/v1/depth?symbol={raw_symbol}&limit={limit}"))
            .await
    }
}
