//! Data client configuration.

use crate::common::{HTTP_URL, WS_URL};

pub(crate) const BOOK_BUFFER_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const SESSION_BOOK_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Configuration for [`crate::data::AsterDataClient`].
#[derive(Clone, Debug)]
pub struct AsterDataClientConfig {
    /// REST base URL.
    pub base_url_http: String,
    /// WebSocket base URL.
    pub base_url_ws: String,
    /// Timeout for REST requests.
    pub http_timeout_secs: u64,
    /// Levels per side requested for book snapshots (5 to 1000).
    pub book_snapshot_depth: u16,
    /// Snapshot deadline after rate admission. Zero disables the per-snapshot deadline.
    pub book_snapshot_timeout_secs: u64,
    /// Pending depth payload and diff storage budget for one book, in bytes.
    pub book_buffer_bytes: usize,
    /// Pending depth payload and diff storage budget across the session, in bytes.
    pub session_book_buffer_bytes: usize,
    /// Seconds between reloads of instrument definitions and funding
    /// intervals. Zero disables the refresh.
    pub instrument_refresh_interval_secs: u64,
    /// Streams carried by one WebSocket connection (the venue allows 200).
    pub max_streams_per_connection: usize,
    /// WebSocket connections the client may open.
    pub max_connections: usize,
    /// Request weight per minute spent on book snapshots. The venue allows
    /// 2400 a minute for all REST requests from one address.
    pub snapshot_weight_per_minute: u32,
    /// Log every raw WebSocket frame at debug level.
    pub debug_frames: bool,
}

impl Default for AsterDataClientConfig {
    fn default() -> Self {
        Self {
            base_url_http: HTTP_URL.to_owned(),
            base_url_ws: WS_URL.to_owned(),
            http_timeout_secs: 20,
            book_snapshot_depth: 1000,
            book_snapshot_timeout_secs: nautilus_live::book::DEFAULT_BOOK_SNAPSHOT_TIMEOUT_SECS,
            book_buffer_bytes: BOOK_BUFFER_BYTES,
            session_book_buffer_bytes: SESSION_BOOK_BUFFER_BYTES,
            instrument_refresh_interval_secs: 3600,
            max_streams_per_connection: 200,
            max_connections: 20,
            snapshot_weight_per_minute: 1800,
            debug_frames: false,
        }
    }
}
