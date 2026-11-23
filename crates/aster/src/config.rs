//! Data client configuration.

use crate::common::{HTTP_URL, WS_URL};

pub(crate) const BOOK_BUFFER_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const SESSION_BOOK_BUFFER_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct AsterDataClientConfig {
    pub base_url_http: String,
    pub base_url_ws: String,
    pub http_timeout_secs: u64,
    pub book_snapshot_depth: u16,
    pub book_snapshot_timeout_secs: u64,
    pub book_buffer_bytes: usize,
    pub session_book_buffer_bytes: usize,
    pub instrument_refresh_interval_secs: u64,
    pub max_streams_per_connection: usize,
    pub max_connections: usize,
    pub snapshot_weight_per_minute: u32,
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
