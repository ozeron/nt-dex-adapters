//! Aster perpetual futures market-data adapter for `NautilusTrader`.
//!
//! The crate is layered so the venue logic can be tested without a network:
//!
//! - [`http`] and [`websocket`] hold wire models and pure parsers.
//! - [`book`] tracks depth sequence numbers for one instrument.
//! - [`feed`] ties them together: text frames and depth snapshots in,
//!   Nautilus data out.
//! - [`data`] is the live client: it feeds the socket and REST snapshots
//!   into a [`feed::AsterFeed`] and publishes what comes out.
//! - [`replay`] drives the same feed from an `nt-dex-capture` session file.

pub mod book;
pub mod common;
pub mod config;
pub mod data;
pub mod factories;
pub mod feed;
pub mod http;
pub mod instruments;
pub mod replay;
mod subscriptions;
pub mod websocket;
