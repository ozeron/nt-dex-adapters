//! A pool of WebSocket connections sharing one output channel.
//!
//! The venue caps the streams on one connection, so subscriptions are spread
//! over as many connections as they need. The assignment follows the Binance
//! futures client: skip streams that are already open, open connections
//! until there is room, then put each stream on the first connection with
//! space. A connection keeps its streams across reconnects and is not closed
//! when it empties.
//!
//! Unlike the Binance client, a stream the venue refuses, or whose
//! `SUBSCRIBE` was never sent, is taken off its connection and handed back
//! to the caller to try again.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use nautilus_core::{UnixNanos, time::AtomicTime};
use nautilus_network::{
    Message, RECONNECTED,
    ratelimiter::quota::Quota,
    websocket::{MessageHandler, TransportBackend, WebSocketClient, WebSocketConfig},
};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc::UnboundedSender;
use ustr::Ustr;

/// Rate-limit key for `SUBSCRIBE` and `UNSUBSCRIBE` messages.
const CONTROL_KEY: &str = "control";
/// One control message per 300 ms on each connection, as the Binance client
/// does. The venue allows 10 incoming messages a second.
const CONTROL_PERIOD: Duration = Duration::from_millis(300);
/// Most bytes of stream names in one control message.
///
/// The venue closes the connection with code 3001 "illegal request" when a
/// message passes about 4096 bytes: 165 depth streams in one `SUBSCRIBE` are
/// accepted and 170 are not. The Binance client sends a connection's 200
/// streams in one message, which this venue refuses.
const CONTROL_MESSAGE_BYTES: usize = 3_000;
/// Seconds between keepalive pings, as in the Binance client.
const HEARTBEAT_SECS: u64 = 20;

/// A frame, the connection it arrived on, and when it arrived.
///
/// The time is taken as the frame leaves the socket, so a busy consumer does
/// not make data look later than it was.
pub(crate) type PoolFrame = (usize, UnixNanos, Message);

/// A reply to a control message: `{"result":null,"id":1}` on success.
#[derive(Deserialize)]
struct ControlReply {
    id: Option<u64>,
    error: Option<serde_json::Value>,
    code: Option<i64>,
}

struct Slot {
    socket: WebSocketClient,
    reconnects: ReconnectEpoch,
    streams: Vec<String>,
}

#[derive(Debug, Default)]
struct ReconnectEpoch {
    /// Advanced in the socket callback before the marker enters the frame queue.
    received: Arc<AtomicU64>,
    /// Advanced on the session task as it processes each reconnect marker.
    consumed: u64,
}

impl ReconnectEpoch {
    fn fence(&self) -> ReconnectFence {
        ReconnectFence {
            received: Arc::clone(&self.received),
            epoch: self.consumed,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReconnectFence {
    received: Arc<AtomicU64>,
    epoch: u64,
}

impl ReconnectFence {
    pub(crate) fn is_current(&self) -> bool {
        self.received.load(Ordering::Acquire) == self.epoch
    }

    pub(crate) fn matches(&self, other: &Self) -> bool {
        self.epoch == other.epoch && Arc::ptr_eq(&self.received, &other.received)
    }
}

/// Splits `streams` into runs that each fit one control message.
fn control_chunks(streams: &[String]) -> Vec<&[String]> {
    let mut chunks = Vec::new();
    let mut rest = streams;
    while !rest.is_empty() {
        let mut bytes = 0_usize;
        let mut count = 0_usize;
        for stream in rest {
            // Quotes and a comma around each name.
            bytes = bytes.saturating_add(stream.len()).saturating_add(3);
            if count > 0 && bytes > CONTROL_MESSAGE_BYTES {
                break;
            }
            count = count.saturating_add(1);
        }
        let (chunk, tail) = rest.split_at(count);
        chunks.push(chunk);
        rest = tail;
    }
    chunks
}

pub(crate) struct StreamPool {
    url: String,
    max_streams: usize,
    max_connections: usize,
    slots: Vec<Slot>,
    frames: UnboundedSender<PoolFrame>,
    frame_bytes: Arc<AtomicUsize>,
    clock: &'static AtomicTime,
    control_key: [Ustr; 1],
    next_request_id: u64,
    /// `SUBSCRIBE` requests the venue has not answered yet, by request id:
    /// the connection and the streams asked for.
    pending: AHashMap<u64, (usize, Vec<String>)>,
}

impl StreamPool {
    pub(crate) fn new(
        url: String,
        max_streams: usize,
        max_connections: usize,
        frames: UnboundedSender<PoolFrame>,
        clock: &'static AtomicTime,
    ) -> Self {
        Self {
            url,
            max_streams,
            max_connections,
            slots: Vec::new(),
            frames,
            frame_bytes: Arc::new(AtomicUsize::new(0)),
            clock,
            control_key: [Ustr::from(CONTROL_KEY)],
            next_request_id: 0,
            pending: AHashMap::new(),
        }
    }

    /// The most streams the pool can carry.
    pub(crate) const fn capacity(max_streams: usize, max_connections: usize) -> usize {
        max_streams.saturating_mul(max_connections)
    }

    /// Snapshots belong to the last socket generation processed by the session.
    pub(crate) fn reconnect_fence(&self, stream: &str) -> Option<ReconnectFence> {
        self.slots
            .iter()
            .find(|slot| slot.streams.iter().any(|open| open == stream))
            .map(|slot| slot.reconnects.fence())
    }

    pub(crate) fn connections(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn queued_frame_bytes(&self, count: usize) -> usize {
        self.frame_bytes
            .load(Ordering::Relaxed)
            .saturating_add(count.saturating_mul(size_of::<PoolFrame>()))
    }

    pub(crate) fn frame_received(&self, message: &Message) {
        self.frame_bytes
            .fetch_sub(message.as_bytes().len(), Ordering::Relaxed);
    }

    fn free(&self) -> usize {
        self.slots
            .iter()
            .map(|slot| self.max_streams.saturating_sub(slot.streams.len()))
            .sum()
    }

    /// Opens one more connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    pub(crate) async fn open_connection(&mut self) -> anyhow::Result<()> {
        let index = self.slots.len();
        let frames = self.frames.clone();
        let frame_bytes = Arc::clone(&self.frame_bytes);
        let clock = self.clock;
        let reconnects = ReconnectEpoch::default();
        let counter = Arc::clone(&reconnects.received);
        let handler: MessageHandler = Arc::new(move |message: Message| {
            if matches!(&message, Message::Text(text) if text.as_ref() == RECONNECTED.as_bytes()) {
                counter.fetch_add(1, Ordering::AcqRel);
            }
            let received_at = clock.get_time_ns();
            let bytes = message.as_bytes().len();
            frame_bytes.fetch_add(bytes, Ordering::Relaxed);
            // The receiver only closes when the session has ended.
            if frames.send((index, received_at, message)).is_err() {
                frame_bytes.fetch_sub(bytes, Ordering::Relaxed);
            }
        });
        let config = WebSocketConfig {
            url: self.url.clone(),
            headers: Vec::new(),
            heartbeat_interval_secs: Some(HEARTBEAT_SECS),
            heartbeat_payload: None,
            connect_timeout_ms: Some(10_000),
            reconnect_delay_initial_ms: Some(500),
            reconnect_delay_max_ms: Some(5_000),
            reconnect_backoff_factor: Some(2.0),
            reconnect_jitter_ms: Some(250),
            reconnect_max_attempts: None,
            heartbeat_timeout_secs: None,
            idle_timeout_ms: None,
            writer_capacity: None,
            backend: TransportBackend::default(),
            proxy_url: None,
            max_message_size_bytes: None,
            max_frame_size_bytes: None,
        };
        let keyed_quotas = Quota::with_period(CONTROL_PERIOD)
            .map(|quota| (CONTROL_KEY.to_owned(), quota))
            .into_iter()
            .collect();
        let socket = WebSocketClient::builder()
            .config(config)
            .message_handler(handler)
            .keyed_quotas(keyed_quotas)
            .connect()
            .await
            .map_err(|error| {
                anyhow::anyhow!("failed to connect Aster WebSocket {index}: {error}")
            })?;
        self.slots.push(Slot {
            socket,
            reconnects,
            streams: Vec::new(),
        });
        log::info!("Aster WebSocket connection {index} opened");
        Ok(())
    }

    /// Sends `streams` in as many messages as the venue's size limit needs.
    ///
    /// Returns how many streams, from the front, were sent. Fewer than all
    /// of them means a send failed, which is logged.
    async fn send(&mut self, slot: usize, method: &str, streams: &[String]) -> usize {
        let mut sent = 0_usize;
        for chunk in control_chunks(streams) {
            self.next_request_id = self.next_request_id.wrapping_add(1);
            let id = self.next_request_id;
            let request = json!({ "method": method, "params": chunk, "id": id });
            let Some(socket) = self.slots.get(slot).map(|slot| &slot.socket) else {
                break;
            };
            if let Err(error) = socket
                .send_text(request.to_string(), Some(&self.control_key))
                .await
            {
                log::error!("Aster {method} on connection {slot} failed: {error}");
                break;
            }
            if method == "SUBSCRIBE" {
                self.pending.insert(id, (slot, chunk.to_vec()));
            }
            sent = sent.saturating_add(chunk.len());
        }
        sent
    }

    /// Opens `streams`, adding connections as needed.
    ///
    /// Returns the streams that could not be opened: a connection could not
    /// be established, a send failed, or the pool is full. The caller is
    /// expected to try those again.
    pub(crate) async fn subscribe(&mut self, streams: &[String]) -> Vec<String> {
        let new: Vec<String> = streams
            .iter()
            .filter(|stream| !self.slots.iter().any(|slot| slot.streams.contains(stream)))
            .cloned()
            .collect();
        if new.is_empty() {
            return Vec::new();
        }

        while self.free() < new.len() && self.slots.len() < self.max_connections {
            if let Err(error) = self.open_connection().await {
                log::error!("{error:#}");
                break;
            }
        }

        // First fit, staged per connection so each gets as few messages as
        // the size limit allows.
        let mut counts: Vec<usize> = self.slots.iter().map(|slot| slot.streams.len()).collect();
        let mut batches: Vec<Vec<String>> = vec![Vec::new(); self.slots.len()];
        let mut unplaced = Vec::new();
        for stream in new {
            let slot = counts.iter().position(|count| *count < self.max_streams);
            match slot.and_then(|slot| Some((counts.get_mut(slot)?, batches.get_mut(slot)?))) {
                Some((count, batch)) => {
                    *count = count.saturating_add(1);
                    batch.push(stream);
                }
                None => unplaced.push(stream),
            }
        }

        for (slot, mut batch) in batches.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let sent = self.send(slot, "SUBSCRIBE", &batch).await;
            // Only what went out belongs to the connection.
            unplaced.extend(batch.split_off(sent.min(batch.len())));
            if let Some(slot) = self.slots.get_mut(slot) {
                slot.streams.extend(batch);
            }
        }
        unplaced
    }

    /// Closes `streams` on whichever connections carry them.
    pub(crate) async fn unsubscribe(&mut self, streams: &[String]) {
        let closing: Vec<(usize, Vec<String>)> = self
            .slots
            .iter_mut()
            .enumerate()
            .filter_map(|(index, slot)| {
                let (closing, kept) = std::mem::take(&mut slot.streams)
                    .into_iter()
                    .partition::<Vec<_>, _>(|stream| streams.contains(stream));
                slot.streams = kept;
                (!closing.is_empty()).then_some((index, closing))
            })
            .collect();
        for (slot, closing) in closing {
            // A send only fails when the connection is down. It then comes
            // back without these streams, because they are off its list.
            self.send(slot, "UNSUBSCRIBE", &closing).await;
        }
    }

    /// Reopens every stream of a connection that has just reconnected.
    ///
    /// Returns the streams the connection carried, and those among them
    /// that could not be sent again and are no longer assigned to it.
    pub(crate) async fn resubscribe(&mut self, slot: usize) -> (Vec<String>, Vec<String>) {
        // Requests sent on the old connection will never be answered.
        self.pending.retain(|_, (owner, _)| *owner != slot);
        let streams = self
            .slots
            .get_mut(slot)
            .map(|slot| {
                slot.reconnects.consumed = slot.reconnects.consumed.wrapping_add(1);
                slot.streams.clone()
            })
            .unwrap_or_default();
        let sent = self.send(slot, "SUBSCRIBE", &streams).await;
        let unsent = streams.get(sent..).unwrap_or_default().to_vec();
        if let Some(slot) = self.slots.get_mut(slot) {
            slot.streams.truncate(sent);
        }
        (streams, unsent)
    }

    /// Handles a control reply from connection `slot`.
    ///
    /// Returns the streams of a `SUBSCRIBE` the venue refused. They are no
    /// longer assigned to the connection.
    pub(crate) fn on_reply(&mut self, slot: usize, text: &str) -> Vec<String> {
        let Ok(reply) = serde_json::from_str::<ControlReply>(text) else {
            log::warn!("Aster sent an unrecognised control frame: {text}");
            return Vec::new();
        };
        let refused = reply.error.is_some() || reply.code.is_some();
        let request = reply.id.and_then(|id| self.pending.remove(&id));
        if !refused {
            return Vec::new();
        }
        log::error!("Aster refused a control request on connection {slot}: {text}");
        let Some((owner, streams)) = request else {
            return Vec::new();
        };
        if let Some(slot) = self.slots.get_mut(owner) {
            slot.streams.retain(|stream| !streams.contains(stream));
        }
        streams
    }

    pub(crate) async fn close(&mut self) {
        for slot in self.slots.drain(..) {
            slot.socket.disconnect().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_fences_use_the_session_generation_while_reconnects_are_queued() {
        let mut epoch = ReconnectEpoch::default();
        let queued = epoch.fence();
        assert!(queued.is_current());
        assert!(queued.matches(&epoch.fence()));
        assert!(!queued.matches(&ReconnectEpoch::default().fence()));
        epoch.received.store(2, Ordering::Release);
        assert!(!queued.is_current());
        assert!(!epoch.fence().is_current());
        epoch.consumed = 1;
        assert!(!epoch.fence().is_current());
        epoch.consumed = 2;
        assert!(epoch.fence().is_current());
        assert!(!queued.is_current());
        assert!(!queued.matches(&epoch.fence()));
    }

    #[test]
    fn control_messages_stay_under_the_venue_limit() {
        let streams: Vec<String> = (0..200)
            .map(|n| format!("symbol{n:04}usdt@depth@100ms"))
            .collect();
        let chunks = control_chunks(&streams);
        assert!(chunks.len() > 1);
        assert_eq!(chunks.iter().map(|chunk| chunk.len()).sum::<usize>(), 200);
        for chunk in &chunks {
            let request = json!({ "method": "SUBSCRIBE", "params": chunk, "id": u64::MAX });
            assert!(request.to_string().len() < 4_000);
        }
        // Order is kept.
        assert_eq!(chunks.concat(), streams);
    }

    #[test]
    fn a_single_oversized_stream_is_still_sent() {
        let streams = vec!["x".repeat(5_000), "y".to_owned()];
        let chunks = control_chunks(&streams);
        assert_eq!(chunks.len(), 2);
        assert_eq!(control_chunks(&[]).len(), 0);
    }
}
