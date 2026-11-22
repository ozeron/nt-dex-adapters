//! Live data client.
//!
//! One background session owns the WebSocket connections, the
//! [`AsterFeed`], and the set of subscriptions. The client methods only send
//! it commands.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use anyhow::Context;
use nautilus_common::{
    clients::{DataClient, execute_instrument_subscriptions},
    live::{get_runtime, runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent,
        data::{
            DataResponse, FundingRatesResponse, InstrumentResponse, InstrumentsResponse,
            RequestFundingRates, RequestInstrument, RequestInstruments, RequestTrades,
            SubscribeBookDeltas, SubscribeFundingRates, SubscribeIndexPrices, SubscribeInstrument,
            SubscribeInstruments, SubscribeMarkPrices, SubscribeQuotes, SubscribeTrades,
            TradesResponse, UnsubscribeBookDeltas, UnsubscribeFundingRates, UnsubscribeIndexPrices,
            UnsubscribeMarkPrices, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{
    AtomicMap, UnixNanos,
    datetime::datetime_to_unix_nanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    data::{Data, InstrumentStatus},
    enums::MarketStatusAction,
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::{Message, RECONNECTED};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use crate::{
    common,
    config::AsterDataClientConfig,
    feed::{AsterFeed, FeedEvent},
    http::{
        client::AsterHttpClient,
        parse::{parse_funding_rates, parse_trades},
    },
    instruments::diff_instruments,
    subscriptions::{Command, Kind, Subscriptions},
    websocket::pool::{PoolFrame, StreamPool},
};

/// How often streams that could not be opened are tried again.
const SUBSCRIBE_RETRY_INTERVAL: Duration = Duration::from_secs(5);
/// How often books that are not in sync are reported.
const BOOK_HEALTH_INTERVAL: Duration = Duration::from_mins(1);
/// Funding history rows per request, the venue's maximum.
const FUNDING_PAGE: usize = 1000;
/// Recent trades per request, the venue's maximum.
const TRADES_PAGE: usize = 1000;
/// Pages fetched for one funding history request at most.
const FUNDING_MAX_PAGES: usize = 20;
/// The longest funding interval the venue uses, eight hours, in milliseconds.
const FUNDING_LONGEST_MS: u64 = 8 * 60 * 60 * 1_000;
const SNAPSHOT_RETRY_DELAY: Duration = Duration::from_secs(1);
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// A depth snapshot response, tagged with the request it answers.
struct SnapshotResult {
    symbol: Ustr,
    /// The symbol's snapshot epoch when the request was made.
    epoch: u64,
    /// The reconnect counter of the book's connection, and its value when
    /// the request was made.
    fence: Arc<AtomicU64>,
    seen: u64,
    body: anyhow::Result<String>,
}

/// The background task that owns the socket and the feed.
struct Session {
    feed: AsterFeed,
    pool: StreamPool,
    /// Streams the pool could not open yet.
    unplaced: Vec<String>,
    http: AsterHttpClient,
    sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    config: AsterDataClientConfig,
    subscriptions: Subscriptions,
    /// Bumped whenever a book is reset, so a response to a request made
    /// before the reset can be told apart and discarded.
    snapshot_epochs: AHashMap<Ustr, u64>,
    snapshots_tx: UnboundedSender<SnapshotResult>,
    cancel: CancellationToken,
    events: Vec<FeedEvent>,
    /// When the next book snapshot may be requested, to stay inside the
    /// venue's request weight.
    next_snapshot_at: Instant,
    snapshot_spacing: Duration,
}

/// Request weight of one depth snapshot at `limit` levels.
const fn snapshot_weight(limit: u16) -> u32 {
    match limit {
        0..=50 => 2,
        51..=100 => 5,
        101..=500 => 10,
        _ => 20,
    }
}

/// Time between snapshot requests that keeps them within the weight budget.
fn snapshot_spacing(config: &AsterDataClientConfig) -> Duration {
    let weight = snapshot_weight(config.book_snapshot_depth);
    Duration::from_mins(1)
        .checked_mul(weight)
        .and_then(|total| total.checked_div(config.snapshot_weight_per_minute))
        .unwrap_or(Duration::ZERO)
}

impl Session {
    async fn open(&mut self, streams: &[String]) {
        if streams.is_empty() {
            return;
        }
        let before = self.pool.connections();
        let unplaced = self.pool.subscribe(streams).await;
        if self.pool.connections() != before {
            log::info!(
                "Aster is using {} WebSocket connections",
                self.pool.connections()
            );
        }
        if !unplaced.is_empty() {
            log::warn!(
                "Aster could not open {} streams; retrying in {SUBSCRIBE_RETRY_INTERVAL:?}",
                unplaced.len()
            );
        }
        self.unplaced.extend(unplaced);
    }

    fn book_wanted(&self, symbol: Ustr) -> bool {
        self.feed
            .instrument_id(symbol.as_str())
            .is_some_and(|id| self.subscriptions.wants(id, Kind::Book))
    }

    /// Invalidates any snapshot request in flight for a book.
    fn bump_snapshot_epoch(&mut self, symbol: Ustr) {
        let epoch = self.snapshot_epochs.entry(symbol).or_insert(0);
        *epoch = epoch.wrapping_add(1);
    }

    /// Forgets a book and invalidates any snapshot request in flight for it.
    fn reset_book(&mut self, symbol: Ustr) {
        self.feed.reset_book(symbol.as_str());
        self.bump_snapshot_epoch(symbol);
    }

    /// Applies a batch of commands with at most one message per direction.
    async fn apply(&mut self, commands: &[Command]) {
        let changes = self.subscriptions.apply(commands);
        for (instrument_id, kind) in &changes.rejected {
            log::error!(
                "Aster subscription to {kind:?} for {instrument_id} refused: \
                 every connection is at its stream limit"
            );
        }
        for instrument_id in &changes.books_ended {
            self.reset_book(Ustr::from(common::raw_symbol(instrument_id)));
        }
        // Streams still waiting for a connection go out with the new ones,
        // unless this batch closed them.
        let mut opening = std::mem::take(&mut self.unplaced);
        opening.retain(|stream| !changes.closed.contains(stream));
        opening.extend(changes.opened);
        self.open(&opening).await;
        self.pool.unsubscribe(&changes.closed).await;
    }

    fn fetch_snapshot(&mut self, symbol: Ustr, delay: Duration) {
        let epoch = *self.snapshot_epochs.entry(symbol).or_insert(0);
        let stream = self
            .feed
            .instrument_id(symbol.as_str())
            .map(|id| Kind::Book.stream(&id))
            .unwrap_or_default();
        let fence = self.pool.reconnects_of(&stream);
        let seen = fence.load(Ordering::Acquire);
        let http = self.http.clone();
        let results = self.snapshots_tx.clone();
        let depth = self.config.book_snapshot_depth;
        let cancel = self.cancel.clone();
        // Requests are spaced out: subscribing to many books at once would
        // otherwise spend the venue's whole request allowance.
        let now = Instant::now();
        let at = now
            .checked_add(delay)
            .unwrap_or(now)
            .max(self.next_snapshot_at);
        self.next_snapshot_at = at.checked_add(self.snapshot_spacing).unwrap_or(at);
        get_runtime().spawn(async move {
            let request = async {
                tokio::time::sleep_until(at).await;
                // A request overtaken by a reconnect while it waited its
                // turn is not worth its request weight.
                anyhow::ensure!(
                    fence.load(Ordering::Acquire) == seen,
                    "superseded by a reconnect"
                );
                http.request_depth(symbol.as_str(), depth).await
            };
            tokio::select! {
                biased;
                () = cancel.cancelled() => {}
                body = request => {
                    // The receiver only closes when the session has ended.
                    let _ = results.send(SnapshotResult { symbol, epoch, fence, seen, body });
                }
            }
        });
    }

    fn publish(&mut self) {
        for event in std::mem::take(&mut self.events) {
            match event {
                FeedEvent::SnapshotRequired(symbol) => {
                    if self.book_wanted(symbol) {
                        // A new need for a snapshot supersedes any request
                        // still in flight for the book.
                        self.bump_snapshot_epoch(symbol);
                        self.fetch_snapshot(symbol, Duration::ZERO);
                    } else {
                        // A depth frame that was already queued when the book
                        // was unsubscribed. Forget it, so a later subscription
                        // starts clean and asks for its own snapshot.
                        self.reset_book(symbol);
                    }
                }
                FeedEvent::Data(data) => {
                    // Mark, index, and funding arrive together on one stream.
                    // Like the Binance client, subscribing to any of them
                    // publishes all three.
                    let wanted = Kind::of(&data).is_some_and(|(id, kind)| match kind {
                        Kind::MarkPrice | Kind::IndexPrice | Kind::FundingRate => {
                            self.subscriptions.wants(id, Kind::MarkPrice)
                                || self.subscriptions.wants(id, Kind::IndexPrice)
                                || self.subscriptions.wants(id, Kind::FundingRate)
                        }
                        _ => self.subscriptions.wants(id, kind),
                    });
                    if wanted && let Err(error) = self.sender.send(DataEvent::Data(data)) {
                        log::error!("Aster failed to publish data: {error}");
                    }
                }
            }
        }
    }

    async fn on_frame(&mut self, slot: usize, ts_init: UnixNanos, text: &str) {
        if text == RECONNECTED {
            log::warn!(
                "Aster WebSocket connection {slot} reconnected; resubscribing and resyncing its books"
            );
            let (streams, unsent) = self.pool.resubscribe(slot).await;
            self.unplaced.extend(unsent);
            // Only the books carried by this connection missed updates.
            let books: Vec<InstrumentId> = self
                .subscriptions
                .instruments(Kind::Book)
                .filter(|id| streams.contains(&Kind::Book.stream(id)))
                .collect();
            for instrument_id in books {
                self.reset_book(Ustr::from(common::raw_symbol(&instrument_id)));
            }
            return;
        }
        if self.config.debug_frames {
            log::debug!("Aster frame: {text}");
        }
        if !text.contains("\"stream\"") {
            // A control reply. Streams the venue refused are tried again.
            let refused = self.pool.on_reply(slot, text);
            self.unplaced.extend(refused);
            // One that was unsubscribed in the meantime is not reopened.
            self.unplaced
                .retain(|stream| self.subscriptions.uses(stream));
            return;
        }
        if let Err(error) = self.feed.handle_text(text, ts_init, &mut self.events) {
            log::warn!("Aster dropped a frame: {error:#}");
        }
        self.publish();
    }

    fn on_snapshot(&mut self, result: SnapshotResult) {
        let SnapshotResult {
            symbol,
            epoch,
            fence,
            seen,
            body,
        } = result;
        if self.snapshot_epochs.get(&symbol) != Some(&epoch) {
            // Requested before the book was last reset: it may describe a
            // book from before a reconnect or an earlier subscription.
            return;
        }
        if fence.load(Ordering::Acquire) != seen {
            // The book's connection has reconnected since the request, and
            // its reconnect may still be queued behind this result, so the
            // snapshot predates the gap. Ask again. The reset that follows
            // the reconnect discards the new request too, and the book's
            // next update starts a fresh one.
            if self.book_wanted(symbol) {
                self.fetch_snapshot(symbol, Duration::ZERO);
            }
            return;
        }
        if !self.book_wanted(symbol) {
            self.reset_book(symbol);
            return;
        }
        let ts_init = self.clock.get_time_ns();
        let applied = body.and_then(|body| {
            self.feed
                .handle_depth_snapshot(symbol.as_str(), &body, ts_init, &mut self.events)
        });
        if let Err(error) = applied {
            log::warn!("Aster book snapshot for {symbol} failed, retrying: {error:#}");
            self.fetch_snapshot(symbol, SNAPSHOT_RETRY_DELAY);
        }
        self.publish();
    }

    async fn on_message(&mut self, (slot, ts_init, message): PoolFrame) {
        if let Message::Text(text) = message {
            match std::str::from_utf8(&text) {
                Ok(text) => self.on_frame(slot, ts_init, text).await,
                Err(error) => log::warn!("Aster dropped a frame: {error}"),
            }
        }
    }

    /// Tells the operator about subscribed books that are not publishing: a
    /// book waiting for its snapshot, or one the venue has sent no update
    /// for since it was subscribed.
    fn report_unsynced_books(&self) {
        let subscribed = self.subscriptions.instruments(Kind::Book).count();
        let silent = self
            .subscriptions
            .instruments(Kind::Book)
            .filter(|id| !self.feed.is_book_live(common::raw_symbol(id)))
            .count();
        if silent > 0 {
            log::warn!(
                "Aster has {silent} of {subscribed} subscribed books not yet in sync, \
                 {} waiting for a snapshot",
                self.feed.books_awaiting_snapshot().len()
            );
        }
        if !self.unplaced.is_empty() {
            log::warn!(
                "Aster has {} streams waiting to be opened",
                self.unplaced.len()
            );
        }
    }

    async fn run(
        mut self,
        mut frames: UnboundedReceiver<PoolFrame>,
        mut commands: UnboundedReceiver<Command>,
        mut snapshots: UnboundedReceiver<SnapshotResult>,
        mut updates: UnboundedReceiver<InstrumentUpdate>,
    ) {
        let mut retry = tokio::time::interval(SUBSCRIBE_RETRY_INTERVAL);
        retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut health = tokio::time::interval(BOOK_HEALTH_INTERVAL);
        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => {
                    // Publish what had already arrived before stopping.
                    while let Ok(frame) = frames.try_recv() {
                        self.on_message(frame).await;
                    }
                    break;
                }
                _ = health.tick() => self.report_unsynced_books(),
                _ = retry.tick(), if !self.unplaced.is_empty() => {
                    let streams = std::mem::take(&mut self.unplaced);
                    self.open(&streams).await;
                }
                Some(command) = commands.recv() => {
                    let mut batch = vec![command];
                    while let Ok(command) = commands.try_recv() {
                        batch.push(command);
                    }
                    self.apply(&batch).await;
                }
                Some(result) = snapshots.recv() => self.on_snapshot(result),
                Some(update) = updates.recv() => {
                    for instrument in &update.instruments {
                        self.feed.add_instrument(instrument);
                        // A changed definition may change the precision of
                        // the levels, so the book starts again.
                        self.reset_book(instrument.raw_symbol().inner());
                    }
                    if let Some(intervals) = &update.funding_intervals {
                        self.feed.replace_funding_intervals(intervals);
                    }
                }
                frame = frames.recv() => match frame {
                    Some(frame) => self.on_message(frame).await,
                    None => break,
                },
            }
        }
        self.pool.close().await;
    }
}

/// Replaces the cached instruments with `instruments`.
fn store_instruments(
    cache: &AtomicMap<InstrumentId, InstrumentAny>,
    instruments: &[InstrumentAny],
) {
    cache.store(
        instruments
            .iter()
            .map(|instrument| (instrument.id(), instrument.clone()))
            .collect(),
    );
}

/// Whether `ts` lies inside the optional, inclusive `start..=end` window.
fn in_window(ts: UnixNanos, start: Option<UnixNanos>, end: Option<UnixNanos>) -> bool {
    start.is_none_or(|start| ts >= start) && end.is_none_or(|end| ts <= end)
}

/// New or changed venue metadata for the session's feed.
struct InstrumentUpdate {
    instruments: Vec<InstrumentAny>,
    /// `None` when the venue's intervals could not be loaded.
    funding_intervals: Option<Vec<(String, u16)>>,
}

/// Reloads instruments and funding intervals on an interval, publishing
/// what changed.
struct Refresher {
    http: AsterHttpClient,
    clock: &'static AtomicTime,
    sender: EventSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    updates: UnboundedSender<InstrumentUpdate>,
}

impl Refresher {
    fn publish(&self, data: Data) {
        if let Err(error) = self.sender.send(DataEvent::Data(data)) {
            log::warn!("Aster failed to publish instrument update: {error}");
        }
    }

    async fn refresh(&self) {
        let ts_init = self.clock.get_time_ns();
        let parsed = match self.http.request_instruments(ts_init).await {
            Ok(parsed) if !parsed.instruments.is_empty() => parsed,
            Ok(_) => {
                log::warn!("Aster instrument refresh returned no instruments; keeping the cache");
                return;
            }
            Err(error) => {
                log::warn!("Aster instrument refresh failed: {error:#}");
                return;
            }
        };
        // A failed or empty load keeps the intervals already known.
        let funding_intervals = match self.http.request_funding_intervals().await {
            Ok(intervals) if !intervals.is_empty() => Some(intervals),
            Ok(_) => None,
            Err(error) => {
                log::warn!("Aster funding interval refresh failed: {error:#}");
                None
            }
        };

        // A symbol the venue still lists but whose definition did not parse
        // keeps its last good definition. It has not been delisted.
        let cached = self.instruments.load();
        let mut loaded = parsed.instruments;
        for (symbol, error) in &parsed.failed {
            log::warn!("Aster instrument {symbol} did not parse on refresh: {error:#}");
            let kept = cached.get(&common::instrument_id(symbol));
            loaded.extend(kept.cloned());
        }
        let changes = diff_instruments(&cached, &loaded);
        if !changes.is_empty() {
            log::info!(
                "Aster instruments changed: {} added, {} changed, {} removed",
                changes.added.len(),
                changes.changed.len(),
                changes.removed.len()
            );
            store_instruments(&self.instruments, &loaded);
        }
        for instrument_id in &changes.removed {
            let status = InstrumentStatus::new(
                *instrument_id,
                MarketStatusAction::NotAvailableForTrading,
                ts_init,
                ts_init,
                None,
                None,
                Some(false),
                Some(false),
                None,
            );
            self.publish(Data::InstrumentStatus(status));
        }
        let mut instruments = changes.added;
        instruments.extend(changes.changed);
        for instrument in &instruments {
            self.publish(instrument.clone().into());
        }
        // The receiver only closes when the session has ended.
        let _ = self.updates.send(InstrumentUpdate {
            instruments,
            funding_intervals,
        });
    }

    async fn run(self, every: Duration, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick fires at once; connect has just loaded everything.
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = ticker.tick() => self.refresh().await,
            }
        }
    }
}

/// Market-data client for Aster perpetual futures.
#[derive(Debug)]
pub struct AsterDataClient {
    client_id: ClientId,
    config: AsterDataClientConfig,
    http: AsterHttpClient,
    clock: &'static AtomicTime,
    sender: EventSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    connected: AtomicBool,
    commands: Option<UnboundedSender<Command>>,
    cancel: CancellationToken,
    session: Option<JoinHandle<()>>,
    refresher: Option<JoinHandle<()>>,
}

impl AsterDataClient {
    /// Creates a client. Must be called on the node's runner thread.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built.
    pub fn new(client_id: ClientId, config: AsterDataClientConfig) -> anyhow::Result<Self> {
        let http = AsterHttpClient::new(&config.base_url_http, config.http_timeout_secs)?;
        Ok(Self {
            client_id,
            config,
            http,
            clock: get_atomic_clock_realtime(),
            sender: get_data_event_sender(),
            instruments: Arc::new(AtomicMap::new()),
            connected: AtomicBool::new(false),
            commands: None,
            cancel: CancellationToken::new(),
            session: None,
            refresher: None,
        })
    }

    fn ensure_known(&self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.instruments.load().contains_key(&instrument_id),
            "unknown Aster instrument {instrument_id}"
        );
        Ok(())
    }

    fn instrument(&self, instrument_id: InstrumentId) -> anyhow::Result<InstrumentAny> {
        self.instruments
            .get_cloned(&instrument_id)
            .with_context(|| format!("unknown Aster instrument {instrument_id}"))
    }

    fn send(&self, command: Command) -> anyhow::Result<()> {
        self.commands
            .as_ref()
            .context("Aster data client is not connected")?
            .send(command)
            .map_err(|_| anyhow::anyhow!("Aster session has stopped"))
    }

    fn subscribe_kind(&self, instrument_ids: &[InstrumentId], kind: Kind) -> anyhow::Result<()> {
        execute_instrument_subscriptions(instrument_ids, |instrument_id| {
            self.ensure_known(instrument_id)?;
            self.send(Command::Subscribe(instrument_id, kind))
        })
    }

    fn unsubscribe_kind(&self, instrument_ids: &[InstrumentId], kind: Kind) -> anyhow::Result<()> {
        // No check that the instrument is known: one the venue has delisted
        // must still be possible to unsubscribe.
        execute_instrument_subscriptions(instrument_ids, |instrument_id| {
            self.send(Command::Unsubscribe(instrument_id, kind))
        })
    }

    fn publish_instrument(&self, instrument: InstrumentAny) {
        if let Err(error) = self.sender.send(DataEvent::Data(instrument.into())) {
            log::warn!("Aster failed to publish instrument: {error}");
        }
    }

    fn halt(&mut self) {
        // A refresh still in flight must not write into a later session.
        if let Some(refresher) = self.refresher.take() {
            refresher.abort();
        }
        self.cancel.cancel();
        self.commands = None;
        self.connected.store(false, Ordering::Release);
    }
}

#[async_trait::async_trait(?Send)]
impl DataClient for AsterDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(common::venue())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting Aster data client {}", self.client_id);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping Aster data client {}", self.client_id);
        self.halt();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.halt();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.halt();
        Ok(())
    }

    fn is_connected(&self) -> bool {
        // A session that ended on its own (a panic, a closed channel) is not
        // connected, whatever the flag says.
        self.connected.load(Ordering::Acquire)
            && self
                .session
                .as_ref()
                .is_some_and(|session| !session.is_finished())
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }

        let parsed = self
            .http
            .request_instruments(self.clock.get_time_ns())
            .await
            .context("failed to load Aster instruments")?;
        for (symbol, error) in &parsed.failed {
            log::warn!("Aster instrument {symbol} skipped: {error:#}");
        }
        log::info!(
            "Loaded {} Aster instruments ({} not trading, {} invalid)",
            parsed.instruments.len(),
            parsed.skipped.len(),
            parsed.failed.len()
        );
        let mut feed = AsterFeed::new(&parsed.instruments);
        match self.http.request_funding_intervals().await {
            Ok(intervals) => feed.set_funding_intervals(&intervals),
            Err(error) => log::warn!("Aster funding intervals unavailable: {error:#}"),
        }
        store_instruments(&self.instruments, &parsed.instruments);
        for instrument in parsed.instruments {
            self.publish_instrument(instrument);
        }

        let (frames_tx, frames) = unbounded_channel();
        let mut pool = StreamPool::new(
            format!("{}/stream", self.config.base_url_ws.trim_end_matches('/')),
            self.config.max_streams_per_connection,
            self.config.max_connections,
            frames_tx,
            self.clock,
        );
        // Open the first connection now, so a venue that cannot be reached
        // fails the connect instead of the first subscription.
        pool.open_connection().await?;

        let (commands_tx, commands_rx) = unbounded_channel();
        let (snapshots_tx, snapshots_rx) = unbounded_channel();
        let (updates_tx, updates_rx) = unbounded_channel();
        self.cancel = CancellationToken::new();
        let session = Session {
            feed,
            pool,
            unplaced: Vec::new(),
            http: self.http.clone(),
            sender: self.sender.clone(),
            clock: self.clock,
            config: self.config.clone(),
            subscriptions: Subscriptions::new(StreamPool::capacity(
                self.config.max_streams_per_connection,
                self.config.max_connections,
            )),
            snapshot_epochs: AHashMap::new(),
            snapshots_tx,
            cancel: self.cancel.clone(),
            events: Vec::new(),
            next_snapshot_at: Instant::now(),
            snapshot_spacing: snapshot_spacing(&self.config),
        };
        self.session =
            Some(get_runtime().spawn(session.run(frames, commands_rx, snapshots_rx, updates_rx)));
        let refresh_every = Duration::from_secs(self.config.instrument_refresh_interval_secs);
        if !refresh_every.is_zero() {
            let refresher = Refresher {
                http: self.http.clone(),
                clock: self.clock,
                sender: self.sender.clone(),
                instruments: Arc::clone(&self.instruments),
                updates: updates_tx,
            };
            self.refresher =
                Some(get_runtime().spawn(refresher.run(refresh_every, self.cancel.clone())));
        }
        self.commands = Some(commands_tx);
        self.connected.store(true, Ordering::Release);
        log::info!("Connected Aster data client {}", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.halt();
        if let Some(mut session) = self.session.take()
            && tokio::time::timeout(DISCONNECT_TIMEOUT, &mut session)
                .await
                .is_err()
        {
            log::warn!("Aster session did not stop within {DISCONNECT_TIMEOUT:?}; aborting it");
            session.abort();
        }
        log::info!("Disconnected Aster data client {}", self.client_id);
        Ok(())
    }

    // Instruments are published on connect and whenever a refresh finds a
    // change, so there is nothing to start here. Publishing them again would
    // write duplicates to the catalog. The Binance client does the same.
    fn subscribe_instruments(&mut self, _cmd: SubscribeInstruments) -> anyhow::Result<()> {
        Ok(())
    }

    fn subscribe_instrument(&mut self, _cmd: SubscribeInstrument) -> anyhow::Result<()> {
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        self.subscribe_kind(&cmd.instrument_ids, Kind::Book)
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        self.subscribe_kind(&cmd.instrument_ids, Kind::Quotes)
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        self.subscribe_kind(&cmd.instrument_ids, Kind::Trades)
    }

    fn subscribe_mark_prices(&mut self, cmd: SubscribeMarkPrices) -> anyhow::Result<()> {
        self.subscribe_kind(&cmd.instrument_ids, Kind::MarkPrice)
    }

    fn subscribe_index_prices(&mut self, cmd: SubscribeIndexPrices) -> anyhow::Result<()> {
        self.subscribe_kind(&cmd.instrument_ids, Kind::IndexPrice)
    }

    fn subscribe_funding_rates(&mut self, cmd: SubscribeFundingRates) -> anyhow::Result<()> {
        self.subscribe_kind(&cmd.instrument_ids, Kind::FundingRate)
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        self.unsubscribe_kind(&cmd.instrument_ids, Kind::Book)
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        self.unsubscribe_kind(&cmd.instrument_ids, Kind::Quotes)
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        self.unsubscribe_kind(&cmd.instrument_ids, Kind::Trades)
    }

    fn unsubscribe_mark_prices(&mut self, cmd: &UnsubscribeMarkPrices) -> anyhow::Result<()> {
        self.unsubscribe_kind(&cmd.instrument_ids, Kind::MarkPrice)
    }

    fn unsubscribe_index_prices(&mut self, cmd: &UnsubscribeIndexPrices) -> anyhow::Result<()> {
        self.unsubscribe_kind(&cmd.instrument_ids, Kind::IndexPrice)
    }

    fn unsubscribe_funding_rates(&mut self, cmd: &UnsubscribeFundingRates) -> anyhow::Result<()> {
        self.unsubscribe_kind(&cmd.instrument_ids, Kind::FundingRate)
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        let instruments: Vec<InstrumentAny> = self.instruments.load().values().cloned().collect();
        let response = DataResponse::Instruments(InstrumentsResponse::new(
            request.request_id,
            request.client_id.unwrap_or(self.client_id),
            common::venue(),
            instruments,
            datetime_to_unix_nanos(request.start),
            datetime_to_unix_nanos(request.end),
            self.clock.get_time_ns(),
            request.params,
        ));
        self.sender
            .send(DataEvent::Response(response))
            .map_err(|error| anyhow::anyhow!("failed to send instruments response: {error}"))
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let instrument_id = request.single_instrument_id()?;
        let instrument = self.instrument(instrument_id)?;
        let response = DataResponse::Instrument(Box::new(InstrumentResponse::new(
            request.request_id,
            request.client_id.unwrap_or(self.client_id),
            instrument_id,
            instrument,
            datetime_to_unix_nanos(request.start),
            datetime_to_unix_nanos(request.end),
            self.clock.get_time_ns(),
            request.params,
        )));
        self.sender
            .send(DataEvent::Response(response))
            .map_err(|error| anyhow::anyhow!("failed to send instrument response: {error}"))
    }

    /// Returns the most recent individual trades inside the request's window.
    ///
    /// The venue serves only its latest 1000 trades of a symbol and has no
    /// time filter, so older history cannot be requested. A window is cut
    /// from a full page; `limit` then keeps the newest trades.
    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let instrument_id = request.single_instrument_id()?;
        let instrument = self.instrument(instrument_id)?;
        let price_precision = instrument.price_precision();
        let size_precision = instrument.size_precision();
        let symbol = common::raw_symbol(&instrument_id).to_owned();
        let start = datetime_to_unix_nanos(request.start);
        let end = datetime_to_unix_nanos(request.end);
        let limit = request
            .limit
            .map_or(TRADES_PAGE, std::num::NonZeroUsize::get)
            .min(TRADES_PAGE);
        let fetch = if start.is_some() || end.is_some() {
            TRADES_PAGE
        } else {
            limit
        };
        let client_id = request.client_id.unwrap_or(self.client_id);
        let http = self.http.clone();
        let sender = self.sender.clone();
        let clock = self.clock;

        get_runtime().spawn(async move {
            let mut trades = match http.request_trades(&symbol, fetch).await.and_then(|body| {
                parse_trades(
                    &body,
                    instrument_id,
                    price_precision,
                    size_precision,
                    clock.get_time_ns(),
                )
            }) {
                Ok(trades) => trades,
                Err(error) => {
                    log::error!("Aster trades request for {instrument_id} failed: {error:#}");
                    return;
                }
            };
            trades.retain(|trade| in_window(trade.ts_event, start, end));
            trades.drain(..trades.len().saturating_sub(limit));
            let response = DataResponse::Trades(TradesResponse::new(
                request.request_id,
                client_id,
                instrument_id,
                trades,
                start,
                end,
                clock.get_time_ns(),
                request.params,
            ));
            if let Err(error) = sender.send(DataEvent::Response(response)) {
                log::error!("Aster failed to send trades response: {error}");
            }
        });
        Ok(())
    }

    fn request_funding_rates(&self, request: RequestFundingRates) -> anyhow::Result<()> {
        let instrument_id = request.single_instrument_id()?;
        self.ensure_known(instrument_id)?;
        let symbol = common::raw_symbol(&instrument_id).to_owned();
        let start = datetime_to_unix_nanos(request.start);
        let end = datetime_to_unix_nanos(request.end);
        let limit = request.limit.map(std::num::NonZeroUsize::get);
        let client_id = request.client_id.unwrap_or(self.client_id);
        let http = self.http.clone();
        let sender = self.sender.clone();
        let clock = self.clock;

        get_runtime().spawn(async move {
            let to_millis = |nanos: UnixNanos| nanos.as_u64() / 1_000_000;
            let end_ms = end.map(to_millis);
            // With an end and no start the venue answers from its oldest
            // row, so search a window wide enough to hold the latest rows
            // before the end at the longest funding interval, and keep the
            // last of them.
            let tail = start.is_none() && end.is_some();
            let mut from_ms = if tail {
                let rows = u64::try_from(limit.unwrap_or(FUNDING_PAGE)).unwrap_or(u64::MAX);
                end_ms.map(|end| end.saturating_sub(rows.saturating_mul(FUNDING_LONGEST_MS)))
            } else {
                start.map(to_millis)
            };
            let mut rates = Vec::new();
            // The last payment of the page before, to time the next one from.
            let mut previous = None;
            for _ in 0..FUNDING_MAX_PAGES {
                let wanted = match limit {
                    Some(limit) if !tail => limit.saturating_sub(rates.len()).min(FUNDING_PAGE),
                    _ => FUNDING_PAGE,
                };
                if wanted == 0 {
                    break;
                }
                let page = match http
                    .request_funding_rates(&symbol, from_ms, end_ms, wanted)
                    .await
                    .and_then(|body| {
                        parse_funding_rates(&body, instrument_id, previous, clock.get_time_ns())
                    }) {
                    Ok(page) => page,
                    Err(error) => {
                        log::error!(
                            "Aster funding rates request for {instrument_id} failed: {error:#}"
                        );
                        return;
                    }
                };
                let full = page.len() >= wanted;
                let last = page.last().map(|rate| to_millis(rate.ts_event));
                previous = last.or(previous);
                rates.extend(page);
                // Without a start the venue returns the most recent rows,
                // which cannot be paged forwards.
                match (full, from_ms, last) {
                    (true, Some(_), Some(last)) => from_ms = Some(last.saturating_add(1)),
                    _ => break,
                }
            }
            // The venue works in milliseconds; the request is in nanoseconds.
            rates.retain(|rate| in_window(rate.ts_event, start, end));
            if let Some(limit) = limit {
                let excess = rates.len().saturating_sub(limit);
                if tail {
                    rates.drain(..excess);
                } else {
                    rates.truncate(limit);
                }
            }
            let response = DataResponse::FundingRates(FundingRatesResponse::new(
                request.request_id,
                client_id,
                instrument_id,
                rates,
                start,
                end,
                clock.get_time_ns(),
                request.params,
            ));
            if let Err(error) = sender.send(DataEvent::Response(response)) {
                log::error!("Aster failed to send funding rates response: {error}");
            }
        });
        Ok(())
    }
}
