//! Live data client.
//!
//! One background session owns the WebSocket connections, the
//! [`AsterFeed`], and the set of subscriptions. The client methods only send
//! it commands.

use std::{collections::VecDeque, sync::Arc, time::Duration};

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
use nautilus_live::book::recovery::BookRecoveryState;
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
    book::recovery::{self, SnapshotAdmission, SnapshotError, SnapshotMessage, SnapshotResult},
    common,
    config::AsterDataClientConfig,
    feed::{AsterFeed, FeedEvent},
    http::{
        client::AsterHttpClient,
        parse::{parse_funding_rates, parse_trades},
    },
    instruments::diff_instruments,
    subscriptions::{Command, Kind, Subscriptions},
    websocket::{
        messages::Envelope,
        pool::{PoolFrame, ReconnectFence, StreamPool},
    },
};

const SUBSCRIBE_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const BOOK_HEALTH_INTERVAL: Duration = Duration::from_mins(1);
const FUNDING_PAGE: usize = 1000;
const TRADES_PAGE: usize = 1000;
const FUNDING_MAX_PAGES: usize = 20;
const FUNDING_LONGEST_MS: u64 = 8 * 60 * 60 * 1_000;
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);

struct Session {
    feed: AsterFeed,
    pool: StreamPool,
    unplaced: Vec<String>,
    http: AsterHttpClient,
    sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    config: AsterDataClientConfig,
    subscriptions: Subscriptions,
    recoveries: AHashMap<Ustr, BookRecoveryState<SnapshotError>>,
    pending_snapshots: VecDeque<SnapshotAdmission>,
    snapshots_tx: UnboundedSender<SnapshotMessage>,
    cancel: CancellationToken,
    events: Vec<FeedEvent>,
    next_snapshot_at: Instant,
    snapshot_spacing: Duration,
}

const fn snapshot_weight(limit: u16) -> u32 {
    match limit {
        0..=50 => 2,
        51..=100 => 5,
        101..=500 => 10,
        _ => 20,
    }
}

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

    fn reset_book(&mut self, symbol: Ustr) {
        self.feed.reset_book(symbol.as_str());
        self.recoveries.remove(&symbol);
        self.pending_snapshots
            .retain(|request| request.symbol != symbol);
    }

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
        let mut opening = std::mem::take(&mut self.unplaced);
        opening.retain(|stream| !changes.closed.contains(stream));
        opening.extend(changes.opened);
        self.open(&opening).await;
        self.pool.unsubscribe(&changes.closed).await;
    }

    fn request_snapshot(&mut self, symbol: Ustr) {
        if !self.cancel.is_cancelled()
            && let Some(owner) = self.recoveries.entry(symbol).or_default().claim()
        {
            recovery::spawn(
                symbol,
                owner,
                self.http.clone(),
                self.snapshots_tx.clone(),
                self.config.book_snapshot_depth,
                Duration::from_secs(self.config.book_snapshot_timeout_secs),
            );
        }
    }

    fn snapshot_fence(&self, symbol: Ustr) -> Option<ReconnectFence> {
        let id = self.feed.instrument_id(symbol.as_str())?;
        self.pool
            .reconnect_fence(&Kind::Book.stream(&id))
            .filter(ReconnectFence::is_current)
    }

    fn admit_snapshot(&mut self) {
        while let Some(SnapshotAdmission {
            symbol,
            owner,
            ready,
        }) = self.pending_snapshots.pop_front()
        {
            if ready.is_closed() || !owner.is_running() || !self.book_wanted(symbol) {
                continue;
            }
            let fence = self.snapshot_fence(symbol);
            let admitted = fence.is_some();
            if ready.send(fence).is_ok() && admitted {
                let now = Instant::now();
                self.next_snapshot_at = now.checked_add(self.snapshot_spacing).unwrap_or(now);
                break;
            }
        }
    }

    fn publish(&mut self) {
        let mut events = std::mem::take(&mut self.events);
        for event in events.drain(..) {
            match event {
                FeedEvent::SnapshotRequired(symbol) => {
                    if self.book_wanted(symbol) {
                        self.request_snapshot(symbol);
                    } else {
                        self.reset_book(symbol);
                    }
                }
                FeedEvent::Data(data) => {
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
        self.events = events;
    }

    async fn on_frame(&mut self, slot: usize, ts_init: UnixNanos, text: &str) {
        if text == RECONNECTED {
            log::warn!(
                "Aster WebSocket connection {slot} reconnected; resubscribing and resyncing its books"
            );
            let (streams, unsent) = self.pool.resubscribe(slot).await;
            self.unplaced.extend(unsent);
            let books: Vec<InstrumentId> = self
                .subscriptions
                .instruments(Kind::Book)
                .filter(|id| streams.contains(&Kind::Book.stream(id)))
                .collect();
            for instrument_id in books {
                let symbol = Ustr::from(common::raw_symbol(&instrument_id));
                self.feed.reset_book(symbol.as_str());
                if let Some(state) = self.recoveries.get_mut(&symbol) {
                    state.reset_on_reconnect();
                }
            }
            return;
        }
        if self.config.debug_frames {
            log::debug!("Aster frame: {text}");
        }
        let envelope = match Envelope::parse(text) {
            Ok(envelope) => envelope,
            Err(error) => {
                log::warn!("Aster dropped a frame: {error:#}");
                return;
            }
        };
        if envelope.stream.is_none() && envelope.data.is_none() {
            let refused = self.pool.on_reply(slot, text);
            self.unplaced.extend(refused);
            self.unplaced
                .retain(|stream| self.subscriptions.uses(stream));
            return;
        }
        if let Err(error) = self
            .feed
            .handle_envelope(&envelope, ts_init, &mut self.events)
        {
            log::warn!("Aster dropped a frame: {error:#}");
        }
        self.publish();
    }

    fn on_snapshot(&mut self, result: SnapshotResult) {
        let outcome = self.apply_snapshot(&result);
        let _ = result.applied.send(outcome);
    }

    fn apply_snapshot(&mut self, snapshot: &SnapshotResult) -> Result<(), SnapshotError> {
        let SnapshotResult {
            symbol,
            owner,
            fence,
            cancellation,
            body,
            ..
        } = snapshot;
        if !owner.is_running()
            || cancellation.is_cancelled()
            || !self
                .snapshot_fence(*symbol)
                .is_some_and(|current| fence.matches(&current))
        {
            return Err(SnapshotError::Obsolete);
        }
        let ts_init = self.clock.get_time_ns();
        let previous_events = self.events.len();
        let result =
            self.feed
                .handle_depth_snapshot(symbol.as_str(), body, ts_init, &mut self.events);
        if cancellation.is_cancelled() || !fence.is_current() {
            self.events.truncate(previous_events);
            self.feed.reset_book(symbol.as_str());
            return Err(SnapshotError::Obsolete);
        }
        let result = result
            .map_err(|error| SnapshotError::Retryable(format!("{error:#}")))
            .and_then(|()| {
                if self.feed.is_book_live(symbol.as_str()) && owner.accept() {
                    Ok(())
                } else {
                    Err(SnapshotError::Unbridged)
                }
            });
        self.publish();
        result
    }

    async fn on_message(&mut self, (slot, ts_init, message): PoolFrame) {
        self.pool.frame_received(&message);
        if let Message::Text(text) = message {
            match std::str::from_utf8(&text) {
                Ok(text) => self.on_frame(slot, ts_init, text).await,
                Err(error) => log::warn!("Aster dropped a frame: {error}"),
            }
        }
    }

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
        mut snapshots: UnboundedReceiver<SnapshotMessage>,
        mut updates: UnboundedReceiver<InstrumentUpdate>,
    ) {
        let mut retry = tokio::time::interval(SUBSCRIBE_RETRY_INTERVAL);
        retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut health = tokio::time::interval(BOOK_HEALTH_INTERVAL);
        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let snapshot_timer = tokio::time::sleep(Duration::ZERO);
        tokio::pin!(snapshot_timer);
        let mut armed_snapshot = None;
        let mut oldest_frame: Option<PoolFrame> = None;
        loop {
            let snapshot_deadline =
                (!self.pending_snapshots.is_empty()).then_some(self.next_snapshot_at);
            if snapshot_deadline != armed_snapshot {
                if let Some(deadline) = snapshot_deadline {
                    snapshot_timer.as_mut().reset(deadline);
                }
                armed_snapshot = snapshot_deadline;
            }
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => {
                    frames.close();
                    if let Some(frame) = oldest_frame.take() {
                        self.on_message(frame).await;
                    }
                    while let Some(frame) = frames.recv().await {
                        self.on_message(frame).await;
                    }
                    break;
                }
                _ = health.tick() => {
                    self.report_unsynced_books();
                    if oldest_frame.is_none() {
                        oldest_frame = frames.try_recv().ok();
                    }
                    let count = frames.len().saturating_add(usize::from(oldest_frame.is_some()));
                    let bytes = self.pool.queued_frame_bytes(count);
                    let age_ms = oldest_frame.as_ref().map_or(0, |(_, ts, _)| {
                        self.clock.get_time_ns().as_u64().saturating_sub(ts.as_u64()) / 1_000_000
                    });
                    log::info!(
                        "Aster ingress backlog frames={count} bytes_estimate={bytes} oldest_ms={age_ms} \
                         pending_book_bytes={} snapshot_jobs={}",
                        self.feed.buffered_depth_bytes(),
                        self.recoveries.values().filter(|state| state.is_running()).count()
                    );
                }
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
                Some(message) = snapshots.recv() => match message {
                    SnapshotMessage::Admit(request) => self.pending_snapshots.push_back(request),
                    SnapshotMessage::Apply(result) => self.on_snapshot(result),
                },
                Some(update) = updates.recv() => {
                    for instrument in &update.instruments {
                        self.feed.add_instrument(instrument);
                        self.reset_book(instrument.raw_symbol().inner());
                    }
                    if let Some(intervals) = &update.funding_intervals {
                        self.feed.replace_funding_intervals(intervals);
                    }
                }
                () = &mut snapshot_timer, if armed_snapshot.is_some() => self.admit_snapshot(),
                frame = async {
                    if let Some(frame) = oldest_frame.take() { Some(frame) } else { frames.recv().await }
                } => match frame {
                    Some(frame) => self.on_message(frame).await,
                    None => break,
                },
            }
        }
        self.recoveries.clear();
        self.pool.close().await;
    }
}

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

fn in_window(ts: UnixNanos, start: Option<UnixNanos>, end: Option<UnixNanos>) -> bool {
    start.is_none_or(|start| ts >= start) && end.is_none_or(|end| ts <= end)
}

struct InstrumentUpdate {
    instruments: Vec<InstrumentAny>,
    funding_intervals: Option<Vec<(String, u16)>>,
}

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
        let funding_intervals = match self.http.request_funding_intervals().await {
            Ok(intervals) if !intervals.is_empty() => Some(intervals),
            Ok(_) => None,
            Err(error) => {
                log::warn!("Aster funding interval refresh failed: {error:#}");
                None
            }
        };

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
        let _ = self.updates.send(InstrumentUpdate {
            instruments,
            funding_intervals,
        });
    }

    async fn run(self, every: Duration, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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

#[derive(Debug)]
struct RunningSession {
    commands: UnboundedSender<Command>,
    cancel: CancellationToken,
    task: JoinHandle<()>,
    refresher: Option<JoinHandle<()>>,
}

impl RunningSession {
    fn is_running(&self) -> bool {
        !self.cancel.is_cancelled() && !self.task.is_finished()
    }

    fn stop(&self) {
        self.cancel.cancel();
        if let Some(refresher) = &self.refresher {
            refresher.abort();
        }
    }

    async fn shutdown(&mut self) {
        self.stop();
        if let Some(refresher) = &mut self.refresher {
            let _ = refresher.await;
            self.refresher = None;
        }
        match tokio::time::timeout(DISCONNECT_TIMEOUT, &mut self.task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => log::warn!("Aster session ended: {error}"),
            Err(_) => {
                log::warn!("Aster session did not stop within {DISCONNECT_TIMEOUT:?}; aborting it");
                self.task.abort();
                let _ = (&mut self.task).await;
            }
        }
    }
}

impl Drop for RunningSession {
    fn drop(&mut self) {
        self.stop();
        self.task.abort();
    }
}

#[derive(Debug)]
pub struct AsterDataClient {
    client_id: ClientId,
    config: AsterDataClientConfig,
    http: AsterHttpClient,
    clock: &'static AtomicTime,
    sender: EventSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    session: Option<RunningSession>,
}

impl AsterDataClient {
    pub fn new(client_id: ClientId, config: AsterDataClientConfig) -> anyhow::Result<Self> {
        let http = AsterHttpClient::new(&config.base_url_http, config.http_timeout_secs)?;
        Ok(Self {
            client_id,
            config,
            http,
            clock: get_atomic_clock_realtime(),
            sender: get_data_event_sender(),
            instruments: Arc::new(AtomicMap::new()),
            session: None,
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

    fn request_instrument_definitions(
        &self,
        into_response: impl FnOnce(Vec<InstrumentAny>, UnixNanos) -> anyhow::Result<DataResponse>
        + Send
        + 'static,
    ) {
        let http = self.http.clone();
        let sender = self.sender.clone();
        let clock = self.clock;
        get_runtime().spawn(async move {
            let response = http
                .request_instruments(clock.get_time_ns())
                .await
                .and_then(|parsed| into_response(parsed.instruments, clock.get_time_ns()));
            match response {
                Ok(response) => {
                    if let Err(error) = sender.send(DataEvent::Response(response)) {
                        log::error!("Aster failed to send instrument response: {error}");
                    }
                }
                Err(error) => log::error!("Aster instrument request failed: {error:#}"),
            }
        });
    }

    fn send(&self, command: Command) -> anyhow::Result<()> {
        self.session
            .as_ref()
            .filter(|session| !session.cancel.is_cancelled())
            .context("Aster data client is not connected")?
            .commands
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
        execute_instrument_subscriptions(instrument_ids, |instrument_id| {
            self.send(Command::Unsubscribe(instrument_id, kind))
        })
    }

    fn publish_instrument(&self, instrument: InstrumentAny) {
        if let Err(error) = self.sender.send(DataEvent::Data(instrument.into())) {
            log::warn!("Aster failed to publish instrument: {error}");
        }
    }

    fn halt(&self) {
        if let Some(session) = &self.session {
            session.stop();
        }
    }

    async fn shutdown_session(&mut self) {
        if let Some(session) = &mut self.session {
            session.shutdown().await;
            self.session = None;
        }
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
        self.session
            .as_ref()
            .is_some_and(RunningSession::is_running)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        self.shutdown_session().await;

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
        let mut feed = AsterFeed::new(&parsed.instruments).with_book_buffer_limits(
            self.config.book_buffer_bytes,
            self.config.session_book_buffer_bytes,
        );
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
        pool.open_connection().await?;

        let (commands_tx, commands_rx) = unbounded_channel();
        let (snapshots_tx, snapshots_rx) = unbounded_channel();
        let (updates_tx, updates_rx) = unbounded_channel();
        let cancel = CancellationToken::new();
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
            recoveries: AHashMap::new(),
            pending_snapshots: VecDeque::new(),
            snapshots_tx,
            cancel: cancel.clone(),
            events: Vec::new(),
            next_snapshot_at: Instant::now(),
            snapshot_spacing: snapshot_spacing(&self.config),
        };
        let task = get_runtime().spawn(session.run(frames, commands_rx, snapshots_rx, updates_rx));
        let refresh_every = Duration::from_secs(self.config.instrument_refresh_interval_secs);
        let refresher = (!refresh_every.is_zero()).then(|| {
            let refresher = Refresher {
                http: self.http.clone(),
                clock: self.clock,
                sender: self.sender.clone(),
                instruments: Arc::clone(&self.instruments),
                updates: updates_tx,
            };
            get_runtime().spawn(refresher.run(refresh_every, cancel.clone()))
        });
        self.session = Some(RunningSession {
            commands: commands_tx,
            cancel,
            task,
            refresher,
        });
        log::info!("Connected Aster data client {}", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.shutdown_session().await;
        log::info!("Disconnected Aster data client {}", self.client_id);
        Ok(())
    }

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
        let client_id = request.client_id.unwrap_or(self.client_id);
        self.request_instrument_definitions(move |instruments, ts_init| {
            Ok(DataResponse::Instruments(InstrumentsResponse::new(
                request.request_id,
                client_id,
                common::venue(),
                instruments,
                datetime_to_unix_nanos(request.start),
                datetime_to_unix_nanos(request.end),
                ts_init,
                request.params,
            )))
        });
        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let instrument_id = request.single_instrument_id()?;
        let client_id = request.client_id.unwrap_or(self.client_id);
        self.request_instrument_definitions(move |instruments, ts_init| {
            let instrument = instruments
                .into_iter()
                .find(|instrument| instrument.id() == instrument_id)
                .with_context(|| format!("unknown Aster instrument {instrument_id}"))?;
            Ok(DataResponse::Instrument(Box::new(InstrumentResponse::new(
                request.request_id,
                client_id,
                instrument_id,
                instrument,
                datetime_to_unix_nanos(request.start),
                datetime_to_unix_nanos(request.end),
                ts_init,
                request.params,
            ))))
        });
        Ok(())
    }

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
            let tail = start.is_none() && end.is_some();
            let mut from_ms = if tail {
                let rows = u64::try_from(limit.unwrap_or(FUNDING_PAGE)).unwrap_or(u64::MAX);
                end_ms.map(|end| end.saturating_sub(rows.saturating_mul(FUNDING_LONGEST_MS)))
            } else {
                start.map(to_millis)
            };
            let mut rates = Vec::new();
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
                match (full, from_ms, last) {
                    (true, Some(_), Some(last)) => from_ms = Some(last.saturating_add(1)),
                    _ => break,
                }
            }
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
