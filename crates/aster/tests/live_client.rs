//! The data client against a mock venue on real sockets.
//!
//! Each test starts an HTTP server and a WebSocket server on localhost,
//! points an [`AsterDataClient`] at them, and drives it the way a node does:
//! connect, subscribe, receive. The mock records what the client sent and
//! lets the test push frames, change the venue's metadata, and cut
//! connections.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::significant_drop_tightening,
    reason = "test code"
)]

use std::{
    collections::BTreeSet,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use nautilus_common::{
    clients::DataClient,
    live::{get_runtime, runner::replace_data_event_sender},
    messages::{
        DataEvent,
        data::{
            DataResponse, RequestFundingRates, RequestTrades, SubscribeBookDeltas,
            SubscribeFundingRates, SubscribeQuotes, SubscribeTrades, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    data::{Data, InstrumentStatus, MarkPriceUpdate},
    enums::{BookType, MarketStatusAction},
    identifiers::{ClientId, InstrumentId},
    instruments::Instrument,
    types::Price,
};
use nt_dex_aster::{
    common::{ASTER, F_SNAPSHOT, instrument_id},
    config::AsterDataClientConfig,
    data::AsterDataClient,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
};
use tokio_tungstenite::tungstenite::Message;

const EXCHANGE_INFO: &str = include_str!("../test_data/http_exchange_info.json");
const FUNDING_INFO: &str = include_str!("../test_data/http_funding_info.json");
const DEPTH_SNAPSHOT: &str = include_str!("../test_data/http_depth_solusdt.json");
const HTTP_TRADES: &str = include_str!("../test_data/http_trades.json");
const WS_TRADE: &str = include_str!("../test_data/ws_trade.json");
const WS_BOOK_TICKER: &str = include_str!("../test_data/ws_book_ticker.json");
const WS_MARK_PRICE: &str = include_str!("../test_data/ws_mark_price.json");

/// The real venue refuses a message somewhere between 3959 and 4077 bytes.
const VENUE_MESSAGE_LIMIT: usize = 4_000;
const TIMEOUT: Duration = Duration::from_secs(10);

/// What the test tells a mock WebSocket connection to do.
enum Outgoing {
    Text(String),
    Drop,
}

/// One WebSocket connection the mock accepted.
struct Connection {
    streams: BTreeSet<String>,
    /// `None` once the connection has ended.
    out: Option<UnboundedSender<Outgoing>>,
}

struct State {
    exchange_info: String,
    snapshot_update_id: u64,
    pause_next_snapshot: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
    http_requests: Vec<String>,
    connections: Vec<Connection>,
    /// Every `SUBSCRIBE` and `UNSUBSCRIBE`: connection, method, streams.
    control: Vec<(usize, String, Vec<String>)>,
    /// Messages refused for passing the venue's size limit.
    oversized: usize,
    /// Streams whose next `SUBSCRIBE` is answered with an error.
    refuse_once: BTreeSet<String>,
    /// Signals that a connection was accepted, then waits before upgrading it.
    pause_next_handshake: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
}

impl State {
    fn requests(&self, prefix: &str) -> usize {
        self.http_requests
            .iter()
            .filter(|path| path.starts_with(prefix))
            .count()
    }
}

#[derive(Clone)]
struct Venue {
    state: Arc<Mutex<State>>,
    http_url: String,
    ws_url: String,
}

impl Venue {
    async fn start() -> Self {
        let state = Arc::new(Mutex::new(State {
            exchange_info: EXCHANGE_INFO.to_owned(),
            snapshot_update_id: 0,
            pause_next_snapshot: None,
            http_requests: Vec::new(),
            connections: Vec::new(),
            control: Vec::new(),
            oversized: 0,
            refuse_once: BTreeSet::new(),
            pause_next_handshake: None,
        }));

        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_url = format!("http://{}", http.local_addr().unwrap());
        tokio::spawn({
            let state = Arc::clone(&state);
            async move {
                while let Ok((socket, _)) = http.accept().await {
                    tokio::spawn(serve_http(socket, Arc::clone(&state)));
                }
            }
        });

        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}", ws.local_addr().unwrap());
        tokio::spawn({
            let state = Arc::clone(&state);
            async move {
                while let Ok((socket, _)) = ws.accept().await {
                    tokio::spawn(serve_ws(socket, Arc::clone(&state)));
                }
            }
        });

        Self {
            state,
            http_url,
            ws_url,
        }
    }

    fn config(&self) -> AsterDataClientConfig {
        AsterDataClientConfig {
            base_url_http: self.http_url.clone(),
            base_url_ws: self.ws_url.clone(),
            instrument_refresh_interval_secs: 0,
            // No pacing between snapshots.
            snapshot_weight_per_minute: u32::MAX,
            ..Default::default()
        }
    }

    /// Polls until `check` returns a value.
    async fn wait<T>(&self, what: &str, check: impl Fn(&State) -> Option<T>) -> T {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        loop {
            if let Some(value) = check(&self.state.lock().unwrap()) {
                return value;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Waits until a live connection carries `stream` and returns its index.
    async fn wait_for_stream(&self, stream: &str) -> usize {
        self.wait(stream, |state| {
            state.connections.iter().position(|connection| {
                connection.out.is_some() && connection.streams.contains(stream)
            })
        })
        .await
    }

    /// Sends a frame on the connection that carries its stream.
    async fn push(&self, frame: &str) {
        let envelope: serde_json::Value = serde_json::from_str(frame).unwrap();
        let stream = envelope["stream"].as_str().unwrap().to_owned();
        let index = self.wait_for_stream(&stream).await;
        self.push_on(index, frame);
    }

    /// Sends a frame on a given connection, whatever it is subscribed to.
    fn push_on(&self, index: usize, frame: &str) {
        let state = self.state.lock().unwrap();
        let out = state.connections[index].out.as_ref().unwrap();
        out.send(Outgoing::Text(frame.to_owned())).unwrap();
    }

    fn live_connections(&self) -> Vec<BTreeSet<String>> {
        let state = self.state.lock().unwrap();
        state
            .connections
            .iter()
            .filter(|connection| connection.out.is_some())
            .map(|connection| connection.streams.clone())
            .collect()
    }

    fn requests(&self, prefix: &str) -> usize {
        self.state.lock().unwrap().requests(prefix)
    }

    /// Waits until more than `more_than` `exchangeInfo` requests have arrived.
    async fn wait_for_refreshes(&self, what: &str, more_than: usize) {
        self.wait(what, |state| {
            (state.requests("/fapi/v1/exchangeInfo") > more_than).then_some(())
        })
        .await;
    }

    /// Waits for an `UNSUBSCRIBE` that `matches` accepts.
    async fn wait_for_unsubscribe(&self, matches: impl Fn(&[String]) -> bool) {
        self.wait("UNSUBSCRIBE", |state| {
            state
                .control
                .iter()
                .any(|(_, method, streams)| method == "UNSUBSCRIBE" && matches(streams))
                .then_some(())
        })
        .await;
    }

    /// Cuts a connection without a close handshake and waits until it has
    /// ended.
    async fn drop_connection(&self, index: usize) {
        {
            let state = self.state.lock().unwrap();
            if let Some(out) = &state.connections[index].out {
                out.send(Outgoing::Drop).unwrap();
            }
        }
        self.wait("the connection to end", |state| {
            state.connections[index].out.is_none().then_some(())
        })
        .await;
    }
}

async fn serve_http(mut socket: tokio::net::TcpStream, state: Arc<Mutex<State>>) {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => request.extend_from_slice(&chunk[..read]),
        }
    }
    let request = String::from_utf8_lossy(&request);
    let path = request
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_owned();
    let (body, pause) = {
        let mut state = state.lock().unwrap();
        state.http_requests.push(path.clone());
        let body = if path.starts_with("/fapi/v1/exchangeInfo") {
            state.exchange_info.clone()
        } else if path.starts_with("/fapi/v1/fundingInfo") {
            FUNDING_INFO.to_owned()
        } else if path.starts_with("/fapi/v1/depth") {
            DEPTH_SNAPSHOT.replace("668462586092", &state.snapshot_update_id.to_string())
        } else if path.starts_with("/fapi/v1/trades") {
            HTTP_TRADES.to_owned()
        } else if path.starts_with("/fapi/v1/fundingRate") {
            r#"[{"symbol":"SOLUSDT","fundingTime":1795305600000,"fundingRate":"-0.00001244"},
                {"symbol":"SOLUSDT","fundingTime":1795334400001,"fundingRate":"0.00006146"}]"#
                .to_owned()
        } else {
            "{}".to_owned()
        };
        let pause = if path.starts_with("/fapi/v1/depth") {
            state.pause_next_snapshot.take()
        } else {
            None
        };
        (body, pause)
    };
    if let Some((received, resume)) = pause {
        let _ = received.send(());
        let _ = resume.await;
    }
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\
         connection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.shutdown().await;
}

async fn serve_ws(socket: tokio::net::TcpStream, state: Arc<Mutex<State>>) {
    let pause = state.lock().unwrap().pause_next_handshake.take();
    if let Some((accepted, resume)) = pause {
        let _ = accepted.send(());
        let _ = resume.await;
    }
    let Ok(stream) = tokio_tungstenite::accept_async(socket).await else {
        return;
    };
    let (mut sink, mut source) = stream.split();
    let (out, mut outgoing) = unbounded_channel();
    let index = {
        let mut state = state.lock().unwrap();
        state.connections.push(Connection {
            streams: BTreeSet::new(),
            out: Some(out),
        });
        state.connections.len() - 1
    };

    loop {
        tokio::select! {
            command = outgoing.recv() => match command {
                Some(Outgoing::Text(text)) => {
                    if sink.send(Message::text(text)).await.is_err() {
                        break;
                    }
                }
                Some(Outgoing::Drop) | None => break,
            },
            message = source.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    if text.len() > VENUE_MESSAGE_LIMIT {
                        // The venue closes with 3001 "illegal request".
                        state.lock().unwrap().oversized += 1;
                        break;
                    }
                    let request: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();
                    let method = request["method"].as_str().unwrap().to_owned();
                    let streams: Vec<String> = request["params"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|stream| stream.as_str().unwrap().to_owned())
                        .collect();
                    let refused = {
                        let mut state = state.lock().unwrap();
                        let refused = method == "SUBSCRIBE"
                            && streams.iter().any(|stream| state.refuse_once.contains(stream));
                        for stream in &streams {
                            state.refuse_once.remove(stream);
                        }
                        let connection = &mut state.connections[index];
                        for stream in &streams {
                            if refused {
                            } else if method == "SUBSCRIBE" {
                                connection.streams.insert(stream.clone());
                            } else {
                                connection.streams.remove(stream);
                            }
                        }
                        state.control.push((index, method, streams));
                        refused
                    };
                    let reply = if refused {
                        // The word "stream" in a reply value is not a stream key.
                        format!(
                            r#"{{"error":{{"code":2,"msg":"stream"}},"id":{}}}"#,
                            request["id"]
                        )
                    } else {
                        format!(r#"{{"result":null,"id":{}}}"#, request["id"])
                    };
                    if sink.send(Message::text(reply)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    let _ = sink.send(Message::Pong(payload)).await;
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            },
        }
    }
    state.lock().unwrap().connections[index].out = None;
}

/// A connected client and the events it publishes.
struct Harness {
    venue: Venue,
    client: AsterDataClient,
    events: UnboundedReceiver<DataEvent>,
}

impl Harness {
    async fn start(configure: impl FnOnce(&mut AsterDataClientConfig)) -> Self {
        Self::connect(Venue::start().await, configure).await
    }

    async fn connect(venue: Venue, configure: impl FnOnce(&mut AsterDataClientConfig)) -> Self {
        let (sender, events) = unbounded_channel::<DataEvent>();
        replace_data_event_sender(sender);
        let mut config = venue.config();
        configure(&mut config);
        let mut client = AsterDataClient::new(ClientId::new(ASTER), config).unwrap();
        client.connect().await.unwrap();
        assert!(client.is_connected());
        Self {
            venue,
            client,
            events,
        }
    }

    /// Receives events until `pick` accepts one.
    async fn next<T>(&mut self, what: &str, pick: impl Fn(&DataEvent) -> Option<T>) -> T {
        let wait = async {
            loop {
                let event = self.events.recv().await.expect("event channel closed");
                if let Some(value) = pick(&event) {
                    return value;
                }
            }
        };
        tokio::time::timeout(TIMEOUT, wait)
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
    }

    /// Asserts that nothing `pick` accepts arrives for a short while.
    async fn expect_none(&mut self, what: &str, pick: impl Fn(&DataEvent) -> bool) {
        let quiet = Duration::from_millis(300);
        let wait = async {
            loop {
                let event = self.events.recv().await.expect("event channel closed");
                if pick(&event) {
                    return event;
                }
            }
        };
        if let Ok(event) = tokio::time::timeout(quiet, wait).await {
            panic!("unexpected {what}: {event:?}");
        }
    }

    fn try_subscribe_trades(&mut self, symbols: &[&str]) -> anyhow::Result<()> {
        let client_id = Some(self.client.client_id());
        self.client.subscribe_trades(SubscribeTrades::new(
            ids(symbols),
            client_id,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
    }

    fn subscribe_trades(&mut self, symbols: &[&str]) {
        self.try_subscribe_trades(symbols).unwrap();
    }

    fn unsubscribe_trades(&mut self, symbols: &[&str]) {
        let client_id = Some(self.client.client_id());
        self.client
            .unsubscribe_trades(&UnsubscribeTrades::new(
                ids(symbols),
                client_id,
                None,
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();
    }

    fn subscribe_quotes(&mut self, symbols: &[&str]) {
        let client_id = Some(self.client.client_id());
        self.client
            .subscribe_quotes(SubscribeQuotes::new(
                ids(symbols),
                client_id,
                None,
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();
    }

    fn subscribe_book(&mut self, symbols: &[&str]) {
        self.client
            .subscribe_book_deltas(SubscribeBookDeltas::new(
                ids(symbols),
                BookType::L2_MBP,
                Some(self.client.client_id()),
                None,
                UUID4::new(),
                UnixNanos::default(),
                None,
                false,
                None,
                None,
            ))
            .unwrap();
    }

    /// Requests recent trades and waits for the response.
    async fn request_trades(
        &mut self,
        instrument_id: InstrumentId,
        window: (Option<u64>, Option<u64>),
        limit: usize,
    ) -> Vec<(String, u64)> {
        let to_timestamp = |ms: u64| UnixNanos::from(ms * 1_000_000).to_datetime_utc();
        self.client
            .request_trades(RequestTrades::new(
                vec![instrument_id],
                window.0.map(to_timestamp),
                window.1.map(to_timestamp),
                NonZeroUsize::new(limit),
                Some(self.client.client_id()),
                UUID4::new(),
                UnixNanos::default(),
                None,
            ))
            .unwrap();
        self.next("trades response", |event| match event {
            DataEvent::Response(DataResponse::Trades(response)) => Some(
                response
                    .data
                    .iter()
                    .map(|trade| {
                        (
                            trade.trade_id.to_string(),
                            trade.ts_event.as_u64() / 1_000_000,
                        )
                    })
                    .collect(),
            ),
            _ => None,
        })
        .await
    }

    async fn stop(mut self) {
        self.client.disconnect().await.unwrap();
        assert!(!self.client.is_connected());
    }
}

fn ids(symbols: &[&str]) -> Vec<InstrumentId> {
    symbols.iter().map(|symbol| instrument_id(symbol)).collect()
}

/// The trade fixture, rewritten for another symbol and trade id, at a price
/// and size every test instrument can hold.
fn trade_frame(symbol: &str, id: u64) -> String {
    WS_TRADE
        .replace("BTCUSDT", symbol)
        .replace("btcusdt", &symbol.to_lowercase())
        .replace("150490341", &id.to_string())
        .replace("60757.6", "71")
        .replace("0.045", "62")
}

fn depth_diff(first: u64, last: u64, previous: u64) -> String {
    format!(
        r#"{{"stream":"solusdt@depth@100ms","data":{{"e":"depthUpdate","E":1795345453681,"T":1795345453600,"s":"SOLUSDT","U":{first},"u":{last},"pu":{previous},"b":[["70.6900","5.00"]],"a":[["70.7200","6.00"]]}}}}"#
    )
}

fn instrument(event: &DataEvent) -> Option<InstrumentId> {
    match event {
        DataEvent::Data(Data::Instrument(instrument)) => Some(instrument.id()),
        _ => None,
    }
}

fn instrument_status(event: &DataEvent) -> Option<InstrumentStatus> {
    match event {
        DataEvent::Data(Data::InstrumentStatus(status)) => Some(*status),
        _ => None,
    }
}

fn quote(event: &DataEvent) -> Option<()> {
    matches!(event, DataEvent::Data(Data::Quote(_))).then_some(())
}

fn mark_price(event: &DataEvent) -> Option<MarkPriceUpdate> {
    match event {
        DataEvent::Data(Data::MarkPrice(mark)) => Some(*mark),
        _ => None,
    }
}

fn trade_id(event: &DataEvent) -> Option<String> {
    match event {
        DataEvent::Data(Data::Trade(trade)) => Some(trade.trade_id.to_string()),
        _ => None,
    }
}

fn trade(event: &DataEvent) -> Option<InstrumentId> {
    match event {
        DataEvent::Data(Data::Trade(trade)) => Some(trade.instrument_id),
        _ => None,
    }
}

/// Returns `(sequence, is_snapshot)` of a book update.
fn book(event: &DataEvent) -> Option<(u64, bool)> {
    match event {
        DataEvent::Data(Data::BookDeltas(deltas)) => {
            Some((deltas.sequence, deltas.flags & F_SNAPSHOT != 0))
        }
        _ => None,
    }
}

fn run(test: impl Future<Output = ()>) {
    get_runtime().block_on(test);
}

#[test]
fn connect_publishes_instruments_and_streams_trades_and_quotes() {
    run(async {
        let mut harness = Harness::start(|_| {}).await;
        let mut loaded = BTreeSet::new();
        while loaded.len() < 4 {
            loaded.insert(harness.next("instrument", instrument).await);
        }
        assert!(loaded.contains(&instrument_id("SOLUSDT")));
        assert_eq!(harness.venue.requests("/fapi/v1/exchangeInfo"), 1);
        assert_eq!(harness.venue.requests("/fapi/v1/fundingInfo"), 1);

        harness.subscribe_trades(&["SOLUSDT"]);
        harness.subscribe_quotes(&["SOLUSDT"]);
        harness.venue.push(&trade_frame("SOLUSDT", 100)).await;
        harness.venue.push(WS_BOOK_TICKER).await;

        let traded = harness.next("trade", trade).await;
        assert_eq!(traded, instrument_id("SOLUSDT"));
        harness.next("quote", quote).await;

        // Both subscriptions arrived close together, on one connection.
        assert_eq!(harness.venue.live_connections().len(), 1);
        harness.stop().await;
    });
}

#[test]
fn connecting_after_stop_waits_for_the_old_session() {
    run(async {
        let mut harness = Harness::start(|config| config.max_streams_per_connection = 1).await;
        let (accepted, pending) = oneshot::channel();
        let (resume, paused) = oneshot::channel();
        harness.venue.state.lock().unwrap().pause_next_handshake = Some((accepted, paused));
        harness.subscribe_trades(&["BTCUSDT", "SOLUSDT"]);
        tokio::time::timeout(TIMEOUT, pending)
            .await
            .unwrap()
            .unwrap();

        harness.client.stop().unwrap();
        assert!(harness.client.is_disconnected());
        assert!(harness.try_subscribe_trades(&["BTCUSDT"]).is_err());

        {
            let restart = harness.client.connect();
            tokio::pin!(restart);
            assert!(
                tokio::time::timeout(Duration::from_secs(1), &mut restart)
                    .await
                    .is_err(),
                "connect returned while the old session was still opening a socket"
            );
            resume.send(()).unwrap();
            tokio::time::timeout(TIMEOUT, restart)
                .await
                .unwrap()
                .unwrap();
        }

        assert!(harness.client.is_connected());
        harness.subscribe_trades(&["SOLUSDT"]);
        harness.venue.push(&trade_frame("SOLUSDT", 100)).await;
        assert_eq!(harness.next("trade", trade).await, instrument_id("SOLUSDT"));
        harness.stop().await;
    });
}

#[test]
fn unsubscribed_data_is_not_published() {
    run(async {
        let mut harness = Harness::start(|_| {}).await;
        harness.subscribe_trades(&["SOLUSDT"]);
        harness.venue.push(&trade_frame("SOLUSDT", 100)).await;
        harness.next("trade", trade).await;

        harness.unsubscribe_trades(&["SOLUSDT"]);
        harness.venue.wait_for_unsubscribe(|_| true).await;
        assert!(harness.venue.live_connections()[0].is_empty());

        // A new trade that was already on its way is not published, while
        // a stream that is still wanted keeps delivering.
        harness.subscribe_quotes(&["SOLUSDT"]);
        harness.venue.wait_for_stream("solusdt@bookTicker").await;
        harness.venue.push_on(0, &trade_frame("SOLUSDT", 101));
        harness.venue.push(WS_BOOK_TICKER).await;
        harness
            .next("quote, with no trade before it", |event| match event {
                DataEvent::Data(Data::Trade(trade)) => panic!("unexpected trade {trade}"),
                DataEvent::Data(Data::Quote(_)) => Some(()),
                _ => None,
            })
            .await;
        harness.stop().await;
    });
}

#[test]
fn funding_subscription_publishes_mark_index_and_funding() {
    run(async {
        let mut harness = Harness::start(|_| {}).await;
        harness
            .client
            .subscribe_funding_rates(SubscribeFundingRates::new(
                ids(&["SOLUSDT"]),
                Some(harness.client.client_id()),
                None,
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();
        harness.venue.push(WS_MARK_PRICE).await;

        let mark = harness.next("mark price", mark_price).await;
        // Eight decimals whatever the instrument's tick: SOL trades at four.
        assert_eq!(mark.value, Price::from("70.71000000"));
        assert_eq!(mark.value.precision, 8);
        let index = harness
            .next("index price", |event| match event {
                DataEvent::Data(Data::IndexPrice(index)) => Some(*index),
                _ => None,
            })
            .await;
        assert_eq!(index.value, Price::from("70.74245126"));
        let funding = harness
            .next("funding rate", |event| match event {
                DataEvent::Data(Data::FundingRate(funding)) => Some(*funding),
                _ => None,
            })
            .await;
        assert_eq!(funding.interval, Some(480), "from fundingInfo");
        assert!(funding.next_funding_ns.is_some());

        // A whole-number mark price still has eight decimals.
        let whole = WS_MARK_PRICE.replace(r#""p":"70.71000000""#, r#""p":"71""#);
        assert_ne!(
            whole, WS_MARK_PRICE,
            "fixture no longer has the expected mark"
        );
        harness.venue.push(&whole).await;
        let mark = harness.next("mark price", mark_price).await;
        assert_eq!(mark.value.precision, 8);
        assert_eq!(mark.value, Price::from("71.00000000"));
        harness.stop().await;
    });
}

#[test]
fn book_syncs_from_a_snapshot_and_resyncs_after_a_gap() {
    run(async {
        let mut harness = Harness::start(|_| {}).await;
        harness.subscribe_book(&["SOLUSDT"]);

        // The first diff makes the client ask for a snapshot. The snapshot
        // falls inside the diff's range, so the diff applies on top of it.
        harness.venue.state.lock().unwrap().snapshot_update_id = 1_000;
        harness.venue.push(&depth_diff(990, 1_005, 980)).await;
        assert_eq!(harness.next("snapshot", book).await, (1_000, true));
        assert_eq!(harness.next("first diff", book).await, (1_005, false));
        assert_eq!(harness.venue.requests("/fapi/v1/depth"), 1);

        // The next diff continues the sequence.
        harness.venue.push(&depth_diff(1_006, 1_010, 1_005)).await;
        assert_eq!(harness.next("second diff", book).await, (1_010, false));
        assert_eq!(harness.venue.requests("/fapi/v1/depth"), 1);

        // A diff that does not follow the last one is a gap: the book is
        // rebuilt from a new snapshot.
        harness.venue.state.lock().unwrap().snapshot_update_id = 2_003;
        harness.venue.push(&depth_diff(2_001, 2_005, 2_000)).await;
        assert_eq!(harness.next("resync snapshot", book).await, (2_003, true));
        assert_eq!(
            harness.next("diff after resync", book).await,
            (2_005, false)
        );
        assert_eq!(harness.venue.requests("/fapi/v1/depth"), 2);
        harness.stop().await;
    });
}

#[test]
fn recovery_budget_discards_old_diffs_and_keeps_one_snapshot_in_flight() {
    run(async {
        let mut harness = Harness::start(|config| config.book_buffer_bytes = 0).await;
        harness.subscribe_book(&["SOLUSDT"]);
        harness.subscribe_trades(&["SOLUSDT"]);
        let (ready, started) = oneshot::channel();
        let (resume, paused) = oneshot::channel();
        {
            let mut state = harness.venue.state.lock().unwrap();
            state.snapshot_update_id = 1_000;
            state.pause_next_snapshot = Some((ready, paused));
        }
        harness.venue.push(&depth_diff(990, 1_005, 980)).await;
        tokio::time::timeout(TIMEOUT, started)
            .await
            .unwrap()
            .unwrap();
        harness.venue.push(&depth_diff(1_006, 1_010, 1_005)).await;
        harness.venue.push(&trade_frame("SOLUSDT", 100)).await;
        harness.next("trade after the buffered diffs", trade).await;
        assert_eq!(harness.venue.requests("/fapi/v1/depth"), 1);

        harness.venue.state.lock().unwrap().snapshot_update_id = 1_010;
        resume.send(()).unwrap();
        assert_eq!(harness.next("covering snapshot", book).await, (1_010, true));
        assert_eq!(harness.venue.requests("/fapi/v1/depth"), 2);
        harness.venue.push(&depth_diff(1_011, 1_015, 1_010)).await;
        assert_eq!(harness.next("linked diff", book).await, (1_015, false));
        harness.stop().await;
    });
}

#[test]
fn stalled_trade_consumer_receives_the_full_burst_in_order() {
    const TRADES: usize = 256;
    run(async {
        let mut harness = Harness::start(|_| {}).await;
        while harness.events.try_recv().is_ok() {}
        harness.subscribe_trades(&["SOLUSDT"]);
        let slot = harness.venue.wait_for_stream("solusdt@trade").await;
        for id in 0..u64::try_from(TRADES).unwrap() {
            harness.venue.push_on(slot, &trade_frame("SOLUSDT", id));
        }
        tokio::time::timeout(TIMEOUT, async {
            while harness.events.len() < TRADES {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("all trades queued while the consumer was stalled");
        for id in 0..TRADES {
            assert_eq!(harness.next("queued trade", trade_id).await, id.to_string());
        }
        harness.stop().await;
    });
}

#[test]
fn reconnect_resubscribes_and_resyncs_the_book() {
    run(async {
        let mut harness = Harness::start(|_| {}).await;
        harness.subscribe_trades(&["SOLUSDT"]);
        harness.subscribe_book(&["SOLUSDT"]);
        harness.venue.state.lock().unwrap().snapshot_update_id = 1_000;
        harness.venue.push(&depth_diff(990, 1_005, 980)).await;
        assert_eq!(harness.next("snapshot", book).await, (1_000, true));
        assert_eq!(harness.next("diff", book).await, (1_005, false));

        let first = harness.venue.wait_for_stream("solusdt@trade").await;
        harness.venue.drop_connection(first).await;

        // The client comes back on a new connection with the same streams.
        let second = harness
            .venue
            .wait("resubscription", |state| {
                state.connections.iter().position(|connection| {
                    connection.out.is_some()
                        && connection.streams.contains("solusdt@trade")
                        && connection.streams.contains("solusdt@depth@100ms")
                })
            })
            .await;
        assert_ne!(first, second);

        harness.venue.push(&trade_frame("SOLUSDT", 100)).await;
        harness.next("trade after reconnect", trade).await;

        // Updates were missed while disconnected, so even a diff that would
        // have continued the old sequence starts from a new snapshot.
        harness.venue.state.lock().unwrap().snapshot_update_id = 1_008;
        harness.venue.push(&depth_diff(1_006, 1_010, 1_005)).await;
        assert_eq!(
            harness.next("snapshot after reconnect", book).await,
            (1_008, true)
        );
        assert_eq!(
            harness.next("diff after reconnect", book).await,
            (1_010, false)
        );
        assert_eq!(harness.venue.requests("/fapi/v1/depth"), 2);
        harness.stop().await;
    });
}

#[test]
fn reconnect_retires_an_in_flight_snapshot_before_starting_a_new_book() {
    run(async {
        let mut harness = Harness::start(|_| {}).await;
        harness.subscribe_book(&["SOLUSDT"]);
        let old_slot = harness.venue.wait_for_stream("solusdt@depth@100ms").await;
        let (received, started) = oneshot::channel();
        let (resume, paused) = oneshot::channel();
        {
            let mut state = harness.venue.state.lock().unwrap();
            state.snapshot_update_id = 1_000;
            state.pause_next_snapshot = Some((received, paused));
        }
        harness
            .venue
            .push_on(old_slot, &depth_diff(990, 1_005, 980));
        tokio::time::timeout(TIMEOUT, started)
            .await
            .unwrap()
            .unwrap();
        harness.venue.drop_connection(old_slot).await;
        let new_slot = harness.venue.wait_for_stream("solusdt@depth@100ms").await;
        assert_ne!(old_slot, new_slot);

        harness.venue.state.lock().unwrap().snapshot_update_id = 2_000;
        harness
            .venue
            .push_on(new_slot, &depth_diff(1_990, 2_005, 1_980));
        assert_eq!(
            harness.next("new generation snapshot", book).await,
            (2_000, true)
        );
        assert_eq!(
            harness.next("new generation diff", book).await,
            (2_005, false)
        );
        resume.send(()).unwrap();
        harness
            .expect_none("retired snapshot", |event| book(event).is_some())
            .await;
        assert_eq!(harness.venue.requests("/fapi/v1/depth"), 2);
        harness.stop().await;
    });
}

#[test]
fn streams_spread_over_connections_at_the_stream_limit() {
    run(async {
        let mut harness = Harness::start(|config| {
            config.max_streams_per_connection = 2;
            config.max_connections = 3;
        })
        .await;
        let symbols = ["SOLUSDT", "BTCUSDT", "YFIUSDT"];
        harness.subscribe_trades(&symbols);
        harness.subscribe_quotes(&symbols);

        harness
            .venue
            .wait("six streams", |state| {
                let open: usize = state.connections.iter().map(|c| c.streams.len()).sum();
                (open == 6).then_some(())
            })
            .await;
        let connections = harness.venue.live_connections();
        assert_eq!(connections.len(), 3, "two streams each: {connections:?}");
        assert!(connections.iter().all(|streams| streams.len() == 2));

        // Data arrives whichever connection carries the stream.
        for symbol in symbols {
            harness.venue.push(&trade_frame(symbol, 100)).await;
            assert_eq!(harness.next("trade", trade).await, instrument_id(symbol));
        }

        // The pool is full: a seventh stream is refused and nothing is sent.
        let sent = harness.venue.state.lock().unwrap().control.len();
        harness.subscribe_trades(&["XAUUSD1"]);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(harness.venue.state.lock().unwrap().control.len(), sent);

        // Closing a stream frees its place for another, on the same
        // connection and without opening a new one.
        harness.unsubscribe_trades(&["BTCUSDT"]);
        let freed = harness
            .venue
            .wait("a free place", |state| {
                state.connections.iter().position(|c| c.streams.len() == 1)
            })
            .await;
        harness.subscribe_trades(&["XAUUSD1"]);
        let index = harness.venue.wait_for_stream("xauusd1@trade").await;
        assert_eq!(index, freed);
        assert_eq!(harness.venue.live_connections().len(), 3);

        // A BTC trade that was already on its way is not published.
        harness.venue.push_on(0, &trade_frame("BTCUSDT", 9_999));
        harness
            .expect_none("trade", |event| {
                trade(event) == Some(instrument_id("BTCUSDT"))
            })
            .await;
        harness.stop().await;
    });
}

#[test]
fn only_the_dropped_connection_is_resubscribed() {
    run(async {
        let mut harness = Harness::start(|config| {
            config.max_streams_per_connection = 1;
            config.max_connections = 2;
        })
        .await;
        harness.subscribe_trades(&["SOLUSDT", "BTCUSDT"]);
        let btc = harness.venue.wait_for_stream("btcusdt@trade").await;
        let sol = harness.venue.wait_for_stream("solusdt@trade").await;
        assert_ne!(btc, sol);

        harness.venue.drop_connection(btc).await;
        let replacement = harness
            .venue
            .wait("resubscription", |state| {
                state
                    .connections
                    .iter()
                    .enumerate()
                    .position(|(index, connection)| {
                        index != btc
                            && connection.out.is_some()
                            && connection.streams.contains("btcusdt@trade")
                    })
            })
            .await;
        assert_ne!(replacement, sol);

        // The other connection was left alone: one SUBSCRIBE, ever.
        let on_sol = harness
            .venue
            .state
            .lock()
            .unwrap()
            .control
            .iter()
            .filter(|(index, ..)| *index == sol)
            .count();
        assert_eq!(on_sol, 1);

        harness.venue.push(&trade_frame("BTCUSDT", 100)).await;
        assert_eq!(harness.next("trade", trade).await, instrument_id("BTCUSDT"));
        harness.stop().await;
    });
}

#[test]
fn refresh_publishes_changed_instruments_and_retires_removed_ones() {
    run(async {
        let mut harness = Harness::start(|config| {
            config.instrument_refresh_interval_secs = 1;
        })
        .await;

        // The venue delists BTC and raises SOL's minimum notional.
        let mut info: serde_json::Value = serde_json::from_str(EXCHANGE_INFO).unwrap();
        let symbols = info["symbols"].as_array_mut().unwrap();
        symbols.retain(|symbol| symbol["symbol"] != "BTCUSDT");
        for symbol in symbols
            .iter_mut()
            .filter(|symbol| symbol["symbol"] == "SOLUSDT")
        {
            for filter in symbol["filters"].as_array_mut().unwrap() {
                if filter["filterType"] == "MIN_NOTIONAL" {
                    filter["notional"] = "10".into();
                }
            }
        }
        // Skip the instruments published on connect.
        while harness.events.try_recv().is_ok() {}
        harness.venue.state.lock().unwrap().exchange_info = info.to_string();

        let status = harness.next("instrument status", instrument_status).await;
        assert_eq!(status.instrument_id, instrument_id("BTCUSDT"));
        assert_eq!(status.action, MarketStatusAction::NotAvailableForTrading);

        let changed = harness.next("changed instrument", instrument).await;
        assert_eq!(changed, instrument_id("SOLUSDT"));

        // A later refresh with nothing new publishes nothing.
        let refreshes = harness.venue.requests("/fapi/v1/exchangeInfo");
        harness
            .venue
            .wait_for_refreshes("another refresh", refreshes)
            .await;
        harness
            .expect_none("instrument event", |event| {
                matches!(
                    event,
                    DataEvent::Data(Data::Instrument(_) | Data::InstrumentStatus(_))
                )
            })
            .await;

        // A delisted instrument can no longer be subscribed.
        assert!(harness.try_subscribe_trades(&["BTCUSDT"]).is_err());
        harness.stop().await;
    });
}

#[test]
fn funding_history_request_returns_settled_rates() {
    run(async {
        let mut harness = Harness::start(|_| {}).await;
        let sol = instrument_id("SOLUSDT");
        harness
            .client
            .request_funding_rates(RequestFundingRates::new(
                vec![sol],
                None,
                None,
                NonZeroUsize::new(2),
                Some(harness.client.client_id()),
                UUID4::new(),
                UnixNanos::default(),
                None,
            ))
            .unwrap();

        let rates = harness
            .next("funding rates response", |event| match event {
                DataEvent::Response(DataResponse::FundingRates(response)) => {
                    Some(response.data.iter().copied().collect::<Vec<_>>())
                }
                _ => None,
            })
            .await;
        assert_eq!(rates.len(), 2);
        assert_eq!(rates[0].instrument_id, sol);
        assert_eq!(rates[0].rate.to_string(), "-0.00001244");
        assert_eq!(
            rates[0].interval, None,
            "no earlier payment to time it from"
        );
        assert_eq!(
            rates[1].interval,
            Some(480),
            "time since the payment before"
        );
        assert_eq!(
            rates[0].ts_event,
            UnixNanos::from(1_795_305_600_000_000_000)
        );
        assert_eq!(
            rates[0].next_funding_ns, None,
            "a settled rate has no next time"
        );
        assert_eq!(
            harness
                .venue
                .requests("/fapi/v1/fundingRate?symbol=SOLUSDT&limit=2"),
            1
        );
        harness.stop().await;
    });
}

#[test]
fn trade_history_request_returns_recent_trades() {
    run(async {
        let mut harness = Harness::start(|_| {}).await;
        let btc = instrument_id("BTCUSDT");

        // The mock returns its three rows whatever the limit; the client
        // keeps the newest `limit`.
        let trades = harness.request_trades(btc, (None, None), 2).await;
        assert_eq!(
            trades,
            [
                ("150490380".to_owned(), 1_795_376_002_400),
                ("150490381".to_owned(), 1_795_376_022_000),
            ]
        );
        assert_eq!(
            harness
                .venue
                .requests("/fapi/v1/trades?symbol=BTCUSDT&limit=2"),
            1
        );

        // A window is cut from a full page, and the limit is capped at the
        // venue's 1000.
        let window = (Some(1_795_375_995_000), Some(1_795_376_010_000));
        let trades = harness.request_trades(btc, window, 5_000).await;
        assert_eq!(trades, [("150490380".to_owned(), 1_795_376_002_400)]);
        assert_eq!(
            harness
                .venue
                .requests("/fapi/v1/trades?symbol=BTCUSDT&limit=1000"),
            1
        );
        let everything = harness.request_trades(btc, (None, None), 5_000).await;
        assert_eq!(everything.len(), 3);
        assert_eq!(
            harness
                .venue
                .requests("/fapi/v1/trades?symbol=BTCUSDT&limit=1000"),
            2
        );
        harness.stop().await;
    });
}

#[test]
fn trade_history_request_for_an_unknown_instrument_is_an_error() {
    run(async {
        let harness = Harness::start(|_| {}).await;
        let error = harness
            .client
            .request_trades(RequestTrades::new(
                vec![instrument_id("NOPEUSDT")],
                None,
                None,
                None,
                Some(harness.client.client_id()),
                UUID4::new(),
                UnixNanos::default(),
                None,
            ))
            .unwrap_err();
        assert!(error.to_string().contains("NOPEUSDT"), "{error}");
        assert_eq!(harness.venue.requests("/fapi/v1/trades"), 0);
        harness.stop().await;
    });
}

#[test]
fn a_full_connection_is_subscribed_within_the_venue_message_limit() {
    run(async {
        // 250 symbols: more than one connection's 200 streams, and far more
        // stream names than fit one message.
        let mut info: serde_json::Value = serde_json::from_str(EXCHANGE_INFO).unwrap();
        let template = info["symbols"]
            .as_array()
            .unwrap()
            .iter()
            .find(|symbol| symbol["symbol"] == "SOLUSDT")
            .unwrap()
            .clone();
        let symbols: Vec<String> = (0..250).map(|n| format!("TEST{n:03}USDT")).collect();
        for symbol in &symbols {
            let mut listing = template.clone();
            listing["symbol"] = symbol.as_str().into();
            listing["pair"] = symbol.as_str().into();
            info["symbols"].as_array_mut().unwrap().push(listing);
        }

        let venue = Venue::start().await;
        venue.state.lock().unwrap().exchange_info = info.to_string();
        let mut harness = Harness::connect(venue, |_| {}).await;
        let names: Vec<&str> = symbols.iter().map(String::as_str).collect();
        harness.subscribe_book(&names);

        harness
            .venue
            .wait("250 streams", |state| {
                let open: usize = state
                    .connections
                    .iter()
                    .filter(|connection| connection.out.is_some())
                    .map(|connection| connection.streams.len())
                    .sum();
                (open == 250).then_some(())
            })
            .await;
        let mut sizes: Vec<usize> = harness
            .venue
            .live_connections()
            .iter()
            .map(BTreeSet::len)
            .collect();
        sizes.sort_unstable();
        assert_eq!(sizes, [50, 200]);
        {
            let state = harness.venue.state.lock().unwrap();
            assert_eq!(
                state.oversized, 0,
                "a message passed the venue's size limit"
            );
            assert!(
                state.control.len() > 2,
                "the streams were split over several messages"
            );
            assert_eq!(
                state.connections.len(),
                2,
                "no connection was closed and replaced"
            );
        }
        harness.stop().await;
    });
}

#[test]
fn a_refused_subscription_is_tried_again() {
    run(async {
        let mut harness = Harness::start(|_| {}).await;
        harness
            .venue
            .state
            .lock()
            .unwrap()
            .refuse_once
            .insert("solusdt@trade".to_owned());
        harness.subscribe_trades(&["SOLUSDT"]);

        // The venue answers the first request with an error, so nothing is
        // open until the client asks again.
        harness.venue.wait_for_stream("solusdt@trade").await;
        let requests = harness
            .venue
            .state
            .lock()
            .unwrap()
            .control
            .iter()
            .filter(|(_, method, _)| method == "SUBSCRIBE")
            .count();
        assert_eq!(requests, 2);
        harness.venue.push(&trade_frame("SOLUSDT", 100)).await;
        harness.next("trade", trade).await;
        harness.stop().await;
    });
}

#[test]
fn a_repeated_trade_is_published_once() {
    run(async {
        let mut harness = Harness::start(|_| {}).await;
        harness.subscribe_trades(&["SOLUSDT"]);
        harness.venue.push(&trade_frame("SOLUSDT", 100)).await;
        harness.venue.push(&trade_frame("SOLUSDT", 100)).await;
        harness.venue.push(&trade_frame("SOLUSDT", 101)).await;

        let first = harness.next("first trade", trade_id).await;
        let second = harness.next("second trade", trade_id).await;
        assert_ne!(first, second, "the repeated trade was published twice");
        harness.stop().await;
    });
}

#[test]
fn a_delisted_instrument_can_still_be_unsubscribed() {
    run(async {
        let mut harness = Harness::start(|config| {
            config.instrument_refresh_interval_secs = 1;
        })
        .await;
        harness.subscribe_trades(&["BTCUSDT"]);
        harness.venue.wait_for_stream("btcusdt@trade").await;

        let mut info: serde_json::Value = serde_json::from_str(EXCHANGE_INFO).unwrap();
        info["symbols"]
            .as_array_mut()
            .unwrap()
            .retain(|symbol| symbol["symbol"] != "BTCUSDT");
        harness.venue.state.lock().unwrap().exchange_info = info.to_string();
        harness.next("instrument status", instrument_status).await;

        harness.unsubscribe_trades(&["BTCUSDT"]);
        harness
            .venue
            .wait_for_unsubscribe(|streams| streams.iter().any(|stream| stream == "btcusdt@trade"))
            .await;
        harness.stop().await;
    });
}

#[test]
fn a_definition_that_stops_parsing_is_not_a_delisting() {
    run(async {
        let mut harness = Harness::start(|config| {
            config.instrument_refresh_interval_secs = 1;
        })
        .await;
        // The venue still lists SOL as trading, with a lot size that does
        // not parse.
        let mut info: serde_json::Value = serde_json::from_str(EXCHANGE_INFO).unwrap();
        for symbol in info["symbols"].as_array_mut().unwrap() {
            if symbol["symbol"] == "SOLUSDT" {
                for filter in symbol["filters"].as_array_mut().unwrap() {
                    if filter["filterType"] == "LOT_SIZE" {
                        filter["stepSize"] = "not a number".into();
                    }
                }
            }
        }
        while harness.events.try_recv().is_ok() {}
        let refreshes = harness.venue.requests("/fapi/v1/exchangeInfo");
        harness.venue.state.lock().unwrap().exchange_info = info.to_string();
        harness
            .venue
            .wait_for_refreshes("two refreshes", refreshes + 1)
            .await;
        harness
            .expect_none("instrument status", |event| {
                instrument_status(event).is_some()
            })
            .await;
        // It can still be subscribed.
        harness.subscribe_trades(&["SOLUSDT"]);
        harness.venue.wait_for_stream("solusdt@trade").await;
        harness.stop().await;
    });
}

#[test]
fn a_snapshot_applied_late_does_not_put_the_book_out_of_order() {
    run(async {
        let mut harness = Harness::start(|_| {}).await;
        harness.subscribe_book(&["SOLUSDT"]);
        harness.venue.state.lock().unwrap().snapshot_update_id = 1_000;
        harness.venue.push(&depth_diff(990, 1_005, 980)).await;
        harness.venue.push(&depth_diff(1_006, 1_010, 1_005)).await;
        harness.venue.push(&depth_diff(1_011, 1_015, 1_010)).await;

        // Whatever order the snapshot and the diffs were handled in, the
        // book comes out in venue order with receive times that never go
        // back, which is the order a catalog replays it in.
        let stamp = |event: &DataEvent| match event {
            DataEvent::Data(Data::BookDeltas(deltas)) => Some((deltas.sequence, deltas.ts_init)),
            _ => None,
        };
        let mut seen = Vec::new();
        while seen.last().is_none_or(|(sequence, _)| *sequence < 1_015) {
            seen.push(harness.next("book update", stamp).await);
        }
        assert_eq!(seen.first().map(|(sequence, _)| *sequence), Some(1_000));
        assert!(seen.is_sorted_by_key(|(sequence, _)| *sequence), "{seen:?}");
        assert!(seen.is_sorted_by_key(|(_, ts_init)| *ts_init), "{seen:?}");
        harness.stop().await;
    });
}
