//! Opt-in Aster L2 check for BTCUSDT, ETHUSDT and SOLUSDT.
//!
//! `cargo test --locked -p nt-dex-aster --example book_stress` runs the offline comparator test.
//! `cargo run --locked -p nt-dex-aster --example book_stress -- preflight [seconds]` checks startup.
//! `cargo run --locked -p nt-dex-aster --example book_stress -- recovery [seconds] [rounds]` also
//! checks a settled SOL unsubscribe/resubscribe, a socket cut and a downstream depth freeze.
//! Seconds applies per active phase (default 15). Public market data only.
//!
//! Like the pinned RC9 Binance futures stress test, this checks every emitted book group with
//! `BookStreamChecker` and retains top levels for a direct, same-symbol, same-`u` comparison.
//! Different cadences can leave samples unpaired. Every active recovery must still produce
//! an exact match on an incremental update from its fresh snapshot episode.

#![expect(
    clippy::print_stdout,
    reason = "command-line phase and coverage output"
)]

mod oracle;
mod relay;

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use nautilus_common::{
    clients::DataClient,
    live::runner::replace_data_event_sender,
    logging::{
        logger::{Logger, LoggerConfig},
        writer::FileWriterConfig,
    },
    messages::{
        DataEvent,
        data::{SubscribeBookDeltas, UnsubscribeBookDeltas},
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::book::conformance::BookStreamChecker;
use nautilus_model::{
    data::{Data, OrderBookDeltas},
    enums::{BookAction, BookType},
    identifiers::{ClientId, InstrumentId, TraderId},
};
use nt_dex_aster::{
    common::{ASTER, F_LAST, WS_URL, instrument_id},
    config::AsterDataClientConfig,
    data::AsterDataClient,
};
use tokio::{
    net::TcpStream,
    sync::mpsc::{UnboundedReceiver, unbounded_channel},
    time::{Instant, timeout},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use self::{
    oracle::{DEPTH, PARTIAL_SUFFIX, Pairing, Top},
    relay::{Fault, Relay},
};

const SYMBOLS: [&str; 3] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
const USAGE: &str =
    "book_stress preflight [seconds, 5–120] | recovery [seconds per phase, 5–120] [rounds, 1–20]";
const SETTLE: Duration = Duration::from_millis(500);
const ALL_FRESH: [Requirement; 3] = [Requirement::Fresh; 3];

#[derive(Debug, Clone, Copy)]
enum Requirement {
    Observe,
    Fresh,
    Active,
    Quiet,
}

#[derive(Debug, Default)]
struct Window {
    snapshots: usize,
    pairing: Pairing,
    first_mismatch: Option<anyhow::Error>,
}

#[derive(Debug)]
struct Probe {
    events: UnboundedReceiver<DataEvent>,
    direct: WebSocketStream<MaybeTlsStream<TcpStream>>,
    checker: BookStreamChecker,
    windows: BTreeMap<InstrumentId, Window>,
    failures: Vec<String>,
}

impl Probe {
    fn emitted(&mut self, batch: &OrderBookDeltas) -> Result<()> {
        ensure!(!batch.deltas.is_empty(), "empty adapter book batch");
        for group in batch
            .deltas
            .split_inclusive(|delta| delta.flags & F_LAST != 0)
        {
            let split;
            let deltas = if group.len() == batch.deltas.len() {
                batch
            } else {
                split = OrderBookDeltas::new_checked(batch.instrument_id, group.to_vec())?;
                &split
            };
            let id = deltas.instrument_id;
            self.checker
                .apply(deltas)
                .with_context(|| format!("{id} emitted u={}", deltas.sequence))?;
            let book = self.checker.book(id).context("missing checked book")?;
            let window = self
                .windows
                .get_mut(&id)
                .context("unexpected adapter instrument")?;
            let snapshot = group
                .first()
                .is_some_and(|delta| delta.action == BookAction::Clear);
            if snapshot {
                window.snapshots = window.snapshots.saturating_add(1);
            }
            let top = Top {
                bids: book.bids_as_map(Some(DEPTH)).into_iter().collect(),
                asks: book.asks_as_map(Some(DEPTH)).into_iter().collect(),
            };
            let episode = (!snapshot).then_some(window.snapshots);
            if let Err(error) = window.pairing.local(deltas.sequence, top, episode) {
                window.first_mismatch.get_or_insert(error);
            }
        }
        Ok(())
    }

    fn reference(&mut self, text: &str) -> Result<()> {
        let frame: oracle::Frame<'_> =
            serde_json::from_str(text).context("invalid partial depth frame")?;
        let subscribed = frame.stream.strip_suffix(PARTIAL_SUFFIX);
        ensure!(
            subscribed.is_some_and(|symbol| symbol.eq_ignore_ascii_case(frame.data.symbol)),
            "unexpected oracle stream {} for {}",
            frame.stream,
            frame.data.symbol,
        );
        let id = instrument_id(frame.data.symbol);
        let window = self
            .windows
            .get_mut(&id)
            .context("unexpected oracle instrument")?;
        let top = frame
            .data
            .top()
            .with_context(|| format!("{id} oracle u={}", frame.data.u))?;
        if let Err(error) = window.pairing.venue(frame.data.u, top) {
            window.first_mismatch.get_or_insert(error);
        }
        Ok(())
    }

    async fn read(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                message = self.direct.next() => match message.transpose()? {
                    Some(Message::Text(text)) => self.reference(&text)?,
                    Some(Message::Ping(_)) => self.direct.flush().await?,
                    Some(Message::Close(frame)) => bail!("direct oracle closed: {frame:?}"),
                    None => bail!("direct oracle disconnected"),
                    Some(message) => ensure!(matches!(message, Message::Pong(_)), "unexpected oracle message"),
                },
                event = self.events.recv() => {
                    if let DataEvent::Data(Data::BookDeltas(deltas)) =
                        event.context("adapter event channel closed")?
                    {
                        self.emitted(&deltas)?;
                    }
                }
            }
        }
    }

    async fn observe(
        &mut self,
        phase: &str,
        duration: Duration,
        requirements: [Requirement; SYMBOLS.len()],
    ) -> Result<()> {
        for window in self.windows.values_mut() {
            *window = Window::default();
        }
        println!("phase={phase} target_s={:.1}", duration.as_secs_f64());
        let start = Instant::now();
        let observed = timeout(duration, self.read()).await.unwrap_or(Ok(()));
        let elapsed = start.elapsed().as_secs_f64();
        for (symbol, want) in SYMBOLS.into_iter().zip(requirements) {
            let id = instrument_id(symbol);
            let window = self
                .windows
                .get_mut(&id)
                .context("missing coverage window")?;
            let evidence = &window.pairing;
            let (local_unmatched, direct_unmatched) = evidence.unmatched();
            let fresh_matched =
                window.snapshots > 0 && evidence.latest_matched_episode == Some(window.snapshots);
            println!(
                "phase={phase} symbol={symbol} elapsed_s={elapsed:.2} snapshots={} local={} direct={} exact={} mismatches={} local_unmatched={local_unmatched} direct_unmatched={direct_unmatched} fresh_matched={fresh_matched}",
                window.snapshots,
                evidence.local_updates,
                evidence.venue_updates,
                evidence.matches,
                evidence.mismatches
            );
            if let Some(error) = window.first_mismatch.take() {
                self.failures.push(format!("{phase} {symbol}: {error:#}"));
            }
            let satisfied = match want {
                Requirement::Observe => true,
                Requirement::Fresh => fresh_matched,
                Requirement::Active => evidence.local_updates > 0 && evidence.venue_updates > 0,
                Requirement::Quiet => evidence.local_updates == 0 && evidence.venue_updates > 0,
            };
            if !satisfied {
                self.failures
                    .push(format!("{phase} {symbol}: missing {want:?} evidence"));
            }
        }
        observed.with_context(|| format!("phase {phase}"))
    }

    async fn exercise(
        &mut self,
        client: &mut AsterDataClient,
        relay: Option<&Relay>,
        seconds: Duration,
        rounds: usize,
    ) -> Result<()> {
        let ids = SYMBOLS.map(instrument_id);
        for id in ids {
            self.checker.open(id);
        }
        subscribe(client, ids.to_vec())?;
        self.observe("preflight", seconds, ALL_FRESH).await?;
        let Some(relay) = relay else { return Ok(()) };
        for round in 1..=rounds {
            println!("round={round}/{rounds}");
            self.exercise_recovery(client, relay, seconds).await?;
        }
        Ok(())
    }

    async fn exercise_recovery(
        &mut self,
        client: &mut AsterDataClient,
        relay: &Relay,
        seconds: Duration,
    ) -> Result<()> {
        let sol = instrument_id("SOLUSDT");
        client.unsubscribe_book_deltas(&UnsubscribeBookDeltas::new(
            vec![sol],
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))?;
        self.observe("unsubscribe_settle", SETTLE, [Requirement::Observe; 3])
            .await?;
        self.checker.close(sol);
        self.observe(
            "sol_unsubscribed",
            Duration::from_secs(2),
            [Requirement::Active, Requirement::Active, Requirement::Quiet],
        )
        .await?;
        self.checker.open(sol);
        subscribe(client, vec![sol])?;
        self.observe(
            "sol_resubscribed",
            seconds,
            [Requirement::Active, Requirement::Active, Requirement::Fresh],
        )
        .await?;

        let (before, cuts_before) = relay.connection_counts();
        relay.inject(Fault::Cut)?;
        self.observe("cut_recovery", seconds, ALL_FRESH).await?;
        let (after, cuts) = relay.connection_counts();
        println!("relay_cut connections_before={before} connections_after={after} cuts={cuts}");
        ensure!(
            cuts.saturating_sub(cuts_before) == 1 && after > before,
            "relay did not cut and reconnect the adapter"
        );

        let previous_dropped = relay.frozen_depth();
        relay.inject(Fault::Freeze)?;
        self.observe("freeze_settle", SETTLE, [Requirement::Observe; 3])
            .await?;
        self.observe("frozen", Duration::from_secs(3), [Requirement::Quiet; 3])
            .await?;
        relay.inject(Fault::Pass)?;
        self.observe("freeze_recovery", seconds, ALL_FRESH).await?;
        let dropped: Vec<_> = relay
            .frozen_depth()
            .into_iter()
            .zip(previous_dropped)
            .map(|(total, previous)| total.saturating_sub(previous))
            .collect();
        let (after_freeze, _) = relay.connection_counts();
        println!(
            "relay_freeze symbols={SYMBOLS:?} dropped={dropped:?} connections_after={after_freeze}"
        );
        ensure!(
            dropped.into_iter().all(|count| count > 0),
            "freeze missed a symbol"
        );
        ensure!(
            after_freeze == after,
            "adapter reconnected during the depth freeze"
        );
        Ok(())
    }
}

fn subscribe(client: &mut AsterDataClient, ids: Vec<InstrumentId>) -> Result<()> {
    client.subscribe_book_deltas(SubscribeBookDeltas::new(
        ids,
        BookType::L2_MBP,
        Some(client.client_id()),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        false,
        None,
        None,
    ))
}

fn args() -> Result<(bool, Duration, usize)> {
    let mut args = std::env::args().skip(1);
    let recovery = match args.next().as_deref() {
        Some("preflight") => false,
        Some("recovery") => true,
        Some("-h" | "--help") => {
            println!("{USAGE}");
            std::process::exit(0);
        }
        _ => bail!("{USAGE}"),
    };
    let seconds = args.next().map_or(Ok(15), |value| value.parse::<u64>())?;
    let rounds = if recovery {
        args.next().map_or(Ok(1), |value| value.parse::<usize>())?
    } else {
        1
    };
    ensure!(
        (5..=120).contains(&seconds) && (1..=20).contains(&rounds) && args.next().is_none(),
        "{USAGE}"
    );
    Ok((recovery, Duration::from_secs(seconds), rounds))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (recovery, seconds, rounds) = args()?;
    let started = Instant::now();
    let _log = Logger::init_with_config(
        TraderId::from("BOOK-STRESS-001"),
        UUID4::new(),
        LoggerConfig {
            stdout_level: log::LevelFilter::Warn,
            ..Default::default()
        },
        FileWriterConfig::default(),
    )?;
    let relay = if recovery {
        Some(Relay::bind().await?)
    } else {
        None
    };
    let mut config = AsterDataClientConfig {
        instrument_refresh_interval_secs: 0,
        http_timeout_secs: 10,
        max_connections: 1,
        snapshot_weight_per_minute: 600,
        ..Default::default()
    };
    if let Some(relay) = &relay {
        config.base_url_ws.clone_from(&relay.base_url);
    }
    let streams = SYMBOLS.map(|symbol| format!("{}{PARTIAL_SUFFIX}", symbol.to_ascii_lowercase()));
    let url = format!("{WS_URL}/stream?streams={}", streams.join("/"));
    let (direct, _) = timeout(Duration::from_secs(8), connect_async(&url))
        .await
        .context("direct oracle connect timed out")?
        .context("direct oracle connect failed")?;

    let (sender, events) = unbounded_channel();
    replace_data_event_sender(sender);
    let mut client = AsterDataClient::new(ClientId::new(ASTER), config)?;
    timeout(Duration::from_secs(35), client.connect())
        .await
        .context("adapter connect timed out")?
        .context("adapter connect failed")?;
    let mut probe = Probe {
        events,
        direct,
        checker: BookStreamChecker::new(BookType::L2_MBP, true),
        windows: SYMBOLS
            .map(|symbol| (instrument_id(symbol), Window::default()))
            .into(),
        failures: Vec::new(),
    };

    let exercise = probe
        .exercise(&mut client, relay.as_ref(), seconds, rounds)
        .await;
    let disconnected = client.disconnect().await;
    println!("run_elapsed_s={:.2}", started.elapsed().as_secs_f64());
    exercise?;
    disconnected?;
    ensure!(probe.failures.is_empty(), "{}", probe.failures.join("\n"));
    Ok(())
}
