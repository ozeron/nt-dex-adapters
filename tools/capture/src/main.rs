//! Raw public-feed capture for Aster perpetual futures.
//!
//! Writes one JSON object per line: `ts_recv_ns`, `kind`, `source`, `raw`.
//! `raw` is the payload text exactly as the venue sent it, so parser and
//! book-sync tests can replay a session without touching the network.
//!
//! A session contains, in receive order:
//! - `rest` `exchangeInfo` (instrument definitions) and `fundingInfo`
//!   (funding intervals) at startup
//! - `ws` combined-stream frames: depth diffs, individual trades, book
//!   tickers, and mark prices for each symbol
//! - `rest` depth snapshots shortly after every (re)connect and then on an
//!   interval, to bootstrap the book and to check it mid-session
//! - `rest` one-minute klines and 24h tickers at the end, as venue
//!   statistics to reconcile trade counts and volumes against
//! - `meta` markers for session start/end, socket open/close, and pings

#![expect(clippy::print_stderr, reason = "command-line tool reporting progress")]

use std::{
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::StreamExt;
use serde_json::json;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    time::{Instant, MissedTickBehavior, interval_at, sleep, sleep_until, timeout_at},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const REST_BASE: &str = "https://fapi.asterdex.com";
const WS_BASE: &str = "wss://fstream.asterdex.com";
const STREAMS: [&str; 4] = ["depth@100ms", "trade", "bookTicker", "markPrice@1s"];
/// Delay before the first depth snapshot, so diffs are already buffered.
const FIRST_SNAPSHOT_DELAY: Duration = Duration::from_secs(2);
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
const PROGRESS_EVERY: Duration = Duration::from_secs(60);
/// One-minute candles per klines request (the venue's maximum).
const KLINE_PAGE: u128 = 1000;
/// Wait before fetching closing klines: the venue's REST klines trail its
/// trade stream, and a minute read 10 seconds after it closed was still
/// missing half its trades.
const KLINE_SETTLE_DELAY: Duration = Duration::from_secs(60);

type BoxError = Box<dyn std::error::Error + Send + Sync>;

struct Args {
    symbols: Vec<String>,
    minutes: u64,
    snapshot_secs: u64,
    out_dir: PathBuf,
}

/// One captured line, stamped when it was received.
struct Record {
    ts_recv_ns: u64,
    kind: &'static str,
    source: String,
    raw: String,
}

impl Record {
    fn now(kind: &'static str, source: impl Into<String>, raw: impl Into<String>) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        Self {
            ts_recv_ns: u64::try_from(nanos).unwrap_or(u64::MAX),
            kind,
            source: source.into(),
            raw: raw.into(),
        }
    }
}

struct Recorder {
    file: BufWriter<File>,
    ws_frames: u64,
    rest_calls: u64,
}

impl Recorder {
    fn write(&mut self, record: &Record) -> io::Result<()> {
        match record.kind {
            "ws" => self.ws_frames = self.ws_frames.saturating_add(1),
            "rest" => self.rest_calls = self.rest_calls.saturating_add(1),
            _ => {}
        }
        let line = json!({
            "ts_recv_ns": record.ts_recv_ns,
            "kind": record.kind,
            "source": record.source,
            "raw": record.raw,
        });
        writeln!(self.file, "{line}")
    }

    fn meta(&mut self, event: &str, detail: impl Into<String>) -> io::Result<()> {
        self.write(&Record::now("meta", event, detail))
    }
}

fn parse_args() -> Result<Args, BoxError> {
    let mut args = Args {
        symbols: vec!["BTCUSDT".into(), "ETHUSDT".into(), "SOLUSDT".into()],
        minutes: 30,
        snapshot_secs: 300,
        out_dir: PathBuf::from("captures/aster"),
    };
    let mut raw_args = std::env::args().skip(1);
    while let Some(flag) = raw_args.next() {
        let mut value = || raw_args.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--symbols" => {
                args.symbols = value()?.split(',').map(str::to_uppercase).collect();
            }
            "--minutes" => args.minutes = value()?.parse()?,
            "--snapshot-secs" => args.snapshot_secs = value()?.parse()?,
            "--out" => args.out_dir = PathBuf::from(value()?),
            _ => {
                return Err(format!(
                    "unknown argument {flag}\n\
                     usage: nt-dex-capture [--symbols BTCUSDT,ETHUSDT] [--minutes 30] \
                     [--snapshot-secs 300] [--out captures/aster]"
                )
                .into());
            }
        }
    }
    if args.symbols.is_empty() || args.snapshot_secs == 0 {
        return Err("need at least one symbol and a non-zero snapshot interval".into());
    }
    Ok(args)
}

/// Fetches `path` and queues the body (or the failure) for recording.
async fn fetch(http: reqwest::Client, path: String, records: UnboundedSender<Record>) {
    let url = format!("{REST_BASE}{path}");
    let body = async {
        http.get(&url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await
    };
    let record = match body.await {
        Ok(text) => Record::now("rest", path, text),
        Err(error) => Record::now("meta", "rest_error", format!("{path}: {error}")),
    };
    // The receiver only closes when the session is over.
    let _ = records.send(record);
}

fn spawn_snapshots(http: &reqwest::Client, symbols: &[String], records: &UnboundedSender<Record>) {
    for symbol in symbols {
        let path = format!("/fapi/v1/depth?symbol={symbol}&limit=1000");
        tokio::spawn(fetch(http.clone(), path, records.clone()));
    }
}

/// Reads one socket until it closes or the session ends. Returns `true` when
/// the session is over and `false` when the caller should reconnect.
async fn run_socket(
    args: &Args,
    http: &reqwest::Client,
    recorder: &mut Recorder,
    records_tx: &UnboundedSender<Record>,
    records_rx: &mut UnboundedReceiver<Record>,
    deadline: Instant,
) -> Result<bool, BoxError> {
    let streams: Vec<String> = args
        .symbols
        .iter()
        .flat_map(|symbol| {
            let symbol = symbol.to_lowercase();
            STREAMS
                .iter()
                .map(move |stream| format!("{symbol}@{stream}"))
        })
        .collect();
    let url = format!("{WS_BASE}/stream?streams={}", streams.join("/"));

    let mut socket = match timeout_at(deadline, connect_async(&url)).await {
        // The session ran out while the venue was unreachable.
        Err(_elapsed) => return Ok(true),
        Ok(Ok((socket, _response))) => socket,
        Ok(Err(error)) => {
            recorder.meta("ws_connect_error", error.to_string())?;
            return Ok(false);
        }
    };
    recorder.meta("ws_open", url)?;

    let first_snapshot = Instant::now()
        .checked_add(FIRST_SNAPSHOT_DELAY)
        .ok_or("clock overflow")?;
    let mut snapshots = interval_at(first_snapshot, Duration::from_secs(args.snapshot_secs));
    snapshots.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let first_progress = Instant::now()
        .checked_add(PROGRESS_EVERY)
        .ok_or("clock overflow")?;
    let mut progress = interval_at(first_progress, PROGRESS_EVERY);

    loop {
        tokio::select! {
            () = sleep_until(deadline) => return Ok(true),
            signal = tokio::signal::ctrl_c() => {
                signal?;
                recorder.meta("interrupted", "")?;
                return Ok(true);
            }
            _ = snapshots.tick() => spawn_snapshots(http, &args.symbols, records_tx),
            _ = progress.tick() => {
                recorder.file.flush()?;
                eprintln!("{} ws frames, {} rest calls", recorder.ws_frames, recorder.rest_calls);
            }
            Some(record) = records_rx.recv() => recorder.write(&record)?,
            frame = socket.next() => match frame {
                Some(Ok(Message::Text(text))) => {
                    recorder.write(&Record::now("ws", "stream", text.as_str()))?;
                }
                Some(Ok(Message::Ping(_))) => recorder.meta("ws_ping", "")?,
                Some(Ok(Message::Close(frame))) => {
                    recorder.meta("ws_close", format!("{frame:?}"))?;
                    return Ok(false);
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    recorder.meta("ws_error", error.to_string())?;
                    return Ok(false);
                }
                None => {
                    recorder.meta("ws_close", "stream ended")?;
                    return Ok(false);
                }
            },
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let args = parse_args()?;
    let started = SystemTime::now().duration_since(UNIX_EPOCH)?;

    fs::create_dir_all(&args.out_dir)?;
    let path = args
        .out_dir
        .join(format!("aster-{}.jsonl", started.as_secs()));
    let mut recorder = Recorder {
        // Never overwrite an earlier session.
        file: BufWriter::new(File::create_new(&path)?),
        ws_frames: 0,
        rest_calls: 0,
    };
    eprintln!(
        "capturing {} for {} min -> {}",
        args.symbols.join(","),
        args.minutes,
        path.display()
    );

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let (records_tx, mut records_rx) = unbounded_channel();
    let session = json!({
        "venue": "ASTER",
        "symbols": args.symbols,
        "streams": STREAMS,
        "minutes": args.minutes,
        "snapshot_secs": args.snapshot_secs,
    });
    recorder.meta("session_start", session.to_string())?;

    fetch(
        http.clone(),
        "/fapi/v1/exchangeInfo".into(),
        records_tx.clone(),
    )
    .await;
    fetch(
        http.clone(),
        "/fapi/v1/fundingInfo".into(),
        records_tx.clone(),
    )
    .await;

    let deadline = Instant::now()
        .checked_add(Duration::from_secs(args.minutes.saturating_mul(60)))
        .ok_or("capture duration too long")?;
    while Instant::now() < deadline
        && !run_socket(
            &args,
            &http,
            &mut recorder,
            &records_tx,
            &mut records_rx,
            deadline,
        )
        .await?
    {
        sleep(RECONNECT_DELAY).await;
    }

    // Venue statistics for the captured window, to reconcile against.
    eprintln!("stream closed; waiting {KLINE_SETTLE_DELAY:?} for klines to settle");
    sleep(KLINE_SETTLE_DELAY).await;
    // Start one minute early so the partially captured first candle is included.
    let start_ms = started.as_millis().saturating_sub(60_000);
    let end_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    for symbol in &args.symbols {
        // One request returns at most 1000 candles, under 17 hours.
        let mut page_start = start_ms;
        while page_start < end_ms {
            let klines = format!(
                "/fapi/v1/klines?symbol={symbol}&interval=1m&startTime={page_start}&limit={KLINE_PAGE}"
            );
            fetch(http.clone(), klines, records_tx.clone()).await;
            page_start = page_start.saturating_add(KLINE_PAGE.saturating_mul(60_000));
        }
        fetch(
            http.clone(),
            format!("/fapi/v1/ticker/24hr?symbol={symbol}"),
            records_tx.clone(),
        )
        .await;
    }
    drop(records_tx);
    while let Some(record) = records_rx.recv().await {
        recorder.write(&record)?;
    }
    recorder.meta("session_end", "")?;
    recorder.file.flush()?;
    eprintln!(
        "done: {} ws frames, {} rest calls -> {}",
        recorder.ws_frames,
        recorder.rest_calls,
        path.display()
    );
    Ok(())
}
