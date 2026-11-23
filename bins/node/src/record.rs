//! Live recording: an Aster data client in a `LiveNode` whose streaming
//! writer stages everything on the message bus as Feather files and promotes
//! them into a Parquet catalog.

use std::{
    collections::BTreeSet,
    ffi::OsStr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use log::LevelFilter;
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    enums::Environment,
    live::dispatch::DispatchMessage,
    logging::logger::LoggerConfig,
    messages::DataEvent,
    nautilus_actor,
};
use nautilus_core::{DurationNanos, UnixNanos};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::{
        FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate, OrderBookDeltas, QuoteTick, TradeTick,
    },
    enums::BookType,
    identifiers::{ClientId, InstrumentId, TraderId},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::config::{DataCatalogConfig, RotationConfig, StreamingConfig};
use nt_dex_aster::{
    common::{ASTER, instrument_id},
    config::AsterDataClientConfig,
    factories::AsterDataClientFactory,
    http::client::AsterHttpClient,
};

#[derive(Debug)]
pub(crate) struct RecordOptions {
    pub(crate) catalog_dir: String,
    pub(crate) symbols: Vec<String>,
    pub(crate) all: bool,
    pub(crate) duration: Option<Duration>,
    pub(crate) rotate: Duration,
}

#[derive(Debug, Default)]
struct Counters {
    instruments: AtomicU64,
    book_batches: AtomicU64,
    book_deltas: AtomicU64,
    quotes: AtomicU64,
    trades: AtomicU64,
    mark_prices: AtomicU64,
    index_prices: AtomicU64,
    funding_rates: AtomicU64,
    trade_lag_sum_us: AtomicU64,
    trade_lag_max_us: AtomicU64,
    mark_dispatch_age_max_us: AtomicU64,
}

fn bump(counter: &AtomicU64, by: u64) {
    counter.fetch_add(by, Ordering::Relaxed);
}

impl Counters {
    fn report_dispatch_backlog(&self, queued: usize) {
        let inline_bytes = queued.saturating_mul(size_of::<DispatchMessage<DataEvent>>());
        let mark_dispatch_age_us = self
            .mark_dispatch_age_max_us
            .swap(0, Ordering::Relaxed)
            .checked_sub(1);
        log::info!(
            "Nautilus data backlog events={queued} inline_bytes={inline_bytes} \
             max_mark_dispatch_age_us={mark_dispatch_age_us:?}"
        );
    }

    fn report(&self, elapsed: Duration) -> String {
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let trades = get(&self.trades);
        let lag_avg_ms = get(&self.trade_lag_sum_us)
            .checked_div(trades)
            .map_or(0, |micros| micros / 1000);
        format!(
            "{}s: {} instruments, {} book deltas in {} batches, {} quotes, {} trades, \
             {} mark, {} index, {} funding; trade lag avg {} ms, max {} ms",
            elapsed.as_secs(),
            get(&self.instruments),
            get(&self.book_deltas),
            get(&self.book_batches),
            get(&self.quotes),
            trades,
            get(&self.mark_prices),
            get(&self.index_prices),
            get(&self.funding_rates),
            lag_avg_ms,
            get(&self.trade_lag_max_us) / 1000,
        )
    }
}

#[derive(Debug)]
struct Recorder {
    core: DataActorCore,
    instrument_ids: Vec<InstrumentId>,
    counters: Arc<Counters>,
}

nautilus_actor!(Recorder);

impl DataActor for Recorder {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let client = Some(ClientId::new(ASTER));
        let ids = self.instrument_ids.clone();
        self.subscribe_book_deltas(ids.clone(), BookType::L2_MBP, None, client, false, None);
        self.subscribe_quotes(ids.clone(), client, None);
        self.subscribe_trades(ids.clone(), client, None);
        self.subscribe_mark_prices(ids.clone(), client, None);
        self.subscribe_index_prices(ids.clone(), client, None);
        self.subscribe_funding_rates(ids, client, None);
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn on_instrument(&mut self, _instrument: &InstrumentAny) -> anyhow::Result<()> {
        bump(&self.counters.instruments, 1);
        Ok(())
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        bump(&self.counters.book_batches, 1);
        bump(
            &self.counters.book_deltas,
            u64::try_from(deltas.deltas.len())?,
        );
        Ok(())
    }

    fn on_quote(&mut self, _quote: &QuoteTick) -> anyhow::Result<()> {
        bump(&self.counters.quotes, 1);
        Ok(())
    }

    fn on_trade(&mut self, tick: &TradeTick) -> anyhow::Result<()> {
        bump(&self.counters.trades, 1);
        let lag_us = tick.ts_init.as_u64().saturating_sub(tick.ts_event.as_u64()) / 1000;
        bump(&self.counters.trade_lag_sum_us, lag_us);
        self.counters
            .trade_lag_max_us
            .fetch_max(lag_us, Ordering::Relaxed);
        Ok(())
    }

    fn on_mark_price(&mut self, mark_price: &MarkPriceUpdate) -> anyhow::Result<()> {
        bump(&self.counters.mark_prices, 1);
        let now = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
        let age_us = now.as_u64().saturating_sub(mark_price.ts_init.as_u64()) / 1_000;
        self.counters
            .mark_dispatch_age_max_us
            .fetch_max(age_us.saturating_add(1), Ordering::Relaxed);
        Ok(())
    }

    fn on_index_price(&mut self, _index_price: &IndexPriceUpdate) -> anyhow::Result<()> {
        bump(&self.counters.index_prices, 1);
        Ok(())
    }

    fn on_funding_rate(&mut self, _funding_rate: &FundingRateUpdate) -> anyhow::Result<()> {
        bump(&self.counters.funding_rates, 1);
        Ok(())
    }
}

fn is_staged_file(path: &Path) -> bool {
    let extension = path.extension();
    let feather = Some(OsStr::new("feather"));
    extension == feather
        || (extension == Some(OsStr::new("partial"))
            && path
                .file_stem()
                .and_then(|stem| Path::new(stem).extension())
                == feather)
}

fn staged_files(dir: &Path) -> anyhow::Result<BTreeSet<PathBuf>> {
    fn walk(dir: &Path, found: &mut BTreeSet<PathBuf>) -> anyhow::Result<()> {
        let entries = std::fs::read_dir(dir)
            .with_context(|| format!("cannot read staging directory {}", dir.display()))?;
        for entry in entries {
            let entry =
                entry.with_context(|| format!("cannot list entries in {}", dir.display()))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .with_context(|| format!("cannot read file type of {}", path.display()))?;
            if file_type.is_dir() {
                walk(&path, found)?;
            } else if is_staged_file(&path) {
                found.insert(path);
            }
        }
        Ok(())
    }
    let mut found = BTreeSet::new();
    walk(dir, &mut found)?;
    Ok(found)
}

fn verify(
    catalog_dir: &str,
    staging: &Path,
    stranded: &BTreeSet<PathBuf>,
    run_start: UnixNanos,
    requested: &[InstrumentId],
    counters: &Counters,
) -> anyhow::Result<()> {
    let unpromoted = staged_files(staging)?.difference(stranded).count();
    anyhow::ensure!(
        unpromoted == 0,
        "{unpromoted} staged files in {} were not promoted into the catalog",
        staging.display()
    );
    let mut catalog = crate::open_catalog(catalog_dir);
    let covered: BTreeSet<InstrumentId> = catalog
        .query::<MarkPriceUpdate>(None, Some(run_start), None, None, None, true)?
        .iter()
        .map(|mark| mark.instrument_id)
        .collect();
    let missing: Vec<&InstrumentId> = requested
        .iter()
        .filter(|id| !covered.contains(id))
        .collect();
    anyhow::ensure!(
        missing.is_empty(),
        "{} of {} requested instruments have no data in the catalog from this run, first: {:?}",
        missing.len(),
        requested.len(),
        missing.first()
    );
    let trades = counters.trades.load(Ordering::Relaxed);
    let stored = catalog.trade_ticks(None, Some(run_start), None)?.len();
    anyhow::ensure!(
        u64::try_from(stored)? >= trades,
        "the catalog holds {stored} trades from this run but {trades} were received"
    );
    Ok(())
}

fn streaming_config(
    options: &RecordOptions,
    staging_dir: String,
) -> anyhow::Result<StreamingConfig> {
    let rotate_ns = u64::try_from(options.rotate.as_nanos()).context("rotation too long")?;
    let promote_ms = u64::try_from(options.rotate.as_millis()).context("rotation too long")?;
    let mut streaming = StreamingConfig::new(
        staging_dir,
        Some(DataCatalogConfig::new(
            options.catalog_dir.clone(),
            None,
            None,
        )),
        1_000,
        false,
        RotationConfig::Interval {
            interval_ns: DurationNanos::new(rotate_ns),
        },
    );
    streaming.promotion_interval_ms = Some(promote_ms);
    streaming.promote_on_close = true;
    streaming.delete_feather_after_promotion = true;
    Ok(streaming)
}

pub(crate) async fn record(options: RecordOptions) -> anyhow::Result<()> {
    let config = AsterDataClientConfig::default();
    let instrument_ids: Vec<InstrumentId> = if options.all {
        anyhow::ensure!(
            options.symbols.is_empty(),
            "give symbols or --all, not both"
        );
        let http = AsterHttpClient::new(&config.base_url_http, config.http_timeout_secs)?;
        let parsed = http.request_instruments(UnixNanos::default()).await?;
        parsed.instruments.iter().map(Instrument::id).collect()
    } else {
        options
            .symbols
            .iter()
            .map(|symbol| instrument_id(symbol))
            .collect()
    };
    anyhow::ensure!(!instrument_ids.is_empty(), "no symbols to record");
    let requested = instrument_ids.clone();

    std::fs::create_dir_all(&options.catalog_dir)?;
    let staging_dir = format!("{}.staging", options.catalog_dir.trim_end_matches('/'));
    let staging = Path::new(&staging_dir);
    std::fs::create_dir_all(staging)
        .with_context(|| format!("cannot create staging directory {staging_dir}"))?;
    let stranded = staged_files(staging)?;
    if !stranded.is_empty() {
        log::warn!(
            "{} staged files from an earlier run are in {staging_dir} and are not in \
             the catalog; that run did not shut down cleanly",
            stranded.len()
        );
    }
    let streaming = streaming_config(&options, staging_dir.clone())?;

    let log_config = LoggerConfig {
        stdout_level: LevelFilter::Info,
        ..Default::default()
    };
    let mut node = LiveNode::builder(TraderId::from("RECORDER-001"), Environment::Live)?
        .with_name("ASTER-RECORDER-001".to_owned())
        .with_logging(log_config)
        .with_delay_post_stop_secs(2)
        .with_streaming_config(streaming)
        .add_data_client(
            Some(ASTER.to_owned()),
            Box::new(AsterDataClientFactory),
            Box::new(config),
        )?
        .build()?;

    let counters = Arc::new(Counters::default());
    let actor_config = DataActorConfig {
        log_events: false,
        log_commands: false,
        ..Default::default()
    };
    node.add_actor(Recorder {
        core: DataActorCore::new(actor_config),
        instrument_ids,
        counters: Arc::clone(&counters),
    })?;

    log::info!("recording {} instruments", requested.len());
    let run_start = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
    let started = Instant::now();
    let handle = node.handle();
    let reporter = tokio::spawn({
        let counters = Arc::clone(&counters);
        async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                log::info!("recorded after {}", counters.report(started.elapsed()));
                counters.report_dispatch_backlog(handle.metrics_snapshot().data_events.queue_depth);
                if options
                    .duration
                    .is_some_and(|limit| started.elapsed() >= limit)
                {
                    handle.stop();
                    break;
                }
            }
        }
    });

    let result = node.run().await;
    reporter.abort();
    node.dispose();
    verify(
        &options.catalog_dir,
        staging,
        &stranded,
        run_start,
        &requested,
        &counters,
    )?;
    log::info!(
        "recorded in total after {}",
        counters.report(started.elapsed())
    );
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_matches_sealed_and_partial_feather_files() {
        assert!(is_staged_file(Path::new("quotes/quotes_0.feather")));
        assert!(is_staged_file(Path::new("quotes/quotes_0.feather.partial")));
        assert!(!is_staged_file(Path::new("quotes.feather/notes.txt")));
        assert!(!is_staged_file(Path::new("quotes/quotes_0.feather.bak")));
    }

    #[test]
    fn verification_fails_when_staging_cannot_be_scanned() {
        let staging = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
        let error = verify(
            env!("CARGO_MANIFEST_DIR"),
            Path::new(staging),
            &BTreeSet::new(),
            UnixNanos::default(),
            &[],
            &Counters::default(),
        )
        .expect_err("verification must fail before querying the catalog");
        let io_error = error.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(io_error.kind(), std::io::ErrorKind::NotADirectory);
        assert!(error.to_string().contains(staging), "{error:#}");
    }
}
