//! Live smoke test: runs the Aster data client inside a `LiveNode` with
//! `NautilusTrader`'s `DataTester` actor, which subscribes and logs what
//! arrives. Stop it with Ctrl-C.
//!
//! Run with: `cargo run -p nt-dex-aster --example data_tester [SYMBOL...]`

use log::LevelFilter;
use nautilus_common::{enums::Environment, logging::logger::LoggerConfig};
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::{ClientId, TraderId};
use nautilus_testkit::testers::{DataTester, DataTesterConfig};
use nt_dex_aster::{
    common::{ASTER, instrument_id},
    config::AsterDataClientConfig,
    factories::AsterDataClientFactory,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut symbols: Vec<String> = std::env::args().skip(1).collect();
    if symbols.is_empty() {
        symbols.push("BTCUSDT".to_owned());
    }
    let instrument_ids = symbols.iter().map(|symbol| instrument_id(symbol)).collect();

    let log_config = LoggerConfig {
        stdout_level: LevelFilter::Info,
        ..Default::default()
    };
    let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Live)?
        .with_name("ASTER-DATA-TESTER-001".to_owned())
        .with_logging(log_config)
        .with_delay_post_stop_secs(2)
        .add_data_client(
            Some(ASTER.to_owned()),
            Box::new(AsterDataClientFactory),
            Box::new(AsterDataClientConfig::default()),
        )?
        .build()?;

    let tester_config = DataTesterConfig::builder()
        .client_id(ClientId::new(ASTER))
        .instrument_ids(instrument_ids)
        .request_instruments(true)
        .subscribe_book_deltas(true)
        .manage_book(true)
        .subscribe_quotes(true)
        .subscribe_trades(true)
        .subscribe_mark_prices(true)
        .subscribe_index_prices(true)
        .subscribe_funding_rates(true)
        .build()?;
    node.add_actor(DataTester::new(tester_config))?;
    node.run().await?;
    Ok(())
}
