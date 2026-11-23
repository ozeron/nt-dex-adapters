use std::{collections::BTreeSet, rc::Rc, time::Duration};

use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    enums::Environment,
    messages::data::{DataCommand, SubscribeCommand, SubscribeQuotes},
    msgbus::{self, switchboard::get_bars_topic},
    nautilus_actor,
    runner::get_data_cmd_sender,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::node::{LiveNode, NodeRunMode};
use nautilus_model::{
    data::{Bar, BarType},
    identifiers::{ActorId, ClientId, InstrumentId, TraderId},
    types::{Price, Quantity},
};
use nt_dex_aster::{
    common::{ASTER, instrument_id},
    factories::AsterDataClientFactory,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use super::{TIMEOUT, Venue, WS_BOOK_TICKER, run, trade_frame};

const TRADE_STREAM: &str = "solusdt@trade";
const QUOTE_STREAM: &str = "solusdt@bookTicker";

#[derive(Debug)]
struct BarSubscriber {
    core: DataActorCore,
    bar_type: BarType,
    deliveries: UnboundedSender<(ActorId, Bar)>,
}

nautilus_actor!(BarSubscriber);

impl DataActor for BarSubscriber {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_bars(self.bar_type, Some(ClientId::new(ASTER)), None);
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        // Let Trader retirement release the subscription.
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        self.deliveries.send((self.actor_id(), *bar))?;
        Ok(())
    }
}

async fn next_bar(deliveries: &mut UnboundedReceiver<(ActorId, Bar)>) -> (ActorId, Bar) {
    tokio::time::timeout(TIMEOUT, deliveries.recv())
        .await
        .expect("timed out waiting for an actor's bar")
        .expect("both bar actors have dropped their senders")
}

fn push_trade_pair(venue: &Venue, connection: usize, first_id: u64, open: &str, close: &str) {
    for (id, price) in [(first_id, open), (first_id + 1, close)] {
        let frame = trade_frame("SOLUSDT", id).replace("\"71\"", &format!("\"{price}\""));
        venue.push_on(connection, &frame);
    }
}

fn assert_bar(bar: Bar, bar_type: BarType, open: &str, close: &str) {
    assert_eq!(bar.bar_type, bar_type);
    assert_eq!(bar.open, Price::from(open));
    assert_eq!(bar.close, Price::from(close));
    assert_eq!(bar.volume, Quantity::from("124"));
}

fn subscribe_quotes(instrument: InstrumentId) {
    get_data_cmd_sender().execute(DataCommand::Subscribe(SubscribeCommand::Quotes(
        SubscribeQuotes::new(
            instrument,
            Some(ClientId::new(ASTER)),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ),
    )));
}

fn wire_unsubscribes(venue: &Venue) -> Vec<(usize, Vec<String>)> {
    venue
        .state
        .lock()
        .unwrap()
        .control
        .iter()
        .filter(|(_, method, _)| method == "UNSUBSCRIBE")
        .map(|(connection, _, streams)| (*connection, streams.clone()))
        .collect()
}

/// TC-D73, following `NautilusTrader` 0.68.0 `trader.rs:6184` through the live socket.
#[test]
fn tc_d73_shared_trade_bar_releases_venue_stream_after_final_actor() {
    run(async {
        let venue = Venue::start().await;
        let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Live)
            .unwrap()
            .with_delay_post_stop_secs(0)
            .with_delay_shutdown_secs(0)
            .add_data_client(
                Some(ASTER.to_owned()),
                Box::new(AsterDataClientFactory),
                Box::new(venue.config()),
            )
            .unwrap()
            .build()
            .unwrap();

        let instrument = instrument_id("SOLUSDT");
        let bar_type = BarType::from("SOLUSDT-PERP.ASTER-2-TICK-LAST-INTERNAL");
        let topic = get_bars_topic(bar_type);
        let first = ActorId::from("Bar-Subscriber-001");
        let peer = ActorId::from("Bar-Subscriber-002");
        let (deliveries, mut bars) = unbounded_channel();
        for actor_id in [first, peer] {
            node.add_actor(BarSubscriber {
                core: DataActorCore::new(DataActorConfig {
                    actor_id: Some(actor_id),
                    ..Default::default()
                }),
                bar_type,
                deliveries: deliveries.clone(),
            })
            .unwrap();
        }
        drop(deliveries);

        let trader = Rc::clone(node.kernel().trader());
        let engine = Rc::clone(&node.kernel().data_engine);
        let cache = node.kernel().cache();
        let handle = node.handle();
        let exercise = async {
            let connection = venue.wait_for_stream(TRADE_STREAM).await;
            assert_eq!(msgbus::exact_subscriber_count_bars(topic), 2);
            push_trade_pair(&venue, connection, 100, "71", "72");
            let (first_receiver, first_bar) = next_bar(&mut bars).await;
            let (second_receiver, second_bar) = next_bar(&mut bars).await;
            assert_eq!(
                BTreeSet::from([first_receiver, second_receiver]),
                BTreeSet::from([first, peer])
            );
            assert_eq!(first_bar, second_bar);
            assert_bar(first_bar, bar_type, "71", "72");

            trader.borrow_mut().remove_actor(&first).unwrap();
            assert_eq!(msgbus::exact_subscriber_count_bars(topic), 1);

            // Queue an independent stream behind the first removal. Its wire subscribe
            // shows the node has processed the preceding data commands, and the quote
            // stream will still be available to fence late trades after final removal.
            subscribe_quotes(instrument);
            assert_eq!(venue.wait_for_stream(QUOTE_STREAM).await, connection);
            assert_eq!(venue.wait_for_stream(TRADE_STREAM).await, connection);
            assert!(engine.borrow().subscribed_bars().contains(&bar_type));

            push_trade_pair(&venue, connection, 102, "73", "74");
            let (receiver, bar) = next_bar(&mut bars).await;
            assert_eq!(receiver, peer);
            assert_bar(bar, bar_type, "73", "74");
            assert!(bars.is_empty());
            assert_eq!(wire_unsubscribes(&venue), []);

            trader.borrow_mut().remove_actor(&peer).unwrap();
            venue
                .wait_for_unsubscribe(|streams| streams.iter().any(|s| s == TRADE_STREAM))
                .await;
            assert_eq!(trader.borrow().actor_ids(), []);
            assert_eq!(msgbus::exact_subscriber_count_bars(topic), 0);
            assert!(!engine.borrow().subscribed_bars().contains(&bar_type));
            assert_eq!(
                venue.live_connections(),
                vec![BTreeSet::from([QUOTE_STREAM.to_owned()])]
            );

            // Send a late pair through the real socket, followed by a quote on the
            // same connection. Seeing the quote in the node drains that input prefix.
            push_trade_pair(&venue, connection, 104, "75", "76");
            venue.push_on(connection, WS_BOOK_TICKER);
            tokio::time::timeout(TIMEOUT, async {
                while cache.borrow().quote(&instrument).is_none() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("timed out waiting for the quote behind late trades");
            handle.stop();
            Ok(connection)
        };

        // Keep both futures on the node's thread-local message bus and service the
        // live runner throughout actor registration, delivery, and retirement.
        let ((), connection) = tokio::time::timeout(TIMEOUT * 2, async {
            tokio::try_join!(node.run_with_mode(NodeRunMode::Hosted), exercise)
        })
        .await
        .expect("timed out running the live node")
        .unwrap();

        assert!(bars.is_empty(), "an actor received a bar after removal");
        venue
            .wait("the node to close the socket", |state| {
                state.connections[connection].out.is_none().then_some(())
            })
            .await;
        assert_eq!(
            wire_unsubscribes(&venue),
            [(connection, vec![TRADE_STREAM.to_owned()])]
        );
    });
}
