//! Parser and feed tests over the committed fixtures in `test_data/`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]

use std::str::FromStr;

use ahash::AHashMap;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::Data,
    enums::{AggressorSide, BookAction, OrderSide},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use nt_dex_aster::{
    common::{F_LAST, F_SNAPSHOT},
    feed::{AsterFeed, FeedEvent},
    http::parse::{parse_exchange_info, parse_funding_intervals, parse_trades},
    instruments::diff_instruments,
};
use rust_decimal::Decimal;

const EXCHANGE_INFO: &str = include_str!("../test_data/http_exchange_info.json");
const FUNDING_INFO: &str = include_str!("../test_data/http_funding_info.json");
const DEPTH_SNAPSHOT: &str = include_str!("../test_data/http_depth_solusdt.json");
const WS_DEPTH: &str = include_str!("../test_data/ws_depth.json");
const HTTP_TRADES: &str = include_str!("../test_data/http_trades.json");
const WS_AGG_TRADE: &str = include_str!("../test_data/ws_agg_trade.json");
const WS_TRADE: &str = include_str!("../test_data/ws_trade.json");
const WS_BOOK_TICKER: &str = include_str!("../test_data/ws_book_ticker.json");
const WS_MARK_PRICE: &str = include_str!("../test_data/ws_mark_price.json");

const TS_INIT: UnixNanos = UnixNanos::new(1_795_345_500_000_000_000);
const SOL: &str = "SOLUSDT-PERP.ASTER";
const BTC: &str = "BTCUSDT-PERP.ASTER";

fn instruments() -> Vec<InstrumentAny> {
    parse_exchange_info(EXCHANGE_INFO, TS_INIT)
        .unwrap()
        .instruments
}

fn instrument(symbol: &str) -> InstrumentAny {
    instruments()
        .into_iter()
        .find(|instrument| instrument.raw_symbol().as_str() == symbol)
        .unwrap_or_else(|| panic!("{symbol} not loaded"))
}

fn millis(value: u64) -> UnixNanos {
    UnixNanos::from(value * 1_000_000)
}

fn data(feed: &mut AsterFeed, frame: &str) -> Vec<Data> {
    let mut events = Vec::new();
    feed.handle_text(frame, TS_INIT, &mut events).unwrap();
    events
        .into_iter()
        .map(|event| match event {
            FeedEvent::Data(data) => data,
            FeedEvent::SnapshotRequired(symbol) => panic!("unexpected snapshot request: {symbol}"),
        })
        .collect()
}

#[test]
fn exchange_info_loads_trading_perpetuals_only() {
    let parsed = parse_exchange_info(EXCHANGE_INFO, TS_INIT).unwrap();
    assert!(parsed.failed.is_empty(), "{:?}", parsed.failed);

    let mut loaded: Vec<String> = parsed
        .instruments
        .iter()
        .map(|instrument| instrument.raw_symbol().to_string())
        .collect();
    loaded.sort_unstable();
    assert_eq!(loaded, ["BTCUSDT", "SOLUSDT", "XAUUSD1", "YFIUSDT"]);

    let mut skipped = parsed.skipped.clone();
    skipped.sort_unstable();
    assert_eq!(
        skipped,
        ["MBLUSDT", "TONUSDT"],
        "settling and pending symbols are skipped"
    );
}

#[test]
fn instrument_uses_venue_precision_with_padded_tick() {
    let sol = instrument("SOLUSDT");
    assert_eq!(sol.id().to_string(), SOL);
    assert_eq!(sol.price_precision(), 4);
    assert_eq!(sol.price_increment(), Price::from("0.0100"));
    assert_eq!(sol.size_precision(), 2);
    assert_eq!(sol.size_increment(), Quantity::from("0.01"));
    assert_eq!(sol.quote_currency(), Currency::from("USDT"));
    assert!(!sol.is_inverse());
}

#[test]
fn instrument_rescales_tick_coarser_than_precision() {
    let yfi = instrument("YFIUSDT");
    assert_eq!(yfi.price_precision(), 3);
    assert_eq!(yfi.price_increment(), Price::from("1.000"));
    assert_eq!(yfi.size_precision(), 4);
    assert_eq!(yfi.size_increment(), Quantity::from("0.0010"));
}

#[test]
fn instrument_margins_and_currencies() {
    let btc = instrument("BTCUSDT");
    assert_eq!(btc.margin_init(), Decimal::from_str("0.05").unwrap());
    assert_eq!(btc.margin_maint(), Decimal::from_str("0.025").unwrap());
    assert_eq!(btc.price_increment(), Price::from("0.1"));
    assert_eq!(btc.size_increment(), Quantity::from("0.001"));

    let gold = instrument("XAUUSD1");
    assert_eq!(gold.quote_currency().code.as_str(), "USD1");
    assert_eq!(gold.settlement_currency().code.as_str(), "USD1");
}

#[test]
fn agg_trade_becomes_trade_tick() {
    let mut feed = AsterFeed::new(&instruments());
    let [Data::Trade(trade)] = data(&mut feed, WS_AGG_TRADE)[..] else {
        panic!("expected a trade")
    };
    assert_eq!(trade.instrument_id.to_string(), SOL);
    assert_eq!(trade.price, Price::from("70.7200"));
    assert_eq!(trade.size, Quantity::from("62.34"));
    assert_eq!(trade.aggressor_side, AggressorSide::Buy);
    assert_eq!(trade.trade_id.to_string(), "30717079");
    assert_eq!(trade.ts_event, millis(1_795_345_469_900));
    assert_eq!(trade.ts_init, TS_INIT);
}

fn trade_frame(id: u64) -> String {
    let frame = WS_TRADE.replace("150490341", &id.to_string());
    assert!(frame.contains(&format!(r#""t":{id}"#)), "fixture changed");
    frame
}

fn trade_ids(feed: &mut AsterFeed, frames: &[String]) -> Vec<String> {
    frames
        .iter()
        .flat_map(|frame| data(feed, frame))
        .map(|data| match data {
            Data::Trade(trade) => trade.trade_id.to_string(),
            other => panic!("expected a trade, got {other:?}"),
        })
        .collect()
}

#[test]
fn trade_becomes_trade_tick() {
    let mut feed = AsterFeed::new(&instruments());
    let [Data::Trade(trade)] = data(&mut feed, WS_TRADE)[..] else {
        panic!("expected a trade")
    };
    assert_eq!(trade.instrument_id.to_string(), BTC);
    assert_eq!(trade.price, Price::from("60757.6"));
    assert_eq!(trade.size, Quantity::from("0.045"));
    assert_eq!(trade.aggressor_side, AggressorSide::Sell);
    assert_eq!(trade.trade_id.to_string(), "150490341");
    assert_eq!(trade.ts_event, millis(1_795_375_790_450));
    assert_eq!(trade.ts_init, TS_INIT);
}

#[test]
fn trade_aggressor_follows_the_maker_flag() {
    let mut feed = AsterFeed::new(&instruments());
    let taker_buy = WS_TRADE.replace(r#""m":true"#, r#""m":false"#);
    assert_ne!(taker_buy, WS_TRADE, "fixture no longer has the maker flag");
    let [Data::Trade(trade)] = data(&mut feed, &taker_buy)[..] else {
        panic!("expected a trade")
    };
    assert_eq!(trade.aggressor_side, AggressorSide::Buy);
}

#[test]
fn trade_with_zero_size_is_rejected() {
    let mut feed = AsterFeed::new(&instruments());
    let mut events = Vec::new();
    let zero_size = WS_TRADE.replace("0.045", "0.000");
    assert_ne!(zero_size, WS_TRADE);
    let error = feed
        .handle_text(&zero_size, TS_INIT, &mut events)
        .unwrap_err();
    assert!(format!("{error:#}").contains("zero size"), "{error:#}");
    assert!(events.is_empty());
    assert_eq!(
        data(&mut feed, WS_TRADE).len(),
        1,
        "the feed is still usable"
    );
}

#[test]
fn trade_order_type_is_not_a_filter() {
    let mut feed = AsterFeed::new(&instruments());
    assert!(WS_TRADE.contains(r#""X":"MARKET""#));
    let unknown = WS_TRADE.replace(r#""X":"MARKET""#, r#""X":"SOMETHING_NEW""#);
    let missing = WS_TRADE.replace(r#","X":"MARKET""#, "");
    assert!(!missing.contains(r#""X""#));
    assert_eq!(
        trade_ids(&mut feed, &[unknown, trade_frame(150_490_342)]),
        ["150490341", "150490342"]
    );
    assert_eq!(
        trade_ids(&mut feed, &[missing.replace("150490341", "150490343")]),
        ["150490343"]
    );
}

#[test]
fn repeated_trades_are_dropped() {
    let mut feed = AsterFeed::new(&instruments());
    let frames = [
        trade_frame(150_490_341),
        trade_frame(150_490_341),
        trade_frame(150_490_342),
        trade_frame(150_490_341),
        trade_frame(150_490_342),
        trade_frame(150_490_343),
    ];
    assert_eq!(
        trade_ids(&mut feed, &frames),
        ["150490341", "150490342", "150490343"]
    );
    assert_eq!(feed.missed_trades(), 0);
}

#[test]
fn a_jump_in_trade_ids_is_counted_as_missed() {
    let mut feed = AsterFeed::new(&instruments());
    let frames = [
        trade_frame(150_490_341),
        trade_frame(150_490_342),
        trade_frame(150_490_346),
        trade_frame(150_490_347),
    ];
    assert_eq!(
        trade_ids(&mut feed, &frames).len(),
        4,
        "a trade after a gap is still published"
    );
    assert_eq!(feed.missed_trades(), 3);
}

#[test]
fn trade_and_aggregate_ids_are_tracked_separately() {
    let mut feed = AsterFeed::new(&instruments());
    let agg = WS_AGG_TRADE.to_owned();
    let trade = WS_TRADE
        .replace("BTCUSDT", "SOLUSDT")
        .replace("btcusdt", "solusdt")
        .replace("60757.6", "70.7200")
        .replace("0.045", "62.34")
        .replace("150490341", "5");
    assert_eq!(
        trade_ids(
            &mut feed,
            &[agg, trade, WS_AGG_TRADE.replace("30717079", "30717080")]
        ),
        ["30717079", "5", "30717080"]
    );
    assert_eq!(feed.missed_trades(), 0);
}

#[test]
fn http_trades_become_trade_ticks() {
    let btc = instrument("BTCUSDT");
    let trades = parse_trades(
        HTTP_TRADES,
        btc.id(),
        btc.price_precision(),
        btc.size_precision(),
        TS_INIT,
    )
    .unwrap();
    let ids: Vec<String> = trades.iter().map(|t| t.trade_id.to_string()).collect();
    assert_eq!(ids, ["150490379", "150490380", "150490381"], "oldest first");
    let first = trades[0];
    assert_eq!(first.instrument_id.to_string(), BTC);
    assert_eq!(first.price, Price::from("60684.1"));
    assert_eq!(first.size, Quantity::from("0.001"));
    assert_eq!(first.aggressor_side, AggressorSide::Sell);
    assert_eq!(first.ts_event, millis(1_795_375_994_250));
    assert_eq!(first.ts_init, TS_INIT);
    assert_eq!(trades[1].aggressor_side, AggressorSide::Buy);

    let zero_size = HTTP_TRADES.replace("0.352", "0.000");
    assert_ne!(zero_size, HTTP_TRADES);
    let error = parse_trades(&zero_size, btc.id(), 1, 3, TS_INIT).unwrap_err();
    assert!(format!("{error:#}").contains("zero size"), "{error:#}");
    assert!(parse_trades("{}", btc.id(), 1, 3, TS_INIT).is_err());
}

#[test]
fn book_ticker_becomes_quote_tick() {
    let mut feed = AsterFeed::new(&instruments());
    let [Data::Quote(quote)] = data(&mut feed, WS_BOOK_TICKER)[..] else {
        panic!("expected a quote")
    };
    assert_eq!(quote.bid_price, Price::from("70.7000"));
    assert_eq!(quote.bid_size, Quantity::from("1516.31"));
    assert_eq!(quote.ask_price, Price::from("70.7100"));
    assert_eq!(quote.ask_size, Quantity::from("153.42"));
    assert_eq!(quote.ts_event, millis(1_795_345_451_550));
}

#[test]
fn mark_price_becomes_mark_index_and_funding() {
    let mut feed = AsterFeed::new(&instruments());
    let events = data(&mut feed, WS_MARK_PRICE);
    let [
        Data::MarkPrice(mark),
        Data::IndexPrice(index),
        Data::FundingRate(funding),
    ] = &events[..]
    else {
        panic!("expected mark, index, funding; was {events:?}")
    };
    assert_eq!(mark.value, Price::from("70.71000000"));
    assert_eq!(index.value, Price::from("70.74245126"));
    assert_eq!(funding.rate, Decimal::from_str("0.00006196").unwrap());
    assert_eq!(funding.next_funding_ns, Some(millis(1_795_363_200_000)));
    assert_eq!(mark.ts_event, millis(1_795_345_451_000));
}

#[test]
fn depth_waits_for_snapshot_then_book_goes_live() {
    let mut feed = AsterFeed::new(&instruments());
    let mut events = Vec::new();

    feed.handle_text(WS_DEPTH, TS_INIT, &mut events).unwrap();
    assert!(matches!(&events[..], [FeedEvent::SnapshotRequired(symbol)] if symbol == "SOLUSDT"));
    assert!(!feed.is_book_live("SOLUSDT"));
    events.clear();

    feed.handle_depth_snapshot("SOLUSDT", DEPTH_SNAPSHOT, TS_INIT, &mut events)
        .unwrap();
    let [FeedEvent::Data(Data::BookDeltas(snapshot))] = &events[..] else {
        panic!("expected one snapshot, was {events:?}")
    };
    assert!(feed.is_book_live("SOLUSDT"));
    assert_eq!(snapshot.sequence, 668_462_586_092);
    assert_eq!(
        snapshot.deltas.len(),
        11,
        "a clear plus five levels per side"
    );
    assert_eq!(snapshot.deltas[0].action, BookAction::Clear);
    assert!(
        snapshot
            .deltas
            .iter()
            .all(|delta| delta.flags & F_SNAPSHOT != 0)
    );
    assert!(
        snapshot.deltas[..10]
            .iter()
            .all(|delta| delta.flags & F_LAST == 0)
    );
    assert_ne!(snapshot.deltas[10].flags & F_LAST, 0);

    let best_bid = &snapshot.deltas[1];
    assert_eq!(best_bid.action, BookAction::Add);
    assert_eq!(best_bid.order.side, Some(OrderSide::Buy));
    assert_eq!(best_bid.order.price, Price::from("70.7000"));
    assert_eq!(best_bid.order.size, Quantity::from("1516.31"));
    assert_eq!(best_bid.ts_event, millis(1_795_345_452_500));
}

#[test]
fn depth_diff_after_snapshot_updates_and_deletes() {
    let mut feed = AsterFeed::new(&instruments());
    let mut events = Vec::new();
    feed.handle_text(WS_DEPTH, TS_INIT, &mut events).unwrap();
    let snapshot = DEPTH_SNAPSHOT.replace("668462586092", "668462549000");
    events.clear();
    feed.handle_depth_snapshot("SOLUSDT", &snapshot, TS_INIT, &mut events)
        .unwrap();

    let [
        FeedEvent::Data(Data::BookDeltas(_)),
        FeedEvent::Data(Data::BookDeltas(diff)),
    ] = &events[..]
    else {
        panic!("expected snapshot then diff, was {events:?}")
    };
    assert_eq!(
        diff.sequence, 668_462_549_397,
        "sequence is the final update id"
    );
    assert_eq!(diff.deltas.len(), 12);
    assert_eq!(diff.ts_event, millis(1_795_345_450_600));

    let bid = &diff.deltas[0];
    assert_eq!(
        (bid.action, bid.order.side),
        (BookAction::Update, Some(OrderSide::Buy))
    );
    assert_eq!(bid.order.price, Price::from("70.6900"));
    assert_eq!(bid.order.size, Quantity::from("5015.22"));

    let removed = &diff.deltas[2];
    assert_eq!(
        (removed.action, removed.order.side),
        (BookAction::Delete, Some(OrderSide::Sell))
    );
    assert_eq!(removed.order.price, Price::from("76.7800"));

    assert!(diff.deltas[..11].iter().all(|delta| delta.flags == 0));
    assert_eq!(diff.deltas[11].flags, F_LAST);
}

#[test]
fn control_replies_and_unknown_streams_are_ignored() {
    let mut feed = AsterFeed::new(&instruments());
    assert_eq!(data(&mut feed, r#"{"result":null,"id":1}"#).len(), 0);
    let kline = r#"{"stream":"solusdt@kline_1m","data":{"e":"kline","s":"SOLUSDT"}}"#;
    assert_eq!(data(&mut feed, kline).len(), 0);
}

#[test]
fn bad_frames_are_errors_and_do_not_poison_the_feed() {
    let mut feed = AsterFeed::new(&instruments());
    let mut events = Vec::new();

    assert!(feed.handle_text("not json", TS_INIT, &mut events).is_err());
    let unknown = WS_AGG_TRADE.replace("SOLUSDT", "NOPEUSDT");
    let error = feed
        .handle_text(&unknown, TS_INIT, &mut events)
        .unwrap_err();
    assert!(error.to_string().contains("NOPEUSDT"), "{error}");
    let bad_price = WS_AGG_TRADE.replace("70.7200", "abc");
    assert!(feed.handle_text(&bad_price, TS_INIT, &mut events).is_err());
    assert!(events.is_empty());

    let zero_size = WS_AGG_TRADE.replace("62.34", "0.00");
    let error = feed
        .handle_text(&zero_size, TS_INIT, &mut events)
        .unwrap_err();
    assert!(format!("{error:#}").contains("zero size"), "{error:#}");

    assert_eq!(data(&mut feed, WS_AGG_TRADE).len(), 1);
}

#[test]
fn rejected_symbols_are_not_interned() {
    let mut feed = AsterFeed::new(&instruments());
    let symbol = "UNKNOWN_WIRE_TEST_SYMBOL";
    let frame = WS_AGG_TRADE.replace("SOLUSDT", symbol);
    assert!(ustr::Ustr::from_existing(symbol).is_none());
    assert!(feed.handle_text(&frame, TS_INIT, &mut Vec::new()).is_err());
    assert!(ustr::Ustr::from_existing(symbol).is_none());
}

#[test]
fn funding_info_gives_intervals_in_minutes() {
    let intervals = parse_funding_intervals(FUNDING_INFO).unwrap();
    assert!(intervals.contains(&("SOLUSDT".to_owned(), 480)));
    assert!(intervals.contains(&("SLXUSDT".to_owned(), 240)));
}

#[test]
fn funding_rate_carries_the_venue_interval() {
    let mut feed = AsterFeed::new(&instruments());
    let without = data(&mut feed, WS_MARK_PRICE);
    let Some(Data::FundingRate(funding)) = without.last() else {
        panic!("expected funding; was {without:?}")
    };
    assert_eq!(funding.interval, None);

    feed.set_funding_intervals(&parse_funding_intervals(FUNDING_INFO).unwrap());
    let with = data(&mut feed, WS_MARK_PRICE);
    let Some(Data::FundingRate(funding)) = with.last() else {
        panic!("expected funding; was {with:?}")
    };
    assert_eq!(funding.interval, Some(480));

    feed.add_instrument(&instrument("SOLUSDT"));
    let reloaded = data(&mut feed, WS_MARK_PRICE);
    let Some(Data::FundingRate(funding)) = reloaded.last() else {
        panic!("expected funding; was {reloaded:?}")
    };
    assert_eq!(funding.interval, Some(480));
}

fn cache(instruments: &[InstrumentAny]) -> AHashMap<InstrumentId, InstrumentAny> {
    instruments
        .iter()
        .map(|instrument| (instrument.id(), instrument.clone()))
        .collect()
}

#[test]
fn reloading_the_same_instruments_changes_nothing() {
    let cached = cache(&instruments());
    let later = parse_exchange_info(EXCHANGE_INFO, millis(1_795_400_000_000))
        .unwrap()
        .instruments;
    assert!(diff_instruments(&cached, &later).is_empty());
}

#[test]
fn instrument_diff_reports_added_changed_and_removed() {
    let loaded = instruments();
    let sol = InstrumentId::from(SOL);
    let btc = InstrumentId::from("BTCUSDT-PERP.ASTER");

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
    let reloaded = parse_exchange_info(&info.to_string(), TS_INIT)
        .unwrap()
        .instruments;

    let mut cached = cache(&loaded);
    let unseen = loaded
        .iter()
        .map(Instrument::id)
        .find(|id| *id != sol && *id != btc)
        .unwrap();
    cached.remove(&unseen);

    let changes = diff_instruments(&cached, &reloaded);
    let ids = |list: &[InstrumentAny]| list.iter().map(Instrument::id).collect::<Vec<_>>();
    assert_eq!(ids(&changes.added), [unseen]);
    assert_eq!(ids(&changes.changed), [sol]);
    assert_eq!(changes.removed, [btc]);
}

#[test]
fn a_book_level_finer_than_the_instrument_is_refused_not_rounded() {
    let mut feed = AsterFeed::new(&instruments());
    let mut events = Vec::new();
    let frame = WS_DEPTH.replacen("\"5015.22\"", "\"0.004\"", 1);
    assert_ne!(frame, WS_DEPTH, "fixture no longer has the expected level");
    let error = feed.handle_text(&frame, TS_INIT, &mut events).unwrap_err();
    assert!(format!("{error:#}").contains("more decimals"), "{error:#}");
    assert!(events.is_empty());
}

#[test]
fn funding_intervals_are_replaced_not_merged() {
    let mut feed = AsterFeed::new(&instruments());
    feed.set_funding_intervals(&parse_funding_intervals(FUNDING_INFO).unwrap());
    feed.replace_funding_intervals(&[("BTCUSDT".to_owned(), 60)]);
    let events = data(&mut feed, WS_MARK_PRICE);
    let Some(Data::FundingRate(funding)) = events.last() else {
        panic!("expected funding; was {events:?}")
    };
    assert_eq!(funding.interval, None);
}
