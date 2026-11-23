//! Mark and index prices are stored at eight decimals, not the instrument's
//! tick precision. These tests check that every venue symbol survives that
//! without loss, that the tick precision would not, and how the parser treats
//! values at the edges.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "test code"
)]

use std::str::FromStr;

use nautilus_core::UnixNanos;
use nautilus_model::{data::Data, types::Price};
use nt_dex_aster::{
    common::{self, REFERENCE_PRICE_PRECISION},
    websocket::{
        messages::MarkPrice,
        parse::{InstrumentSpec, parse_mark_price, parse_mark_price_parts},
    },
};
use rust_decimal::Decimal;
use serde::Deserialize;

const PREMIUM_INDEX: &str = include_str!("../test_data/premium_index_all.json");

const TS_INIT: UnixNanos = UnixNanos::new(1_795_375_924_000_000_000);
const EVENT_TIME_MS: u64 = 1_795_375_923_000;
const NEXT_FUNDING_MS: u64 = 1_795_392_000_000;

const LARGEST_PRICE: &str = "17014118346046";

#[derive(Debug, Deserialize)]
struct Row {
    symbol: String,
    #[serde(rename = "markPrice")]
    mark_price: String,
    #[serde(rename = "indexPrice")]
    index_price: String,
    #[serde(rename = "pricePrecision")]
    price_precision: u8,
}

fn rows() -> Vec<Row> {
    serde_json::from_str(PREMIUM_INDEX).unwrap()
}

fn spec(symbol: &str, price_precision: u8) -> InstrumentSpec {
    InstrumentSpec {
        id: common::instrument_id(symbol),
        price_precision,
        size_precision: 3,
        funding_interval_mins: Some(480),
    }
}

fn message<'a>(mark: &'a str, index: &'a str, funding: &'a str) -> MarkPrice<'a> {
    MarkPrice {
        event_time: EVENT_TIME_MS,
        symbol: "TESTUSDT",
        mark_price: mark,
        index_price: index,
        funding_rate: funding,
        next_funding_time: NEXT_FUNDING_MS,
    }
}

fn dec(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn mark_of(value: &str) -> Price {
    let parts =
        parse_mark_price_parts(&message(value, "1", "0.0001"), spec("TESTUSDT", 2), TS_INIT)
            .unwrap();
    parts.mark.unwrap().value
}

fn bps(original: Decimal, stored: Decimal) -> Decimal {
    (stored - original).abs() / original * Decimal::from(10_000)
}

#[test]
fn fixture_covers_the_trading_venue() {
    let rows = rows();
    assert!(rows.len() > 500, "{} symbols", rows.len());
    assert!(rows.iter().all(|row| !row.symbol.is_empty()));
}

#[test]
fn every_symbol_survives_at_eight_decimals() {
    for row in rows() {
        let parts = parse_mark_price_parts(
            &message(&row.mark_price, &row.index_price, "0.00010000"),
            spec(&row.symbol, row.price_precision),
            TS_INIT,
        )
        .unwrap();
        let mark = parts.mark.unwrap();
        let index = parts.index.unwrap();

        assert_eq!(mark.value.precision, REFERENCE_PRICE_PRECISION, "{row:?}");
        assert_eq!(index.value.precision, REFERENCE_PRICE_PRECISION, "{row:?}");
        assert_eq!(mark.value.as_decimal(), dec(&row.mark_price), "{row:?}");
        assert_eq!(index.value.as_decimal(), dec(&row.index_price), "{row:?}");
        assert!(!parts.mark_rounded && !parts.index_rounded, "{row:?}");
        assert!(parts.funding.is_ok(), "{row:?}");
    }
}

#[test]
fn rounding_to_the_tick_would_lose_more_than_ten_bps_on_some_symbol() {
    let mut worst_tick = Decimal::ZERO;
    let mut worst_eight = Decimal::ZERO;
    let mut over_ten_bps = 0_usize;
    for row in rows() {
        for value in [&row.mark_price, &row.index_price] {
            let original = dec(value);
            assert!(original > Decimal::ZERO, "{row:?}");

            let at_tick = Price::from_decimal_dp(original, row.price_precision)
                .unwrap()
                .as_decimal();
            let at_eight = common::reference_price(value).unwrap().price.as_decimal();

            let tick_loss = bps(original, at_tick);
            if tick_loss > Decimal::from(10) {
                over_ten_bps += 1;
            }
            worst_tick = worst_tick.max(tick_loss);
            worst_eight = worst_eight.max(bps(original, at_eight));
        }
    }
    assert!(
        worst_tick > Decimal::from(10),
        "worst tick rounding loss was {worst_tick} bps"
    );
    assert!(over_ten_bps >= 1);
    assert_eq!(worst_eight, Decimal::ZERO);
}

#[test]
fn nine_decimals_are_rounded_half_even_and_flagged() {
    let down = parse_mark_price_parts(
        &message("70.123456785", "70.123456775", "0.0001"),
        spec("TESTUSDT", 2),
        TS_INIT,
    )
    .unwrap();
    assert_eq!(down.mark.unwrap().value.as_decimal(), dec("70.12345678"));
    assert_eq!(down.index.unwrap().value.as_decimal(), dec("70.12345678"));
    assert!(down.mark_rounded);
    assert!(down.index_rounded);

    let up = common::reference_price("70.123456786").unwrap();
    assert_eq!(up.price.as_decimal(), dec("70.12345679"));
    assert!(up.rounded);
    assert_eq!(up.price.precision, 8);
}

#[test]
fn trailing_zeros_beyond_eight_decimals_are_not_a_rounding() {
    let reference = common::reference_price("70.710000000").unwrap();
    assert_eq!(reference.price.as_decimal(), dec("70.71"));
    assert!(!reference.rounded);
}

#[test]
fn rounded_flags_are_per_field() {
    let parts = parse_mark_price_parts(
        &message("70.71000000", "70.123456789", "0.0001"),
        spec("TESTUSDT", 2),
        TS_INIT,
    )
    .unwrap();
    assert!(!parts.mark_rounded);
    assert!(parts.index_rounded);
}

#[test]
fn whole_numbers_keep_precision_eight() {
    let parts = parse_mark_price_parts(
        &message("1666", "1663", "0.0001"),
        spec("YFIUSDT", 3),
        TS_INIT,
    )
    .unwrap();
    let mark = parts.mark.unwrap();
    assert_eq!(mark.value.precision, 8);
    assert_eq!(mark.value.as_decimal(), dec("1666"));
    assert_eq!(mark.value.to_string(), "1666.00000000");
    let index = parts.index.unwrap();
    assert_eq!(index.value.precision, 8);
    assert_eq!(index.value.as_decimal(), dec("1663"));
    assert!(!parts.mark_rounded && !parts.index_rounded);
}

#[test]
fn zero_is_accepted() {
    for value in ["0", "0.00000000"] {
        let price = mark_of(value);
        assert!(price.is_zero(), "{value}");
        assert_eq!(price.precision, 8);
    }
}

#[test]
fn largest_price_at_precision_eight_is_17_014_118_346_046() {
    assert_eq!(
        Decimal::try_from(nautilus_model::types::price::PRICE_MAX).unwrap(),
        dec(LARGEST_PRICE),
        "the nautilus-model limit changed; update LARGEST_PRICE"
    );
    for value in [LARGEST_PRICE, "17014118346046.00000000"] {
        let price = mark_of(value);
        assert_eq!(price.as_decimal(), dec(LARGEST_PRICE), "{value}");
        assert_eq!(price.precision, 8);
    }
}

#[test]
fn prices_above_the_limit_fail_only_their_own_field() {
    for value in ["17014118346046.00000001", "17014118346047"] {
        let parts = parse_mark_price_parts(
            &message(value, "70.71000000", "0.0001"),
            spec("TESTUSDT", 2),
            TS_INIT,
        )
        .unwrap();
        assert!(parts.mark.is_err(), "{value}");
        assert!(parts.index.is_ok(), "{value}");
        assert!(parts.funding.is_ok(), "{value}");
        assert!(!parts.mark_rounded);
    }
}

#[test]
fn unparseable_mark_still_publishes_index_and_funding() {
    let message = message("not-a-number", "70.74245126", "0.00006196");
    let parts = parse_mark_price_parts(&message, spec("SOLUSDT", 2), TS_INIT).unwrap();
    assert!(parts.mark.is_err());
    assert_eq!(
        parts.index.as_ref().unwrap().value.as_decimal(),
        dec("70.74245126")
    );
    let funding = parts.funding.as_ref().unwrap();
    assert_eq!(funding.rate, dec("0.00006196"));
    assert_eq!(funding.interval, Some(480));
    assert_eq!(
        funding.next_funding_ns,
        Some(UnixNanos::from(NEXT_FUNDING_MS * 1_000_000))
    );

    let results = parts.into_results();
    assert!(results[0].is_err());
    assert!(matches!(results[1], Ok(Data::IndexPrice(_))));
    assert!(matches!(results[2], Ok(Data::FundingRate(_))));

    assert!(parse_mark_price(&message, spec("SOLUSDT", 2), TS_INIT).is_err());
}

#[test]
fn unparseable_index_still_publishes_mark_and_funding() {
    let parts = parse_mark_price_parts(
        &message("70.71000000", "", "0.00006196"),
        spec("SOLUSDT", 2),
        TS_INIT,
    )
    .unwrap();
    assert!(parts.index.is_err());
    assert!(parts.mark.is_ok());
    assert!(parts.funding.is_ok());
}

#[test]
fn unparseable_funding_still_publishes_both_prices() {
    let parts = parse_mark_price_parts(
        &message("70.71000000", "70.74245126", "n/a"),
        spec("SOLUSDT", 2),
        TS_INIT,
    )
    .unwrap();
    assert!(parts.funding.is_err());
    assert!(parts.mark.is_ok());
    assert!(parts.index.is_ok());
}

#[test]
fn unrepresentable_next_funding_time_fails_only_funding() {
    let mut message = message("70.71000000", "70.74245126", "0.0001");
    message.next_funding_time = u64::MAX;
    let parts = parse_mark_price_parts(&message, spec("SOLUSDT", 2), TS_INIT).unwrap();
    assert!(parts.funding.is_err());
    assert!(parts.mark.is_ok());
    assert!(parts.index.is_ok());
}

#[test]
fn unrepresentable_event_time_fails_the_whole_message() {
    let mut message = message("70.71000000", "70.74245126", "0.0001");
    message.event_time = u64::MAX;
    assert!(parse_mark_price_parts(&message, spec("SOLUSDT", 2), TS_INIT).is_err());
    assert!(parse_mark_price(&message, spec("SOLUSDT", 2), TS_INIT).is_err());
}

#[test]
fn mark_price_wrapper_matches_the_parts() {
    let message = message("70.71000000", "70.74245126", "0.00006196");
    let data = parse_mark_price(&message, spec("SOLUSDT", 2), TS_INIT).unwrap();
    assert_eq!(data.mark.value.as_decimal(), dec("70.71"));
    assert_eq!(data.index.value.as_decimal(), dec("70.74245126"));
    assert_eq!(data.funding.rate, dec("0.00006196"));
    assert_eq!(
        data.mark.ts_event,
        UnixNanos::from(EVENT_TIME_MS * 1_000_000)
    );
    assert_eq!(data.mark.ts_init, TS_INIT);
}
