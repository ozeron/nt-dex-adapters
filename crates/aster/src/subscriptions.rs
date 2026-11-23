//! Which data subscribers want, and which venue streams that requires.
//!
//! Kept free of I/O so the bookkeeping can be tested on its own.

use ahash::{AHashMap, AHashSet};
use nautilus_model::{data::Data, identifiers::InstrumentId};

use crate::feed::stream_symbol;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    Book,
    Trades,
    Quotes,
    MarkPrice,
    IndexPrice,
    FundingRate,
}

impl Kind {
    pub(crate) fn stream(self, instrument_id: &InstrumentId) -> String {
        let channel = match self {
            Self::Book => "depth@100ms",
            Self::Trades => "trade",
            Self::Quotes => "bookTicker",
            Self::MarkPrice | Self::IndexPrice | Self::FundingRate => "markPrice@1s",
        };
        format!("{}@{channel}", stream_symbol(instrument_id))
    }

    pub(crate) fn of(data: &Data) -> Option<(InstrumentId, Self)> {
        match data {
            Data::BookDeltas(deltas) => Some((deltas.instrument_id, Self::Book)),
            Data::Trade(trade) => Some((trade.instrument_id, Self::Trades)),
            Data::Quote(quote) => Some((quote.instrument_id, Self::Quotes)),
            Data::MarkPrice(mark) => Some((mark.instrument_id, Self::MarkPrice)),
            Data::IndexPrice(index) => Some((index.instrument_id, Self::IndexPrice)),
            Data::FundingRate(funding) => Some((funding.instrument_id, Self::FundingRate)),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Command {
    Subscribe(InstrumentId, Kind),
    Unsubscribe(InstrumentId, Kind),
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Changes {
    pub(crate) opened: Vec<String>,
    pub(crate) closed: Vec<String>,
    pub(crate) books_ended: Vec<InstrumentId>,
    pub(crate) rejected: Vec<(InstrumentId, Kind)>,
}

#[derive(Debug)]
pub(crate) struct Subscriptions {
    capacity: usize,
    wanted: AHashSet<(InstrumentId, Kind)>,
    streams: AHashMap<String, usize>,
}

impl Subscriptions {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            wanted: AHashSet::new(),
            streams: AHashMap::new(),
        }
    }

    pub(crate) fn instruments(&self, kind: Kind) -> impl Iterator<Item = InstrumentId> + '_ {
        self.wanted
            .iter()
            .filter(move |(_, wanted)| *wanted == kind)
            .map(|(instrument_id, _)| *instrument_id)
    }

    pub(crate) fn uses(&self, stream: &str) -> bool {
        self.streams.contains_key(stream)
    }

    pub(crate) fn wants(&self, instrument_id: InstrumentId, kind: Kind) -> bool {
        self.wanted.contains(&(instrument_id, kind))
    }

    pub(crate) fn apply(&mut self, commands: &[Command]) -> Changes {
        let mut changes = Changes::default();
        let mut before: AHashMap<String, bool> = AHashMap::new();

        for command in commands {
            match *command {
                Command::Subscribe(instrument_id, kind) => {
                    if self.wants(instrument_id, kind) {
                        continue;
                    }
                    let stream = kind.stream(&instrument_id);
                    let is_open = self.streams.contains_key(&stream);
                    if !is_open && self.streams.len() >= self.capacity {
                        changes.rejected.push((instrument_id, kind));
                        continue;
                    }
                    before.entry(stream.clone()).or_insert(is_open);
                    self.wanted.insert((instrument_id, kind));
                    let users = self.streams.entry(stream).or_insert(0);
                    *users = users.saturating_add(1);
                }
                Command::Unsubscribe(instrument_id, kind) => {
                    if !self.wanted.remove(&(instrument_id, kind)) {
                        continue;
                    }
                    if kind == Kind::Book {
                        changes.books_ended.push(instrument_id);
                    }
                    let stream = kind.stream(&instrument_id);
                    before.entry(stream.clone()).or_insert(true);
                    if let Some(users) = self.streams.get_mut(&stream) {
                        *users = users.saturating_sub(1);
                        if *users == 0 {
                            self.streams.remove(&stream);
                        }
                    }
                }
            }
        }

        for (stream, was_open) in before {
            match (was_open, self.streams.contains_key(&stream)) {
                (false, true) => changes.opened.push(stream),
                (true, false) => changes.closed.push(stream),
                _ => {}
            }
        }
        changes.opened.sort_unstable();
        changes.closed.sort_unstable();
        changes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::instrument_id;

    const MAX_STREAMS: usize = 200;

    fn btc() -> InstrumentId {
        instrument_id("BTCUSDT")
    }

    #[test]
    fn subscribe_opens_stream_once() {
        let mut subs = Subscriptions::new(MAX_STREAMS);
        let changes = subs.apply(&[Command::Subscribe(btc(), Kind::Trades)]);
        assert_eq!(changes.opened, ["btcusdt@trade"]);
        assert!(subs.wants(btc(), Kind::Trades));

        let again = subs.apply(&[Command::Subscribe(btc(), Kind::Trades)]);
        assert_eq!(again, Changes::default());
    }

    #[test]
    fn mark_index_and_funding_share_one_stream() {
        let mut subs = Subscriptions::new(MAX_STREAMS);
        let changes = subs.apply(&[
            Command::Subscribe(btc(), Kind::MarkPrice),
            Command::Subscribe(btc(), Kind::IndexPrice),
            Command::Subscribe(btc(), Kind::FundingRate),
        ]);
        assert_eq!(changes.opened, ["btcusdt@markPrice@1s"]);

        let partial = subs.apply(&[Command::Unsubscribe(btc(), Kind::MarkPrice)]);
        assert_eq!(partial, Changes::default(), "two users remain");
        assert!(!subs.wants(btc(), Kind::MarkPrice));

        let rest = subs.apply(&[
            Command::Unsubscribe(btc(), Kind::IndexPrice),
            Command::Unsubscribe(btc(), Kind::FundingRate),
        ]);
        assert_eq!(rest.closed, ["btcusdt@markPrice@1s"]);
        assert!(subs.streams.is_empty());
    }

    #[test]
    fn batch_sends_only_the_net_change() {
        let on = Command::Subscribe(btc(), Kind::Quotes);
        let off = Command::Unsubscribe(btc(), Kind::Quotes);

        let mut subs = Subscriptions::new(MAX_STREAMS);
        assert_eq!(subs.apply(&[on, off, on]).opened, ["btcusdt@bookTicker"]);

        assert_eq!(subs.apply(&[off, on, off]).closed, ["btcusdt@bookTicker"]);

        assert_eq!(subs.apply(&[on, off]), Changes::default());
        subs.apply(&[on]);
        assert_eq!(subs.apply(&[off, on]), Changes::default());
        assert!(subs.wants(btc(), Kind::Quotes));
    }

    #[test]
    fn ending_a_book_subscription_is_reported_even_if_resubscribed() {
        let mut subs = Subscriptions::new(MAX_STREAMS);
        subs.apply(&[Command::Subscribe(btc(), Kind::Book)]);
        let changes = subs.apply(&[
            Command::Unsubscribe(btc(), Kind::Book),
            Command::Subscribe(btc(), Kind::Book),
        ]);
        assert_eq!(changes.books_ended, [btc()]);
        assert!(changes.opened.is_empty() && changes.closed.is_empty());
    }

    #[test]
    fn subscriptions_past_the_stream_limit_are_rejected() {
        let mut subs = Subscriptions::new(MAX_STREAMS);
        let commands: Vec<Command> = (0..=MAX_STREAMS)
            .map(|index| Command::Subscribe(instrument_id(&format!("S{index}USDT")), Kind::Trades))
            .collect();
        let changes = subs.apply(&commands);
        assert_eq!(changes.opened.len(), MAX_STREAMS);
        assert_eq!(changes.rejected.len(), 1);
        let (rejected, _) = changes.rejected[0];
        assert!(
            !subs.wants(rejected, Kind::Trades),
            "a rejected subscription is not recorded"
        );

        let first = instrument_id("S0USDT");
        subs.apply(&[Command::Subscribe(first, Kind::MarkPrice)]);
        assert!(
            !subs.wants(first, Kind::MarkPrice),
            "a new stream does not fit"
        );
    }
}
