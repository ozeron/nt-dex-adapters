use std::collections::BTreeMap;

use anyhow::{Context, Result, bail, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;

pub(super) const DEPTH: usize = 20;
pub(super) const PARTIAL_SUFFIX: &str = "@depth20@100ms";
const HISTORY: usize = 1024;

type Levels = BTreeMap<Decimal, Decimal>;

#[derive(Debug)]
pub(super) struct Top {
    pub(super) bids: Levels,
    pub(super) asks: Levels,
}

#[derive(Debug, Deserialize)]
pub(super) struct Frame<'a> {
    pub(super) stream: &'a str,
    #[serde(borrow)]
    pub(super) data: Partial<'a>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Partial<'a> {
    #[serde(rename = "s")]
    pub(super) symbol: &'a str,
    pub(super) u: u64,
    #[serde(borrow, rename = "b")]
    bids: Vec<[&'a str; 2]>,
    #[serde(borrow, rename = "a")]
    asks: Vec<[&'a str; 2]>,
}

impl Partial<'_> {
    pub(super) fn top(&self) -> Result<Top> {
        Ok(Top {
            bids: levels(&self.bids).context("invalid oracle bids")?,
            asks: levels(&self.asks).context("invalid oracle asks")?,
        })
    }
}

fn levels(rows: &[[&str; 2]]) -> Result<Levels> {
    ensure!(rows.len() <= DEPTH, "oracle reported {} levels", rows.len());
    let mut levels = BTreeMap::new();
    for [price, size] in rows {
        let (price, size) = (price.parse::<Decimal>()?, size.parse::<Decimal>()?);
        ensure!(
            price > Decimal::ZERO && size > Decimal::ZERO,
            "invalid level"
        );
        ensure!(
            levels.insert(price, size).is_none(),
            "duplicate price {price}"
        );
    }
    Ok(levels)
}

/// Keeps both arrival orders. Only two views with exactly the same update ID can pair.
#[derive(Debug, Default)]
pub(super) struct Pairing {
    local_pending: BTreeMap<u64, (Top, Option<usize>)>,
    venue_pending: BTreeMap<u64, Top>,
    pub(super) local_updates: usize,
    pub(super) venue_updates: usize,
    pub(super) matches: usize,
    pub(super) mismatches: usize,
    pub(super) latest_matched_episode: Option<usize>,
}

impl Pairing {
    pub(super) fn local(&mut self, u: u64, top: Top, episode: Option<usize>) -> Result<()> {
        self.local_updates = self.local_updates.saturating_add(1);
        if let Some(venue) = self.venue_pending.remove(&u) {
            self.compare(u, &top, &venue, episode)?;
        } else {
            retain(&mut self.local_pending, u, (top, episode));
        }
        Ok(())
    }

    pub(super) fn venue(&mut self, u: u64, top: Top) -> Result<()> {
        self.venue_updates = self.venue_updates.saturating_add(1);
        if let Some((local, episode)) = self.local_pending.remove(&u) {
            self.compare(u, &local, &top, episode)?;
        } else {
            retain(&mut self.venue_pending, u, top);
        }
        Ok(())
    }

    pub(super) fn unmatched(&self) -> (usize, usize) {
        let compared = self.matches.saturating_add(self.mismatches);
        (
            self.local_updates.saturating_sub(compared),
            self.venue_updates.saturating_sub(compared),
        )
    }

    fn compare(&mut self, u: u64, local: &Top, venue: &Top, episode: Option<usize>) -> Result<()> {
        for (side, actual, expected) in [
            ("bid", &local.bids, &venue.bids),
            ("ask", &local.asks, &venue.asks),
        ] {
            for (price, size) in actual {
                let wanted = expected.get(price);
                if wanted != Some(size) {
                    self.mismatches = self.mismatches.saturating_add(1);
                    bail!(
                        "u={u} {side} {price} local={:?} venue={wanted:?}",
                        Some(size)
                    );
                }
            }
            if let Some((price, size)) = expected
                .iter()
                .find(|(price, _)| !actual.contains_key(price))
            {
                self.mismatches = self.mismatches.saturating_add(1);
                bail!("u={u} {side} {price} local=None venue={:?}", Some(size));
            }
        }
        self.matches = self.matches.saturating_add(1);
        self.latest_matched_episode = self.latest_matched_episode.max(episode);
        Ok(())
    }
}

fn retain<T>(history: &mut BTreeMap<u64, T>, u: u64, view: T) {
    history.insert(u, view);
    if history.len() > HISTORY {
        history.pop_first();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(ask_size: i64) -> Top {
        Top {
            bids: [(Decimal::from(10), Decimal::ONE)].into(),
            asks: [(Decimal::from(11), Decimal::from(ask_size))].into(),
        }
    }

    #[test]
    fn rejects_planted_ask_size_at_the_same_update_id() -> Result<()> {
        let mut pairing = Pairing::default();
        pairing.local(41, view(2), Some(1))?;
        pairing.venue(42, view(1))?;
        assert_eq!(pairing.matches, 0, "different IDs must never compare");

        let error = pairing
            .local(42, view(2), Some(1))
            .err()
            .context("bad level passed")?;
        assert!(
            error
                .to_string()
                .contains("u=42 ask 11 local=Some(2) venue=Some(1)")
        );
        assert_eq!(pairing.matches, 0);
        assert_eq!(pairing.mismatches, 1);

        pairing.local(43, view(1), Some(1))?;
        pairing.venue(43, view(1))?;
        assert_eq!(pairing.matches, 1);
        assert_eq!(pairing.unmatched(), (1, 0));
        assert_eq!(pairing.latest_matched_episode, Some(1));
        Ok(())
    }
}
