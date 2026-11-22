//! Depth sequence tracking for one instrument.
//!
//! Aster's update ids are venue-global: a symbol's consecutive diffs are
//! not numerically adjacent. Continuity is established only by each diff's
//! `pu` equalling the previous diff's `u`.

use nautilus_model::data::OrderBookDeltas;

/// A parsed depth diff with the ids needed to sequence it.
#[derive(Debug)]
pub struct DepthDiff {
    /// `u`: the last update id covered by this diff.
    pub final_update_id: u64,
    /// `pu`: the `u` of the previous diff for this symbol.
    pub prev_final_update_id: u64,
    /// `None` when the diff changes no level.
    pub deltas: Option<OrderBookDeltas>,
}

/// What the caller must do after handing a diff or snapshot to [`BookSync`].
#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    /// Nothing to emit.
    None,
    /// A depth snapshot must be fetched and passed to [`BookSync::on_snapshot`].
    RequestSnapshot,
}

/// Diffs buffered while waiting for a snapshot before the wait is given up
/// and started again. Snapshots are paced, so with every book of the venue
/// subscribed at once the last one waits about seven minutes, which is some
/// 4,000 diffs at ten a second.
const MAX_BUFFERED: usize = 20_000;

#[derive(Debug)]
enum State {
    /// Waiting for a snapshot; diffs are held until it arrives.
    Buffering {
        diffs: Vec<DepthDiff>,
        requested: bool,
    },
    /// Applying diffs. `linked` is false until the first diff after the
    /// snapshot has been accepted.
    Live { last_update_id: u64, linked: bool },
}

/// Orders depth diffs around REST snapshots and detects gaps.
#[derive(Debug)]
pub struct BookSync {
    state: State,
}

impl Default for BookSync {
    fn default() -> Self {
        Self::new()
    }
}

enum Verdict {
    Stale,
    Apply,
    Gap,
}

impl BookSync {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: State::Buffering {
                diffs: Vec::new(),
                requested: false,
            },
        }
    }

    /// Returns `true` while diffs are being held for a requested snapshot.
    #[must_use]
    pub fn is_awaiting_snapshot(&self) -> bool {
        matches!(
            self.state,
            State::Buffering {
                requested: true,
                ..
            }
        )
    }

    /// Returns `true` once a snapshot has been applied and no gap is pending.
    #[must_use]
    pub fn is_live(&self) -> bool {
        matches!(self.state, State::Live { .. })
    }

    /// Drops all progress, as after a reconnect. The next diff requests a
    /// new snapshot.
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    fn judge(last_update_id: u64, linked: bool, diff: &DepthDiff) -> Verdict {
        if linked {
            if diff.prev_final_update_id == last_update_id {
                Verdict::Apply
            } else {
                Verdict::Gap
            }
        } else if diff.final_update_id <= last_update_id {
            // Everything in it is already in the snapshot.
            Verdict::Stale
        } else if diff.prev_final_update_id <= last_update_id {
            // The snapshot lies inside this diff, or between it and the
            // previous one. Quantities are absolute, so applying it whole
            // is correct either way.
            Verdict::Apply
        } else {
            Verdict::Gap
        }
    }

    /// Starts over, keeping `diffs` to replay after the next snapshot.
    fn restart(&mut self, diffs: Vec<DepthDiff>) -> Action {
        self.state = State::Buffering {
            diffs,
            requested: true,
        };
        Action::RequestSnapshot
    }

    /// Handles a diff from the stream, pushing deltas to emit onto `out`.
    pub fn on_diff(&mut self, diff: DepthDiff, out: &mut Vec<OrderBookDeltas>) -> Action {
        match &mut self.state {
            State::Buffering { diffs, requested } => {
                if diffs.len() >= MAX_BUFFERED {
                    // The snapshot never arrived; start over from this diff.
                    return self.restart(vec![diff]);
                }
                diffs.push(diff);
                if *requested {
                    Action::None
                } else {
                    *requested = true;
                    Action::RequestSnapshot
                }
            }
            State::Live {
                last_update_id,
                linked,
            } => match Self::judge(*last_update_id, *linked, &diff) {
                Verdict::Stale => Action::None,
                Verdict::Apply => {
                    *last_update_id = diff.final_update_id;
                    *linked = true;
                    out.extend(diff.deltas);
                    Action::None
                }
                // Keep the diff that exposed the gap: it follows the new snapshot
                // unless that snapshot already includes it.
                Verdict::Gap => self.restart(vec![diff]),
            },
        }
    }

    /// Handles a REST snapshot, pushing the snapshot and any buffered diffs
    /// that follow it onto `out`.
    ///
    /// A snapshot older than the buffered diffs is discarded and a new one
    /// requested. A snapshot that was not asked for is ignored: one arriving
    /// while live, or after a [`reset`](Self::reset) and before the next diff.
    pub fn on_snapshot(
        &mut self,
        last_update_id: u64,
        snapshot: OrderBookDeltas,
        out: &mut Vec<OrderBookDeltas>,
    ) -> Action {
        let State::Buffering {
            diffs,
            requested: true,
        } = &mut self.state
        else {
            return Action::None;
        };
        let buffered = std::mem::take(diffs);
        let mut pending = buffered
            .into_iter()
            .skip_while(|diff| diff.final_update_id <= last_update_id)
            .peekable();
        if let Some(first) = pending.peek()
            && first.prev_final_update_id > last_update_id
        {
            // The snapshot predates the buffer, which already lost the diffs
            // needed to bridge it.
            *diffs = pending.collect();
            return Action::RequestSnapshot;
        }

        out.push(snapshot);
        self.state = State::Live {
            last_update_id,
            linked: false,
        };
        while let Some(diff) = pending.next() {
            if self.on_diff(diff, out) == Action::RequestSnapshot {
                // A gap inside the buffer. Keep what follows it too, so one
                // more snapshot is enough to catch up.
                if let State::Buffering { diffs, .. } = &mut self.state {
                    diffs.extend(pending);
                }
                return Action::RequestSnapshot;
            }
        }
        Action::None
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::{BookOrder, OrderBookDelta},
        enums::{BookAction, OrderSide},
        identifiers::InstrumentId,
        types::{Price, Quantity},
    };

    use super::*;

    fn deltas(sequence: u64) -> OrderBookDeltas {
        let id = InstrumentId::from("BTCUSDT-PERP.ASTER");
        let order = BookOrder::new(OrderSide::Buy, Price::from("1.0"), Quantity::from("1"), 0);
        let ts = UnixNanos::default();
        let delta = OrderBookDelta::new(id, BookAction::Update, order, 0, sequence, ts, ts);
        OrderBookDeltas::new(id, vec![delta])
    }

    fn diff(prev_final_update_id: u64, final_update_id: u64) -> DepthDiff {
        DepthDiff {
            final_update_id,
            prev_final_update_id,
            deltas: Some(deltas(final_update_id)),
        }
    }

    fn sequences(out: &[OrderBookDeltas]) -> Vec<u64> {
        out.iter().map(|deltas| deltas.sequence).collect()
    }

    #[test]
    fn first_diff_requests_one_snapshot() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        assert_eq!(book.on_diff(diff(1, 5), &mut out), Action::RequestSnapshot);
        assert_eq!(book.on_diff(diff(5, 9), &mut out), Action::None);
        assert_eq!(sequences(&out), Vec::<u64>::new());
        assert!(!book.is_live());
    }

    #[test]
    fn snapshot_drops_stale_diffs_and_replays_the_rest() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        for (prev, last) in [(1, 5), (5, 9), (9, 12)] {
            book.on_diff(diff(prev, last), &mut out);
        }
        // The snapshot lies inside the second diff.
        assert_eq!(book.on_snapshot(7, deltas(7), &mut out), Action::None);
        assert_eq!(sequences(&out), [7, 9, 12]);
        assert!(book.is_live());
    }

    #[test]
    fn snapshot_between_two_diffs_bridges_on_pu() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        // Update ids are venue-global, so 6 belongs to another symbol.
        book.on_diff(diff(1, 5), &mut out);
        book.on_diff(diff(5, 9), &mut out);
        assert_eq!(book.on_snapshot(6, deltas(6), &mut out), Action::None);
        assert_eq!(sequences(&out), [6, 9]);
    }

    #[test]
    fn snapshot_older_than_the_buffer_is_refused() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        book.on_diff(diff(10, 15), &mut out);
        assert_eq!(
            book.on_snapshot(7, deltas(7), &mut out),
            Action::RequestSnapshot
        );
        assert_eq!(sequences(&out), Vec::<u64>::new());
        assert!(!book.is_live());

        // The buffered diff survives for the next snapshot.
        assert_eq!(book.on_snapshot(12, deltas(12), &mut out), Action::None);
        assert_eq!(sequences(&out), [12, 15]);
    }

    #[test]
    fn diffs_after_snapshot_link_then_chain() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        book.on_diff(diff(1, 5), &mut out);
        book.on_snapshot(20, deltas(20), &mut out);
        out.clear();

        assert_eq!(book.on_diff(diff(15, 18), &mut out), Action::None, "stale");
        assert_eq!(
            book.on_diff(diff(18, 24), &mut out),
            Action::None,
            "bridges"
        );
        assert_eq!(book.on_diff(diff(24, 30), &mut out), Action::None, "chains");
        assert_eq!(sequences(&out), [24, 30]);
    }

    #[test]
    fn gap_requests_snapshot_and_keeps_the_diff() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        book.on_diff(diff(1, 5), &mut out);
        book.on_snapshot(5, deltas(5), &mut out);
        book.on_diff(diff(5, 9), &mut out);
        out.clear();

        assert_eq!(
            book.on_diff(diff(11, 14), &mut out),
            Action::RequestSnapshot
        );
        assert!(!book.is_live());
        assert_eq!(
            book.on_diff(diff(14, 17), &mut out),
            Action::None,
            "buffered"
        );
        assert_eq!(sequences(&out), Vec::<u64>::new());

        assert_eq!(book.on_snapshot(12, deltas(12), &mut out), Action::None);
        assert_eq!(sequences(&out), [12, 14, 17]);
    }

    #[test]
    fn snapshot_while_live_is_ignored() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        book.on_diff(diff(1, 5), &mut out);
        book.on_snapshot(5, deltas(5), &mut out);
        out.clear();
        assert_eq!(book.on_snapshot(50, deltas(50), &mut out), Action::None);
        assert_eq!(sequences(&out), Vec::<u64>::new());
    }

    #[test]
    fn diff_ending_at_the_snapshot_is_not_emitted_again() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        book.on_diff(diff(1, 5), &mut out);
        book.on_diff(diff(5, 9), &mut out);
        assert_eq!(book.on_snapshot(5, deltas(5), &mut out), Action::None);
        assert_eq!(sequences(&out), [5, 9]);
    }

    #[test]
    fn gap_inside_the_buffer_keeps_the_diffs_after_it() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        // 105 follows the snapshot, then 110 -> 115 is missing.
        for (prev, last) in [(95, 105), (110, 115), (115, 120)] {
            book.on_diff(diff(prev, last), &mut out);
        }
        assert_eq!(
            book.on_snapshot(100, deltas(100), &mut out),
            Action::RequestSnapshot
        );
        assert_eq!(sequences(&out), [100, 105]);
        out.clear();

        assert_eq!(book.on_snapshot(112, deltas(112), &mut out), Action::None);
        assert_eq!(sequences(&out), [112, 115, 120]);
        assert!(book.is_live());
    }

    #[test]
    fn unsolicited_snapshot_after_reset_is_ignored() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        book.on_diff(diff(1, 5), &mut out);
        book.reset();
        // The answer to the request made before the reset.
        assert_eq!(book.on_snapshot(5, deltas(5), &mut out), Action::None);
        assert_eq!(sequences(&out), Vec::<u64>::new());
        assert!(!book.is_live());
    }

    #[test]
    fn reset_starts_over() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        book.on_diff(diff(1, 5), &mut out);
        book.on_snapshot(5, deltas(5), &mut out);
        book.reset();
        assert!(!book.is_live());
        assert_eq!(book.on_diff(diff(5, 9), &mut out), Action::RequestSnapshot);
    }
}
