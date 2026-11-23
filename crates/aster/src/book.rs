//! Depth sequence tracking for one instrument.
//!
//! Aster's update ids are venue-global: continuity is established by each
//! diff's pu equalling its predecessor's u.

use std::collections::VecDeque;

use nautilus_model::data::OrderBookDeltas;

pub(crate) mod recovery;

#[cfg(test)]
mod model;

#[derive(Debug)]
pub struct DepthDiff<T = OrderBookDeltas> {
    pub final_update_id: u64,
    pub prev_final_update_id: u64,
    pub deltas: Option<T>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    None,
    RequestSnapshot,
}

const MAX_BUFFERED: usize = 20_000;

#[derive(Debug)]
struct Pending<T> {
    diffs: VecDeque<(DepthDiff<T>, usize)>,
    bytes: usize,
    evicted_through: Option<u64>,
}

impl<T> Pending<T> {
    const SLOT_BYTES: usize = size_of::<(DepthDiff<T>, usize)>();

    const fn new() -> Self {
        Self {
            diffs: VecDeque::new(),
            bytes: 0,
            evicted_through: None,
        }
    }

    fn pop_front(&mut self) -> Option<(DepthDiff<T>, usize)> {
        let (diff, bytes) = self.diffs.pop_front()?;
        self.bytes = self.bytes.saturating_sub(bytes);
        Some((diff, bytes))
    }

    fn evict(&mut self, final_update_id: u64) {
        self.evicted_through = Some(
            self.evicted_through
                .map_or(final_update_id, |previous| previous.max(final_update_id)),
        );
    }

    fn retained_bytes(&self) -> usize {
        self.bytes
            .saturating_add(self.diffs.capacity().saturating_mul(Self::SLOT_BYTES))
    }

    fn trim_storage(&mut self) {
        if self.diffs.len() <= self.diffs.capacity() / 4 {
            self.diffs.shrink_to_fit();
        }
    }

    fn push(&mut self, diff: DepthDiff<T>, bytes: usize, budget: usize) {
        let can_fit = bytes.saturating_add(Self::SLOT_BYTES) <= budget;
        while self.diffs.len() >= MAX_BUFFERED
            || !can_fit
            || self.bytes.saturating_add(bytes).saturating_add(
                self.diffs
                    .len()
                    .saturating_add(1)
                    .saturating_mul(Self::SLOT_BYTES),
            ) > budget
        {
            let Some((old, _)) = self.pop_front() else {
                break;
            };
            self.evict(old.final_update_id);
        }
        if !can_fit {
            self.diffs = VecDeque::new();
            self.evict(diff.final_update_id);
            return;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.diffs.push_back((diff, bytes));
        if self.retained_bytes() > budget {
            self.diffs.shrink_to_fit();
        }
    }
}

#[derive(Debug)]
enum State<T> {
    Idle,
    Buffering(Pending<T>),
    Live { last_update_id: u64, linked: bool },
}

#[derive(Debug)]
pub struct BookSync<T = OrderBookDeltas> {
    state: State<T>,
}

impl Default for BookSync {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(PartialEq, Eq)]
enum Verdict {
    Stale,
    Apply,
    Gap,
}

impl BookSync {
    #[must_use]
    pub const fn new() -> Self {
        Self::empty()
    }
}

impl<T> BookSync<T> {
    pub(crate) const fn empty() -> Self {
        Self { state: State::Idle }
    }

    #[must_use]
    pub fn is_awaiting_snapshot(&self) -> bool {
        matches!(self.state, State::Buffering(_))
    }

    #[must_use]
    pub fn is_live(&self) -> bool {
        matches!(self.state, State::Live { .. })
    }

    pub fn reset(&mut self) {
        *self = Self::empty();
    }

    pub(crate) fn buffered_bytes(&self) -> usize {
        match &self.state {
            State::Buffering(pending) => pending.retained_bytes(),
            _ => 0,
        }
    }

    pub(crate) fn buffers(&self, diff: &DepthDiff<T>) -> bool {
        match self.state {
            State::Live {
                last_update_id,
                linked,
            } => Self::judge(last_update_id, linked, diff) == Verdict::Gap,
            _ => true,
        }
    }

    pub(crate) fn payload_fits(bytes: usize, budget: usize) -> bool {
        bytes.saturating_add(Pending::<T>::SLOT_BYTES) <= budget
    }

    fn judge(last_update_id: u64, linked: bool, diff: &DepthDiff<T>) -> Verdict {
        if linked {
            if diff.prev_final_update_id == last_update_id {
                Verdict::Apply
            } else {
                Verdict::Gap
            }
        } else if diff.final_update_id <= last_update_id {
            Verdict::Stale
        } else if diff.prev_final_update_id <= last_update_id {
            Verdict::Apply
        } else {
            Verdict::Gap
        }
    }

    pub fn on_diff(&mut self, diff: DepthDiff<T>, out: &mut Vec<T>) -> Action {
        self.on_diff_with_budget(diff, 0, usize::MAX, out)
    }

    pub(crate) fn on_diff_with_budget(
        &mut self,
        diff: DepthDiff<T>,
        bytes: usize,
        budget: usize,
        out: &mut Vec<T>,
    ) -> Action {
        match &mut self.state {
            State::Buffering(pending) => {
                pending.push(diff, bytes, budget);
                return Action::None;
            }
            State::Live {
                last_update_id,
                linked,
            } => match Self::judge(*last_update_id, *linked, &diff) {
                Verdict::Stale => return Action::None,
                Verdict::Apply => {
                    *last_update_id = diff.final_update_id;
                    *linked = true;
                    out.extend(diff.deltas);
                    return Action::None;
                }
                Verdict::Gap => {}
            },
            State::Idle => {}
        }

        let mut pending = Pending::new();
        pending.push(diff, bytes, budget);
        self.state = State::Buffering(pending);
        Action::RequestSnapshot
    }

    pub fn on_snapshot(&mut self, last_update_id: u64, snapshot: T, out: &mut Vec<T>) -> Action {
        let State::Buffering(pending) = &mut self.state else {
            return Action::None;
        };
        while pending
            .diffs
            .front()
            .is_some_and(|(diff, _)| diff.final_update_id <= last_update_id)
        {
            pending.pop_front();
        }
        if pending
            .evicted_through
            .is_some_and(|id| id > last_update_id)
            || pending
                .diffs
                .front()
                .is_some_and(|(diff, _)| diff.prev_final_update_id > last_update_id)
        {
            pending.trim_storage();
            return Action::RequestSnapshot;
        }
        pending.evicted_through = None;

        out.push(snapshot);
        let mut sequence = last_update_id;
        let mut linked = false;
        while let Some((diff, bytes)) = pending.pop_front() {
            match Self::judge(sequence, linked, &diff) {
                Verdict::Apply => {
                    sequence = diff.final_update_id;
                    linked = true;
                    out.extend(diff.deltas);
                }
                Verdict::Gap => {
                    pending.bytes = pending.bytes.saturating_add(bytes);
                    pending.diffs.push_front((diff, bytes));
                    pending.trim_storage();
                    return Action::RequestSnapshot;
                }
                Verdict::Stale => {}
            }
        }
        self.state = State::Live {
            last_update_id: sequence,
            linked,
        };
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
    fn byte_budget_keeps_the_newest_suffix_without_reissuing_the_snapshot() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        let budget = (Pending::<OrderBookDeltas>::SLOT_BYTES + 10) * 2;
        for (previous, last, expected) in [
            (1, 10, Action::RequestSnapshot),
            (10, 20, Action::None),
            (20, 30, Action::None),
        ] {
            assert_eq!(
                book.on_diff_with_budget(diff(previous, last), 10, budget, &mut out),
                expected
            );
            assert!(book.buffered_bytes() <= budget);
        }
        assert_eq!(
            book.on_snapshot(9, deltas(9), &mut out),
            Action::RequestSnapshot
        );
        assert_eq!(sequences(&out), Vec::<u64>::new());
        assert_eq!(book.on_snapshot(10, deltas(10), &mut out), Action::None);
        assert_eq!(sequences(&out), [10, 20, 30]);
        assert_eq!(book.buffered_bytes(), 0);
    }

    #[test]
    fn a_snapshot_must_cover_an_oversized_diff_even_with_an_empty_buffer() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        assert_eq!(
            book.on_diff_with_budget(diff(1, 10), 100, 1, &mut out),
            Action::RequestSnapshot
        );
        assert_eq!(book.buffered_bytes(), 0);
        assert_eq!(
            book.on_snapshot(9, deltas(9), &mut out),
            Action::RequestSnapshot
        );
        assert_eq!(sequences(&out), Vec::<u64>::new());
        assert!(!book.is_live());
        assert_eq!(book.on_snapshot(10, deltas(10), &mut out), Action::None);
        assert_eq!(sequences(&out), [10]);
    }

    #[test]
    fn snapshot_drops_stale_diffs_and_replays_the_rest() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
        for (prev, last) in [(1, 5), (5, 9), (9, 12)] {
            book.on_diff(diff(prev, last), &mut out);
        }
        assert_eq!(book.on_snapshot(7, deltas(7), &mut out), Action::None);
        assert_eq!(sequences(&out), [7, 9, 12]);
        assert!(book.is_live());
    }

    #[test]
    fn snapshot_between_two_diffs_bridges_on_pu() {
        let mut book = BookSync::new();
        let mut out = Vec::new();
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
