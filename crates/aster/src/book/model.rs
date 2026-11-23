use proptest::prelude::*;

use super::{Action, BookSync, DepthDiff, Pending};

#[derive(Clone, Copy, Debug)]
enum Input {
    Diff {
        previous: u64,
        end: u64,
        empty: bool,
        oversized: bool,
    },
    Snapshot(u64),
    Reset,
}

#[derive(Clone, Copy)]
struct Diff {
    previous: u64,
    end: u64,
    payload: Option<u64>,
}

#[derive(Default)]
struct Reference {
    pending: Option<Vec<Diff>>,
    covered_through: u64,
    live: Option<(u64, bool)>,
}

impl Reference {
    fn update(&mut self, diff: Diff, oversized: bool, emitted: &mut Vec<u64>) -> Action {
        if let Some((last, linked)) = self.live {
            if !linked && diff.end <= last {
                return Action::None;
            }
            if diff.previous == last || (!linked && diff.previous < last) {
                self.live = Some((diff.end, true));
                emitted.extend(diff.payload);
                return Action::None;
            }
        }

        self.live = None;
        let needs_snapshot = self.pending.is_none();
        let pending = self.pending.get_or_insert_with(Vec::new);
        pending.push(diff);
        let discard = if oversized {
            pending.len()
        } else {
            pending.len().saturating_sub(2)
        };
        for obsolete in pending.drain(..discard) {
            self.covered_through = self.covered_through.max(obsolete.end);
        }
        if needs_snapshot {
            Action::RequestSnapshot
        } else {
            Action::None
        }
    }

    fn snapshot(&mut self, sequence: u64, emitted: &mut Vec<u64>) -> Action {
        let Some(pending) = &mut self.pending else {
            return Action::None;
        };
        let stale = pending
            .iter()
            .take_while(|diff| diff.end <= sequence)
            .count();
        pending.drain(..stale);
        if sequence < self.covered_through
            || pending
                .first()
                .is_some_and(|first| first.previous > sequence)
        {
            return Action::RequestSnapshot;
        }

        let mut last = sequence;
        let mut included = 0_usize;
        emitted.push(sequence);
        for diff in pending.iter() {
            if included != 0 && diff.previous != last {
                break;
            }
            emitted.extend(diff.payload);
            last = diff.end;
            included = included.saturating_add(1);
        }
        pending.drain(..included);
        self.covered_through = 0;
        if pending.is_empty() {
            self.pending = None;
            self.live = Some((last, included != 0));
            Action::None
        } else {
            Action::RequestSnapshot
        }
    }
}

fn inputs() -> impl Strategy<Value = Input> {
    prop_oneof![
        6 => (1_u64..20, 1_u64..5, any::<bool>(), 0_u8..10).prop_map(
            |(previous, span, empty, size)| Input::Diff {
                previous,
                end: previous.saturating_add(span),
                empty,
                oversized: size == 0,
            },
        ),
        3 => (1_u64..25).prop_map(Input::Snapshot),
        1 => Just(Input::Reset),
    ]
}

proptest! {
    #[test]
    fn schedule_keeps_aster_sequence_contract(schedule in prop::collection::vec(inputs(), 1..100)) {
        const BUDGET: usize = Pending::<u64>::SLOT_BYTES.saturating_mul(2);
        let mut book = BookSync::<u64>::empty();
        let mut reference = Reference::default();
        for input in schedule {
            let mut actual = Vec::new();
            let mut expected = Vec::new();
            let (action, expected_action) = match input {
                Input::Diff { previous, end, empty, oversized } => {
                    let payload = (!empty).then_some(end);
                    let diff = DepthDiff {
                        final_update_id: end,
                        prev_final_update_id: previous,
                        deltas: payload,
                    };
                    (
                        book.on_diff_with_budget(diff, if oversized { BUDGET } else { 0 }, BUDGET, &mut actual),
                        reference.update(Diff { previous, end, payload }, oversized, &mut expected),
                    )
                }
                Input::Snapshot(sequence) => (
                    book.on_snapshot(sequence, sequence, &mut actual),
                    reference.snapshot(sequence, &mut expected),
                ),
                Input::Reset => {
                    book.reset();
                    reference = Reference::default();
                    (Action::None, Action::None)
                }
            };
            prop_assert_eq!(action, expected_action, "input {:?}", input);
            prop_assert_eq!(actual, expected, "input {:?}", input);
            prop_assert_eq!(book.is_live(), reference.live.is_some());
            prop_assert_eq!(book.is_awaiting_snapshot(), reference.pending.is_some());
            prop_assert!(book.buffered_bytes() <= BUDGET);
        }
    }
}
