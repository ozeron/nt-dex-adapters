//! Detects what changed between two loads of the venue's instruments.

use ahash::{AHashMap, AHashSet};
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};

/// The difference between the cached instruments and a fresh load.
#[derive(Debug, Default)]
pub struct InstrumentChanges {
    /// Instruments that were not known before.
    pub added: Vec<InstrumentAny>,
    /// Known instruments whose definition differs.
    pub changed: Vec<InstrumentAny>,
    /// Instruments no longer listed as trading perpetuals.
    pub removed: Vec<InstrumentId>,
}

impl InstrumentChanges {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.changed.is_empty() && self.removed.is_empty()
    }
}

/// The parts of a definition that matter to a consumer, as one comparable
/// value. Load timestamps are left out: they differ on every load.
fn definition(instrument: &InstrumentAny) -> String {
    format!(
        "{:?}|{:?}|{}|{}|{}|{}|{}|{:?}|{:?}|{:?}|{:?}|{:?}|{}|{}",
        instrument.base_currency(),
        instrument.quote_currency(),
        instrument.settlement_currency(),
        instrument.price_precision(),
        instrument.size_precision(),
        instrument.price_increment(),
        instrument.size_increment(),
        instrument.max_quantity(),
        instrument.min_quantity(),
        instrument.min_notional(),
        instrument.max_price(),
        instrument.min_price(),
        instrument.margin_init(),
        instrument.margin_maint(),
    )
}

/// Compares the cached instruments with a fresh load.
#[must_use]
pub fn diff_instruments(
    cached: &AHashMap<InstrumentId, InstrumentAny>,
    loaded: &[InstrumentAny],
) -> InstrumentChanges {
    let mut changes = InstrumentChanges::default();
    let mut seen = AHashSet::with_capacity(loaded.len());
    for instrument in loaded {
        seen.insert(instrument.id());
        match cached.get(&instrument.id()) {
            None => changes.added.push(instrument.clone()),
            Some(known) if definition(known) != definition(instrument) => {
                changes.changed.push(instrument.clone());
            }
            Some(_) => {}
        }
    }
    changes.removed = cached
        .keys()
        .filter(|id| !seen.contains(*id))
        .copied()
        .collect();
    changes.removed.sort_unstable();
    changes
}
