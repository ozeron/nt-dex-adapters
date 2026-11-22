//! Detects what changed between two loads of the venue's instruments.

use ahash::{AHashMap, AHashSet};
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

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

/// The definition fields that affect consumers. Load timestamps differ on every load.
#[derive(PartialEq)]
struct InstrumentDefinition {
    base_currency: Option<Currency>,
    quote_currency: Currency,
    settlement_currency: Currency,
    price_precision: u8,
    size_precision: u8,
    price_increment: Price,
    size_increment: Quantity,
    max_quantity: Option<Quantity>,
    min_quantity: Option<Quantity>,
    min_notional: Option<Money>,
    max_price: Option<Price>,
    min_price: Option<Price>,
    margin_init: Decimal,
    margin_maint: Decimal,
}

impl From<&InstrumentAny> for InstrumentDefinition {
    fn from(instrument: &InstrumentAny) -> Self {
        Self {
            base_currency: instrument.base_currency(),
            quote_currency: instrument.quote_currency(),
            settlement_currency: instrument.settlement_currency(),
            price_precision: instrument.price_precision(),
            size_precision: instrument.size_precision(),
            price_increment: instrument.price_increment(),
            size_increment: instrument.size_increment(),
            max_quantity: instrument.max_quantity(),
            min_quantity: instrument.min_quantity(),
            min_notional: instrument.min_notional(),
            max_price: instrument.max_price(),
            min_price: instrument.min_price(),
            margin_init: instrument.margin_init(),
            margin_maint: instrument.margin_maint(),
        }
    }
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
            Some(known)
                if InstrumentDefinition::from(known) != InstrumentDefinition::from(instrument) =>
            {
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
