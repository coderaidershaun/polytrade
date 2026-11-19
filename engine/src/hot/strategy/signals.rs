//! What `signals!` builds on: the list of the names and kinds of the signals of a strategy, and
//! the record of each signal that the strategy assigned.

use crate::msg::ids::InstrumentId;
use crate::msg::kinds::FeatureKind;
use crate::msg::persist::FeatureId;

use super::StrategyCtx;
use super::units::{Bps, Fair, Notional, Price, Qty, Ticks};
use super::view::Market;

/// The names and the kinds of the signals of a strategy, in the order of their feature ids.
#[derive(Debug, Clone, Copy)]
pub struct SignalList {
    pub names: &'static [&'static str],
    pub kinds: &'static [FeatureKind],
}

impl SignalList {
    /// The feature id of `name`. Ids are dense and follow the order of the list.
    ///
    /// # Panics
    /// At compile time, when `name` is not in the list.
    pub const fn id_of(&self, name: &str) -> FeatureId {
        let mut index = 0;
        while index < self.names.len() {
            if is_same(self.names[index], name) {
                assert!(index <= u16::MAX as usize, "a feature id fits u16");
                return FeatureId(index as u16);
            }
            index += 1;
        }
        panic!("the signal is not in the list of its strategy")
    }

    pub const fn has_unique_names(&self) -> bool {
        let mut first = 0;
        while first < self.names.len() {
            let mut second = first + 1;
            while second < self.names.len() {
                if is_same(self.names[first], self.names[second]) {
                    return false;
                }
                second += 1;
            }
            first += 1;
        }
        true
    }
}

const fn is_same(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// A struct of signals that `signals!` declared.
pub trait SignalSet {
    fn record_into(&self, instrument: InstrumentId, ctx: &mut StrategyCtx<'_>);
}

/// The value that a signal of this unit records in its column.
pub trait SignalValue: Copy {
    fn recorded(self) -> f64;
}

impl SignalValue for f64 {
    fn recorded(self) -> f64 {
        self
    }
}

impl SignalValue for Fair {
    fn recorded(self) -> f64 {
        self.value()
    }
}

impl SignalValue for Bps {
    fn recorded(self) -> f64 {
        self.value()
    }
}

impl SignalValue for Ticks {
    fn recorded(self) -> f64 {
        self.count() as f64
    }
}

impl SignalValue for Price {
    fn recorded(self) -> f64 {
        self.to_f64()
    }
}

impl SignalValue for Qty {
    fn recorded(self) -> f64 {
        self.to_f64()
    }
}

impl SignalValue for Notional {
    fn recorded(self) -> f64 {
        self.to_f64()
    }
}

impl StrategyCtx<'_> {
    /// Records each signal of `signals` that holds a value, in the column of its name, for this
    /// market. A signal that is not ready writes no row.
    pub fn record<S: SignalSet>(&mut self, market: &Market, signals: &S) {
        signals.record_into(market.instrument(), self);
    }
}
