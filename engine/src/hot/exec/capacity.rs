//! The size of each execution table, taken once from the instrument rows of a run. No constant caps
//! the instruments or the assets; the one limit is the slot index that a client order id can hold.

#[cfg(not(target_arch = "wasm32"))]
use crate::runtime::StartError;
use crate::runtime::config::MAX_QUOTE_LEVELS;
use crate::runtime::registry::InstrumentRow;

#[cfg(not(target_arch = "wasm32"))]
use super::order::MAX_ORDER_SLOTS;
use super::order::SLOTS_PER_INSTRUMENT;

/// Commands one spin can bank for an instrument beyond the order table itself: an open-orders
/// request, a flatten, one action for each side, the sweep a halt issues, and a prior-run cancel.
const COMMANDS_PER_INSTRUMENT_PER_SPIN: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecCapacity {
    instruments: usize,
    assets: usize,
}

impl ExecCapacity {
    /// # Errors
    /// [`StartError::ExecutionCapacity`] when the order table needs more slots than a client order
    /// id addresses. Two orders under one id would make the engine unable to tell its own orders
    /// apart.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn of(rows: &[InstrumentRow]) -> Result<Self, StartError> {
        let capacity = Self::sized_for(rows);
        let needed = capacity.order_slots();
        if needed > MAX_ORDER_SLOTS {
            return Err(StartError::ExecutionCapacity {
                table: "order slots",
                needed,
                addressable: MAX_ORDER_SLOTS,
            });
        }
        Ok(capacity)
    }

    /// The sizes with no check: the start checks the same rows with [`ExecCapacity::of`], and a
    /// client order id past the addressable slots still panics where it is minted.
    pub(crate) fn sized_for(rows: &[InstrumentRow]) -> Self {
        let assets = rows
            .iter()
            .map(|row| usize::from(row.base_asset.0.max(row.quote_asset.0)) + 1)
            .max()
            .unwrap_or(0);
        Self {
            instruments: rows.len(),
            assets,
        }
    }

    #[inline]
    pub fn instruments(self) -> usize {
        self.instruments
    }

    /// One more than the highest asset id that a row names: the account table is indexed by it.
    #[inline]
    pub fn assets(self) -> usize {
        self.assets
    }

    #[inline]
    pub fn order_slots(self) -> usize {
        self.instruments * SLOTS_PER_INSTRUMENT
    }

    /// One entry for each level of each side of each instrument, for the orders an earlier run
    /// left resting.
    #[inline]
    pub fn prior_run_slots(self) -> usize {
        self.instruments * 2 * MAX_QUOTE_LEVELS
    }

    /// Sized so a single spin can never fill it. Every producer banks at most one command for each
    /// order slot (a timeout chases a slot, a closing window pulls it, and no slot can be both at
    /// once), plus a few commands for each instrument. Anything smaller turns an ordinary busy spin,
    /// such as a window closing on a full ladder, into a halt.
    ///
    /// Anything larger buys nothing. Past one spin the bank holds whatever the lane below refused,
    /// so an edge that stays stalled fills any bank there is, and that is the designed capacity
    /// event: the engine halts rather than drop a command whose state transition it has already
    /// applied.
    #[inline]
    pub fn command_bank(self) -> usize {
        self.order_slots() + COMMANDS_PER_INSTRUMENT_PER_SPIN * self.instruments
    }
}
