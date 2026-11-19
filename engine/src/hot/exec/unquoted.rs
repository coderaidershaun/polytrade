//! Which traded sides hold no quote after the execution pass, and why, so a reader can count the
//! time that the engine did not quote by cause.

use crate::msg::ids::{InstrumentId, Side};

use super::engine::ExecEngine;
use super::reconcile::RejectReason;

impl ExecEngine {
    /// Each side of each instrument of a venue that trades and that holds no working order of
    /// this run, with the reason that the pass recorded for it. A side with no recorded reason
    /// declared no quote.
    pub(crate) fn for_each_unquoted_side(
        &self,
        mut each: impl FnMut(InstrumentId, Side, RejectReason),
    ) {
        // A venue with no command lane sends no order, so it has no quote to miss.
        let traded = self.venues.iter().filter(|venue| venue.sink.is_some());
        for venue in traded {
            for instrument in venue.instrument_ids() {
                for side in [Side::Buy, Side::Sell] {
                    let is_quoting = self
                        .orders
                        .side_in_use(instrument, side)
                        .any(|slot| slot.state().is_working());
                    if is_quoting {
                        continue;
                    }
                    let reason = self.refusals.current(instrument, side);
                    each(
                        instrument,
                        side,
                        reason.unwrap_or(RejectReason::NoQuoteDeclared),
                    );
                }
            }
        }
    }
}
