//! An open order that this run cannot name, read mid-run. It may be the order whose placement
//! answer was lost, or a person's order from the venue's own UI, which this engine never touches.

use crate::msg::exec::{ExecEvent, ExecKind};
use crate::msg::ids::ClientOrderId;
use crate::msg::time::LocalTs;
use crate::venues::exec::{Outgoing, blank};
use crate::{error, warn};

use super::super::codec::{KnownOrder, UnmappedOrder, venue_order_id_digest};
use super::super::correlate::{UnmappedVerdict, classify_unmapped};
use super::Actor;

impl Actor {
    /// An order resting under an id this run never recorded.
    pub(super) fn on_unmapped_order(
        &mut self,
        unmapped: &UnmappedOrder,
        outgoing: &mut Vec<Outgoing>,
    ) {
        if !self.session.core.has_opened_quoting() {
            self.cancel_prior_run_order(&unmapped.venue_order_id);
            return;
        }
        let verdict = classify_unmapped(self.session.core.mirror(), unmapped, |client_id| {
            self.orders.venue_order_id(client_id).is_some()
        });
        let client_id = match verdict {
            UnmappedVerdict::Adopt(client_id) => client_id,
            // A placement on this side is still in flight. The resync re-reads once it answers, and
            // by then our own order carries its venue id, so this one is left alone rather than
            // adopted into our slot.
            UnmappedVerdict::Defer => return,
            UnmappedVerdict::LeaveAlone => {
                self.counts.unmapped_left_alone += 1;
                warn!(
                    "polymarket execution left an unmapped order alone on instrument {}: {:?} {} @ {} (venue id {}) — nothing this run placed matches it",
                    unmapped.instrument.0,
                    unmapped.side,
                    unmapped.qty.mantissa(),
                    unmapped.price.mantissa(),
                    unmapped.venue_order_id
                );
                return;
            }
        };
        self.counts.adopted += 1;
        warn!(
            "polymarket execution adopted venue order {} as {:016x} — its placement answer never arrived",
            unmapped.venue_order_id, client_id.0
        );
        if let Err(error) = self.orders.record(
            &unmapped.venue_order_id,
            KnownOrder {
                client_id,
                instrument: unmapped.instrument,
            },
        ) {
            error!(
                "polymarket execution could not record the adopted order {}: {error}",
                unmapped.venue_order_id
            );
            return;
        }
        let (received_ts_us, event) = self.adopted_event(unmapped, client_id);
        if let Err(error) = self.session.fold(&event, 0, outgoing) {
            self.control.fail(error);
        }
        self.forward_exec(received_ts_us, event);
        self.drain_pending(outgoing);
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "an edge actor stamps received and queued time on each message and paces its own I/O"
    )]
    fn adopted_event(
        &self,
        unmapped: &UnmappedOrder,
        client_id: ClientOrderId,
    ) -> (LocalTs, ExecEvent) {
        let now = self.control.clock.now();
        let event = ExecEvent {
            venue_order_id: Some(venue_order_id_digest(&unmapped.venue_order_id)),
            status: Some(unmapped.status),
            side: unmapped.side,
            price: unmapped.price,
            qty: unmapped.qty,
            cumulative_qty: unmapped.filled,
            cumulative_quote: unmapped.price.notional(unmapped.filled),
            amends_remaining: ExecEvent::AMENDS_EXHAUSTED,
            ..blank(
                ExecKind::SnapshotOrder,
                Some(unmapped.instrument),
                client_id,
                now,
            )
        };
        (now, event)
    }
}
