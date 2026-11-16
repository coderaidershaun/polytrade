//! An open order that this run cannot name: the order whose placement answer was lost, an order of
//! an earlier run, or a person's order from the venue's own UI. The engine cancels none of them.

use crate::msg::exec::{ExecEvent, ExecKind};
use crate::msg::ids::ClientOrderId;
use crate::msg::time::LocalTs;
use crate::venues::exec::{Outgoing, blank};
use crate::{error, warn};

use super::super::codec::{KnownOrder, UnmappedOrder, venue_order_id_digest};
use super::super::correlate::{
    TokenRead, UnmappedVerdict, classify_unmapped, unproven_order_event,
};
use super::Actor;

impl Actor {
    /// An order resting under an id this run never recorded, named by a read received at
    /// `received_ts_us`. The verdict goes back to the read, which ends the first read of the token
    /// only when no order deferred.
    pub(super) fn on_unmapped_order(
        &mut self,
        unmapped: &UnmappedOrder,
        read: TokenRead,
        received_ts_us: LocalTs,
        outgoing: &mut Vec<Outgoing>,
    ) -> UnmappedVerdict {
        let verdict = classify_unmapped(
            self.session.core.mirror(),
            unmapped,
            |client_id| self.orders.venue_order_id(client_id).is_some(),
            read,
        );
        match verdict {
            UnmappedVerdict::Adopt(client_id) => self.adopt(unmapped, client_id, outgoing),
            // A placement on this side is still in flight. At a first read its answer asks for one
            // more read of the token, and by then our own order carries its venue id, so this one
            // is not adopted into our slot.
            UnmappedVerdict::Defer => {}
            UnmappedVerdict::LeaveAlone => self.leave_alone(unmapped),
            UnmappedVerdict::Unproven => {
                self.on_unproven_order(unmapped, received_ts_us, outgoing);
            }
        }
        verdict
    }

    fn leave_alone(&mut self, unmapped: &UnmappedOrder) {
        self.session.core.count_foreign_order();
        warn!(
            "polymarket execution left an unmapped order alone on instrument {}: {:?} {} @ {} (venue id {}) — no single lost placement of this run equals it",
            unmapped.instrument.0,
            unmapped.side,
            unmapped.qty.mantissa(),
            unmapped.price.mantissa(),
            unmapped.venue_order_id
        );
    }

    fn adopt(
        &mut self,
        unmapped: &UnmappedOrder,
        client_id: ClientOrderId,
        outgoing: &mut Vec<Outgoing>,
    ) {
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

    /// The session counts the order with those this engine cannot claim, and the fold halts quoting.
    fn on_unproven_order(
        &mut self,
        unmapped: &UnmappedOrder,
        received_ts_us: LocalTs,
        outgoing: &mut Vec<Outgoing>,
    ) {
        error!(
            "polymarket execution found an order it cannot prove its own on instrument {}: {:?} {} @ {} (venue id {}) — it is not cancelled, quoting halts, and a person decides",
            unmapped.instrument.0,
            unmapped.side,
            unmapped.qty.mantissa(),
            unmapped.price.mantissa(),
            unmapped.venue_order_id
        );
        let event = unproven_order_event(unmapped, received_ts_us);
        if let Err(error) = self.session.fold(&event, 0, outgoing) {
            self.control.fail(error);
        }
        self.forward_exec(received_ts_us, event);
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
