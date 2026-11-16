//! The quoting admission gate, and the one place that rebuilds state after a disconnect: the stream
//! of this venue replays nothing, so what rests is read again.

use crate::msg::exec::{ExecCommand, ExecEvent, ExecKind, Provenance};
use crate::msg::ids::{ClientOrderId, InstrumentId};
use crate::msg::time::LocalTs;
use crate::venues::edge::EdgeDriver;
use crate::venues::exec::{ExecSessionError, MirroredOrder, Outgoing, ResyncPoll, SessionOutcome};
use crate::{error, info, warn};

use super::super::codec::{
    OrdersRead, UnattributableOrder, VenueAnswer, WireError, decode_orders_page,
    open_orders_for_token, open_orders_page, trades_page, venue_order_id_digest,
};
use super::super::lanes::{Lane, RestJob};
use super::super::rest::ClobResponse;
use super::rest::{OpenOrdersRead, OrdersWalk, RestPurpose};
use super::{Actor, Writer};

/// Open orders, fills, and the balance sweep. The sweep counts as one read however many assets it
/// costs, because a partial sweep is not a partial answer.
const PASS_READS: usize = 3;

/// A page walk that never ends would hold a resync pass open for the life of the run, so it stops
/// and says so. This engine's own book is a handful of orders; anything near this many pages means
/// the account is being read for something other than what this run placed.
pub(super) const MAX_PAGES: u32 = 8;

impl Actor {
    /// Reads all three admission gates: open orders, fills, and balances. Any outstanding
    /// pass is abandoned here, since its answers would describe a socket that is now closed.
    pub(super) fn start_resync(&mut self) {
        let resync_seq = self.session.begin_pass(PASS_READS);
        self.request_pass_reads(resync_seq);
    }

    /// Requests a resync when none is already outstanding — used after a lost answer, an
    /// unmirrored fill, or an unexplained frame.
    pub(super) fn nudge_resync(&mut self) {
        if self.session.resync.is_outstanding() {
            return;
        }
        self.start_resync();
    }

    fn request_pass_reads(&mut self, resync_seq: u64) {
        let orders = RestPurpose::OpenOrders {
            read: OpenOrdersRead::Pass { resync_seq },
            walk: OrdersWalk::default(),
        };
        let mut is_queued = self
            .submit(
                Lane::Control,
                RestJob::signed(orders, open_orders_page(None)),
            )
            .is_queued();
        let trades = RestPurpose::Trades {
            resync_seq: Some(resync_seq),
            page: 0,
        };
        is_queued &= self
            .submit(Lane::Control, RestJob::signed(trades, trades_page(None)))
            .is_queued();
        is_queued &= self.request_balance_sweep(Some(resync_seq));
        if is_queued {
            return;
        }
        self.fail_resync(Some(resync_seq));
    }

    /// One page of open orders. Whether it settles a resync read is [`OpenOrdersRead`]'s to say, and
    /// the pass only settles on the page that exhausts the venue's cursor — a walk cut short mid-way
    /// would claim a completeness the read never had.
    pub(super) async fn on_open_orders_answer(
        &mut self,
        read: OpenOrdersRead,
        mut walk: OrdersWalk,
        response: &ClobResponse,
        writer: Option<&mut Writer>,
    ) -> Option<SessionOutcome> {
        let recon_seq = hot_recon_seq(read);
        let orders_read = OrdersRead {
            instrument: read.instrument().unwrap_or(InstrumentId(0)),
            recon_seq,
        };
        let context = self.decode_context();
        let received_ts_us = context.received_ts_us;
        let decoded = decode_orders_page(response.answer(), orders_read, &context);
        let decoded = match decoded {
            Ok(VenueAnswer::Answered(decoded)) => decoded,
            Ok(VenueAnswer::Unavailable(availability)) => {
                self.on_unavailable(availability);
                self.fail_orders_read(read);
                return None;
            }
            // An open order that this build cannot read leaves the mirror incomplete, and quoting
            // against an incomplete mirror is what strands somebody's order.
            Err(WireError::UndecodableOpenOrder {
                venue_order_id,
                source,
            }) => {
                error!(
                    "polymarket execution cannot read open order {venue_order_id} ({source}) — the engine halts"
                );
                let digest = venue_order_id_digest(&venue_order_id);
                let error = self.session.undecodable_open_order(digest.0);
                self.fail_orders_read(read);
                self.control.fail(error);
                return None;
            }
            Err(error) => {
                error!("polymarket execution could not read its open orders: {error}");
                self.fail_orders_read(read);
                return None;
            }
        };

        let mut outgoing = Vec::new();
        // The decoder closes each instrument's stream individually, but this read covers
        // every instrument at once, so those per-instrument SnapshotEnd markers are dropped here.
        let events: Vec<ExecEvent> = decoded
            .events
            .into_iter()
            .filter(|event| event.kind != ExecKind::SnapshotEnd)
            .collect();
        for event in &events {
            let Some(mut mirrored) = MirroredOrder::observed(event) else {
                continue;
            };
            if !self.session.core.has_opened_quoting() {
                mirrored.provenance = Provenance::PriorRun;
            }
            walk.seen.push(mirrored.client_id);
            if let Err(error) = self.session.core.observe_venue_order(mirrored) {
                error!(
                    "polymarket execution cannot retain venue order {:016x} in its possibly-live set ({error:?}) — quoting is disabled",
                    mirrored.client_id.0
                );
                self.control.fail(ExecSessionError::MirrorRetainFailed {
                    venue: self.venue(),
                    source: error,
                });
                self.fail_orders_read(read);
                return None;
            }
        }
        for unmapped in &decoded.unmapped {
            let token_read = self.first_reads.read_of(unmapped.instrument);
            let verdict =
                self.on_unmapped_order(unmapped, token_read, received_ts_us, &mut outgoing);
            walk.verdicts.push(verdict);
        }
        self.leave_unattributable_alone(&decoded.unattributable);
        for event in events {
            if let Err(error) = self.session.fold(&event, recon_seq, &mut outgoing) {
                self.control.fail(error);
            }
            self.forward_exec(received_ts_us, event);
        }
        if self.next_orders_page(read, &walk, decoded.next_cursor.as_deref()) == PageWalk::Complete
        {
            self.end_first_read(read, &walk);
            self.probe_missing(&walk.seen, read, &mut outgoing);
            if let OpenOrdersRead::Pass { resync_seq } = read {
                self.settle_pass_read(Some(resync_seq));
            }
        }
        let ended = self.dispatch(outgoing, writer).await;
        self.finish_readiness_if_clean();
        ended
    }

    /// Asks for the page after this one when the venue says there is one.
    ///
    /// A walk that cannot go on ends here rather than hanging. Ending is not the same as lying about
    /// completeness: every order the read never named is then probed one at a time, so the mirror
    /// still converges on the truth — it just costs a status call each instead of a page.
    fn next_orders_page(
        &mut self,
        read: OpenOrdersRead,
        walk: &OrdersWalk,
        cursor: Option<&str>,
    ) -> PageWalk {
        let Some(cursor) = cursor else {
            return PageWalk::Complete;
        };
        if walk.page + 1 >= MAX_PAGES {
            warn!(
                "polymarket execution stopped walking its open orders at {MAX_PAGES} pages — the rest of the account goes unread and its own orders are probed individually"
            );
            return PageWalk::Complete;
        }
        let request = match read.instrument() {
            None => open_orders_page(Some(cursor)),
            Some(instrument) => {
                let Some(binding) = self.tokens.live_binding(instrument) else {
                    warn!(
                        "polymarket execution lost instrument {}'s token binding mid page walk — the remaining pages go unread",
                        instrument.0
                    );
                    return PageWalk::Complete;
                };
                open_orders_for_token(&binding.token_id, Some(cursor))
            }
        };
        let next = RestPurpose::OpenOrders {
            read,
            walk: walk.next_page(),
        };
        if !self
            .submit(Lane::Control, RestJob::signed(next, request))
            .is_queued()
        {
            self.fail_orders_read(read);
        }
        PageWalk::Pending
    }

    /// Only a pass read has a retry to schedule; the other two are asked for again by whatever asks
    /// for them at all.
    pub(super) fn fail_orders_read(&mut self, read: OpenOrdersRead) {
        if let OpenOrdersRead::Pass { resync_seq } = read {
            self.fail_resync(Some(resync_seq));
        }
    }

    /// One of a pass's reads answered. A read belonging to no pass settles nothing, which is the
    /// whole reason membership is a type rather than a sentinel number.
    pub(super) fn settle_pass_read(&mut self, resync_seq: Option<u64>) {
        if let Some(resync_seq) = resync_seq
            && self.session.resync.on_read(resync_seq)
        {
            self.session.finish_pass();
            self.finish_readiness_if_clean();
        }
    }

    /// Only the walk of a fresh-binding read ends the first read of its token, and only while that
    /// token is still bound: a pass, or a read of a token since rebound, may not have named the
    /// orders of the token that is bound now.
    fn end_first_read(&mut self, read: OpenOrdersRead, walk: &OrdersWalk) {
        let (OpenOrdersRead::FreshBinding { instrument }, Some(token_id)) =
            (read, walk.fresh_token.as_deref())
        else {
            return;
        };
        let is_still_bound = self
            .tokens
            .live_binding(instrument)
            .is_some_and(|binding| &*binding.token_id == token_id);
        if is_still_bound {
            self.first_reads
                .on_read_answered(instrument, &walk.verdicts);
        }
    }

    /// An order on a token that no binding names belongs to a market this engine does not trade,
    /// or to a window whose token is not bound yet; its own first read decides then.
    fn leave_unattributable_alone(&mut self, unattributable: &[UnattributableOrder]) {
        let Some(first) = unattributable.first() else {
            return;
        };
        for _ in unattributable {
            self.session.core.count_foreign_order();
        }
        warn!(
            "polymarket execution left {} open order(s) alone on tokens that no binding names, venue id {} among them",
            unattributable.len(),
            first.venue_order_id
        );
    }

    /// Absence from the read could mean fill or lag, so each mirrored order the read never named is
    /// probed. Scoped to what the read actually covered: an instrument-scoped page says nothing
    /// about the sibling leg, and probing it would cost one status call per resting order per
    /// reconcile.
    fn probe_missing(
        &mut self,
        seen_owned: &[ClientOrderId],
        read: OpenOrdersRead,
        outgoing: &mut Vec<Outgoing>,
    ) {
        if matches!(read, OpenOrdersRead::FreshBinding { .. }) {
            return;
        }
        let covered = read.instrument();
        let recon_seq = hot_recon_seq(read);
        let missing: Vec<(InstrumentId, ClientOrderId)> = self
            .session
            .core
            .mirror()
            .iter()
            .filter(|order| {
                covered.is_none_or(|instrument| order.instrument == instrument)
                    && order.provenance != Provenance::Foreign
                    && !seen_owned.contains(&order.client_id)
            })
            .map(|order| (order.instrument, order.client_id))
            .collect();
        for (instrument, client_id) in missing {
            self.session.core.mark_ambiguous(client_id);
            let command = ExecCommand::ReconcileOrder {
                instrument,
                client_id,
                recon_seq,
            };
            self.session
                .core
                .on_command(command, &mut Outgoing::sink(outgoing, recon_seq));
        }
    }

    /// Checks whether the pass has stalled. A stalled pass always ends by retrying or by
    /// dropping the connection, never by hanging.
    pub(super) fn poll_resync(&mut self, now: LocalTs) -> Option<SessionOutcome> {
        if self.control.exit.is_some() {
            return None;
        }
        match self.session.poll_resync(now, PASS_READS) {
            ResyncPoll::Wait => None,
            ResyncPoll::Reread { resync_seq } => {
                self.request_pass_reads(resync_seq);
                None
            }
            ResyncPoll::GiveUp => Some(SessionOutcome::Reconnect),
        }
    }

    /// A read belonging to no pass has no retry to schedule: nothing is waiting on it, and whatever
    /// asks for it asks again on its own cadence.
    pub(super) fn fail_resync(&mut self, resync_seq: Option<u64>) {
        let Some(resync_seq) = resync_seq else {
            return;
        };
        let retry_at = self.control.next_attempt_at(self.session.resync.attempts());
        self.session.resync.on_failure(resync_seq, retry_at);
    }

    /// Quoting opens once the pass has finished.
    #[expect(
        clippy::disallowed_methods,
        reason = "an edge actor stamps received and queued time on each message and paces its own I/O"
    )]
    pub(super) fn finish_readiness_if_clean(&mut self) {
        // After a heartbeat reset the venue may cancel the book at any moment, so quoting waits for
        // a beat that the venue answers.
        if self
            .heartbeat
            .as_ref()
            .is_some_and(|schedule| schedule.is_placing_held())
        {
            return;
        }
        if self.session.arm_readiness(self.control.clock.now()) {
            info!("polymarket execution quoting — the state of the venue was read again");
        }
    }

    /// New bindings may hold orders from before a reconnect. Checked outside the main pass.
    pub(super) fn read_bound_token(&mut self, instrument: InstrumentId, token_id: &str) {
        // A drop here leaves the first read of the token due, and the lane's own warning names it,
        // so the next resync pass reads the same orders account-wide as a first read.
        let purpose = RestPurpose::OpenOrders {
            read: OpenOrdersRead::FreshBinding { instrument },
            walk: OrdersWalk::of_fresh_token(token_id),
        };
        let request = open_orders_for_token(token_id, None);
        let _dropped = self.submit(Lane::Control, RestJob::signed(purpose, request));
    }

    /// A first read that deferred an order waits for this read, after the placement answered.
    pub(super) fn read_token_again(&mut self, instrument: InstrumentId) {
        let Some(token_id) = self
            .tokens
            .live_binding(instrument)
            .map(|binding| binding.token_id.clone())
        else {
            return;
        };
        self.read_bound_token(instrument, &token_id);
    }
}

/// Whether a paged read has answered. Only `Complete` settles anything: a page still on the way and
/// a walk that broke are both reads that have not answered yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PageWalk {
    Complete,
    Pending,
}

/// The hot side's reconcile counter, or zero when the read belongs to no hot pass. A resync pass
/// counter must never reach here: the hot side folds this number into the sequence its own sweep
/// compares against, and a foreign value there retires live orders or masks gone ones.
fn hot_recon_seq(read: OpenOrdersRead) -> u64 {
    match read {
        OpenOrdersRead::Instrument { recon_seq, .. } => recon_seq,
        OpenOrdersRead::Pass { .. } | OpenOrdersRead::FreshBinding { .. } => 0,
    }
}
