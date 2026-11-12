//! The quoting admission gate: it reads the open orders for each instrument. A pass that stalls
//! ends by retrying or by reconnecting, never by wedging.

use crate::adapters::binance::rest::OrderRecord;
use crate::adapters::exec::{MirroredOrder, Outgoing, ResyncPoll, SessionOutcome};
use crate::msg::exec::{ExecCommand, ExecKind, Provenance};
use crate::time::TsUs;
use crate::{error, info, warn};

use super::rest::RestJob;
use super::{Actor, Writer};

impl Actor {
    /// Asks every instrument for its open orders. This actor owns this pass itself, separate
    /// from hot-table reconciliation.
    pub(super) fn start_resync(&mut self) {
        let resync_seq = self.session.begin_pass(self.pass_reads());
        self.request_pass_reads(resync_seq);
    }

    /// Counts the reads a full pass needs: open orders for every instrument, plus balances.
    /// Both gate quoting, and a reconnect needs a fresh snapshot of each.
    pub(super) fn pass_reads(&self) -> usize {
        self.session.instruments().len() + 1
    }

    pub(super) async fn on_open_orders(
        &mut self,
        job: RestJob,
        orders: &[OrderRecord],
        writer: Option<&mut Writer>,
    ) {
        let RestJob::OpenOrders {
            instrument,
            resync_seq,
        } = job
        else {
            return;
        };
        let mut seen_owned = Vec::new();
        for order in orders {
            // The codec already de-dupes, and this pass only ever decodes as SnapshotOrder —
            // it never needs a SnapshotEnd, because it feeds the mirror, not the hot table.
            // An order this build cannot read leaves the mirror incomplete, and quoting against an
            // incomplete mirror is what strands somebody's order.
            let Some(event) = self.decode_order(order, ExecKind::SnapshotOrder, 0) else {
                self.session.core.stop_quoting();
                self.fail_resync(resync_seq);
                self.control.fatal.trip(format!(
                    "binance execution cannot decode a possibly-owned open order {}",
                    order.order_id
                ));
                return;
            };
            let mut mirrored = MirroredOrder::observed(&event);
            let is_cold_start = !self.session.core.has_opened_quoting();
            if is_cold_start && mirrored.provenance != Provenance::Foreign {
                // On a cold start, quoting has not opened yet, so any order already resting
                // must predate this process — that timing decides it, not the nonce in its
                // client id.
                mirrored.provenance = Provenance::PriorRun;
            }
            match mirrored.provenance {
                // Foreign orders are never mirrored, and since none carry this engine's
                // client id, the operator is warned directly instead.
                Provenance::Foreign => warn!(
                    "binance execution sees a foreign order resting on {}: {:?} {} @ {} — it will never be cancelled by this engine",
                    self.symbols.symbol(mirrored.instrument).unwrap_or("?"),
                    mirrored.side,
                    mirrored.qty.0,
                    mirrored.price.0
                ),
                provenance => {
                    seen_owned.push(mirrored.client_id);
                    info!(
                        "binance execution mirroring a resting {provenance:?} order {:016x}",
                        mirrored.client_id.0
                    );
                    if let Err(error) = self.session.core.observe_venue_order(mirrored) {
                        error!(
                            "binance execution cannot retain venue order {:016x} in its possibly-live set ({error:?}) — quoting is disabled",
                            mirrored.client_id.0
                        );
                        self.control.fatal.trip(format!(
                            "binance execution possibly-live mirror failed: {error:?}"
                        ));
                        self.fail_resync(resync_seq);
                        return;
                    }
                }
            }
        }
        // An order missing from this read may simply be gone, or the read may have raced or
        // lagged a fill, so each missing order is probed rather than assumed gone.
        if resync_seq != 0 {
            let missing: Vec<_> = self
                .session
                .core
                .mirror()
                .iter()
                .filter(|order| {
                    order.instrument == instrument
                        && order.provenance != Provenance::Foreign
                        && !seen_owned.contains(&order.client_id)
                })
                .map(|order| order.client_id)
                .collect();
            for client_id in missing {
                self.session.core.mark_ambiguous(client_id);
                if !self.submit(RestJob::OrderStatus {
                    instrument,
                    client_id,
                    recon_seq: resync_seq,
                }) {
                    self.fail_resync(resync_seq);
                }
            }
        }
        // Every order is now accounted for in the mirror, mirrored or already there, so all
        // that remains is to retire this pass.
        if !self.session.resync.on_read(resync_seq) {
            return;
        }
        self.finish_resync(writer).await;
    }

    pub(super) fn poll_resync(&mut self, now: TsUs) -> Option<SessionOutcome> {
        // On the exit path the admission read is moot, and a reconnect starts a clean pass anyway.
        if self.control.exit.is_some() {
            return None;
        }
        match self.session.poll_resync(now, self.pass_reads()) {
            ResyncPoll::Wait => None,
            ResyncPoll::Reread { resync_seq } => {
                self.request_pass_reads(resync_seq);
                None
            }
            // Waiting would only delay the same reconnect by up to 23 hours, so the connection ends
            // now.
            ResyncPoll::GiveUp => Some(SessionOutcome::Reconnect),
        }
    }

    /// A failed read and a refused queue both fail the pass. Backoff paces the retry.
    pub(super) fn fail_resync(&mut self, resync_seq: u64) {
        let retry_at = self.control.next_attempt_at(self.session.resync.attempts());
        self.session.resync.on_failure(resync_seq, retry_at);
    }

    fn request_pass_reads(&mut self, resync_seq: u64) {
        let mut is_queued = self.submit(RestJob::Account { resync_seq });
        for &instrument in self.session.instruments() {
            is_queued &= self.submit(RestJob::OpenOrders {
                instrument,
                resync_seq,
            });
        }
        if is_queued {
            return;
        }
        self.fail_resync(resync_seq);
    }

    pub(super) async fn finish_resync(&mut self, writer: Option<&mut Writer>) {
        self.session.finish_pass();
        let mut outgoing = Vec::new();
        for instrument in self.session.instruments().to_vec() {
            let command = ExecCommand::CancelPriorRun { instrument };
            self.session
                .core
                .on_command(command, &mut Outgoing::sink(&mut outgoing, 0));
        }
        self.dispatch(outgoing, writer).await;
        self.session.arm_readiness(self.control.clock.now());
    }
}
