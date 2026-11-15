//! What the edge does when a REST call of its reconcilers fails: a failed read fails its pass, a
//! lost fill is asked again, and a refused cancel is probed or re-armed by its class.

use crate::msg::exec::RejectClass;
use crate::msg::ids::{ClientOrderId, InstrumentId, Qty};
use crate::venues::binance::rest::{FailureVerdict, RestError};
use crate::venues::exec::Outgoing;
use crate::{error, warn};

use super::super::{RejectSubject, classify_error};
use super::rest::{RestJob, RestJobError, verdict_of};
use super::{Actor, Writer};

impl Actor {
    pub(super) async fn on_rest_failure(
        &mut self,
        job: RestJob,
        error: &RestJobError,
        writer: Option<&mut Writer>,
    ) {
        let verdict = verdict_of(error);
        match job {
            // A failed balance read fails the readiness gate.
            RestJob::Account { resync_seq } => {
                warn!(
                    "binance execution could not read its balances ({verdict:?}): {error} — quoting stays closed until the read lands"
                );
                self.fail_resync(resync_seq);
            }
            RestJob::OpenOrders {
                instrument,
                resync_seq,
            } => self.on_open_orders_failure(instrument, resync_seq, verdict, error),
            RestJob::OrderByVenueId {
                instrument,
                venue_order_id,
                trade_id,
            } => {
                self.on_fill_recovery_failure(instrument, venue_order_id, trade_id, verdict, error)
            }
            RestJob::OrderStatus { client_id, .. } => {
                self.on_order_read_failure(
                    RejectSubject::StatusQuery,
                    client_id,
                    verdict,
                    error,
                    writer,
                )
                .await;
            }
            RestJob::Cancel { client_id, .. } => {
                self.on_order_read_failure(
                    RejectSubject::Cancellation,
                    client_id,
                    verdict,
                    error,
                    writer,
                )
                .await;
            }
            // Both ride a fixed cadence and ask again shortly.
            RestJob::SyncClock | RestJob::MyTrades { .. } => {
                warn!("binance execution rest call failed ({verdict:?}): {error}");
            }
        }
    }

    fn on_open_orders_failure(
        &mut self,
        instrument: InstrumentId,
        resync_seq: u64,
        verdict: FailureVerdict,
        error: &RestJobError,
    ) {
        let symbol = self.symbols.symbol(instrument).unwrap_or("?");
        // A sequence of 0 marks the hot pass, which is not an admission gate.
        if resync_seq == 0 {
            warn!(
                "binance execution could not read open orders on {symbol} for the hot table ({verdict:?}): {error}"
            );
            return;
        }
        // This is the actor's own resync pass, where a failure refuses every order.
        warn!(
            "binance execution could not read open orders on {symbol} ({verdict:?}): {error} — quoting stays closed until the read lands"
        );
        self.fail_resync(resync_seq);
    }

    /// The account stream never delivered this fill, and `myTrades` will not offer the trade again
    /// because the cursor only moves forward. So a failure that could succeed later has to put the
    /// trade back in front of the cursor, and one that cannot has to be shouted: a fill nobody books
    /// leaves the position wrong for the rest of the run.
    #[cold]
    fn on_fill_recovery_failure(
        &mut self,
        instrument: InstrumentId,
        venue_order_id: i64,
        trade_id: i64,
        verdict: FailureVerdict,
        error: &RestJobError,
    ) {
        if verdict == FailureVerdict::Retry {
            self.rewind_cursor(instrument, trade_id);
            warn!(
                "binance execution could not read order {venue_order_id} to book trade {trade_id} ({verdict:?}): {error} — re-asking myTrades for it next pass"
            );
            return;
        }
        self.counts.unrecovered_fills += 1;
        error!(
            "binance execution cannot book trade {trade_id} on order {venue_order_id} — the venue refused the read ({verdict:?}): {error}. The position is short this fill until it is reconciled by hand"
        );
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "an edge actor stamps received and queued time on each message and paces its own I/O"
    )]
    async fn on_order_read_failure(
        &mut self,
        subject: RejectSubject,
        client_id: ClientOrderId,
        verdict: FailureVerdict,
        error: &RestJobError,
        writer: Option<&mut Writer>,
    ) {
        // Uses the same classifier the socket path does, so venue codes share one vocabulary.
        let class = match error {
            RestJobError::Rest(RestError::Status { code, message, .. })
            | RestJobError::Rest(RestError::Unauthorized { code, message, .. }) => classify_error(
                code.unwrap_or_default() as i32,
                message.as_deref().unwrap_or_default(),
                subject,
            ),
            _ => RejectClass::Ambiguous,
        };
        if class != RejectClass::Gone {
            warn!(
                "binance execution reconcile of order {:016x} answered {class:?} ({verdict:?}): {error}",
                client_id.0
            );
            return self
                .unresolved_over_rest(class, subject, client_id, writer)
                .await;
        }
        let mut outgoing = Vec::new();
        // Gone means it never rested, so the cumulative quantity is zero.
        let core = &mut self.session.core;
        core.on_order_gone(
            client_id,
            Qty::from_mantissa(0),
            &mut Outgoing::sink(&mut outgoing, 0),
        );
        self.dispatch(outgoing, writer).await;
        self.session.arm_readiness(self.control.clock.now());
    }

    // An ambiguous cancel (-2011, an execution status unknown, or no answer) probes whether the
    // order is open and holds the latch. Any other class re-arms it.
    async fn unresolved_over_rest(
        &mut self,
        class: RejectClass,
        subject: RejectSubject,
        client_id: ClientOrderId,
        writer: Option<&mut Writer>,
    ) {
        let is_probe_due =
            class == RejectClass::Ambiguous && subject == RejectSubject::Cancellation;
        if !is_probe_due {
            return self.session.core.re_arm_cancel(client_id);
        }
        let mut outgoing = Vec::new();
        let core = &mut self.session.core;
        core.on_ambiguous(client_id, &mut Outgoing::sink(&mut outgoing, 0));
        self.dispatch(outgoing, writer).await;
    }
}
