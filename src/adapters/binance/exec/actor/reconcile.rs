//! REST-only repairs: lost-answer reads, balance snapshots (not deltas), and fill verification.
//! `myTrades` is detection only: it asks for the order, and never invents the trade.

use crate::adapters::binance::rest::{
    AccountInfo, AccountTrade, FailureVerdict, OrderRecord, RestError,
};
use crate::adapters::exec::{EventSink, Outgoing, SessionOutcome};
use crate::ids::{ClientOrderId, InstrumentId, Qty};
use crate::msg::exec::{
    ExecCommand, ExecEvent, ExecKind, Provenance, RejectClass, VenueOrderStatus,
};
use crate::{error, warn};

use super::super::{
    ClockOffset, RejectSubject, account_snapshot_chunks, classify_error, decode_order_record,
};
use super::rest::{RestAnswer, RestJob, RestJobError, RestOutcome, verdict_of};
use super::{Actor, TradeCursor, Writer};

impl Actor {
    pub(super) async fn on_rest_answer(
        &mut self,
        outcome: Option<RestOutcome>,
        writer: &mut Writer,
    ) -> Option<SessionOutcome> {
        let Some(outcome) = outcome else {
            error!("binance execution lost its rest worker — the reconcilers are gone");
            self.is_rest_gone = true;
            self.control
                .fatal
                .trip("binance execution: the rest worker stopped answering");
            return None;
        };
        self.on_rest_outcome(outcome, Some(writer)).await;
        None
    }

    pub(super) async fn on_rest_outcome(
        &mut self,
        outcome: RestOutcome,
        writer: Option<&mut Writer>,
    ) {
        let job = outcome.job;
        match outcome.answer {
            Ok(RestAnswer::Clock(offset)) => self.on_clock(offset),
            Ok(RestAnswer::Account(account)) => self.on_account(job, &account, writer).await,
            Ok(RestAnswer::Orders(orders)) => self.on_open_orders(job, &orders, writer).await,
            Ok(RestAnswer::Order(order)) => self.on_order_record(job, &order, writer).await,
            Ok(RestAnswer::Trades(trades)) => self.on_trades(job, trades, writer).await,
            Err(error) => self.on_rest_failure(job, &error, writer).await,
        }
    }

    fn on_clock(&mut self, offset: ClockOffset) {
        self.clock_offset = offset;
        let skew = offset.correction().micros();
        if skew.abs() > self.loud_clock_skew.micros() {
            warn!(
                "binance venue clock is {skew}us from this host's — every signed request is stamped through that correction, and beyond recvWindow the venue refuses them outright"
            );
        }
    }

    async fn on_account(
        &mut self,
        job: RestJob,
        account: &AccountInfo,
        writer: Option<&mut Writer>,
    ) {
        let context = self.decode_context(self.control.clock.now());
        match account_snapshot_chunks(&account.balances, account.update_time_ms, &context) {
            Ok(chunks) => self.session.events.send_account(chunks),
            // An unreadable balance fails the pass. Reporting without re-reading would leave the
            // mirror empty.
            Err(error) => {
                error!("binance execution could not read its own balances: {error}");
                let RestJob::Account { resync_seq } = job else {
                    return;
                };
                return self.fail_resync(resync_seq);
            }
        }
        let RestJob::Account { resync_seq } = job else {
            return;
        };
        if !self.session.resync.on_read(resync_seq) {
            return;
        }
        self.finish_resync(writer).await;
    }

    // The codec de-dupes, so the mirror and the hot state both see the same reading of this order.
    async fn on_order_record(
        &mut self,
        job: RestJob,
        order: &OrderRecord,
        writer: Option<&mut Writer>,
    ) {
        if let RestJob::OrderByVenueId { venue_order_id, .. } = job {
            return self.on_trade_owner(order, venue_order_id, writer).await;
        }
        let (RestJob::OrderStatus {
            client_id,
            recon_seq,
            ..
        }
        | RestJob::Cancel {
            client_id,
            recon_seq,
            ..
        }) = job
        else {
            return;
        };
        let Some(event) = self.decode_order(order, ExecKind::SnapshotOrder, recon_seq) else {
            return;
        };
        self.session.events.send_exec(event);
        let core = &mut self.session.core;
        let mut outgoing = Vec::new();
        if !event.status.is_some_and(VenueOrderStatus::is_terminal) {
            // Still resting, so the cancel latch is re-armed; leaving it would strand the order.
            core.re_arm_cancel(client_id);
            if core
                .mirrored(client_id)
                .is_some_and(|order| order.provenance == Provenance::PriorRun)
            {
                let command = ExecCommand::CancelPriorRun {
                    instrument: event.instrument,
                };
                core.on_command(command, &mut Outgoing::sink(&mut outgoing, recon_seq));
                self.dispatch(outgoing, writer).await;
            }
            return;
        }
        core.on_order_gone(
            client_id,
            event.cumulative_qty,
            &mut Outgoing::sink(&mut outgoing, recon_seq),
        );
        self.dispatch(outgoing, writer).await;
        self.session.arm_readiness(self.control.clock.now());
    }

    // `None` means the order is untracked or failed to decode, so it has no mirror entry and no
    // cancel path.
    pub(super) fn decode_order(
        &self,
        order: &OrderRecord,
        kind: ExecKind,
        recon_seq: u64,
    ) -> Option<ExecEvent> {
        let context = self.decode_context(self.control.clock.now());
        match decode_order_record(order, kind, recon_seq, &context) {
            Ok(event) => event,
            Err(error) => {
                warn!(
                    "binance execution could not read resting order {}: {error} — it will not be mirrored, so no exit path will cancel it",
                    order.order_id
                );
                None
            }
        }
    }

    // `myTrades` omits the client_id, so the order is asked in order to find the fill's owner.
    async fn on_trade_owner(
        &mut self,
        order: &OrderRecord,
        venue_order_id: i64,
        writer: Option<&mut Writer>,
    ) {
        let Some(event) = self.decode_order(order, ExecKind::SnapshotOrder, 0) else {
            return;
        };
        if event.provenance == Provenance::Foreign {
            warn!(
                "binance execution: order {venue_order_id} on {} is not this engine's — its fills are somebody else's and are left alone",
                self.symbols.symbol(event.instrument).unwrap_or("?")
            );
            return;
        }
        self.counts.missed_fills += 1;
        warn!(
            "binance execution recovered a fill the account stream never delivered: order {venue_order_id} is {:?} order {:016x}, filled {} of {}",
            event.provenance, event.client_id.0, event.cumulative_qty.0, event.qty.0
        );
        self.note_event(&event);
        self.session.events.send_exec(event);
        if !event.status.is_some_and(VenueOrderStatus::is_terminal) {
            return;
        }
        let mut outgoing = Vec::new();
        self.session.core.on_order_gone(
            event.client_id,
            event.cumulative_qty,
            &mut Outgoing::sink(&mut outgoing, 0),
        );
        self.dispatch(outgoing, writer).await;
        self.session.arm_readiness(self.control.clock.now());
    }

    // Only detects missing fills, never invents one. Recovery asks for the order's absolute
    // state, which makes it idempotent by construction.
    async fn on_trades(
        &mut self,
        job: RestJob,
        trades: Vec<AccountTrade>,
        writer: Option<&mut Writer>,
    ) {
        let RestJob::MyTrades { instrument, .. } = job else {
            return;
        };
        let mut missed = Vec::new();
        let mut highest = None;
        let is_primed = self
            .cursor_mut(instrument)
            .is_some_and(|cursor| cursor.is_primed);
        for trade in &trades {
            highest = Some(highest.map_or(trade.id, |seen: i64| seen.max(trade.id)));
            if is_primed && !self.has_seen_trade(trade.id) {
                missed.push((trade.id, trade.order_id));
            }
        }
        self.advance_cursor(instrument, highest);
        if missed.is_empty() {
            return;
        }
        let mut outgoing = Vec::new();
        for (trade_id, venue_order_id) in missed {
            let owner = self
                .recent_orders
                .iter()
                .find(|order| order.venue_id.0 == venue_order_id)
                .map(|order| order.client_id);
            match owner {
                Some(client_id) => {
                    self.counts.missed_fills += 1;
                    warn!(
                        "binance execution never saw trade {trade_id} on order {venue_order_id} — reconciling order {:016x}",
                        client_id.0
                    );
                    let core = &mut self.session.core;
                    core.on_ambiguous(client_id, &mut Outgoing::sink(&mut outgoing, 0));
                }
                // `myTrades` lacks the client_id, so the order record is asked, which carries it.
                None => {
                    let is_queued = self.submit(RestJob::OrderByVenueId {
                        instrument,
                        venue_order_id,
                        trade_id,
                    });
                    if !is_queued {
                        self.rewind_cursor(instrument, trade_id);
                        warn!(
                            "binance execution could not queue the owner lookup for trade {trade_id} on order {venue_order_id} — re-asking myTrades for it next pass"
                        );
                    }
                }
            }
        }
        self.dispatch(outgoing, writer).await;
    }

    async fn on_rest_failure(
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
        core.on_order_gone(client_id, Qty(0), &mut Outgoing::sink(&mut outgoing, 0));
        self.dispatch(outgoing, writer).await;
        self.session.arm_readiness(self.control.clock.now());
    }

    // A -2011 on cancel probes whether the order is open and holds the latch. Anything else
    // re-arms it.
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

    fn cursor_mut(&mut self, instrument: InstrumentId) -> Option<&mut TradeCursor> {
        self.cursors
            .iter_mut()
            .find(|cursor| cursor.instrument == instrument)
    }

    fn has_seen_trade(&self, trade_id: i64) -> bool {
        self.recent_trades.iter().any(|seen| seen.0 == trade_id)
    }

    // The first answer only primes the cursor; it never flags that page of history as missed fills.
    fn advance_cursor(&mut self, instrument: InstrumentId, highest: Option<i64>) {
        let Some(cursor) = self.cursor_mut(instrument) else {
            return;
        };
        cursor.is_primed = true;
        let Some(highest) = highest else {
            return;
        };
        let next = highest.saturating_add(1);
        if cursor.from_id.is_none_or(|from_id| next > from_id) {
            cursor.from_id = Some(next);
        }
    }

    /// The venue's balance deltas are unusable without a snapshot to fold them onto, and this is
    /// the only request that produces one. A refused queue therefore latches rather than passing:
    /// without it the balances stay wrong until the next reconnect, up to a day away.
    pub(super) fn request_balance_snapshot(&mut self) {
        self.is_balance_snapshot_due = !self.submit(RestJob::Account { resync_seq: 0 });
    }

    /// The cursor is this actor's whole memory of which fills it has examined, so putting a trade
    /// back in front of it is what turns an abandoned recovery into a retried one.
    fn rewind_cursor(&mut self, instrument: InstrumentId, trade_id: i64) {
        let Some(cursor) = self.cursor_mut(instrument) else {
            return;
        };
        if cursor.from_id.is_none_or(|from_id| trade_id < from_id) {
            cursor.from_id = Some(trade_id);
        }
    }
}
