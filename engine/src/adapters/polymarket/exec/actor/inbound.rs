//! The user stream. Unsolicited, account-wide, and it replays nothing after a disconnect.
//!
//! A frame naming an order this run cannot map is the venue's normal behaviour here, not a fault:
//! the placement answer that mints the mapping may still be in flight. Such a frame is HELD and
//! re-read when a mapping lands. Only a frame still unattributable after the hold is a problem, and
//! then it is loud — it describes a fill the ledger has not seen.

use tokio_tungstenite::tungstenite::{Error as ProtocolError, Message};

use crate::adapters::edge::RECENT_TRADES;
use crate::adapters::exec::{Outgoing, SessionOutcome};
use crate::msg::exec::{ExecEvent, Liquidity};
use crate::{error, warn};

use super::super::codec::{
    IgnoredReason, StreamEvent, TradeLineage, TradeSettlement, decode_stream_frame,
};
use super::{Actor, Writer};

impl Actor {
    pub(super) async fn on_frame(
        &mut self,
        frame: Option<Result<Message, ProtocolError>>,
        writer: &mut Writer,
    ) -> Option<SessionOutcome> {
        match frame {
            Some(Ok(Message::Text(text))) => self.on_text(text.as_str(), writer).await,
            Some(Ok(Message::Close(_))) | None => Some(SessionOutcome::Reconnect),
            Some(Ok(_)) => None,
            Some(Err(error)) => {
                warn!("polymarket execution stream: {error}");
                Some(SessionOutcome::Reconnect)
            }
        }
    }

    async fn on_text(&mut self, text: &str, writer: &mut Writer) -> Option<SessionOutcome> {
        // This is the vendor's only non-JSON frame on this channel; the loop simply resumes
        // once it's handled.
        if text.trim() == "PONG" {
            return None;
        }
        let mut outgoing = Vec::new();
        let held = self.consume_stream_frame(text, &mut outgoing);
        if held {
            self.pending.hold(text.to_owned(), self.control.clock.now());
            self.report_holding();
        }
        let ended = self.dispatch(outgoing, Some(writer)).await;
        self.finish_readiness_if_clean();
        ended
    }

    /// Returns `true` if the frame names an unmapped order and should be held for later retry.
    pub(super) fn consume_stream_frame(
        &mut self,
        text: &str,
        outgoing: &mut Vec<Outgoing>,
    ) -> bool {
        match decode_stream_frame(text, &self.decode_context()) {
            Ok(StreamEvent::Order(event)) => {
                self.on_stream_order(event, outgoing);
                false
            }
            Ok(StreamEvent::Trade(lineage)) => {
                self.on_trade(lineage);
                false
            }
            Ok(StreamEvent::Ignored(IgnoredReason::UnknownOrder)) => true,
            Ok(StreamEvent::Ignored(reason)) => {
                if matches!(reason, IgnoredReason::UntrackedToken) {
                    self.counts.untracked_events += 1;
                }
                false
            }
            Err(error) if error.is_fatal() => {
                error!("polymarket execution fatal stream decode: {error}");
                self.control
                    .fatal
                    .trip(format!("polymarket execution: {error}"));
                false
            }
            Err(error) => {
                self.counts.dropped_frames += 1;
                warn!("polymarket execution could not read a stream frame: {error}");
                false
            }
        }
    }

    fn on_stream_order(&mut self, event: ExecEvent, outgoing: &mut Vec<Outgoing>) {
        if event.status.is_some_and(|status| status.is_terminal()) {
            self.orders.forget(event.client_id);
            self.delayed.forget(event.client_id);
        }
        self.session.fold(&event, 0, outgoing);
        self.forward_exec(event);
    }

    /// Reads lineage, fees, settlement only — never quantity (would multiply by settlement depth).
    /// Same trade id re-sends on each settlement step; only the first is folded into the ledger.
    pub(super) fn on_trade(&mut self, lineage: TradeLineage) {
        // This venue publishes no account clock, so a fill's own trade reaching the chain is the
        // only evidence the balances behind it moved, and the reservation the hot side took against
        // that fill is released against nothing else. It sits ahead of the dedup below because the
        // settlement step carrying that evidence is never a trade's first sighting, and it re-reads
        // balances because a read taken before this moment answers with the pre-fill number.
        if lineage.is_ours
            && lineage.settlement.is_on_chain()
            && self.settled_through.advance_to(lineage.exchange_ts_us)
        {
            self.restate_balances();
        }
        // Settlement failure must be caught before dedup, because the same id re-sends as FAILED
        // after being seen MATCHED; without this check the dedup below would swallow the change.
        if lineage.settlement == TradeSettlement::Failed {
            self.on_settlement_failure(&lineage);
            return;
        }
        // Each settlement step re-sends the same id. Dedup avoids re-running recovery every tick.
        let is_new = !self
            .seen_trades
            .iter()
            .any(|seen| **seen == *lineage.venue_trade_id);
        if !is_new {
            return;
        }
        if self.seen_trades.len() == RECENT_TRADES {
            self.seen_trades.remove(0);
        }
        self.seen_trades.push(lineage.venue_trade_id.clone());
        // A maker fill without a mirrored order means the UPDATE carrying cumulative size hasn't
        // arrived yet; the resync read recovers it.
        let is_unmirrored_maker = lineage.role == Some(Liquidity::Maker)
            && lineage
                .maker_fills
                .iter()
                .any(|fill| !self.session.core.is_mirrored(fill.client_id));
        if is_unmirrored_maker {
            warn!(
                "polymarket execution saw a maker fill on an order it is not mirroring — re-reading orders and balances"
            );
            self.nudge_resync();
        }
    }

    /// A fill the engine already folded did not settle on chain. The position the ledger believes in
    /// does not exist, and the edge cannot unfold it, so the run cannot keep quoting: this is
    /// TERMINAL, because corrupt state must never keep producing research data. The fatal latch
    /// drives the existing coordinated drain, which sweeps every resting order once; routing through
    /// a plain halt sweep instead would let `re_arm_after_sweep` re-open quoting against the wrong
    /// ledger.
    ///
    /// The trades feed is account-wide and the same wallet is reachable from the venue's own order
    /// entry, so a person's manual fill failing to settle must not halt a live engine over a
    /// position it never held. Only a trade the VENUE attributes to another credential is left
    /// alone: attribution this engine derives for itself would depend on still being able to name
    /// the order, and a settlement failure arrives long after that order went terminal.
    #[cold]
    fn on_settlement_failure(&mut self, lineage: &TradeLineage) {
        if self.control.fatal.is_tripped() {
            return;
        }
        if !lineage.is_ours {
            self.counts.foreign_settlement_failures += 1;
            warn!(
                "polymarket execution saw trade {} on instrument {} settle FAILED — the venue names another credential on it, so this engine placed no part of it and quoting continues",
                lineage.venue_trade_id, lineage.instrument.0
            );
            return;
        }
        self.counts.settlement_failures += 1;
        error!(
            "polymarket execution trade {} on instrument {} settled FAILED after being folded — the ledger holds a fill that does not exist, halting the run ({} so far)",
            lineage.venue_trade_id, lineage.instrument.0, self.counts.settlement_failures
        );
        self.session.core.stop_quoting();
        self.control.fatal.trip(format!(
            "polymarket execution settlement failed on trade {} — a folded fill did not settle on chain",
            lineage.venue_trade_id
        ));
    }

    /// Re-reads held frames in order; mappings landed since their first arrival may now explain them.
    pub(super) fn drain_pending(&mut self, outgoing: &mut Vec<Outgoing>) {
        if self.pending.is_empty() {
            return;
        }
        for frame in self.pending.drain() {
            if self.consume_stream_frame(&frame.text, outgoing) {
                self.pending.re_hold(frame);
            }
        }
    }

    /// A frame held past its TTL names a real order at the venue; resync recovers it.
    #[cold]
    pub(super) fn abandon_expired_frames(&mut self) {
        let expired = self.pending.expired(self.control.clock.now());
        if expired.is_empty() {
            return;
        }
        error!(
            "polymarket execution abandoned {} stream frames it could never attribute ({} in total) — forcing a re-read of orders, trades and balances",
            expired.len(),
            self.pending.abandoned()
        );
        self.nudge_resync();
    }

    #[cold]
    fn report_holding(&mut self) {
        let dropped = self.pending.dropped();
        if self.pending.len() < super::super::correlate::PENDING_CAPACITY
            || !dropped.is_power_of_two()
        {
            return;
        }
        warn!(
            "polymarket execution correlation buffer full at {} — dropped {dropped} of the oldest held frames",
            super::super::correlate::PENDING_CAPACITY
        );
    }
}
