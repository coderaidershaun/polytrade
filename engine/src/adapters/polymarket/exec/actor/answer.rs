//! HTTP answers. Every request this engine makes is answered here, and the placement answer is the
//! only place an order's venue id ever appears — so this is also where correlation is established
//! and where the frames held waiting for it are released.

use crate::adapters::exec::{ExecRequest, Outgoing, PlaceNotSentReason, SessionOutcome};
use crate::adapters::http::TransportError;
use crate::msg::exec::ExecEvent;
use crate::{error, info, warn};

use super::super::binding::BindingStep;
use super::super::codec::{
    DecodeContext, KnownOrder, PlaceRequestContext, PlacementStatus, VenueAnswer, WireError,
    decode_cancel, decode_neg_risk, decode_place, decode_single_order,
};
use super::super::rest::ClobResponse;
use super::rest::{OpenOrdersRead, RestOutcome, RestPurpose};
use super::{Actor, Writer};

impl Actor {
    pub(super) async fn on_rest_answer(
        &mut self,
        outcome: Option<RestOutcome>,
        writer: &mut Writer,
    ) -> Option<SessionOutcome> {
        let outcome = outcome?;
        self.on_rest_outcome(outcome, Some(writer)).await
    }

    pub(super) async fn on_rest_outcome(
        &mut self,
        outcome: RestOutcome,
        writer: Option<&mut Writer>,
    ) -> Option<SessionOutcome> {
        let RestOutcome { purpose, answer } = outcome;
        match purpose {
            RestPurpose::Core {
                request_id,
                request,
                recon_seq,
                place,
            } => {
                self.on_core_answer(request_id, request, recon_seq, place, answer, writer)
                    .await
            }
            RestPurpose::OpenOrders { read, page, seen } => match answer {
                Ok(response) => {
                    self.on_open_orders_answer(read, page, seen, &response, writer)
                        .await
                }
                Err(error) => {
                    warn!("polymarket execution open-orders read failed: {error}");
                    self.fail_orders_read(read);
                    None
                }
            },
            RestPurpose::Balance { asset, resync_seq } => {
                self.on_balance(asset, resync_seq, answer);
                None
            }
            RestPurpose::PriorRunCancel { venue_order_id } => {
                let is_answered = read_body("prior-run cancel", &answer).is_some();
                self.on_prior_run_cancelled(&venue_order_id, is_answered);
                None
            }
            RestPurpose::Trades { resync_seq, page } => {
                self.on_trades_page(resync_seq, page, answer);
                None
            }
            RestPurpose::Market { condition_id } => {
                self.on_market_answer(&condition_id, answer);
                None
            }
            RestPurpose::NegRisk {
                condition_id,
                instrument,
            } => {
                let step = match read_body("neg-risk", &answer) {
                    Some(body) => match decode_neg_risk(body) {
                        Ok(is_neg_risk) => {
                            self.bindings
                                .on_neg_risk(&condition_id, instrument, is_neg_risk)
                        }
                        Err(error) => {
                            warn!("polymarket execution could not read a neg-risk answer: {error}");
                            BindingStep::Wait
                        }
                    },
                    None => BindingStep::Wait,
                };
                self.apply_binding_step(step);
                None
            }
            RestPurpose::Allowance { token_id } => {
                self.on_allowance(&token_id, answer);
                None
            }
            RestPurpose::Heartbeat => {
                self.on_heartbeat_answer(answer);
                None
            }
            RestPurpose::MarketCancel { instrument } => {
                if let Some(body) = read_body("cancel-market-orders", &answer) {
                    info!(
                        "polymarket execution swept instrument {} at its window close: {body}",
                        instrument.0
                    );
                }
                None
            }
        }
    }

    async fn on_core_answer(
        &mut self,
        request_id: crate::adapters::exec::RequestId,
        request: ExecRequest,
        recon_seq: u64,
        place: Option<PlaceRequestContext>,
        answer: Result<ClobResponse, TransportError>,
        writer: Option<&mut Writer>,
    ) -> Option<SessionOutcome> {
        let Some(entry) = self.session.inflight.take(request_id) else {
            self.counts.unmatched_answers += 1;
            return None;
        };
        let response = match answer {
            Ok(response) => response,
            Err(error) => {
                warn!("polymarket execution request failed in transport: {error}");
                self.on_transport_failure(request);
                return None;
            }
        };
        self.note_rate_limit(&request, &response);
        // The hot side asks for one instrument's orders through core. Its counter is named as such
        // here so it can never be mistaken for a resync pass and retire one of that pass's reads.
        if let ExecRequest::OpenOrders { instrument } = request {
            let read = OpenOrdersRead::Instrument {
                instrument,
                recon_seq,
            };
            return self
                .on_open_orders_answer(read, 0, Vec::new(), &response, writer)
                .await;
        }

        let mut outgoing = Vec::new();
        let events = self.decode_core(&request, place, recon_seq, &response, &mut outgoing);
        for event in events {
            let event = ExecEvent {
                request_sent_ts_us: Some(entry.sent_at),
                ..event
            };
            self.session.fold(&event, recon_seq, &mut outgoing);
            // Finding the order still alive on a status probe is what settles an ambiguous cancel here.
            if matches!(request, ExecRequest::OrderStatus { client_id, .. } if client_id == event.client_id)
                && event.status.is_some_and(|status| !status.is_terminal())
            {
                self.session.core.re_arm_cancel(event.client_id);
            }
            self.forward_exec(event);
        }
        let ended = self.dispatch(outgoing, writer).await;
        self.finish_readiness_if_clean();
        ended
    }

    fn decode_core(
        &mut self,
        request: &ExecRequest,
        place: Option<PlaceRequestContext>,
        recon_seq: u64,
        response: &ClobResponse,
        outgoing: &mut Vec<Outgoing>,
    ) -> Vec<ExecEvent> {
        match request {
            ExecRequest::Place { .. } => {
                let Some(context) = place else {
                    return Vec::new();
                };
                self.on_place_answer(*request, &context, response, outgoing)
            }
            ExecRequest::Cancel { .. } => {
                let decoded = decode_cancel(response.answer(), &self.decode_context());
                self.answered(decoded, "a cancel answer")
                    .unwrap_or_default()
            }
            ExecRequest::OrderStatus { .. } => {
                let decoded =
                    decode_single_order(response.answer(), recon_seq, &self.decode_context());
                self.answered(decoded, "an order answer")
                    .flatten()
                    .into_iter()
                    .collect()
            }
            ExecRequest::OpenOrders { .. }
            | ExecRequest::AmendQty { .. }
            | ExecRequest::SubscribeUserStream => Vec::new(),
        }
    }

    /// Parks the driver on a venue-availability verdict and logs an answer that cannot be read;
    /// either way there is nothing to fold.
    pub(super) fn answered<T>(
        &mut self,
        decoded: Result<VenueAnswer<T>, WireError>,
        what: &str,
    ) -> Option<T> {
        match decoded {
            Ok(VenueAnswer::Answered(answer)) => Some(answer),
            Ok(VenueAnswer::Unavailable(availability)) => {
                self.on_unavailable(availability);
                None
            }
            Err(error) => {
                warn!("polymarket execution could not read {what}: {error}");
                None
            }
        }
    }

    /// The only answer that mints a mapping. All downstream flows depend on this record.
    fn on_place_answer(
        &mut self,
        request: ExecRequest,
        context: &PlaceRequestContext,
        response: &ClobResponse,
        outgoing: &mut Vec<Outgoing>,
    ) -> Vec<ExecEvent> {
        let decoded = decode_place(response.answer(), context, &self.decode_context());
        let outcome = match decoded {
            Ok(VenueAnswer::Answered(outcome)) => outcome,
            Ok(VenueAnswer::Unavailable(availability)) => {
                self.on_unavailable(availability);
                // The venue is unavailable, so the hot side's pending slot must be released
                // here or it hangs waiting forever.
                self.session.refuse(
                    request,
                    PlaceNotSentReason::NoTransport,
                    self.control.clock.now(),
                );
                return Vec::new();
            }
            Err(error) => {
                error!("polymarket execution could not read a placement answer: {error}");
                self.session
                    .mark_in_doubt(request, self.control.clock.now());
                self.nudge_resync();
                return Vec::new();
            }
        };
        self.parked = None;
        if let Some(placed) = &outcome.placed {
            self.bind_venue_order(
                &placed.venue_order_id,
                KnownOrder {
                    client_id: placed.client_id,
                    instrument: context.instrument,
                },
            );
            if placed.status == PlacementStatus::Delayed {
                self.delayed
                    .on_delayed(placed.client_id, self.control.clock.now());
            }
            self.drain_pending(outgoing);
        }
        vec![outcome.event]
    }

    /// Stores the mapping from venue order id to client id. If the index is full, stale
    /// orders are retired first to make room.
    fn bind_venue_order(&mut self, venue_order_id: &str, known: KnownOrder) {
        if self.orders.record(venue_order_id, known).is_ok() {
            return;
        }
        let live: Vec<crate::ids::ClientOrderId> = self
            .session
            .core
            .mirror()
            .iter()
            .map(|order| order.client_id)
            .collect();
        let retired = self.orders.retain(|known| live.contains(&known.client_id));
        warn!(
            "polymarket execution order index was full — retired {retired} mappings whose orders are no longer live"
        );
        if let Err(error) = self.orders.record(venue_order_id, known) {
            error!(
                "polymarket execution cannot record venue order {venue_order_id} ({error}) — it can never be cancelled"
            );
            self.control
                .fatal
                .trip(format!("polymarket execution correlation: {error}"));
        }
    }

    /// The request's bytes may have already reached the venue, so resync adoption is the
    /// recovery path here rather than a blind retry.
    fn on_transport_failure(&mut self, request: ExecRequest) {
        self.session
            .mark_in_doubt(request, self.control.clock.now());
        if matches!(request, ExecRequest::Place { .. }) {
            self.nudge_resync();
        }
    }

    pub(super) fn decode_context(&self) -> DecodeContext<'_> {
        DecodeContext {
            tokens: &self.tokens,
            orders: &self.orders,
            api_key: self.signer.api_key(),
            received_ts_us: self.control.clock.now(),
        }
    }

    fn note_rate_limit(&self, request: &ExecRequest, response: &ClobResponse) {
        let is_mutating = matches!(
            request,
            ExecRequest::Place { .. } | ExecRequest::Cancel { .. }
        );
        if !is_mutating || response.rate_limit.is_absent() {
            return;
        }
        let remaining = response.rate_limit.remaining;
        if let Some(warning) = &response.rate_limit.warning {
            warn!(
                "polymarket execution rate-limit warning {warning}, {} tokens remaining",
                remaining.map_or_else(|| "-".to_owned(), |budget| budget.to_string())
            );
            return;
        }
        if response.rate_limit.is_overdrawn() {
            warn!(
                "polymarket execution rate-limit budget is negative ({}) — cancels are blocked until it recovers",
                remaining.unwrap_or(0)
            );
        }
    }
}

/// Returns the response body on success. On failure the reason is logged and `None` is
/// returned instead.
pub(super) fn read_body<'a>(
    what: &str,
    answer: &'a Result<ClobResponse, TransportError>,
) -> Option<&'a str> {
    match answer {
        Ok(response) if response.is_success() => Some(&response.body),
        Ok(response) => {
            warn!(
                "polymarket execution {what} read answered http {}: {}",
                response.status,
                response.excerpt()
            );
            None
        }
        Err(error) => {
            warn!("polymarket execution {what} read failed: {error}");
            None
        }
    }
}
