//! Routes each of the engine's decisions over the socket or over REST, and remembers the order and
//! fill ids the reconciler needs.

use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::Message;

use crate::adapters::edge::RECENT_TRADES;
use crate::adapters::exec::{
    ExecEffect, ExecRequest, InFlightRequest, Outgoing, PlaceNotSentReason, RequestId,
    SessionOutcome, SkipReason,
};
use crate::ids::ClientOrderId;
use crate::msg::exec::{CancelReason, ExecEvent};
use crate::{error, info, warn};

use super::super::EncodeContext;
use super::frame::FrameCredentials;
use super::rest::RestJob;
use super::{Actor, RECENT_ORDERS, RecentOrder, Writer, frame};

/// Three outcomes the caller must tell apart: bytes are queued and want a flush, nothing was sent
/// and the request has already been refused, or the socket is gone and the session with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FeedOutcome {
    Fed,
    NotSent,
    SocketDead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    Socket,
    Rest,
    Refused,
}

fn transport_for(request: ExecRequest, has_socket: bool) -> Transport {
    match request {
        // Place and amend need the socket.
        ExecRequest::Place { .. }
        | ExecRequest::AmendQty { .. }
        | ExecRequest::SubscribeUserStream => match has_socket {
            true => Transport::Socket,
            false => Transport::Refused,
        },
        // Cancel and the reads are fine over REST.
        ExecRequest::Cancel { .. }
        | ExecRequest::OrderStatus { .. }
        | ExecRequest::OpenOrders { .. } => match has_socket {
            true => Transport::Socket,
            false => Transport::Rest,
        },
    }
}

impl Actor {
    // Binance Spot has no batch-order endpoint, so requests go out one at a time and
    // share a single flush instead.
    pub(super) async fn dispatch(
        &mut self,
        outgoing: Vec<Outgoing>,
        mut writer: Option<&mut Writer>,
    ) -> Option<SessionOutcome> {
        let mut is_fed = false;
        let mut ended = None;
        for Outgoing { effect, recon_seq } in outgoing {
            match effect {
                ExecEffect::Send {
                    request_id,
                    request,
                } => match transport_for(request, writer.is_some()) {
                    Transport::Socket => match writer.as_deref_mut() {
                        Some(writer) => {
                            match self.feed(request_id, request, recon_seq, writer).await {
                                FeedOutcome::Fed => is_fed = true,
                                FeedOutcome::NotSent => {}
                                FeedOutcome::SocketDead => {
                                    ended = Some(SessionOutcome::Reconnect);
                                }
                            }
                        }
                        None => ended = Some(SessionOutcome::Reconnect),
                    },
                    Transport::Rest => self.submit_over_rest(request, recon_seq),
                    Transport::Refused => {
                        let now = self.control.clock.now();
                        self.session
                            .refuse(request, PlaceNotSentReason::NoTransport, now);
                    }
                },
                ExecEffect::Skipped { client_id, reason } => self.on_skipped(client_id, reason),
                ExecEffect::PlaceNotSent { .. } | ExecEffect::AmendNotSent { .. } => {
                    self.session.on_unsent(&effect, self.control.clock.now());
                }
                ExecEffect::SweepComplete { reason } => self.on_sweep_complete(reason),
            }
        }
        if is_fed
            && let Some(writer) = writer
            && writer.flush().await.is_err()
        {
            warn!("binance execution could not flush its request batch — reconnecting");
            ended = Some(SessionOutcome::Reconnect);
        }
        ended
    }

    async fn feed(
        &mut self,
        request_id: RequestId,
        request: ExecRequest,
        recon_seq: u64,
        writer: &mut Writer,
    ) -> FeedOutcome {
        let now = self.control.clock.now();
        let frame = {
            let context = EncodeContext {
                symbols: &self.symbols,
                identity: self.identity,
            };
            frame::frame_request(
                request_id,
                request,
                &context,
                FrameCredentials {
                    signer: &self.signer,
                    api_key: &self.api_key,
                    recv_window: self.recv_window,
                    stamp: self.clock_offset.stamp(now),
                },
            )
        };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                error!("binance execution could not build a request: {error}");
                self.session
                    .refuse(request, PlaceNotSentReason::Encoding, now);
                return FeedOutcome::NotSent;
            }
        };
        if writer.feed(Message::Text(frame.into())).await.is_err() {
            self.session
                .mark_in_doubt(request, self.control.clock.now());
            return FeedOutcome::SocketDead;
        }
        self.session.inflight.record(InFlightRequest {
            request_id,
            request,
            sent_at: now,
            recon_seq,
        });
        FeedOutcome::Fed
    }

    fn submit_over_rest(&mut self, request: ExecRequest, recon_seq: u64) {
        let job = match request {
            ExecRequest::Cancel {
                instrument,
                client_id,
            } => RestJob::Cancel {
                instrument,
                client_id,
                recon_seq,
            },
            ExecRequest::OrderStatus {
                instrument,
                client_id,
            } => RestJob::OrderStatus {
                instrument,
                client_id,
                recon_seq,
            },
            // resync_seq stays 0 because this request comes from the hot pass, not a
            // background resync sweep.
            ExecRequest::OpenOrders { instrument } => RestJob::OpenOrders {
                instrument,
                resync_seq: 0,
            },
            other => {
                let now = self.control.clock.now();
                return self
                    .session
                    .refuse(other, PlaceNotSentReason::NoTransport, now);
            }
        };
        if self.submit(job) {
            return;
        }
        // The cancel latch re-arms so the next sweep pass retries this order.
        if let RestJob::Cancel { client_id, .. } = job {
            self.session.core.re_arm_cancel(client_id);
        }
    }

    fn on_skipped(&mut self, client_id: Option<ClientOrderId>, reason: SkipReason) {
        let described = client_id.and_then(|id| self.describe(id));
        match reason {
            SkipReason::ForeignOrder => warn!(
                "binance execution left a foreign order alone: {} — the engine never cancels an order it did not place",
                described.unwrap_or_else(|| "unknown order".to_owned())
            ),
            // A symptom of the cancel latch, so it is counted and warned about.
            SkipReason::AlreadyCancelling => {
                self.counts.cancels_skipped += 1;
                warn!(
                    "binance execution already has a cancel in flight for {} ({} skipped so far)",
                    described.unwrap_or_else(|| "an unknown order".to_owned()),
                    self.counts.cancels_skipped
                );
            }
        }
    }

    fn on_sweep_complete(&mut self, reason: CancelReason) {
        info!("binance execution swept every order it owns: {reason:?}");
        self.control.is_swept = true;
    }

    fn describe(&self, client_id: ClientOrderId) -> Option<String> {
        let order = self.session.core.mirrored(client_id)?;
        Some(format!(
            "{:?} {} {} @ {} (client {:016x})",
            order.side,
            order.qty.0,
            self.symbols.symbol(order.instrument).unwrap_or("?"),
            order.price.0,
            client_id.0
        ))
    }

    // Tracked for the reconciler: the venue order id to client id mapping, and the fill ids.
    pub(super) fn note_event(&mut self, event: &ExecEvent) {
        if let Some(venue_id) = event.venue_order_id
            && event.client_id != ClientOrderId(0)
            && !self
                .recent_orders
                .iter()
                .any(|order| order.venue_id == venue_id)
        {
            if self.recent_orders.len() == RECENT_ORDERS {
                self.recent_orders.remove(0);
            }
            self.recent_orders.push(RecentOrder {
                venue_id,
                client_id: event.client_id,
            });
        }
        if let Some(trade_id) = event.trade_id {
            if self.recent_trades.len() == RECENT_TRADES {
                self.recent_trades.remove(0);
            }
            self.recent_trades.push(trade_id);
        }
    }
}
