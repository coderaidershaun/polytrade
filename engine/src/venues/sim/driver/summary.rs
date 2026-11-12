//! End-of-run simulator counters.

use super::super::core::queue::QueueAhead;
use super::super::core::{SimEmission, VenueEvent};
use super::SimExecDriver;
use crate::info;

#[derive(Debug, Default)]
pub(super) struct SimRunSummary {
    joins: u64,
    refusals: u64,
    fills: u64,
    full_fills: u64,
    cancels: u64,
    amends: u64,
    resets: u64,
    filled_base: i128,
    filled_quote: i128,
}

impl SimExecDriver {
    pub fn log_summary(&self) {
        self.summary.log();
    }
}

impl SimRunSummary {
    fn log(&self) {
        info!(
            "sim SUMMARY joins={} refusals={} fills={} full_fills={} cancels={} amends={} resets={} filled_base={} filled_quote={}",
            self.joins,
            self.refusals,
            self.fills,
            self.full_fills,
            self.cancels,
            self.amends,
            self.resets,
            self.filled_base,
            self.filled_quote
        );
    }

    pub(super) fn observe(&mut self, emission: &SimEmission) {
        match emission.event {
            VenueEvent::Rested {
                snapshot,
                queue_ahead,
            } => {
                self.joins += 1;
                let ahead = match queue_ahead {
                    QueueAhead::Known(qty) => qty.0.to_string(),
                    QueueAhead::Unobservable => "unknown".to_owned(),
                };
                info!(
                    "sim JOIN client={} side={:?} price={} qty={} queue_ahead={ahead}",
                    snapshot.order.client_id.0,
                    snapshot.order.side,
                    snapshot.order.price.0,
                    snapshot.order.qty.0
                );
            }
            VenueEvent::PostOnlyCrossed { snapshot } => {
                self.refusals += 1;
                info!(
                    "sim REFUSE client={} side={:?} price={} reason=post_only_cross",
                    snapshot.order.client_id.0, snapshot.order.side, snapshot.order.price.0
                );
            }
            VenueEvent::PlaceRefused { snapshot, reason } => {
                self.refusals += 1;
                info!(
                    "sim REFUSE client={} side={:?} price={} reason={reason:?}",
                    snapshot.order.client_id.0, snapshot.order.side, snapshot.order.price.0
                );
            }
            VenueEvent::Filled {
                snapshot,
                trade_id,
                settlement,
                ..
            } => {
                self.fills += 1;
                self.filled_base += i128::from(settlement.last_qty.0);
                self.filled_quote += i128::from(settlement.last_quote);
                if snapshot.order.is_complete() {
                    self.full_fills += 1;
                }
                let resting_us = emission
                    .at_ts_us
                    .diff(snapshot.joined_ts_us)
                    .micros()
                    .max(0);
                info!(
                    "sim FILL client={} side={:?} price={} last_qty={} cumulative_qty={} trade_id={} prints_seen={} resting_us={resting_us}",
                    snapshot.order.client_id.0,
                    snapshot.order.side,
                    snapshot.order.price.0,
                    settlement.last_qty.0,
                    settlement.cumulative_qty.0,
                    trade_id.0,
                    snapshot.prints_seen
                );
            }
            VenueEvent::Canceled { snapshot } => {
                self.cancels += 1;
                info!(
                    "sim CANCEL client={} side={:?} price={} filled={} prints_seen={} resyncs={}",
                    snapshot.order.client_id.0,
                    snapshot.order.side,
                    snapshot.order.price.0,
                    snapshot.order.filled.0,
                    snapshot.prints_seen,
                    snapshot.resyncs_while_resting
                );
            }
            VenueEvent::Amended {
                snapshot,
                total_qty,
            } => {
                self.amends += 1;
                info!(
                    "sim AMEND client={} side={:?} price={} total_qty={} filled={}",
                    snapshot.order.client_id.0,
                    snapshot.order.side,
                    snapshot.order.price.0,
                    total_qty.0,
                    snapshot.order.filled.0
                );
            }
            VenueEvent::MarketReset { reason } => {
                self.resets += 1;
                info!("sim RESET reason={reason:?}");
            }
            VenueEvent::CancelRefused { .. } | VenueEvent::AmendRefused { .. } => {
                self.refusals += 1;
            }
            VenueEvent::OrderStatus { .. }
            | VenueEvent::NoSuchOrder { .. }
            | VenueEvent::OpenOrders { .. }
            | VenueEvent::StreamSubscribed => {}
        }
    }
}
