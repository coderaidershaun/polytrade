//! Turns hot-thread commands into wire requests. The phase gate is checked per command: only
//! Quoting admits new orders, while every connected phase admits cancels, since cancels are
//! the exit path when something has failed.

use crate::ids::{ClientOrderId, InstrumentId, Price, Qty, Side};
use crate::msg::exec::{ExecCommand, ExecLaneItem, OrderStyle, Provenance};

use super::super::effect::{ExecEffect, ExecRequest, Outgoing, PlaceNotSentReason};
use super::super::mirror::{MirrorInsertError, MirroredOrder};
use super::ExecCore;

impl ExecCore {
    /// Empties the command lane and folds every command through the phase machine, returning what
    /// has to reach the venue. A simulated venue's watermarks ride the same lane and belong to it
    /// alone, so a live edge steps over them.
    pub(crate) fn drain_commands(
        &mut self,
        commands: &mut rtrb::Consumer<ExecLaneItem>,
    ) -> Vec<Outgoing> {
        let mut outgoing = Vec::new();
        while let Ok(item) = commands.pop() {
            let ExecLaneItem::Command(stamped) = item else {
                continue;
            };
            let command = stamped.command;
            let recon_seq = command.recon_seq();
            self.on_command(command, &mut Outgoing::sink(&mut outgoing, recon_seq));
        }
        outgoing
    }

    /// Phase is the gate; refused commands are normal, not errors.
    pub fn on_command(&mut self, command: ExecCommand, emit: &mut dyn FnMut(ExecEffect)) {
        match command {
            ExecCommand::Place {
                instrument,
                client_id,
                side,
                price,
                qty,
                style,
            } => self.on_place(
                Placement {
                    instrument,
                    client_id,
                    side,
                    price,
                    qty,
                    style,
                },
                emit,
            ),
            ExecCommand::Cancel {
                instrument,
                client_id,
            } => self.cancel_one(instrument, client_id, emit),
            ExecCommand::AmendQty {
                instrument,
                client_id,
                qty,
            } => {
                if !self.phase.admits_new_orders() {
                    emit(ExecEffect::AmendNotSent {
                        instrument,
                        client_id,
                    });
                    return;
                }
                self.send(
                    ExecRequest::AmendQty {
                        instrument,
                        client_id,
                        qty,
                    },
                    emit,
                );
            }
            ExecCommand::ReconcileOrder {
                instrument,
                client_id,
                ..
            } => self.send(
                ExecRequest::OrderStatus {
                    instrument,
                    client_id,
                },
                emit,
            ),
            ExecCommand::ReconcileOpenOrders { instrument, .. } => {
                self.send(ExecRequest::OpenOrders { instrument }, emit);
            }
            ExecCommand::CancelOurs { instrument, reason } => {
                self.begin_sweep(reason, Some(instrument), emit);
            }
            ExecCommand::CancelPriorRun { instrument } => {
                self.cancel_matching(Some(instrument), Provenance::PriorRun, emit);
            }
        }
    }

    fn on_place(&mut self, placement: Placement, emit: &mut dyn FnMut(ExecEffect)) {
        let Placement {
            instrument,
            client_id,
            side,
            price,
            qty,
            style,
        } = placement;
        if !self.phase.admits_new_orders() {
            self.refuse_place(
                instrument,
                client_id,
                side,
                PlaceNotSentReason::PhaseClosed,
                emit,
            );
            return;
        }
        if self.mirror.possibly_live_count(instrument, side) >= self.max_orders_per_side {
            self.refuse_place(
                instrument,
                client_id,
                side,
                PlaceNotSentReason::SideCapacity,
                emit,
            );
            return;
        }
        let reservation = MirroredOrder {
            instrument,
            client_id,
            side,
            price,
            qty,
            provenance: Provenance::Mine,
            has_sent_cancel: false,
            is_ambiguous: false,
        };
        if let Err(error) = self.mirror.insert(reservation) {
            self.stop_quoting();
            let reason = match error {
                MirrorInsertError::DuplicateClientId => PlaceNotSentReason::DuplicateClientId,
                MirrorInsertError::StorageExhausted => PlaceNotSentReason::MirrorStorage,
            };
            self.refuse_place(instrument, client_id, side, reason, emit);
            return;
        }
        self.send(
            ExecRequest::Place {
                instrument,
                client_id,
                side,
                price,
                qty,
                style,
            },
            emit,
        );
    }
}

struct Placement {
    instrument: InstrumentId,
    client_id: ClientOrderId,
    side: Side,
    price: Price,
    qty: Qty,
    style: OrderStyle,
}
