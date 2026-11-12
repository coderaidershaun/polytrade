//! Typed simulated venue answers before wire encoding.

use crate::ids::{ClientOrderId, Qty, TradeId};
use crate::msg::exec::VenueOrderStatus;
use crate::time::TsUs;

use super::super::orders::SimOrder;
use super::super::resting::RefusalReason;
use super::super::wallet::FillSettlement;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rejection {
    WouldMatchImmediately,
    InsufficientBalance,
    FilterFailure,
    TooManyOrders,
    CancelRejected,
    NoSuchOrder,
    AmendBudgetSpent,
    AmendQuantityIncrease,
    AmendFilterFailure,
}

impl Rejection {
    pub const fn of(reason: RefusalReason) -> Self {
        match reason {
            RefusalReason::TickGrid
            | RefusalReason::StepGrid
            | RefusalReason::MinQty
            | RefusalReason::MinNotional
            | RefusalReason::StyleNotPermitted => Rejection::FilterFailure,
            RefusalReason::MaxOrders => Rejection::TooManyOrders,
            RefusalReason::InsufficientFunds => Rejection::InsufficientBalance,
            RefusalReason::NoSuchOrder | RefusalReason::OrderGone => Rejection::CancelRejected,
            RefusalReason::AmendBudgetSpent => Rejection::AmendBudgetSpent,
            RefusalReason::AmendQuantityIncrease => Rejection::AmendQuantityIncrease,
            RefusalReason::AmendFilterFailure => Rejection::AmendFilterFailure,
        }
    }

    pub const fn has_rejected_report(self) -> bool {
        matches!(
            self,
            Rejection::WouldMatchImmediately | Rejection::InsufficientBalance
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AnswerSubject {
    Place,
    Cancel,
    Amend,
    Query,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueAnswer {
    pub event_ts_us: TsUs,
    pub due_ts_us: TsUs,
    pub voice: VenueVoice,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VenueVoice {
    Response(VenueAnswer),
    Report(VenueReport),
    Synthesised(SynthesisedEvent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VenueAnswer {
    PlaceAccepted(SimOrder),
    CancelAccepted(SimOrder),
    AmendAccepted(SimOrder),
    Status {
        order: SimOrder,
        status: VenueOrderStatus,
    },
    OpenOrders(Vec<SimOrder>),
    Refused {
        client_id: ClientOrderId,
        rejection: Rejection,
        about: AnswerSubject,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueReport {
    New(SimOrder),
    Trade {
        order: SimOrder,
        trade_id: TradeId,
        settlement: FillSettlement,
    },
    Canceled(SimOrder),
    Rejected(SimOrder),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SynthesisedEvent {
    PlaceNotSent(SimOrder),
    StreamSubscribed,
    StreamReset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TerminalHalf {
    Ack,
    Report,
}

impl VenueVoice {
    pub(super) fn terminal_half(&self) -> Option<TerminalHalf> {
        match self {
            VenueVoice::Response(answer) => match answer {
                VenueAnswer::CancelAccepted(_) => Some(TerminalHalf::Ack),
                VenueAnswer::Refused {
                    about: AnswerSubject::Place,
                    ..
                } => Some(TerminalHalf::Ack),
                _ => None,
            },
            VenueVoice::Report(VenueReport::Canceled(_) | VenueReport::Rejected(_)) => {
                Some(TerminalHalf::Report)
            }
            VenueVoice::Report(VenueReport::Trade { order, .. }) if order.is_complete() => {
                Some(TerminalHalf::Report)
            }
            VenueVoice::Synthesised(SynthesisedEvent::PlaceNotSent(_)) => {
                Some(TerminalHalf::Report)
            }
            _ => None,
        }
    }

    pub(super) fn cumulative_qty(&self) -> Option<Qty> {
        let order = match self {
            VenueVoice::Response(answer) => match answer {
                VenueAnswer::PlaceAccepted(order)
                | VenueAnswer::CancelAccepted(order)
                | VenueAnswer::AmendAccepted(order)
                | VenueAnswer::Status { order, .. } => order,
                VenueAnswer::OpenOrders(_) | VenueAnswer::Refused { .. } => return None,
            },
            VenueVoice::Report(report) => match report {
                VenueReport::New(order)
                | VenueReport::Trade { order, .. }
                | VenueReport::Canceled(order)
                | VenueReport::Rejected(order) => order,
            },
            VenueVoice::Synthesised(SynthesisedEvent::PlaceNotSent(order)) => order,
            VenueVoice::Synthesised(_) => return None,
        };
        Some(order.filled)
    }
}

pub(super) const fn refusal(
    client_id: ClientOrderId,
    rejection: Rejection,
    about: AnswerSubject,
) -> VenueVoice {
    VenueVoice::Response(VenueAnswer::Refused {
        client_id,
        rejection,
        about,
    })
}
