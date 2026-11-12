//! The execution half of the UI event lane: order state, fill liquidity, refusals, and the
//! halt latch.

use crate::hot::exec::{
    CloseReason, ExecHalt, HaltReason, OrderState, ReadinessGap, RejectOrigin, RejectReason,
};
use crate::msg::exec::{Liquidity, RejectClass};
use crate::time::TsUs;

use super::super::envelope::{ByteReader, ByteWriter, LinkDecodeError};
use super::super::wire::{WireField, wire_enum};

pub(super) const REJECT_ORIGIN_LEN: usize = 1 + 1 + 4;
pub(super) const HALT_LEN: usize = 1 + 1 + 8;

// Closed states are flattened in alongside the open ones rather than carried in a separate
// reason byte, which fills the tail exactly. Discriminants start at 1, so an all-zero byte
// is invalid rather than a valid state.
wire_enum! {
    OrderState, "order state";
    (OrderState::Free) = 1,
    (OrderState::PendingNew) = 2,
    (OrderState::Live) = 3,
    (OrderState::CancelInFlight) = 4,
    (OrderState::AmendInFlight) = 5,
    (OrderState::Unknown) = 6,
    (OrderState::Closed(CloseReason::Filled)) = 7,
    (OrderState::Closed(CloseReason::Canceled)) = 8,
    (OrderState::Closed(CloseReason::Rejected)) = 9,
    (OrderState::Closed(CloseReason::Expired)) = 10,
    (OrderState::Closed(CloseReason::ReconciledGone)) = 11,
}

// Absent is zero here rather than 1-based, since "the venue didn't say" is harmless — an
// all-zero byte decodes to anything but `Maker`.
wire_enum! {
    Option<Liquidity>, "liquidity";
    (None) = 0,
    (Some(Liquidity::Maker)) = 1,
    (Some(Liquidity::Taker)) = 2,
}

// Each gap in the numbering is a discriminant with no payload of its own. The local arm's
// 4 padding bytes line up with the venue arm's code field; giving that space dual meaning
// would only confuse a reader.
wire_enum! {
    RejectReason, "reject reason";
    (RejectReason::QtyBelowMin) = 1,
    (RejectReason::NotionalBelowMin) = 2,
    (RejectReason::NotionalAboveMax) = 3,
    (RejectReason::WouldCross) = 4,
    (RejectReason::OutsideBand) = 5,
    (RejectReason::Underfunded) = 6,
    (RejectReason::StyleNotPermitted) = 7,
    (RejectReason::NotReady(ReadinessGap::Stream)) = 8,
    (RejectReason::NotReady(ReadinessGap::Balances)) = 9,
    (RejectReason::NotReady(ReadinessGap::OpenOrders)) = 10,
    (RejectReason::Halted) = 11,
    (RejectReason::SessionReducingOnly) = 12,
    (RejectReason::ExposureCeiling) = 13,
    (RejectReason::NoQuoteDeclared) = 14,
    (RejectReason::BookNotQuotable) = 15,
    (RejectReason::DuplicatePrice) = 16,
    (RejectReason::OrderLimit) = 17,
    (RejectReason::OutsideWindow) = 18,
    (RejectReason::RateBudget) = 19,
}

// `Refused` is appended rather than slotted beside `Gone`, so splitting the class renumbered nothing
// a workstation already on the wire would decode differently.
wire_enum! {
    RejectClass, "reject class";
    (RejectClass::StillLive) = 1,
    (RejectClass::Gone) = 2,
    (RejectClass::Ambiguous) = 3,
    (RejectClass::Fatal) = 4,
    (RejectClass::Refused) = 5,
}

// Reasons are appended (never renumbered) so old workstations decode correctly.
wire_enum! {
    HaltReason, "halt reason";
    (HaltReason::RejectStreak) = 1,
    (HaltReason::RealisedLoss) = 2,
    (HaltReason::FatalReject) = 3,
    (HaltReason::SlotLeak) = 4,
    (HaltReason::FilterViolation) = 5,
    (HaltReason::CommandBankOverflow) = 6,
    (HaltReason::DuplicateResting) = 7,
}

const REJECT_LOCAL: u8 = 1;
const REJECT_VENUE: u8 = 2;
const HALT_ARMED: u8 = 1;
const HALT_HALTED: u8 = 2;

/// The local arm pads out the venue arm's code field; the reader simply skips over it.
impl WireField for RejectOrigin {
    fn write(&self, writer: &mut ByteWriter<'_>) {
        match *self {
            RejectOrigin::Local(reason) => {
                REJECT_LOCAL.write(writer);
                reason.write(writer);
                0_i32.write(writer);
            }
            RejectOrigin::Venue { class, code } => {
                REJECT_VENUE.write(writer);
                class.write(writer);
                code.write(writer);
            }
        }
    }

    fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
        let tag = u8::read(reader)?;
        match tag {
            REJECT_LOCAL => {
                let reason = WireField::read(reader)?;
                let _padding = i32::read(reader)?;
                Ok(RejectOrigin::Local(reason))
            }
            REJECT_VENUE => Ok(RejectOrigin::Venue {
                class: WireField::read(reader)?,
                code: WireField::read(reader)?,
            }),
            _ => Err(LinkDecodeError::unknown("reject origin", tag)),
        }
    }
}

impl WireField for ExecHalt {
    fn write(&self, writer: &mut ByteWriter<'_>) {
        match *self {
            ExecHalt::Armed => {
                HALT_ARMED.write(writer);
                0_u8.write(writer);
                0_i64.write(writer);
            }
            ExecHalt::Halted {
                reason,
                halted_ts_us,
            } => {
                HALT_HALTED.write(writer);
                reason.write(writer);
                halted_ts_us.write(writer);
            }
        }
    }

    fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
        let tag = u8::read(reader)?;
        match tag {
            HALT_ARMED => {
                let _padding = u8::read(reader)?;
                let _stamp = TsUs::read(reader)?;
                Ok(ExecHalt::Armed)
            }
            HALT_HALTED => Ok(ExecHalt::Halted {
                reason: WireField::read(reader)?,
                halted_ts_us: WireField::read(reader)?,
            }),
            _ => Err(LinkDecodeError::unknown("halt", tag)),
        }
    }
}
