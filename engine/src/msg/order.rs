//! The words of an order's life that leave the fold: the ladder level, the state, why an order was
//! refused and why execution halted. The records, the link and the screen read them as the fold
//! wrote them.

use core::fmt;
use core::mem::discriminant;

use crate::msg::exec::RejectClass;
use crate::msg::labelled_enum::labelled_enum;
use crate::msg::log_value::{LogValue, Spec};
use crate::msg::persist::OrderLifecycle;

/// The declarative ladder is fixed-size so quote publication, reconciliation and the UI all share
/// one compile-time bound. Configuration chooses how much of it is active, never its allocation.
pub const MAX_QUOTE_LEVELS: usize = 8;

/// A stable ladder identity, not a position in a sorted list: a price change never makes one live
/// order masquerade as another level, and an absent level has exactly one meaning, which is to
/// withdraw the order carrying that identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QuoteLevel(u8);

impl QuoteLevel {
    pub const ZERO: Self = Self(0);

    /// Every rung, in ladder order. A pass over the ladder walks this rather than round-tripping
    /// each index through the fallible constructor.
    pub const ALL: [Self; MAX_QUOTE_LEVELS] = {
        let mut levels = [Self(0); MAX_QUOTE_LEVELS];
        let mut index = 0;
        while index < MAX_QUOTE_LEVELS {
            levels[index] = Self(index as u8);
            index += 1;
        }
        levels
    };

    #[inline]
    pub const fn new(value: u8) -> Option<Self> {
        if (value as usize) < MAX_QUOTE_LEVELS { Some(Self(value)) } else { None }
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.0 as usize
    }

    #[inline]
    pub const fn get(self) -> u8 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CloseReason {
    Filled,
    Canceled,
    Rejected,
    Expired,
    ReconciledGone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderState {
    Free,
    PendingNew,
    Live,
    CancelInFlight,
    AmendInFlight,
    Unknown,
    Closed(CloseReason),
}

impl OrderState {
    #[inline]
    pub fn is_in_flight(self) -> bool {
        matches!(
            self,
            OrderState::PendingNew
                | OrderState::CancelInFlight
                | OrderState::AmendInFlight
                | OrderState::Unknown
        )
    }

    #[inline]
    pub fn is_terminal(self) -> bool {
        matches!(self, OrderState::Free | OrderState::Closed(_))
    }

    #[inline]
    pub fn is_working(self) -> bool {
        !self.is_terminal()
    }
}

impl From<OrderState> for OrderLifecycle {
    fn from(state: OrderState) -> Self {
        match state {
            OrderState::Free => OrderLifecycle::Free,
            OrderState::PendingNew => OrderLifecycle::PendingNew,
            OrderState::Live => OrderLifecycle::Live,
            OrderState::CancelInFlight => OrderLifecycle::CancelInFlight,
            OrderState::AmendInFlight => OrderLifecycle::AmendInFlight,
            OrderState::Unknown => OrderLifecycle::Unknown,
            OrderState::Closed(CloseReason::Filled) => OrderLifecycle::ClosedFilled,
            OrderState::Closed(CloseReason::Canceled) => OrderLifecycle::ClosedCanceled,
            OrderState::Closed(CloseReason::Rejected) => OrderLifecycle::ClosedRejected,
            OrderState::Closed(CloseReason::Expired) => OrderLifecycle::ClosedExpired,
            OrderState::Closed(CloseReason::ReconciledGone) => OrderLifecycle::ClosedReconciledGone,
        }
    }
}

labelled_enum! {
    /// The fact an unready engine is still waiting for.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ReadinessGap {
        /// Stream not subscribed or reset invalidated it.
        Stream = "account stream",
        /// No balance snapshot consumed.
        Balances = "balances",
        /// Open-order snapshot not landed.
        OpenOrders = "open orders",
    }
    pub fn label;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RejectReason {
    QtyBelowMin,
    NotionalBelowMin,
    NotionalAboveMax,
    WouldCross,
    OutsideBand,
    Underfunded,
    NotReady(ReadinessGap),
    Halted,
    SessionReducingOnly,
    ExposureCeiling,
    NoQuoteDeclared,
    BookNotQuotable,
    DuplicatePrice,
    OrderLimit,
    /// The venue's account-wide placement budget for this run is spent.
    RateBudget,
    /// Declared before the market opened, or inside the margin before it closes.
    OutsideWindow,
    /// Declared for a market whose warmup has not passed: the quote is held until it is live.
    Warming,
    /// Each idle slot of the level is retired: its next client id would wrap to one already sent.
    IdsSpent,
}

impl RejectReason {
    /// Operator words for the refusal an order carries onto the screen. The match is exhaustive, so
    /// a new reason cannot ship as a bare Rust identifier the way a `Debug` rendering would let it.
    pub fn label(self) -> &'static str {
        match self {
            Self::QtyBelowMin => "quantity below venue minimum",
            Self::NotionalBelowMin => "notional below venue minimum",
            Self::NotionalAboveMax => "notional above configured maximum",
            Self::WouldCross => "would cross the book",
            Self::OutsideBand => "outside the price band",
            Self::Underfunded => "underfunded",
            Self::NotReady(ReadinessGap::Stream) => "waiting for the account stream",
            Self::NotReady(ReadinessGap::Balances) => "waiting for balances",
            Self::NotReady(ReadinessGap::OpenOrders) => "waiting for open orders",
            Self::Halted => "halted",
            Self::SessionReducingOnly => "session is reducing only",
            Self::ExposureCeiling => "exposure ceiling reached",
            Self::NoQuoteDeclared => "no quote declared",
            Self::BookNotQuotable => "book not quotable",
            Self::DuplicatePrice => "duplicate price on this side",
            Self::OrderLimit => "order limit reached",
            Self::RateBudget => "order rate budget spent",
            Self::OutsideWindow => "outside the trading window",
            Self::Warming => "market still warming up",
            Self::IdsSpent => "client ids of this level spent",
        }
    }
}

/// Every kind of refusal, in the order whose place a hot log record carries. `NotReady` stands for
/// each gap here, and the gap itself travels in the second word.
const REJECT_KINDS: [RejectReason; 18] = [
    RejectReason::QtyBelowMin,
    RejectReason::NotionalBelowMin,
    RejectReason::NotionalAboveMax,
    RejectReason::WouldCross,
    RejectReason::OutsideBand,
    RejectReason::Underfunded,
    RejectReason::NotReady(ReadinessGap::Stream),
    RejectReason::Halted,
    RejectReason::SessionReducingOnly,
    RejectReason::ExposureCeiling,
    RejectReason::NoQuoteDeclared,
    RejectReason::BookNotQuotable,
    RejectReason::DuplicatePrice,
    RejectReason::OrderLimit,
    RejectReason::RateBudget,
    RejectReason::OutsideWindow,
    RejectReason::Warming,
    RejectReason::IdsSpent,
];

/// Prints what `{:?}` prints, `NotReady(Stream)` included, whatever the placeholder: the operator
/// words live in [`RejectReason::label`], and log lines have always shown the variant.
impl LogValue for RejectReason {
    const WORDS: usize = 2;

    fn to_words(self, words: &mut [u64]) {
        let kind = REJECT_KINDS
            .iter()
            .position(|kind| discriminant(kind) == discriminant(&self));
        debug_assert!(kind.is_some(), "{self:?} is missing from REJECT_KINDS");
        words[0] = kind.map_or(u64::MAX, |kind| kind as u64);
        match self {
            Self::NotReady(gap) => gap.to_words(&mut words[1..]),
            _ => words[1] = 0,
        }
    }

    fn render(words: &[u64], _spec: Spec, out: &mut dyn fmt::Write) -> fmt::Result {
        let kind = usize::try_from(words[0])
            .ok()
            .and_then(|kind| REJECT_KINDS.get(kind));
        match kind {
            Some(Self::NotReady(_)) => {
                out.write_str("NotReady(")?;
                ReadinessGap::render(&words[1..], Spec::Debug, out)?;
                out.write_str(")")
            }
            Some(kind) => write!(out, "{kind:?}"),
            None => out.write_str("?"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RejectOrigin {
    Local(RejectReason),
    Venue { class: RejectClass, code: i32 },
}

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum HaltReason {
        RejectStreak = "reject streak",
        RealisedLoss = "realised loss",
        FatalReject = "fatal reject",
        SlotLeak = "slot leak",
        FilterViolation = "filter violation",
        DuplicateResting = "duplicate resting order",
        CommandBankOverflow = "command bank overflow",
        SealNotAccepted = "persistence seal not accepted",
        OrphanFill = "fill on an order with no slot",
        PriorRunOverflow = "prior-run order table full",
        StatusProbesSpent = "order status probes spent",
        StreamSilent = "order stream silent",
        WriterFailed = "file writer failed",
        PriorRunFillUnbooked = "prior-run fill not booked",
        UnprovenOrderResting = "an order the engine cannot prove its own rests on its market",
        AuditBankFull = "audit bank full",
    }
    /// Operator words for the halt banner: a halted engine's reason is read under pressure, and the
    /// table sits beside the variants so a new reason cannot ship as a bare Rust identifier.
    pub fn label;
}
