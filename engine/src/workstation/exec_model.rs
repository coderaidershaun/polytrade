//! The workstation's fold of the execution feed: which of this engine's orders exist and in what
//! state, what the account holds, the last refusal and the kill switch. The ladder and the account
//! band both read this one model, so they can never disagree about what is resting.
//!
//! The newest update replaces an order's cell, which is correct only because events arrive in
//! order. A lost event shows as a gap in their sequence, which the model counts and the monitor
//! reports, so a stale cell is never silent.

use super::history::grow_to;
use crate::hot::exec::{ExecHalt, OrderState, QuoteLevel, RejectOrigin};
use crate::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side};
use crate::msg::ui::{UI_ORDER_SNAPSHOT_MAX_TOTAL, UiEvent, UiWorkingOrder};
use crate::time::TsUs;

const WORKING_PER_SIDE: usize = 8;

/// Whether the venue has confirmed this order exists. The distinction an operator watching real
/// money has to make at a glance: "I think I have a bid there" and "the venue has confirmed a bid
/// there" are different facts, and so is "I no longer know".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderStatus {
    /// The venue has acknowledged it and may fill it.
    Confirmed,
    /// A command is outstanding — placed, cancelling, amending or replacing. Nothing about it may be
    /// assumed until the venue answers.
    InFlight,
    /// Venue truth was LOST (an ambiguous answer or a timeout). Not the same as in-flight: nothing
    /// is outstanding, we simply do not know whether this order exists.
    Lost,
}

impl OrderStatus {
    /// The ladder tag and the hover readout must call an order's state the same thing; there is one
    /// table so they cannot drift apart.
    pub(crate) fn word(self) -> &'static str {
        match self {
            Self::Confirmed => "live",
            Self::InFlight => "sent",
            Self::Lost => "lost",
        }
    }

    pub fn of(state: OrderState) -> Option<Self> {
        match state {
            OrderState::Live => Some(Self::Confirmed),
            OrderState::PendingNew | OrderState::CancelInFlight | OrderState::AmendInFlight => {
                Some(Self::InFlight)
            }
            OrderState::Unknown => Some(Self::Lost),
            OrderState::Free | OrderState::Closed(_) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderCell {
    pub client_id: ClientOrderId,
    pub quote_level: Option<QuoteLevel>,
    pub status: OrderStatus,
    pub price: Price,
    pub qty: Qty,
    pub filled: Qty,
    pub at: TsUs,
}

impl OrderCell {
    #[inline]
    pub fn remaining(self) -> Qty {
        self.qty.remaining_after(self.filled)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SideOrders {
    working: Vec<OrderCell>,
    /// Orders past the fixed detail capacity, kept by id so `leaked()` counts distinct working
    /// orders: a repeated report does not inflate it, and a final update can retire it.
    untracked: Vec<ClientOrderId>,
    /// How many orders the latest side snapshot left out. No ids are invented for them, and the
    /// next snapshot replaces the count.
    snapshot_overflow: u64,
}

impl SideOrders {
    pub fn working(&self) -> &[OrderCell] {
        &self.working
    }

    pub fn leaked(&self) -> u64 {
        self.snapshot_overflow + self.untracked.len() as u64
    }

    pub fn count(&self, status: OrderStatus) -> usize {
        self.working
            .iter()
            .filter(|order| order.status == status)
            .count()
    }

    fn lose_confirmation(&mut self, at: TsUs) {
        for order in &mut self.working {
            order.status = OrderStatus::Lost;
            order.at = at;
        }
    }

    fn apply(&mut self, client_id: ClientOrderId, cell: Option<OrderCell>) {
        let existing = self
            .working
            .iter()
            .position(|order| order.client_id == client_id);
        if let Some(index) = existing {
            match cell {
                Some(cell) => self.working[index] = cell,
                None => {
                    self.working.remove(index);
                }
            }
            return;
        }

        let untracked_index = self.untracked.iter().position(|held| *held == client_id);
        match (untracked_index, cell) {
            (Some(index), Some(cell)) if self.working.len() < WORKING_PER_SIDE => {
                self.untracked.remove(index);
                self.working.push(cell);
            }
            (Some(_), Some(_)) => {}
            (Some(index), None) => {
                self.untracked.remove(index);
            }
            (None, Some(cell)) if self.working.len() < WORKING_PER_SIDE => self.working.push(cell),
            (None, Some(_)) => self.untracked.push(client_id),
            (None, None) => {}
        }
    }

    fn replace_snapshot(
        &mut self,
        at: TsUs,
        detail_len: u8,
        total_working: u16,
        orders: &[UiWorkingOrder],
    ) {
        let detail_len = usize::from(detail_len);
        let Some(details) = orders.get(..detail_len) else {
            return;
        };
        let detail_count = u16::try_from(detail_len).ok();
        // Validating through `OrderStatus::of` rather than through `is_working` is what lets the
        // commit below be infallible: a state this rejects is exactly a state that has no cell.
        let valid = detail_count.is_some_and(|count| count <= total_working)
            && total_working <= UI_ORDER_SNAPSHOT_MAX_TOTAL
            && details
                .iter()
                .all(|order| OrderStatus::of(order.state).is_some())
            && details.iter().enumerate().all(|(index, order)| {
                details[..index]
                    .iter()
                    .all(|held| held.client_id != order.client_id)
            });
        if !valid {
            self.lose_confirmation(at);
            let identified = self.working.len().saturating_add(self.untracked.len()) as u64;
            self.snapshot_overflow = self
                .snapshot_overflow
                .max(u64::from(total_working).saturating_sub(identified));
            return;
        }

        self.working.clear();
        self.untracked.clear();
        self.snapshot_overflow = u64::from(total_working) - detail_len as u64;
        for order in details {
            let Some(status) = OrderStatus::of(order.state) else { continue };
            self.working.push(OrderCell {
                client_id: order.client_id,
                quote_level: order.quote_level,
                status,
                price: order.price,
                qty: order.qty,
                filled: order.filled,
                at,
            });
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BalanceCell {
    pub free: i64,
    pub locked: i64,
    pub at: TsUs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RejectCell {
    pub instrument: InstrumentId,
    pub side: Side,
    pub origin: RejectOrigin,
    pub at: TsUs,
}

#[derive(Debug, Default)]
pub struct ExecModel {
    sides: Vec<[SideOrders; 2]>,
    /// Keyed rather than indexed by [`AssetId`], because [`AssetId::UNKNOWN`] is `u16::MAX` and
    /// indexing by it would size the vector at 65536 entries.
    balances: Vec<(AssetId, BalanceCell)>,
    unknown_asset_balances: u64,
    last_reject: Option<RejectCell>,
    halt: Option<ExecHalt>,
}

impl ExecModel {
    pub fn with_capacity(instrument_count: usize) -> Self {
        let mut model = Self::default();
        model.configure(instrument_count);
        model
    }

    pub(crate) fn configure(&mut self, instrument_count: usize) {
        self.ensure_instruments(instrument_count);
    }

    pub(crate) fn apply_event(&mut self, event: &UiEvent) {
        match *event {
            UiEvent::OrderUpdate {
                instrument,
                event_ts_us,
                client_id,
                quote_level,
                side,
                state,
                price,
                qty,
                filled,
                ..
            } => {
                let cell = OrderStatus::of(state).map(|status| OrderCell {
                    client_id,
                    quote_level,
                    status,
                    price,
                    qty,
                    filled,
                    at: event_ts_us,
                });
                self.side_mut(instrument, side).apply(client_id, cell);
            }
            UiEvent::OrderSnapshot {
                instrument,
                event_ts_us,
                side,
                detail_len,
                total_working,
                ref orders,
                ..
            } => self.side_mut(instrument, side).replace_snapshot(
                event_ts_us,
                detail_len,
                total_working,
                orders,
            ),
            UiEvent::Balance {
                asset,
                event_ts_us,
                free,
                locked,
                ..
            } => self.apply_balance(
                asset,
                BalanceCell {
                    free,
                    locked,
                    at: event_ts_us,
                },
            ),
            UiEvent::Reject {
                instrument,
                event_ts_us,
                side,
                origin,
                ..
            } => {
                self.last_reject = Some(RejectCell {
                    instrument,
                    side,
                    origin,
                    at: event_ts_us,
                });
            }
            UiEvent::Execution { halt, .. } => self.halt = Some(halt),
            _ => {}
        }
    }

    pub(crate) fn note_events_lost(&mut self, at: TsUs) {
        for sides in &mut self.sides {
            for orders in sides {
                orders.lose_confirmation(at);
            }
        }
    }

    pub fn working(&self, instrument: InstrumentId, side: Side) -> &[OrderCell] {
        self.side(instrument, side).map_or(&[], SideOrders::working)
    }

    pub fn side(&self, instrument: InstrumentId, side: Side) -> Option<&SideOrders> {
        self.sides
            .get(instrument.0 as usize)
            .map(|sides| &sides[side as usize])
    }

    pub fn balance(&self, asset: AssetId) -> Option<BalanceCell> {
        self.balances
            .iter()
            .find(|(held, _)| *held == asset)
            .map(|(_, cell)| *cell)
    }

    pub fn unknown_asset_balances(&self) -> u64 {
        self.unknown_asset_balances
    }

    pub(crate) fn last_reject(&self) -> Option<RejectCell> {
        self.last_reject
    }

    pub fn halt(&self) -> Option<ExecHalt> {
        self.halt
    }

    fn apply_balance(&mut self, asset: AssetId, cell: BalanceCell) {
        if asset == AssetId::UNKNOWN {
            self.unknown_asset_balances += 1;
            return;
        }
        match self.balances.iter_mut().find(|(held, _)| *held == asset) {
            Some((_, slot)) => *slot = cell,
            None => self.balances.push((asset, cell)),
        }
    }

    fn side_mut(&mut self, instrument: InstrumentId, side: Side) -> &mut SideOrders {
        let index = instrument.0 as usize;
        self.ensure_instruments(index + 1);
        &mut self.sides[index][side as usize]
    }

    fn ensure_instruments(&mut self, len: usize) {
        grow_to(&mut self.sides, len, <[SideOrders; 2]>::default);
    }
}
