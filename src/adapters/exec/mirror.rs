//! The orders this run believes are resting, which the exit sweep cancels. The mirror owns its
//! copies, because after a hot-thread panic anything it borrowed may already be gone.

use crate::ids::{ClientOrderId, InstrumentId, Price, Qty, Side};
use crate::msg::exec::{ExecEvent, Provenance};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MirrorInsertError {
    DuplicateClientId,
    StorageExhausted,
}

/// One order this run believes is resting on the venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MirroredOrder {
    pub instrument: InstrumentId,
    pub client_id: ClientOrderId,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub provenance: Provenance,
    pub has_sent_cancel: bool,
    pub is_ambiguous: bool,
}

impl MirroredOrder {
    pub(crate) fn observed(event: &ExecEvent) -> Self {
        Self {
            instrument: event.instrument,
            client_id: event.client_id,
            side: event.side,
            price: event.price,
            qty: event.qty,
            provenance: event.provenance,
            has_sent_cancel: false,
            is_ambiguous: false,
        }
    }
}

/// Fixed-capacity set of the orders this run believes are live, keyed by client id.
#[derive(Debug)]
pub struct OrderMirror {
    orders: Vec<MirroredOrder>,
    capacity: usize,
}

impl OrderMirror {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            orders: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub fn as_slice(&self) -> &[MirroredOrder] {
        &self.orders
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The key is held here alone. Read-only callers that hand-roll the scan would each have to be
    /// found again if the key ever widens — foreign orders all collide on `ClientOrderId(0)`, which
    /// is the change already on the table.
    pub fn find(&self, client_id: ClientOrderId) -> Option<&MirroredOrder> {
        self.orders
            .iter()
            .find(|order| order.client_id == client_id)
    }

    pub fn find_mut(&mut self, client_id: ClientOrderId) -> Option<&mut MirroredOrder> {
        self.orders
            .iter_mut()
            .find(|order| order.client_id == client_id)
    }

    pub fn remove(&mut self, client_id: ClientOrderId) {
        self.orders.retain(|order| order.client_id != client_id);
    }

    /// Reserve identity before bytes reach venue. Failure fatal -> sending after fails creates
    /// unnamed order no shutdown sweep can touch.
    pub fn insert(&mut self, order: MirroredOrder) -> Result<(), MirrorInsertError> {
        if self
            .orders
            .iter()
            .any(|existing| existing.client_id == order.client_id)
        {
            return Err(MirrorInsertError::DuplicateClientId);
        }
        if self.orders.len() >= self.capacity {
            return Err(MirrorInsertError::StorageExhausted);
        }
        self.orders.push(order);
        Ok(())
    }

    /// Refresh venue fields without clearing lifecycle latches. Open-orders snapshot can't
    /// clear has_sent_cancel (only correlated status answer proves cancel was answered).
    pub fn refresh(&mut self, order: MirroredOrder) -> bool {
        let Some(existing) = self.find_mut(order.client_id) else {
            return false;
        };
        existing.instrument = order.instrument;
        existing.side = order.side;
        existing.price = order.price;
        existing.qty = order.qty;
        existing.provenance = order.provenance;
        true
    }

    /// Unproven non-existence consumes side capacity.
    pub fn possibly_live_count(&self, instrument: InstrumentId, side: Side) -> usize {
        self.orders
            .iter()
            .filter(|order| {
                order.instrument == instrument
                    && order.side == side
                    && !matches!(order.provenance, Provenance::Foreign)
            })
            .count()
    }

    /// Dropped socket -> all orders guesses. Re-arm cancels (may not have reached venue).
    /// Entries stay (still resting).
    pub fn mark_all_stale(&mut self) {
        for order in &mut self.orders {
            order.is_ambiguous = true;
            order.has_sent_cancel = false;
        }
    }

    /// Cancel answered, next can go. Latch prevents concurrent unanswered cancels. Left latched
    /// -> excluded from all sweeps (including shutdown) -> exits leaving it resting + unknown.
    pub fn re_arm_cancel(&mut self, client_id: ClientOrderId) {
        let Some(order) = self.find_mut(client_id) else {
            return;
        };
        order.has_sent_cancel = false;
    }

    /// Orders by provenance + instrument, cancel not yet sent.
    pub fn cancellable(
        &self,
        instrument: Option<InstrumentId>,
        provenance: Provenance,
    ) -> Vec<(InstrumentId, ClientOrderId)> {
        self.matching(instrument)
            .filter(|order| order.provenance == provenance && !order.has_sent_cancel)
            .map(|order| (order.instrument, order.client_id))
            .collect()
    }

    /// Cancels sent, state never settled. Two flags: has_sent_cancel=true + is_ambiguous=true.
    pub fn unresolved(
        &self,
        instrument: Option<InstrumentId>,
    ) -> Vec<(InstrumentId, ClientOrderId)> {
        self.matching(instrument)
            .filter(|order| order.has_sent_cancel && order.is_ambiguous)
            .map(|order| (order.instrument, order.client_id))
            .collect()
    }

    fn matching(&self, instrument: Option<InstrumentId>) -> impl Iterator<Item = &MirroredOrder> {
        self.orders
            .iter()
            .filter(move |order| instrument.is_none_or(|wanted| order.instrument == wanted))
    }

    /// Any order this run owns still live (Foreign never counts -> sweep would never finish).
    pub fn has_ours(&self) -> bool {
        self.orders
            .iter()
            .any(|order| !matches!(order.provenance, Provenance::Foreign))
    }

    pub fn has_prior_run(&self) -> bool {
        self.orders
            .iter()
            .any(|order| order.provenance == Provenance::PriorRun)
    }
}
