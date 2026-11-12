//! Polymarket accepts no client order id, so nothing the engine minted ever comes back. The only
//! correlation that exists is the map built here: a place answer names an `orderID`, and every later
//! stream event, cancel and read names that id instead of ours.
//!
//! The consequence worth stating plainly is that an order is UNATTRIBUTABLE between the moment its
//! bytes leave and the moment its answer lands. The mirror still reserves the slot first (so a
//! shutdown sweep can reach it), but a stream event racing the HTTP answer resolves to nothing here
//! and the driver must hold it rather than discard it.

use crate::ids::{ClientOrderId, InstrumentId, TradeId, VenueOrderId};

/// An order this run placed, resolved from the venue's own id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KnownOrder {
    pub client_id: ClientOrderId,
    pub instrument: InstrumentId,
}

#[derive(Debug, Clone)]
struct IndexRow {
    venue_order_id: Box<str>,
    known: KnownOrder,
}

/// Fixed-capacity `orderID` ↔ [`ClientOrderId`] map. Sized at startup like every other execution
/// structure; a full index is a bug in the caller's retirement policy, not a growth event.
#[derive(Debug)]
pub struct OrderIndex {
    rows: Vec<IndexRow>,
    capacity: usize,
}

impl OrderIndex {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Adopting an unmapped venue order re-points an existing client id, so a repeated id REPLACES
    /// rather than duplicating: two rows naming one order would resolve nondeterministically.
    ///
    /// # Errors
    /// The index is full and the id is new.
    pub fn record(
        &mut self,
        venue_order_id: &str,
        known: KnownOrder,
    ) -> Result<(), OrderIndexFull> {
        if let Some(row) = self
            .rows
            .iter_mut()
            .find(|row| &*row.venue_order_id == venue_order_id)
        {
            row.known = known;
            return Ok(());
        }
        if self.rows.len() >= self.capacity {
            return Err(OrderIndexFull {
                capacity: self.capacity,
            });
        }
        self.rows.push(IndexRow {
            venue_order_id: venue_order_id.into(),
            known,
        });
        Ok(())
    }

    pub fn resolve(&self, venue_order_id: &str) -> Option<KnownOrder> {
        self.rows
            .iter()
            .find(|row| &*row.venue_order_id == venue_order_id)
            .map(|row| row.known)
    }

    pub fn venue_order_id(&self, client_id: ClientOrderId) -> Option<&str> {
        self.rows
            .iter()
            .find(|row| row.known.client_id == client_id)
            .map(|row| &*row.venue_order_id)
    }

    /// Terminal state reached; the id will never be named again.
    pub fn forget(&mut self, client_id: ClientOrderId) -> bool {
        let before = self.rows.len();
        self.rows.retain(|row| row.known.client_id != client_id);
        self.rows.len() != before
    }

    /// Bulk retirement against the caller's own notion of which orders are still live. Mappings are
    /// kept past an order's terminal event on purpose — the trade events that describe its fill
    /// arrive after it — so retirement is a pressure response rather than a per-order step.
    pub fn retain(&mut self, is_live: impl Fn(&KnownOrder) -> bool) -> usize {
        let before = self.rows.len();
        self.rows.retain(|row| is_live(&row.known));
        before - self.rows.len()
    }
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("order index is full at {capacity} entries — retired orders are not being forgotten")]
pub struct OrderIndexFull {
    pub capacity: usize,
}

/// Display lineage only. The venue's `orderID` is a 32-byte hash and its trade ids are opaque
/// strings; neither fits the POD `i64` the tape carries, and neither of these digests can be handed
/// back to the venue as a query. Anything that must ADDRESS an order uses [`OrderIndex`].
pub fn venue_order_id_digest(venue_order_id: &str) -> VenueOrderId {
    VenueOrderId(fnv1a64(venue_order_id.as_bytes()) as i64)
}

/// See [`venue_order_id_digest`] — same rule, and dedupe by the STRING id upstream of this.
pub fn trade_id_digest(trade_id: &str) -> TradeId {
    TradeId(fnv1a64(trade_id.as_bytes()) as i64)
}

/// Hand-rolled for the same reason [`crate::adapters::exec::TeTag`] is: the std hasher is not stable
/// across toolchains, and a tape's ids must not change under a compiler upgrade.
fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for byte in bytes {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(PRIME);
    }
    hash
}
