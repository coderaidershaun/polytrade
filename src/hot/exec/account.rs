//! Absolute balances only. Guard against double-spend: ack beats update.

use crate::ids::AssetId;
use crate::msg::exec::{AccountChunk, AccountChunkKind, AssetBalance};

const MAX_ASSETS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct AccountRow {
    free: i64,
    locked: i64,
}

/// The venue's own account-update clock in whole milliseconds — never a local read, so a replay
/// releases reservations at exactly the same points the live run did.
///
/// Monotone by construction: it only ever advances to the newest chunk's stamp. A reservation taken
/// at one watermark may be released once the venue has restated balances at a LATER one, which is
/// what stops an ack alone from freeing money the venue has not yet moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct AccountWatermark(u64);

impl AccountWatermark {
    /// Before the venue has stated anything. Every real chunk is at or beyond it, so a reservation
    /// taken here is released by the first update that names a later millisecond.
    pub const ZERO: Self = Self(0);
}

/// Venue state + our reserved (separate, not folded in).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Balance {
    pub free: i64,
    pub locked: i64,
    pub reserved: i64,
}

impl Balance {
    #[inline]
    pub fn spendable(self) -> i64 {
        self.free - self.reserved
    }
}

#[derive(Default)]
pub struct AccountTable {
    rows: [AccountRow; MAX_ASSETS],
    reserved: [i64; MAX_ASSETS],
    /// Distinguishes 0 balance from unknown asset.
    is_known: [bool; MAX_ASSETS],
    watermark: AccountWatermark,
    has_snapshot: bool,
}

impl AccountTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot consumed (readiness gate component).
    #[inline]
    pub fn has_snapshot(&self) -> bool {
        self.has_snapshot
    }

    #[inline]
    pub fn balance(&self, asset: AssetId) -> Balance {
        let Some(index) = Self::index_of(asset) else {
            return Balance::default();
        };
        Balance {
            free: self.rows[index].free,
            locked: self.rows[index].locked,
            reserved: self.reserved[index],
        }
    }

    /// Last chunk arms readiness gate (no half-arming).
    pub fn apply(&mut self, chunk: &AccountChunk) {
        for balance in chunk.active_balances() {
            self.apply_balance(*balance);
        }
        self.watermark = self
            .watermark
            .max(AccountWatermark(chunk.venue_update_ts_ms));
        if chunk.kind == AccountChunkKind::Snapshot && chunk.is_last_chunk {
            self.has_snapshot = true;
        }
    }

    fn apply_balance(&mut self, balance: AssetBalance) {
        // An asset no configured instrument names — dust, or a fee asset like BNB. Ignored rather
        // than misattributed to an asset we do trade.
        let Some(index) = Self::index_of(balance.asset) else {
            return;
        };
        self.rows[index] = AccountRow {
            free: balance.free,
            locked: balance.locked,
        };
        self.is_known[index] = true;
    }

    /// Fixed-length, borrow-only (allocator-free on restatement).
    pub fn balances(&self) -> impl Iterator<Item = (AssetId, Balance)> + '_ {
        (0..MAX_ASSETS)
            .filter(|index| self.is_known[*index])
            .map(|index| {
                let asset = AssetId(index as u16);
                (asset, self.balance(asset))
            })
    }

    /// Returns the watermark that decides when this reservation may be released.
    #[inline]
    pub fn reserve(&mut self, asset: AssetId, amount: i64) -> AccountWatermark {
        if let Some(index) = Self::index_of(asset) {
            self.reserved[index] += amount;
        }
        self.watermark
    }

    /// An ack alone never releases: the venue must have restated balances at a watermark strictly
    /// later than the one the reservation was taken at. Two updates inside one millisecond do not
    /// advance it, so the release stays `Held` until the next — conservative, and the safe direction.
    #[inline]
    pub fn release(
        &mut self,
        asset: AssetId,
        amount: i64,
        reserved_at: AccountWatermark,
    ) -> ReleaseOutcome {
        if self.watermark <= reserved_at {
            return ReleaseOutcome::Held;
        }
        let Some(index) = Self::index_of(asset) else {
            return ReleaseOutcome::Released;
        };
        self.debit_reservation(index, amount, "released");
        ReleaseOutcome::Released
    }

    /// No account update needed (venue never saw this).
    #[inline]
    pub fn release_unsent(&mut self, asset: AssetId, amount: i64) {
        if let Some(index) = Self::index_of(asset) {
            self.debit_reservation(index, amount, "released unsent");
        }
    }

    /// A double release absorbed silently would fund an order nobody wanted, so it fails loud. The
    /// row index is the asset id, so the message names the asset.
    #[inline]
    fn debit_reservation(&mut self, index: usize, amount: i64, released: &str) {
        let remaining = self.reserved[index] - amount;
        assert!(
            remaining >= 0,
            "{released} {amount} of asset {index} against {} reserved — reservation freed twice",
            self.reserved[index]
        );
        self.reserved[index] = remaining;
    }

    /// Out-of-range -> not accounted (same as UNKNOWN).
    #[inline]
    fn index_of(asset: AssetId) -> Option<usize> {
        (usize::from(asset.0) < MAX_ASSETS).then_some(usize::from(asset.0))
    }
}

/// Release blocked (caller retries next spin).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseOutcome {
    Released,
    Held,
}
