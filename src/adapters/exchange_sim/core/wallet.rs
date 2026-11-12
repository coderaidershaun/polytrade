//! Exact balances, reservations, fills, and fees for simulated execution.

use crate::ids::{AssetId, FIXED_SCALE, Price, Qty, Side};
use crate::msg::exec::AssetBalance;

const WHOLE_FILL_BPS: i64 = 10_000;

/// Basis points as a 1e-8 mantissa, so a fractional rate stays exact. Negative is a rebate: the
/// fill pays the maker on top of what it traded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FeeBps(i64);

impl FeeBps {
    const WHOLE_FILL: i64 = WHOLE_FILL_BPS * FIXED_SCALE;

    /// # Errors
    /// Returns [`WalletError::FeeOutOfRange`] when the rate is more than the whole fill, as a fee
    /// or as a rebate.
    pub fn new(mantissa: i64) -> Result<Self, WalletError> {
        match (-Self::WHOLE_FILL..=Self::WHOLE_FILL).contains(&mantissa) {
            true => Ok(Self(mantissa)),
            false => Err(WalletError::FeeOutOfRange { mantissa }),
        }
    }

    fn charge(self, amount: i64) -> i64 {
        narrow(i128::from(amount) * i128::from(self.0) / i128::from(Self::WHOLE_FILL))
    }

    fn is_rebate(self) -> bool {
        self.0 < 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SimWalletSetup {
    pub base_asset: AssetId,
    pub quote_asset: AssetId,
    pub opening_base: i64,
    pub opening_quote: i64,
    pub maker_fee_bps: FeeBps,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReservationRequest {
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReserveOutcome {
    Reserved(Reservation),
    InsufficientFunds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ReservationState {
    Live,
    Settled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Reservation {
    side: Side,
    price: Price,
    total_qty: Qty,
    total_reservation: i64,
    cumulative_qty: Qty,
    cumulative_quote: i64,
    cumulative_debit: i64,
    state: ReservationState,
}

impl Reservation {
    fn residual(&self) -> i64 {
        self.total_reservation - self.cumulative_debit
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FillSettlement {
    pub last_qty: Qty,
    pub last_quote: i64,
    pub cumulative_qty: Qty,
    pub cumulative_quote: i64,
    pub debit: i64,
    pub received_gross: i64,
    pub received_net: i64,
    pub fee: i64,
    pub fee_asset: AssetId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Settlement {
    pub balances: [AssetBalance; 2],
    pub update_ts_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AssetFunds {
    free: i64,
    locked: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Reserved,
    Received,
}

use Role::{Received, Reserved};

/// The wallet holds exactly two slots, so naming one closes the door an `AssetId` leaves open: an
/// asset that is neither base nor quote has no slot, and every lookup here would have to answer for
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalletSlot {
    Base,
    Quote,
}

impl WalletSlot {
    const fn of(side: Side, role: Role) -> Self {
        match (side, role) {
            (Side::Buy, Reserved) | (Side::Sell, Received) => WalletSlot::Quote,
            (Side::Buy, Received) | (Side::Sell, Reserved) => WalletSlot::Base,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimWallet {
    base_asset: AssetId,
    quote_asset: AssetId,
    base: AssetFunds,
    quote: AssetFunds,
    maker_fee_bps: FeeBps,
    account_ts_ms: u64,
}

impl SimWallet {
    /// # Errors
    /// [`WalletError::SameAsset`] or [`WalletError::NegativeOpeningBalance`].
    pub fn new(setup: SimWalletSetup) -> Result<Self, WalletError> {
        if setup.base_asset == setup.quote_asset {
            return Err(WalletError::SameAsset {
                asset: setup.base_asset,
            });
        }
        for (asset, amount) in [
            (setup.base_asset, setup.opening_base),
            (setup.quote_asset, setup.opening_quote),
        ] {
            if amount < 0 {
                return Err(WalletError::NegativeOpeningBalance { asset, amount });
            }
        }
        Ok(Self {
            base_asset: setup.base_asset,
            quote_asset: setup.quote_asset,
            base: AssetFunds {
                free: setup.opening_base,
                locked: 0,
            },
            quote: AssetFunds {
                free: setup.opening_quote,
                locked: 0,
            },
            maker_fee_bps: setup.maker_fee_bps,
            account_ts_ms: 0,
        })
    }

    pub fn reserve(&mut self, request: ReservationRequest) -> ReserveOutcome {
        let amount = request.side.spend(request.price, request.qty);
        let funds = self.funds_mut(WalletSlot::of(request.side, Reserved));
        if funds.free < amount {
            return ReserveOutcome::InsufficientFunds;
        }
        let free = checked_sub(funds.free, amount, "free balance while reserving");
        let locked = checked_add(funds.locked, amount, "locked balance while reserving");
        funds.free = free;
        funds.locked = locked;
        ReserveOutcome::Reserved(Reservation {
            side: request.side,
            price: request.price,
            total_qty: request.qty,
            total_reservation: amount,
            cumulative_qty: Qty(0),
            cumulative_quote: 0,
            cumulative_debit: 0,
            state: ReservationState::Live,
        })
    }

    /// # Panics
    /// If the fill or reservation violates wallet invariants.
    pub fn fill(&mut self, reservation: &mut Reservation, last_qty: Qty) -> FillSettlement {
        assert_eq!(
            reservation.state,
            ReservationState::Live,
            "filled an order whose reservation was already settled"
        );
        assert!(last_qty.0 > 0, "filled {} of an order", last_qty.0);
        let cumulative_qty = Qty(checked_add(
            reservation.cumulative_qty.0,
            last_qty.0,
            "cumulative fill quantity",
        ));
        assert!(
            cumulative_qty <= reservation.total_qty,
            "filled {} cumulative against a total of {}",
            cumulative_qty.0,
            reservation.total_qty.0
        );

        let cumulative_quote = reservation.price.notional(cumulative_qty);
        let last_quote = cumulative_quote - reservation.cumulative_quote;
        let (debit, received_gross) = match reservation.side {
            Side::Buy => (last_quote, last_qty.0),
            Side::Sell => (last_qty.0, last_quote),
        };

        let reserved = WalletSlot::of(reservation.side, Reserved);
        let received = WalletSlot::of(reservation.side, Received);
        let fee_slot = self.fee_slot(received);
        let fee_basis = match fee_slot {
            WalletSlot::Base => last_qty.0,
            WalletSlot::Quote => last_quote,
        };
        let fee = self.maker_fee_bps.charge(fee_basis);
        let received_net = match fee_slot == received {
            true => received_gross - fee,
            false => received_gross,
        };
        let locked = self.funds(reserved).locked;
        assert!(
            locked >= debit,
            "debited {debit} against {locked} locked — the reservation did not cover its own fill"
        );
        let locked_after = checked_sub(locked, debit, "locked balance after a fill");
        let received_after = checked_add(
            self.funds(received).free,
            received_gross,
            "received free balance",
        );
        let cumulative_debit = checked_add(
            reservation.cumulative_debit,
            debit,
            "cumulative reservation debit",
        );

        self.funds_mut(reserved).locked = locked_after;
        self.funds_mut(received).free = received_after;
        let fee_slot_after =
            checked_sub(self.funds(fee_slot).free, fee, "free balance after a fee");
        self.funds_mut(fee_slot).free = fee_slot_after;
        reservation.cumulative_qty = cumulative_qty;
        reservation.cumulative_quote = cumulative_quote;
        reservation.cumulative_debit = cumulative_debit;

        FillSettlement {
            last_qty,
            last_quote,
            cumulative_qty,
            cumulative_quote,
            debit,
            received_gross,
            received_net,
            fee,
            fee_asset: self.asset_of(fee_slot),
        }
    }

    /// # Panics
    /// If the amended total is outside the valid shrink range.
    pub fn amend(&mut self, reservation: &mut Reservation, total_qty: Qty) {
        assert_eq!(
            reservation.state,
            ReservationState::Live,
            "amended an order whose reservation was already settled"
        );
        assert!(
            total_qty < reservation.total_qty,
            "amended {} up from {} — an increase is a cancel and replace",
            total_qty.0,
            reservation.total_qty.0
        );
        assert!(
            total_qty > reservation.cumulative_qty,
            "amended {} below the {} already filled",
            total_qty.0,
            reservation.cumulative_qty.0
        );
        let new_total = reservation.side.spend(reservation.price, total_qty);
        let released = reservation.total_reservation - new_total;
        reservation.total_qty = total_qty;
        reservation.total_reservation = new_total;
        self.unlock(reservation.side, released);
    }

    /// # Panics
    /// If a reservation is released twice.
    pub fn release(&mut self, reservation: &mut Reservation) {
        assert_eq!(
            reservation.state,
            ReservationState::Live,
            "released a reservation twice"
        );
        let residual = reservation.residual();
        reservation.total_reservation = reservation.cumulative_debit;
        reservation.state = ReservationState::Settled;
        self.unlock(reservation.side, residual);
    }

    /// # Panics
    /// The account stamp reaching `u64::MAX`.
    pub fn settle(&mut self, due_ts_ms: u64) -> Settlement {
        let stamp = self.account_ts_ms.max(due_ts_ms);
        let update_ts_ms = stamp
            .checked_add(1)
            .unwrap_or_else(|| panic!("account stamp {stamp}ms cannot advance"));
        self.account_ts_ms = update_ts_ms;
        Settlement {
            balances: [
                AssetBalance {
                    asset: self.base_asset,
                    free: self.base.free,
                    locked: self.base.locked,
                },
                AssetBalance {
                    asset: self.quote_asset,
                    free: self.quote.free,
                    locked: self.quote.locked,
                },
            ],
            update_ts_ms,
        }
    }

    fn unlock(&mut self, side: Side, amount: i64) {
        assert!(amount >= 0, "unlocked a negative {amount}");
        let funds = self.funds_mut(WalletSlot::of(side, Reserved));
        assert!(
            funds.locked >= amount,
            "unlocked {amount} against {} locked",
            funds.locked
        );
        let locked = checked_sub(funds.locked, amount, "locked balance while unlocking");
        let free = checked_add(funds.free, amount, "free balance while unlocking");
        funds.locked = locked;
        funds.free = free;
    }

    /// A rebate is always paid in the quote asset: paid in base it rounds away on a small order
    /// and never reaches PnL, which only counts quote commissions. A charge comes out of what
    /// the fill received.
    fn fee_slot(&self, received: WalletSlot) -> WalletSlot {
        match self.maker_fee_bps.is_rebate() {
            true => WalletSlot::Quote,
            false => received,
        }
    }

    fn asset_of(&self, slot: WalletSlot) -> AssetId {
        match slot {
            WalletSlot::Base => self.base_asset,
            WalletSlot::Quote => self.quote_asset,
        }
    }

    fn funds(&self, slot: WalletSlot) -> &AssetFunds {
        match slot {
            WalletSlot::Base => &self.base,
            WalletSlot::Quote => &self.quote,
        }
    }

    fn funds_mut(&mut self, slot: WalletSlot) -> &mut AssetFunds {
        match slot {
            WalletSlot::Base => &mut self.base,
            WalletSlot::Quote => &mut self.quote,
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletError {
    #[error("maker fee {mantissa}e-8bps is outside -10000..=10000bps")]
    FeeOutOfRange { mantissa: i64 },
    #[error("base and quote asset both resolve to {}", asset.0)]
    SameAsset { asset: AssetId },
    #[error("opening balance {amount} for asset {} is negative", asset.0)]
    NegativeOpeningBalance { asset: AssetId, amount: i64 },
}

fn narrow(amount: i128) -> i64 {
    i64::try_from(amount)
        .unwrap_or_else(|_| panic!("simulated wallet amount {amount} does not fit a mantissa"))
}

fn checked_add(left: i64, right: i64, kind: &str) -> i64 {
    left.checked_add(right)
        .unwrap_or_else(|| panic!("simulated wallet {kind} overflows a mantissa: {left} + {right}"))
}

fn checked_sub(left: i64, right: i64, kind: &str) -> i64 {
    left.checked_sub(right)
        .unwrap_or_else(|| panic!("simulated wallet {kind} overflows a mantissa: {left} - {right}"))
}
