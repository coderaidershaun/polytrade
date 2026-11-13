//! Typed identity and exact money: dense indices for lookups, and prices, quantities and quote
//! money as i64 mantissas at one fixed scale, never floats.

use crate::msg::labelled_enum::labelled_enum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InstrumentId(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceId(pub u16);

/// One input queue of the hot thread. Its index is below [`MAX_INPUT_QUEUES`] by construction, so a
/// table of queues never meets an id past its end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueId(u8);

/// The most input queues the hot thread reads.
pub(crate) const MAX_INPUT_QUEUES: usize = 20;

impl QueueId {
    /// # Errors
    /// [`QueueIdError`] when `index` is not below the cap of input queues.
    pub const fn new(index: usize) -> Result<QueueId, QueueIdError> {
        if index < MAX_INPUT_QUEUES {
            Ok(QueueId(index as u8))
        } else {
            Err(QueueIdError {
                index,
                max: MAX_INPUT_QUEUES,
            })
        }
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[error("input queue {index} is past the {max} input queues that one hot thread reads")]
pub struct QueueIdError {
    pub index: usize,
    pub max: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AssetId(pub u16);

impl AssetId {
    /// An asset no configured instrument names. Balance/commission: counted+ignored (not misattributed).
    pub const UNKNOWN: AssetId = AssetId(u16::MAX);
}

/// Assigned before venue acknowledgement; VenueOrderId does not exist until then.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientOrderId(pub u64);

/// Binance issues this number; Polymarket issues a string, and its edge stores a digest of that
/// string here. So this identifies lineage across records and is NEVER a handle to send back to a
/// venue — the edge keeps whatever the venue actually answers to on its own side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VenueOrderId(pub i64);

/// Signed because execution reports use `-1` for “no trade.” Carries a digest rather than a venue
/// number on a venue whose trade ids are strings — see [`VenueOrderId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TradeId(pub i64);

/// One trade in the venue's per-symbol sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RawTradeId(pub u64);

/// Identifies an aggregate that may span several [`RawTradeId`] values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AggregateTradeId(pub u64);

/// Connection generation for venue sequence numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamEpoch(u64);

impl StreamEpoch {
    #[must_use]
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Side {
        Buy = "buy",
        Sell = "sell",
    }
    pub fn as_str;
}

impl Side {
    /// Snap PASSIVE direction: buy DOWN, sell UP. Rounding opposite = aggression + crossing risk. Method (not match) catches inversions at compile time.
    #[inline]
    pub fn snap_passive(self, price: Price, increment: i64) -> Price {
        assert!(
            increment > 0,
            "snapping onto a non-positive grid increment {increment}"
        );
        Price(match self {
            Side::Buy => price.0.div_euclid(increment) * increment,
            Side::Sell => -((-price.0).div_euclid(increment) * increment),
        })
    }

    /// Position of this side in any two-slot array keyed by side. One encoding, so reordering the
    /// variants can never leave one array's geography disagreeing with another's.
    #[inline]
    pub fn index(self) -> usize {
        self as usize
    }

    /// Side that reduces position in this direction (e.g., sell reduces long).
    #[inline]
    pub fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }

    /// # Panics
    /// If a buy's notional does not fit an i64 mantissa, exactly as [`Price::notional`] does.
    #[inline]
    pub fn spend(self, price: Price, qty: Qty) -> Spend {
        match self {
            Side::Buy => Spend::Quote(price.notional(qty)),
            Side::Sell => Spend::Base(qty),
        }
    }

    pub(crate) const fn wire_name(self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }
}

pub const FIXED_SCALE: i64 = 100_000_000;

/// The mantissa `units` denotes at [`FIXED_SCALE`], or `None` when it cannot be one.
///
/// Deliberately dimensionless: quote money, base quantities and fee rates all enter the engine at
/// this one scale, so a converter named for any of them invites the other two through unnoticed.
///
/// Rounds rather than truncates — 1e8 is not a power of two, so the scaled product lands a mantissa
/// short for about 8% of values. The range test is what keeps a hostile or absurd `f64` out, since
/// a bare cast never fails: `inf` and `1e300` saturate to `i64::MAX` and `NaN` becomes zero, so an
/// untrusted wire value would arrive looking like a real amount.
pub(crate) fn fixed_mantissa(units: f64) -> Option<i64> {
    let scaled = (units * FIXED_SCALE as f64).round();
    (scaled >= i64::MIN as f64 && scaled < i64::MAX as f64).then_some(scaled as i64)
}

/// A mantissa at [`FIXED_SCALE`]. A venue's parser refuses a magnitude above `i64::MAX / 1e8`
/// (about 9.2e10) and never truncates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(i64);

/// A base quantity, as a mantissa at [`FIXED_SCALE`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Qty(i64);

/// Quote money, as a mantissa at [`FIXED_SCALE`]. No conversion leads to it from a [`Qty`] or a
/// [`Price`], so a base quantity can never be booked as cash. [`Price::notional`] makes one from a
/// trade.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Notional(i64);

/// What an order reserves: quote money for a buy, base quantity for a sell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Spend {
    Quote(Notional),
    Base(Qty),
}

impl Spend {
    /// The amount in the unit of the asset that it reserves.
    #[inline]
    pub fn mantissa(self) -> i64 {
        match self {
            Spend::Quote(notional) => notional.mantissa(),
            Spend::Base(qty) => qty.mantissa(),
        }
    }
}

impl Price {
    #[inline]
    pub const fn from_mantissa(mantissa: i64) -> Self {
        Self(mantissa)
    }

    #[inline]
    pub const fn mantissa(self) -> i64 {
        self.0
    }

    pub fn parse_decimal(s: &str) -> Result<Self, DecimalError> {
        parse_fixed_mantissa(s).map(Price::from_mantissa)
    }

    /// Display and statistics only; never book key or accumulator.
    #[inline]
    pub fn to_f64(self) -> f64 {
        self.0 as f64 / FIXED_SCALE as f64
    }

    /// The exact product, truncated toward zero.
    ///
    /// # Panics
    /// When the notional does not fit an i64 mantissa (about 9.2e10), because a wrapped one would
    /// book the wrong cash with no error.
    #[inline]
    pub fn notional(self, qty: Qty) -> Notional {
        let product = self.0 as i128 * qty.0 as i128;
        let mantissa = product / FIXED_SCALE as i128;
        let mantissa = i64::try_from(mantissa).unwrap_or_else(|_| {
            panic!(
                "notional overflows i64 mantissa: price {} qty {}",
                self.0, qty.0
            )
        });
        Notional(mantissa)
    }
}

impl Notional {
    pub const ZERO: Notional = Notional(0);

    pub fn parse_decimal(s: &str) -> Result<Self, DecimalError> {
        parse_fixed_mantissa(s).map(Notional)
    }

    #[inline]
    pub const fn from_mantissa(mantissa: i64) -> Self {
        Self(mantissa)
    }

    #[inline]
    pub const fn mantissa(self) -> i64 {
        self.0
    }

    /// Display and statistics only, never an accumulator.
    #[inline]
    pub fn to_f64(self) -> f64 {
        self.0 as f64 / FIXED_SCALE as f64
    }

    #[inline]
    pub fn abs(self) -> Self {
        Self(self.0.abs())
    }
}

impl std::ops::Add for Notional {
    type Output = Notional;
    #[inline]
    fn add(self, other: Notional) -> Notional {
        Notional(self.0 + other.0)
    }
}

impl std::ops::Sub for Notional {
    type Output = Notional;
    #[inline]
    fn sub(self, other: Notional) -> Notional {
        Notional(self.0 - other.0)
    }
}

impl std::ops::Neg for Notional {
    type Output = Notional;
    #[inline]
    fn neg(self) -> Notional {
        Notional(-self.0)
    }
}

impl std::ops::AddAssign for Notional {
    #[inline]
    fn add_assign(&mut self, other: Notional) {
        self.0 += other.0;
    }
}

impl std::ops::SubAssign for Notional {
    #[inline]
    fn sub_assign(&mut self, other: Notional) {
        self.0 -= other.0;
    }
}

impl Qty {
    #[inline]
    pub const fn from_mantissa(mantissa: i64) -> Self {
        Self(mantissa)
    }

    #[inline]
    pub const fn mantissa(self) -> i64 {
        self.0
    }

    pub fn parse_decimal(s: &str) -> Result<Self, DecimalError> {
        parse_fixed_mantissa(s).map(Qty::from_mantissa)
    }

    /// Statistics only; never money math.
    #[inline]
    pub fn to_f64(self) -> f64 {
        self.0 as f64 / FIXED_SCALE as f64
    }

    /// Never negative, so an overfill reads as nothing left rather than a negative amount.
    #[inline]
    pub fn remaining_after(self, filled: Qty) -> Qty {
        Qty((self.0 - filled.0).max(0))
    }

    /// # Panics
    /// If `step` is zero.
    #[inline]
    pub fn floor_to_step(self, step: i64) -> Qty {
        Qty(self.0 - self.0.rem_euclid(step))
    }
}

const MAX_FRACTIONAL_PLACES: usize = 8;

fn parse_fixed_mantissa(s: &str) -> Result<i64, DecimalError> {
    if s.is_empty() {
        return Err(DecimalError::Empty);
    }

    let (integer, fraction) = match s.find('.') {
        Some(dot) => (&s[..dot], &s[dot + 1..]),
        None => (s, ""),
    };

    if integer.is_empty() && fraction.is_empty() {
        return Err(DecimalError::InvalidChar {
            input: s.into(),
            ch: '.',
        });
    }
    if fraction.len() > MAX_FRACTIONAL_PLACES {
        return Err(DecimalError::TooPrecise { input: s.into() });
    }

    let mut mantissa: i64 = 0;
    for ch in integer.chars().chain(fraction.chars()) {
        let digit = ch.to_digit(10).ok_or_else(|| DecimalError::InvalidChar {
            input: s.into(),
            ch,
        })?;
        mantissa = mantissa
            .checked_mul(10)
            .and_then(|scaled| scaled.checked_add(i64::from(digit)))
            .ok_or_else(|| DecimalError::Overflow { input: s.into() })?;
    }

    let pad = MAX_FRACTIONAL_PLACES - fraction.len();
    let pad_factor = 10i64.pow(pad as u32);
    mantissa
        .checked_mul(pad_factor)
        .ok_or_else(|| DecimalError::Overflow { input: s.into() })
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum DecimalError {
    #[error("empty decimal string")]
    Empty,
    #[error("invalid decimal char {ch:?} in {input}")]
    InvalidChar { input: Box<str>, ch: char },
    #[error("too many decimal places in {input}, max 8")]
    TooPrecise { input: Box<str> },
    #[error("decimal overflows i64 mantissa: {input}")]
    Overflow { input: Box<str> },
}
