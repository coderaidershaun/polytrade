//! A fixed hash of the fold state. Each state type folds its own fields into it, so two folds of
//! the same messages can be compared after each message, and not only by the rows they wrote.

use std::hash::Hasher;

use crate::link::{LinkHash, LinkPayload, OutboundLink, TopicId};
use crate::log::HotRecord;
use crate::msg::exec::{Liquidity, OrderStyle, Provenance, RejectClass};
use crate::msg::ids::{
    AssetId, ClientOrderId, InstrumentId, Notional, Price, Qty, Side, Spend, TradeId, VenueOrderId,
};
use crate::msg::inbound::{Level, TradeEvent};
use crate::msg::latency::LaneDrops;
use crate::msg::persist::{
    BookEventKind, BookEventRow, FeatureId, FeatureRow, FillRow, KlineRow, LinkFrameRow,
    LinkRowKind, OrderLifecycle, OrderRow, OrderTransition, PersistRecord, TradeRow,
};
use crate::msg::run_state::RunAssertion;
use crate::msg::time::{DurationUs, FoldTime, LocalTs, VenueTs};
use crate::runtime::config::{KlineInterval, RecordedTables, SpinField};
use crate::runtime::exposure::{ExposureSnapshot, InstrumentExposure};

/// The multiplier of the FxHash step.
const MULTIPLIER: u64 = 0x517c_c1b7_2722_0a95;

/// Folds each word into one running value by the FxHash step. For a fixed value before it the step
/// is one-to-one in the word, and for a fixed word it is one-to-one in that value, so two inputs of
/// one shape that differ in one word always end in different values.
#[derive(Debug, Default)]
pub(crate) struct Digester {
    value: u64,
}

impl Digester {
    /// The digest of `value`, folded from a fresh start.
    pub(crate) fn of(value: &impl Digest) -> u64 {
        let mut digester = Digester::default();
        value.digest(&mut digester);
        digester.value
    }

    #[inline]
    fn fold(&mut self, word: u64) {
        self.value = (self.value.rotate_left(5) ^ word).wrapping_mul(MULTIPLIER);
    }

    /// The tag goes in before the fields, so two variants with equal fields give different digests.
    fn variant(&mut self, tag: u8, fields: &impl Digest) {
        tag.digest(self);
        fields.digest(self);
    }
}

/// A type that derives `Hash` writes its fields through these, so the derive can digest it.
impl Hasher for Digester {
    fn finish(&self) -> u64 {
        self.value
    }

    fn write(&mut self, bytes: &[u8]) {
        self.fold(bytes.len() as u64);
        for chunk in bytes.chunks(8) {
            let mut word = [0; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.fold(u64::from_le_bytes(word));
        }
    }

    fn write_u8(&mut self, value: u8) {
        self.fold(u64::from(value));
    }

    fn write_u16(&mut self, value: u16) {
        self.fold(u64::from(value));
    }

    fn write_u32(&mut self, value: u32) {
        self.fold(u64::from(value));
    }

    fn write_u64(&mut self, value: u64) {
        self.fold(value);
    }

    fn write_u128(&mut self, value: u128) {
        self.fold(value as u64);
        self.fold((value >> 64) as u64);
    }

    fn write_usize(&mut self, value: usize) {
        self.fold(value as u64);
    }

    fn write_i8(&mut self, value: i8) {
        self.write_u8(value as u8);
    }

    fn write_i16(&mut self, value: i16) {
        self.write_u16(value as u16);
    }

    fn write_i32(&mut self, value: i32) {
        self.write_u32(value as u32);
    }

    fn write_i64(&mut self, value: i64) {
        self.fold(value as u64);
    }

    fn write_i128(&mut self, value: i128) {
        self.write_u128(value as u128);
    }

    fn write_isize(&mut self, value: isize) {
        self.fold(value as u64);
    }
}

/// What one type of the fold state adds to the digest. A float adds its bits, so two folds that
/// differ in one bit of one float give different digests.
pub(crate) trait Digest {
    fn digest(&self, digester: &mut Digester);
}

/// Digests a type that derives `Hash` and holds no float by that derive, which covers each field,
/// so no field can be left out.
macro_rules! digest_by_hash {
    ($($type:ty),+ $(,)?) => {
        $(
            impl $crate::hot::digest::Digest for $type {
                #[inline]
                fn digest(&self, digester: &mut $crate::hot::digest::Digester) {
                    ::std::hash::Hash::hash(self, digester);
                }
            }
        )+
    };
}
pub(crate) use digest_by_hash;

/// Digests a struct field by field, in the order named. The pattern names each field and has no
/// rest pattern, so a field added to the struct does not compile until it is named here, or under
/// `not_state` with the reason that it is left out.
macro_rules! digest_fields {
    ($type:ident { $($field:ident),* $(,)? } $(not_state { $($skipped:ident),* $(,)? })?) => {
        impl $crate::hot::digest::Digest for $type {
            fn digest(&self, digester: &mut $crate::hot::digest::Digester) {
                let $type { $($field,)* $($($skipped: _,)*)? } = self;
                $($crate::hot::digest::Digest::digest($field, digester);)*
            }
        }
    };
}
pub(crate) use digest_fields;

digest_by_hash!(
    (),
    bool,
    u8,
    u16,
    u32,
    u64,
    usize,
    i32,
    i64,
    i128,
    &'static str
);

impl Digest for f64 {
    #[inline]
    fn digest(&self, digester: &mut Digester) {
        digester.fold(self.to_bits());
    }
}

impl<T: Digest> Digest for [T] {
    fn digest(&self, digester: &mut Digester) {
        self.len().digest(digester);
        for item in self {
            item.digest(digester);
        }
    }
}

impl<T: Digest> Digest for Vec<T> {
    fn digest(&self, digester: &mut Digester) {
        self.as_slice().digest(digester);
    }
}

impl<T: Digest, const N: usize> Digest for [T; N] {
    fn digest(&self, digester: &mut Digester) {
        self.as_slice().digest(digester);
    }
}

impl<T: Digest + ?Sized> Digest for Box<T> {
    fn digest(&self, digester: &mut Digester) {
        (**self).digest(digester);
    }
}

impl<T: Digest> Digest for Option<T> {
    fn digest(&self, digester: &mut Digester) {
        self.is_some().digest(digester);
        if let Some(value) = self {
            value.digest(digester);
        }
    }
}

impl<A: Digest, B: Digest> Digest for (A, B) {
    fn digest(&self, digester: &mut Digester) {
        let (first, second) = self;
        first.digest(digester);
        second.digest(digester);
    }
}

digest_by_hash!(
    LocalTs,
    FoldTime,
    VenueTs,
    DurationUs,
    Price,
    Qty,
    Notional,
    Spend,
    Side,
    InstrumentId,
    AssetId,
    ClientOrderId,
    VenueOrderId,
    TradeId,
    Level,
    RunAssertion,
    OrderStyle,
    Liquidity,
    Provenance,
    RejectClass,
    KlineInterval,
    SpinField,
    RecordedTables,
    LinkHash,
    TopicId,
    LaneDrops,
    FeatureId,
    BookEventKind,
    LinkRowKind,
    OrderTransition,
    OrderLifecycle,
    InstrumentExposure,
);

digest_fields! {
    TradeEvent { instrument, price, qty, side, exchange_ts_us, exchange_sent_ts_us }
}
digest_fields! { FeatureRow { instrument, feature, value, event_ts_us } }
digest_fields! { TradeRow { instrument, price, qty, side, exchange_ts_us, received_ts_us } }
digest_fields! { BookEventRow { instrument, kind, side, price, qty, update_id, received_ts_us } }
digest_fields! {
    KlineRow {
        instrument, interval, open_ts_us, open, high, low, close, base_volume, quote_volume,
        trade_count, is_closed, exchange_ts_us, received_ts_us,
    }
}
digest_fields! {
    LinkFrameRow {
        kind, sender_te_hash, topic, seq, slot, count, value, event_ts_us, received_ts_us,
    }
}
digest_fields! {
    OrderRow {
        instrument, client_id, quote_level, venue_order_id, transition, state, previous_state,
        provenance, side, style, price, qty, filled_qty, filled_quote, reject, reject_code,
        exchange_ts_us, received_ts_us,
    }
}
digest_fields! {
    FillRow {
        instrument, trade_id, venue_order_id, client_id, quote_level, provenance, side, liquidity,
        last_price, last_qty, booked_qty, booked_quote, commission, commission_asset,
        exchange_ts_us, received_ts_us,
    }
}
digest_fields! { OutboundLink { topic, payload } }
digest_fields! { ExposureSnapshot { instruments, len, seq, exposure_quote, emitted_ts_us } }

impl Digest for PersistRecord {
    fn digest(&self, digester: &mut Digester) {
        match self {
            PersistRecord::Feature(row) => digester.variant(0, row),
            PersistRecord::Trade(row) => digester.variant(1, row),
            PersistRecord::BookEvent(row) => digester.variant(2, row),
            PersistRecord::Kline(row) => digester.variant(3, row),
            PersistRecord::LinkFrame(row) => digester.variant(4, row),
            PersistRecord::Order(row) => digester.variant(5, row),
            PersistRecord::Fill(row) => digester.variant(6, row),
            PersistRecord::SealAll => digester.variant(7, &()),
        }
    }
}

/// The slots of a payload past its count are padding, so only the values that it carries count.
impl Digest for LinkPayload {
    fn digest(&self, digester: &mut Digester) {
        self.schema_hash.digest(digester);
        self.event_ts_us.digest(digester);
        self.values().digest(digester);
    }
}

/// A call site counts by its file and line, never by its address, which a build can move.
impl Digest for HotRecord {
    fn digest(&self, digester: &mut Digester) {
        let callsite = self.callsite();
        callsite.file().digest(digester);
        callsite.line().digest(digester);
        self.at().digest(digester);
        self.words().digest(digester);
    }
}
