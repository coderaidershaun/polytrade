//! The field list of each type that a message carries onto the tape. A tag of an enum is appended,
//! never renumbered: the digest of the kind changes with it, and an old tape is refused rather than
//! misread.

use crate::msg::codec::LayoutDigest;
use crate::msg::exec::{
    AccountChunk, AccountChunkKind, AssetBalance, ExecEvent, ExecKind, Liquidity, Provenance,
    RejectClass, VenueOrderStatus,
};
use crate::msg::ids::{
    AggregateTradeId, AssetId, ClientOrderId, InstrumentId, Notional, Price, Qty, RawTradeId, Side,
    StreamEpoch, TradeId, VenueId, VenueOrderId,
};
use crate::msg::inbound::{
    AuditBankFull, BOOK_CHUNK_LEVELS, BookChunk, BookChunkKind, BookReset, FileWriter, KlineEvent,
    Level, MarketRotation, RunControl, SpinTick, TradeEvent, WriterFailure,
};
use crate::msg::kinds::KlineInterval;
use crate::msg::link::{LINK_MAX_FIELDS, LinkFrame, LinkHash, LinkOrigin, LinkPayload, TopicId};
use crate::msg::run_state::{ControllerEpoch, RunAssertion, RunState};
use crate::msg::time::{LocalTs, VenueTs};

use super::{TapeDecodeError, TapeField, TapeReader, tape_enum, tape_struct};

/// A type stored as the one integer inside it.
macro_rules! tape_as {
    ($($ty:ty => $inner:ty: |$value:ident| $to:expr, |$raw:ident| $from:expr);+ $(;)?) => {
        $(impl TapeField for $ty {
            const WIDTH: usize = <$inner as TapeField>::WIDTH;
            const DIGEST: u64 = LayoutDigest::START
                .text(stringify!($ty))
                .part(<$inner as TapeField>::WIDTH, <$inner as TapeField>::DIGEST)
                .finish();

            fn write(&self, out: &mut Vec<u8>) {
                let $value = *self;
                let stored: $inner = $to;
                stored.write(out);
            }

            fn read(input: &mut TapeReader<'_>) -> Result<Self, TapeDecodeError> {
                let $raw = <$inner as TapeField>::read(input)?;
                Ok($from)
            }
        })+
    };
}

tape_as! {
    LocalTs => i64: |value| value.micros(), |raw| LocalTs::from_micros(raw);
    VenueTs => i64: |value| value.micros(), |raw| VenueTs::from_micros(raw);
    Price => i64: |value| value.mantissa(), |raw| Price::from_mantissa(raw);
    Qty => i64: |value| value.mantissa(), |raw| Qty::from_mantissa(raw);
    Notional => i64: |value| value.mantissa(), |raw| Notional::from_mantissa(raw);
    InstrumentId => u16: |value| value.0, |raw| InstrumentId(raw);
    AssetId => u16: |value| value.0, |raw| AssetId(raw);
    VenueId => u8: |value| value.0, |raw| VenueId(raw);
    TopicId => u16: |value| value.0, |raw| TopicId(raw);
    ClientOrderId => u64: |value| value.0, |raw| ClientOrderId(raw);
    VenueOrderId => i64: |value| value.0, |raw| VenueOrderId(raw);
    TradeId => i64: |value| value.0, |raw| TradeId(raw);
    AggregateTradeId => u64: |value| value.0, |raw| AggregateTradeId(raw);
    RawTradeId => u64: |value| value.0, |raw| RawTradeId(raw);
    LinkHash => u64: |value| value.0, |raw| LinkHash(raw);
    StreamEpoch => u64: |value| value.get(), |raw| StreamEpoch::from_stored(raw);
}

/// An epoch at 2^63 or above is refused as it is read: the run-state cell cannot hold it.
impl TapeField for ControllerEpoch {
    const WIDTH: usize = u64::WIDTH;
    const DIGEST: u64 = LayoutDigest::START
        .text("ControllerEpoch")
        .part(u64::WIDTH, u64::DIGEST)
        .finish();

    fn write(&self, out: &mut Vec<u8>) {
        self.get().write(out);
    }

    fn read(input: &mut TapeReader<'_>) -> Result<Self, TapeDecodeError> {
        let epoch = u64::read(input)?;
        ControllerEpoch::try_from(epoch).map_err(|_| TapeDecodeError::OutOfRange {
            field: "controller epoch",
            value: epoch,
        })
    }
}

tape_enum!(Side, "side"; (Side::Buy) = 0, (Side::Sell) = 1);
tape_enum!(BookChunkKind, "book chunk kind";
    (BookChunkKind::Delta) = 0, (BookChunkKind::Snapshot) = 1);
tape_enum!(AccountChunkKind, "account chunk kind";
    (AccountChunkKind::Snapshot) = 0, (AccountChunkKind::Update) = 1);
tape_enum!(RunState, "run state"; (RunState::Running) = 0, (RunState::Idle) = 1);
tape_enum!(FileWriter, "file writer"; (FileWriter::Exposure) = 0, (FileWriter::Tape) = 1);
tape_enum!(KlineInterval, "kline interval";
    (KlineInterval::OneMinute) = 0, (KlineInterval::ThreeMinutes) = 1,
    (KlineInterval::FiveMinutes) = 2, (KlineInterval::FifteenMinutes) = 3,
    (KlineInterval::ThirtyMinutes) = 4, (KlineInterval::OneHour) = 5,
    (KlineInterval::TwoHours) = 6, (KlineInterval::FourHours) = 7,
    (KlineInterval::SixHours) = 8, (KlineInterval::EightHours) = 9,
    (KlineInterval::TwelveHours) = 10, (KlineInterval::OneDay) = 11,
    (KlineInterval::ThreeDays) = 12, (KlineInterval::OneWeek) = 13,
    (KlineInterval::OneMonth) = 14);
tape_enum!(ExecKind, "exec kind";
    (ExecKind::AckPlaced) = 0, (ExecKind::PlaceNotSent) = 1, (ExecKind::AmendNotSent) = 2,
    (ExecKind::AckCanceled) = 3, (ExecKind::AckAmended) = 4, (ExecKind::AckFailed) = 5,
    (ExecKind::ReportNew) = 6, (ExecKind::ReportTrade) = 7, (ExecKind::ReportCanceled) = 8,
    (ExecKind::ReportExpired) = 9, (ExecKind::ReportRejected) = 10,
    (ExecKind::ReportAmended) = 11, (ExecKind::SnapshotOrder) = 12,
    (ExecKind::SnapshotEnd) = 13, (ExecKind::StreamReset) = 14, (ExecKind::StreamReady) = 15,
    (ExecKind::UnprovenOrder) = 16);
tape_enum!(VenueOrderStatus, "venue order status";
    (VenueOrderStatus::New) = 0, (VenueOrderStatus::PartiallyFilled) = 1,
    (VenueOrderStatus::Filled) = 2, (VenueOrderStatus::Canceled) = 3,
    (VenueOrderStatus::PendingCancel) = 4, (VenueOrderStatus::Rejected) = 5,
    (VenueOrderStatus::Expired) = 6, (VenueOrderStatus::ExpiredInMatch) = 7);
tape_enum!(RejectClass, "reject class";
    (RejectClass::StillLive) = 0, (RejectClass::Refused) = 1, (RejectClass::Gone) = 2,
    (RejectClass::Ambiguous) = 3, (RejectClass::Fatal) = 4);
tape_enum!(Provenance, "provenance";
    (Provenance::Mine) = 0, (Provenance::PriorRun) = 1, (Provenance::Foreign) = 2);
tape_enum!(Liquidity, "liquidity"; (Liquidity::Maker) = 0, (Liquidity::Taker) = 1);

tape_struct!(Level { price, qty });
tape_struct!(AssetBalance {
    asset,
    free,
    locked
});
tape_struct!(TradeEvent {
    instrument,
    price,
    qty,
    side,
    exchange_ts_us,
    exchange_sent_ts_us,
    aggregate_id,
    first_trade_id,
    last_trade_id,
    stream_epoch,
});
tape_struct!(BookReset {
    instrument,
    exchange_ts_us
});
tape_struct!(MarketRotation {
    instrument,
    window_open_ts_us,
    window_close_ts_us
});
tape_struct!(KlineEvent {
    instrument,
    interval,
    open_ts_us,
    open,
    high,
    low,
    close,
    base_volume,
    quote_volume,
    trade_count,
    is_closed,
    exchange_ts_us,
    exchange_sent_ts_us,
});
tape_struct!(SpinTick { seq });
tape_struct!(LinkOrigin {
    sender_te_hash,
    boot_ts_us,
    topic,
    seq
});
tape_struct!(LinkFrame { origin, payload });
tape_struct!(RunAssertion { state, epoch });
tape_struct!(RunControl { desired });
tape_struct!(WriterFailure {
    writer,
    consecutive_failures
});
tape_struct!(AuditBankFull { banked, capacity });
tape_struct!(ExecEvent {
    venue,
    instrument,
    client_id,
    venue_order_id,
    trade_id,
    kind,
    status,
    reject,
    provenance,
    side,
    liquidity,
    price,
    qty,
    last_price,
    last_qty,
    cumulative_qty,
    cumulative_quote,
    commission,
    commission_asset,
    reject_code,
    amends_remaining,
    recon_seq,
    exchange_ts_us,
    request_sent_ts_us,
});

const NO_LEVEL: Level = Level {
    price: Price::from_mantissa(0),
    qty: Qty::from_mantissa(0),
};

/// Each of the levels, the unused ones zero, then the count, so each chunk has one width. The
/// levels are read back through the constructor, so a count past the array is refused.
impl TapeField for BookChunk {
    const WIDTH: usize = InstrumentId::WIDTH
        + BookChunkKind::WIDTH
        + Side::WIDTH
        + <[Level; BOOK_CHUNK_LEVELS]>::WIDTH
        + u8::WIDTH
        + bool::WIDTH
        + u64::WIDTH
        + Option::<VenueTs>::WIDTH;
    const DIGEST: u64 = LayoutDigest::START
        .text("struct BookChunk")
        .text("instrument")
        .part(InstrumentId::WIDTH, InstrumentId::DIGEST)
        .text("kind")
        .part(BookChunkKind::WIDTH, BookChunkKind::DIGEST)
        .text("side")
        .part(Side::WIDTH, Side::DIGEST)
        .text("levels")
        .part(
            <[Level; BOOK_CHUNK_LEVELS]>::WIDTH,
            <[Level; BOOK_CHUNK_LEVELS]>::DIGEST,
        )
        .text("len")
        .part(u8::WIDTH, u8::DIGEST)
        .text("is_last_chunk")
        .part(bool::WIDTH, bool::DIGEST)
        .text("update_id")
        .part(u64::WIDTH, u64::DIGEST)
        .text("exchange_ts_us")
        .part(Option::<VenueTs>::WIDTH, Option::<VenueTs>::DIGEST)
        .finish();

    fn write(&self, out: &mut Vec<u8>) {
        self.instrument.write(out);
        self.kind.write(out);
        self.side.write(out);
        let active = self.active_levels();
        let mut levels = [NO_LEVEL; BOOK_CHUNK_LEVELS];
        levels[..active.len()].copy_from_slice(active);
        levels.write(out);
        (active.len() as u8).write(out);
        self.is_last_chunk.write(out);
        self.update_id.write(out);
        self.exchange_ts_us.write(out);
    }

    fn read(input: &mut TapeReader<'_>) -> Result<Self, TapeDecodeError> {
        let instrument = InstrumentId::read(input)?;
        let kind = BookChunkKind::read(input)?;
        let side = Side::read(input)?;
        let levels = <[Level; BOOK_CHUNK_LEVELS]>::read(input)?;
        let len = u8::read(input)?;
        if usize::from(len) > BOOK_CHUNK_LEVELS {
            return Err(TapeDecodeError::OutOfRange {
                field: "book chunk length",
                value: u64::from(len),
            });
        }
        let mut chunk = BookChunk::new(instrument, kind, side);
        chunk.push_levels(&levels[..usize::from(len)]);
        chunk.is_last_chunk = bool::read(input)?;
        chunk.update_id = u64::read(input)?;
        chunk.exchange_ts_us = Option::<VenueTs>::read(input)?;
        Ok(chunk)
    }
}

const NO_BALANCE: AssetBalance = AssetBalance {
    asset: AssetId::UNKNOWN,
    free: 0,
    locked: 0,
};

const ACCOUNT_SLOTS: usize = crate::msg::exec::ACCOUNT_CHUNK_ASSETS;

/// As a book chunk: each balance slot, the unused ones zero, then the count.
impl TapeField for AccountChunk {
    const WIDTH: usize = VenueId::WIDTH
        + AccountChunkKind::WIDTH
        + <[AssetBalance; ACCOUNT_SLOTS]>::WIDTH
        + u8::WIDTH
        + bool::WIDTH
        + u64::WIDTH
        + VenueTs::WIDTH;
    const DIGEST: u64 = LayoutDigest::START
        .text("struct AccountChunk")
        .text("venue")
        .part(VenueId::WIDTH, VenueId::DIGEST)
        .text("kind")
        .part(AccountChunkKind::WIDTH, AccountChunkKind::DIGEST)
        .text("balances")
        .part(
            <[AssetBalance; ACCOUNT_SLOTS]>::WIDTH,
            <[AssetBalance; ACCOUNT_SLOTS]>::DIGEST,
        )
        .text("len")
        .part(u8::WIDTH, u8::DIGEST)
        .text("is_last_chunk")
        .part(bool::WIDTH, bool::DIGEST)
        .text("venue_update_ts_ms")
        .part(u64::WIDTH, u64::DIGEST)
        .text("exchange_ts_us")
        .part(VenueTs::WIDTH, VenueTs::DIGEST)
        .finish();

    fn write(&self, out: &mut Vec<u8>) {
        self.venue.write(out);
        self.kind.write(out);
        let active = self.active_balances();
        let mut balances = [NO_BALANCE; ACCOUNT_SLOTS];
        balances[..active.len()].copy_from_slice(active);
        balances.write(out);
        (active.len() as u8).write(out);
        self.is_last_chunk.write(out);
        self.venue_update_ts_ms.write(out);
        self.exchange_ts_us.write(out);
    }

    fn read(input: &mut TapeReader<'_>) -> Result<Self, TapeDecodeError> {
        let venue = VenueId::read(input)?;
        let kind = AccountChunkKind::read(input)?;
        let balances = <[AssetBalance; ACCOUNT_SLOTS]>::read(input)?;
        let len = u8::read(input)?;
        if usize::from(len) > ACCOUNT_SLOTS {
            return Err(TapeDecodeError::OutOfRange {
                field: "account chunk length",
                value: u64::from(len),
            });
        }
        let is_last_chunk = bool::read(input)?;
        let venue_update_ts_ms = u64::read(input)?;
        let exchange_ts_us = VenueTs::read(input)?;
        let mut chunk = AccountChunk::new(kind, venue_update_ts_ms, exchange_ts_us);
        chunk.venue = venue;
        chunk.push_balances(&balances[..usize::from(len)]);
        chunk.is_last_chunk = is_last_chunk;
        Ok(chunk)
    }
}

/// The schema hash, the stamp, the count, then every slot, as the link carries them.
impl TapeField for LinkPayload {
    const WIDTH: usize =
        LinkHash::WIDTH + VenueTs::WIDTH + u8::WIDTH + <[f64; LINK_MAX_FIELDS]>::WIDTH;
    const DIGEST: u64 = LayoutDigest::START
        .text("struct LinkPayload")
        .text("schema_hash")
        .part(LinkHash::WIDTH, LinkHash::DIGEST)
        .text("event_ts_us")
        .part(VenueTs::WIDTH, VenueTs::DIGEST)
        .text("count")
        .part(u8::WIDTH, u8::DIGEST)
        .text("values")
        .part(
            <[f64; LINK_MAX_FIELDS]>::WIDTH,
            <[f64; LINK_MAX_FIELDS]>::DIGEST,
        )
        .finish();

    fn write(&self, out: &mut Vec<u8>) {
        let (count, values) = self.wire_parts();
        self.schema_hash.write(out);
        self.event_ts_us.write(out);
        count.write(out);
        values.write(out);
    }

    fn read(input: &mut TapeReader<'_>) -> Result<Self, TapeDecodeError> {
        let schema_hash = LinkHash::read(input)?;
        let event_ts_us = VenueTs::read(input)?;
        let count = u8::read(input)?;
        let values = <[f64; LINK_MAX_FIELDS]>::read(input)?;
        LinkPayload::from_wire(schema_hash, event_ts_us, count, values).ok_or(
            TapeDecodeError::OutOfRange {
                field: "link payload count",
                value: u64::from(count),
            },
        )
    }
}
