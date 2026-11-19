//! The kind id of each tape row and the digest of its payload. A message kind keeps its id below
//! 256 in the order of the message kinds; a changed payload takes a new id, and the old id and its
//! decoder stay until a person retires them, so each recorded tape stays readable.

use crate::msg::codec::LayoutDigest;
use crate::msg::exec::{AccountChunk, ExecEvent};
use crate::msg::inbound::{
    AuditBankFull, BookChunk, BookReset, KlineEvent, MarketRotation, MessageBody, RunControl,
    SpinTick, TradeEvent, WriterFailure,
};
use crate::msg::link::LinkFrame;
use crate::msg::persist::{BootExposure, BootRecord, BootVenue, TapeGap};

use super::{TapeDecodeError, TapeField, TapeReader};

/// One kind of tape row as the footer names it: its id, its name, and the digest of its payload.
/// A reader refuses a file whose digest of a kind it reads differs from its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TapeKindInfo {
    pub id: u16,
    pub name: &'static str,
    pub digest: u64,
}

impl TapeKindInfo {
    /// The digest as the footer and the contract write it: `0x` and 16 lowercase hex digits.
    pub fn digest_text(&self) -> String {
        format!("0x{:016x}", self.digest)
    }
}

pub(in crate::tape) const BOOT_KIND: u16 = 256;
pub(in crate::tape) const BOOK_SNAPSHOT_KIND: u16 = 257;
pub(in crate::tape) const GAP_KIND: u16 = 258;

const fn kind(id: u16, name: &'static str, width: usize, digest: u64) -> TapeKindInfo {
    TapeKindInfo {
        id,
        name,
        digest: LayoutDigest::START
            .text("tape")
            .text(name)
            .number(id as u64)
            .part(width, digest)
            .finish(),
    }
}

const fn of<T: TapeField>(id: u16, name: &'static str) -> TapeKindInfo {
    kind(id, name, T::WIDTH, T::DIGEST)
}

/// The boot row holds two counted lists, so its width depends on the run.
const BOOT_DIGEST: u64 = LayoutDigest::START
    .text("counted")
    .part(BootVenue::WIDTH, BootVenue::DIGEST)
    .text("counted")
    .part(BootExposure::WIDTH, BootExposure::DIGEST)
    .finish();

/// The two sequence numbers of a gap; its stamp is the stamp of the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GapSpan {
    pub first_lost_seq: u64,
    pub last_lost_seq: u64,
}

super::tape_struct!(GapSpan {
    first_lost_seq,
    last_lost_seq
});
super::tape_struct!(BootVenue { venue, run_nonce });
super::tape_struct!(BootExposure {
    instrument,
    position_base,
    cash_quote,
    basis_quote
});

/// Each kind this build writes and reads, in id order.
pub const TAPE_KINDS: &[TapeKindInfo] = &[
    of::<TradeEvent>(1, "trade"),
    of::<BookChunk>(2, "book"),
    of::<BookReset>(3, "book_reset"),
    of::<MarketRotation>(4, "market_rotation"),
    of::<KlineEvent>(5, "kline"),
    of::<SpinTick>(6, "spin_tick"),
    of::<LinkFrame>(7, "link"),
    of::<RunControl>(8, "run_control"),
    of::<ExecEvent>(9, "exec"),
    of::<AccountChunk>(10, "account"),
    of::<WriterFailure>(11, "writer_failed"),
    of::<AuditBankFull>(12, "audit_bank_full"),
    kind(BOOT_KIND, "boot", 0, BOOT_DIGEST),
    of::<BookChunk>(BOOK_SNAPSHOT_KIND, "book_snapshot"),
    of::<GapSpan>(GAP_KIND, "gap"),
];

/// What one tape row holds, read back.
#[derive(Debug, Clone, PartialEq)]
pub enum TapePayload {
    Message(MessageBody),
    BookSnapshot(BookChunk),
    Gap(GapSpan),
    Boot(BootRecord),
}

/// Writes the payload of `body` into `out` and gives its kind id.
pub(in crate::tape) fn encode_message(body: &MessageBody, out: &mut Vec<u8>) -> u16 {
    match body {
        MessageBody::Trade(event) => write(1, event, out),
        MessageBody::Book(chunk) => write(2, chunk, out),
        MessageBody::BookReset(reset) => write(3, reset, out),
        MessageBody::MarketRotation(rotation) => write(4, rotation, out),
        MessageBody::Kline(event) => write(5, event, out),
        MessageBody::SpinTick(tick) => write(6, tick, out),
        MessageBody::Link(frame) => write(7, frame, out),
        MessageBody::RunControl(control) => write(8, control, out),
        MessageBody::Exec(event) => write(9, event, out),
        MessageBody::Account(chunk) => write(10, chunk, out),
        MessageBody::WriterFailed(failure) => write(11, failure, out),
        MessageBody::AuditBankFull(full) => write(12, full, out),
    }
}

pub(in crate::tape) fn encode_snapshot(chunk: &BookChunk, out: &mut Vec<u8>) -> u16 {
    write(BOOK_SNAPSHOT_KIND, chunk, out)
}

pub(in crate::tape) fn encode_gap(gap: &TapeGap, out: &mut Vec<u8>) -> u16 {
    let span = GapSpan {
        first_lost_seq: gap.first_lost_seq,
        last_lost_seq: gap.last_lost_seq,
    };
    write(GAP_KIND, &span, out)
}

/// A count of each list, then its entries.
pub(in crate::tape) fn encode_boot(boot: &BootRecord, out: &mut Vec<u8>) -> u16 {
    (boot.venues.len() as u16).write(out);
    for venue in &boot.venues {
        venue.write(out);
    }
    (boot.exposures.len() as u16).write(out);
    for exposure in &boot.exposures {
        exposure.write(out);
    }
    BOOT_KIND
}

fn write<T: TapeField>(kind: u16, value: &T, out: &mut Vec<u8>) -> u16 {
    value.write(out);
    kind
}

/// Reads the payload of one tape row.
///
/// # Errors
/// [`TapeDecodeError`] when `kind` is no kind of this build, or `bytes` do not hold its layout
/// exactly.
pub fn decode_payload(kind: u16, bytes: &[u8]) -> Result<TapePayload, TapeDecodeError> {
    let mut input = TapeReader::new(bytes);
    let payload = match kind {
        1 => TapePayload::Message(MessageBody::Trade(TradeEvent::read(&mut input)?)),
        2 => TapePayload::Message(MessageBody::Book(BookChunk::read(&mut input)?)),
        3 => TapePayload::Message(MessageBody::BookReset(BookReset::read(&mut input)?)),
        4 => TapePayload::Message(MessageBody::MarketRotation(MarketRotation::read(
            &mut input,
        )?)),
        5 => TapePayload::Message(MessageBody::Kline(KlineEvent::read(&mut input)?)),
        6 => TapePayload::Message(MessageBody::SpinTick(SpinTick::read(&mut input)?)),
        7 => TapePayload::Message(MessageBody::Link(LinkFrame::read(&mut input)?)),
        8 => TapePayload::Message(MessageBody::RunControl(RunControl::read(&mut input)?)),
        9 => TapePayload::Message(MessageBody::Exec(ExecEvent::read(&mut input)?)),
        10 => TapePayload::Message(MessageBody::Account(AccountChunk::read(&mut input)?)),
        11 => TapePayload::Message(MessageBody::WriterFailed(WriterFailure::read(&mut input)?)),
        12 => TapePayload::Message(MessageBody::AuditBankFull(AuditBankFull::read(&mut input)?)),
        BOOT_KIND => TapePayload::Boot(read_boot(&mut input)?),
        BOOK_SNAPSHOT_KIND => TapePayload::BookSnapshot(BookChunk::read(&mut input)?),
        GAP_KIND => TapePayload::Gap(GapSpan::read(&mut input)?),
        other => return Err(TapeDecodeError::UnknownKind { kind: other }),
    };
    input.finish()?;
    Ok(payload)
}

fn read_boot(input: &mut TapeReader<'_>) -> Result<BootRecord, TapeDecodeError> {
    let venues = (0..u16::read(input)?)
        .map(|_| BootVenue::read(input))
        .collect::<Result<_, _>>()?;
    let exposures = (0..u16::read(input)?)
        .map(|_| BootExposure::read(input))
        .collect::<Result<_, _>>()?;
    Ok(BootRecord { venues, exposures })
}
