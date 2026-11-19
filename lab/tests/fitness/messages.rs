//! Turns drawn steps into messages, with the message time, the sequence numbers and the venue
//! totals that each kind of message carries.

use engine::hot::exec::{ClientIdLayout, side_base};
use engine::link::{LinkFrame, LinkHash, LinkOrigin, LinkPayload, TopicId};
use engine::msg::exec::{
    AccountChunk, AccountChunkKind, AssetBalance, ExecEvent, ExecKind, Liquidity, Provenance,
    VenueOrderStatus,
};
use engine::msg::ids::{
    AggregateTradeId, AssetId, ClientOrderId, InstrumentId, Notional, Price, Qty, RawTradeId, Side,
    StreamEpoch, VenueId,
};
use engine::msg::inbound::{
    AuditBankFull, BookChunk, BookChunkKind, BookReset, FileWriter, InboundMessage, KlineEvent,
    Level, MarketRotation, MessageBody, RunControl, SpinTick, TradeEvent, WriterFailure,
};
use engine::msg::run_state::{ControllerEpoch, RunAssertion, RunState};
use engine::msg::time::{LocalTs, VenueTs};
use engine::runtime::config::KlineInterval;
use engine::venues::exec::open_orders_snapshot_end;

use crate::draws::{Answer, Chunk, Draw, Step};

pub(crate) const ONE: i64 = 100_000_000;

/// The size that the probe quotes on each side, and of each answer about one of its orders.
pub(crate) const QUOTE_QTY: Qty = Qty::from_mantissa(ONE / 10);

/// What a drawn sequence keeps from one message to the next: message time, the next spin and peer
/// frame numbers, the run epoch, and the fills of the order an earlier run left on each market.
pub(crate) struct Builder {
    when: i64,
    spin_seq: u64,
    link_seq: u64,
    epoch: u64,
    pens: [FillPen; 2],
}

impl Builder {
    pub(crate) fn sequence(steps: &[Step]) -> Vec<InboundMessage> {
        let mut builder = Self {
            when: 1_000_000,
            spin_seq: 0,
            link_seq: 0,
            epoch: 0,
            pens: [FillPen::new(0), FillPen::new(1)],
        };
        steps.iter().map(|step| builder.message(step)).collect()
    }

    fn message(&mut self, step: &Step) -> InboundMessage {
        self.when += step.gap_us;
        let when = self.when;
        let body = match step.draw {
            Draw::Trade {
                market,
                offset,
                lots,
                side,
            } => MessageBody::Trade(trade(
                market,
                (98 + offset) * ONE,
                lots * ONE / 10,
                side,
                when,
            )),
            Draw::Book {
                market,
                chunk,
                side,
                offset,
                lots,
            } => MessageBody::Book(book_chunk(market, chunk, side, offset, lots, when)),
            Draw::BookReset { market } => MessageBody::BookReset(BookReset {
                instrument: InstrumentId(market),
                exchange_ts_us: venue_ts(when),
            }),
            Draw::Rotation { market } => MessageBody::MarketRotation(MarketRotation {
                instrument: InstrumentId(market),
                window_open_ts_us: venue_ts(when),
                window_close_ts_us: venue_ts(when + 300_000_000),
            }),
            Draw::Kline {
                market,
                offset,
                is_closed,
                is_tracked,
            } => {
                let interval =
                    if is_tracked { KlineInterval::OneMinute } else { KlineInterval::FiveMinutes };
                MessageBody::Kline(kline(
                    market,
                    interval,
                    (99 + offset) * ONE,
                    is_closed,
                    when,
                ))
            }
            Draw::Spin => {
                self.spin_seq += 1;
                MessageBody::SpinTick(SpinTick { seq: self.spin_seq })
            }
            Draw::Link { value } => {
                self.link_seq += 1;
                MessageBody::Link(peer_frame(self.link_seq, f64::from(value), when))
            }
            Draw::RunControl {
                is_running,
                is_new_epoch,
            } => {
                self.epoch += u64::from(is_new_epoch);
                let state = if is_running { RunState::Running } else { RunState::Idle };
                let desired = RunAssertion {
                    state,
                    epoch: ControllerEpoch::new(self.epoch),
                };
                MessageBody::RunControl(RunControl { desired })
            }
            Draw::Exec {
                market,
                side,
                answer,
            } => self.answer(market, side, answer, when),
            Draw::Account {
                base_units,
                quote_units,
            } => MessageBody::Account(account(
                &[
                    (AssetId(0), base_units * ONE),
                    (AssetId(1), quote_units * ONE),
                ],
                when,
            )),
            Draw::WriterFailed {
                consecutive_failures,
            } => MessageBody::WriterFailed(WriterFailure {
                writer: FileWriter::Exposure,
                consecutive_failures,
            }),
            Draw::AuditBankFull { banked } => MessageBody::AuditBankFull(AuditBankFull {
                banked,
                capacity: 65_536,
            }),
        };
        InboundMessage::new(ts(when), body)
    }

    fn answer(&mut self, market: u16, side: Side, answer: Answer, when: i64) -> MessageBody {
        let instrument = InstrumentId(market);
        let event = match answer {
            Answer::StreamReady => ExecEvent {
                kind: ExecKind::StreamReady,
                ..exec_event(instrument, ClientOrderId(0), Side::Buy, 0, when)
            },
            Answer::SnapshotEnd => open_orders_snapshot_end(instrument, ts(when)),
            Answer::PriorFill { offset, lots } => {
                let pen = &mut self.pens[usize::from(market)];
                let price = (99 + offset) * ONE;
                pen.adopt(side, price, when)
                    .unwrap_or_else(|| pen.report(side, price, lots * ONE / 10, when))
            }
            Answer::Slot {
                slot,
                generation,
                kind,
                status,
            } => {
                let client_id = ClientIdLayout { run_nonce: 0 }
                    .encode(side_base(instrument, side) + slot, generation);
                let price = if side == Side::Buy { 99 * ONE } else { 101 * ONE };
                let is_fill = kind == ExecKind::ReportTrade;
                let filled = if is_fill { QUOTE_QTY } else { Qty::from_mantissa(0) };
                ExecEvent {
                    kind,
                    status: Some(status),
                    qty: QUOTE_QTY,
                    last_price: Price::from_mantissa(if is_fill { price } else { 0 }),
                    last_qty: filled,
                    cumulative_qty: filled,
                    cumulative_quote: Price::from_mantissa(price).notional(filled),
                    ..exec_event(instrument, client_id, side, price, when)
                }
            }
        };
        MessageBody::Exec(event)
    }
}

/// Seats one order of an earlier run on each side of one market, then reports fills against it
/// with the cumulative totals that a venue sends.
struct FillPen {
    instrument: InstrumentId,
    cumulative: [(i64, i64); 2],
    is_adopted: [bool; 2],
}

impl FillPen {
    fn new(instrument: u16) -> Self {
        Self {
            instrument: InstrumentId(instrument),
            cumulative: [(0, 0); 2],
            is_adopted: [false; 2],
        }
    }

    fn client_id(&self, side: Side) -> ClientOrderId {
        ClientIdLayout { run_nonce: 0 }.encode(side_base(self.instrument, side) + 1, 1)
    }

    fn adopt(&mut self, side: Side, price: i64, when: i64) -> Option<ExecEvent> {
        if self.is_adopted[side as usize] {
            return None;
        }
        self.is_adopted[side as usize] = true;
        Some(ExecEvent {
            kind: ExecKind::SnapshotOrder,
            qty: Qty::from_mantissa(i64::MAX / 4),
            ..exec_event(self.instrument, self.client_id(side), side, price, when)
        })
    }

    fn report(&mut self, side: Side, price: i64, qty: i64, when: i64) -> ExecEvent {
        let total = &mut self.cumulative[side as usize];
        total.0 += qty;
        total.1 += Price::from_mantissa(price)
            .notional(Qty::from_mantissa(qty))
            .mantissa();
        ExecEvent {
            kind: ExecKind::ReportTrade,
            status: Some(VenueOrderStatus::PartiallyFilled),
            last_price: Price::from_mantissa(price),
            last_qty: Qty::from_mantissa(qty),
            cumulative_qty: Qty::from_mantissa(total.0),
            cumulative_quote: Notional::from_mantissa(total.1),
            qty: Qty::from_mantissa(i64::MAX / 4),
            ..exec_event(self.instrument, self.client_id(side), side, price, when)
        }
    }
}

fn ts(us: i64) -> LocalTs {
    LocalTs::from_micros(us)
}

fn venue_ts(us: i64) -> VenueTs {
    VenueTs::from_micros(us)
}

fn trade(market: u16, price: i64, qty: i64, side: Side, when: i64) -> TradeEvent {
    TradeEvent {
        instrument: InstrumentId(market),
        price: Price::from_mantissa(price),
        qty: Qty::from_mantissa(qty),
        side,
        exchange_ts_us: venue_ts(when),
        exchange_sent_ts_us: None,
        aggregate_id: AggregateTradeId(0),
        first_trade_id: RawTradeId(0),
        last_trade_id: RawTradeId(0),
        stream_epoch: StreamEpoch::default(),
    }
}

/// Prices a few units from 100 on their own side. The first chunk of a snapshot is not the last,
/// so the book waits for the asks before it is whole.
fn book_chunk(
    market: u16,
    chunk: Chunk,
    side: Side,
    offset: i64,
    lots: i64,
    when: i64,
) -> BookChunk {
    let bid = (99 - offset) * ONE;
    let ask = (101 + offset) * ONE;
    let (kind, side, levels, is_last_chunk) = match chunk {
        Chunk::Delta => {
            let price = if side == Side::Buy { bid } else { ask };
            (
                BookChunkKind::Delta,
                side,
                vec![(price, lots * ONE / 2)],
                true,
            )
        }
        Chunk::SnapshotBids => (
            BookChunkKind::Snapshot,
            Side::Buy,
            vec![(bid, ONE), (bid - ONE, 2 * ONE)],
            false,
        ),
        Chunk::SnapshotAsks => (
            BookChunkKind::Snapshot,
            Side::Sell,
            vec![(ask, ONE), (ask + ONE, 2 * ONE)],
            true,
        ),
    };
    let mut book = BookChunk::new(InstrumentId(market), kind, side);
    for (price, qty) in levels {
        book.push_levels(&[Level {
            price: Price::from_mantissa(price),
            qty: Qty::from_mantissa(qty),
        }]);
    }
    book.is_last_chunk = is_last_chunk;
    book.update_id = when as u64;
    book
}

fn kline(
    market: u16,
    interval: KlineInterval,
    close: i64,
    is_closed: bool,
    when: i64,
) -> KlineEvent {
    KlineEvent {
        instrument: InstrumentId(market),
        interval,
        open_ts_us: venue_ts(when),
        open: Price::from_mantissa(100 * ONE),
        high: Price::from_mantissa(104 * ONE),
        low: Price::from_mantissa(98 * ONE),
        close: Price::from_mantissa(close),
        base_volume: Qty::from_mantissa(ONE),
        quote_volume: 42,
        trade_count: 3,
        is_closed,
        exchange_ts_us: venue_ts(when),
        exchange_sent_ts_us: None,
    }
}

/// A state frame of a peer engine, carrying one value.
fn peer_frame(seq: u64, value: f64, when: i64) -> LinkFrame {
    LinkFrame {
        origin: LinkOrigin {
            sender_te_hash: LinkHash::of_name("peer-te"),
            boot_ts_us: venue_ts(1),
            topic: TopicId::FIRST_STRATEGY,
            seq,
        },
        payload: LinkPayload::new(
            LinkHash::of_name("peer-schema"),
            venue_ts(when - 1),
            &[value],
        ),
    }
}

/// A whole-account snapshot in one chunk with nothing locked. It carries its own venue time, or
/// the engine sets it aside as stale.
fn account(balances: &[(AssetId, i64)], when: i64) -> AccountChunk {
    let mut chunk = AccountChunk::new(AccountChunkKind::Snapshot, 1, venue_ts(when));
    for &(asset, free) in balances {
        chunk.push_balances(&[AssetBalance {
            asset,
            free,
            locked: 0,
        }]);
    }
    chunk.is_last_chunk = true;
    chunk.venue_update_ts_ms = u64::try_from(when / 1_000).expect("message time is positive");
    chunk
}

/// A zero-valued event with the identity fields set, so each caller states only what it varies.
fn exec_event(
    instrument: InstrumentId,
    client_id: ClientOrderId,
    side: Side,
    price: i64,
    when: i64,
) -> ExecEvent {
    ExecEvent {
        venue: VenueId(0),
        instrument: Some(instrument),
        client_id,
        venue_order_id: None,
        trade_id: None,
        kind: ExecKind::ReportNew,
        status: None,
        reject: None,
        provenance: Provenance::Mine,
        side,
        liquidity: Some(Liquidity::Maker),
        price: Price::from_mantissa(price),
        qty: Qty::from_mantissa(0),
        last_price: Price::from_mantissa(0),
        last_qty: Qty::from_mantissa(0),
        cumulative_qty: Qty::from_mantissa(0),
        cumulative_quote: Notional::from_mantissa(0),
        commission: 0,
        commission_asset: AssetId::UNKNOWN,
        reject_code: 0,
        amends_remaining: ExecEvent::AMENDS_UNKNOWN,
        recon_seq: 0,
        exchange_ts_us: venue_ts(when),
        request_sent_ts_us: None,
    }
}
