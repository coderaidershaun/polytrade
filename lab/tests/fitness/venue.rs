//! A venue of the test that answers the orders of a recorded run: an ack 15 ms after each place and
//! each cancel, and a fill when a print trades through a resting quote. Case 1 of X1 uses this lab
//! test venue; through the real simulator it is re-run by S4 when VS-25 lifts the stepper. The
//! re-fold reads the answers from the tape, so it does not matter for X1 who made them.

use engine::msg::exec::{
    AccountChunk, AccountChunkKind, AssetBalance, ExecCommand, ExecEvent, ExecKind, Liquidity,
    Provenance, StampedExecCommand, VenueOrderStatus,
};
use engine::msg::ids::{AssetId, ClientOrderId, InstrumentId, Notional, Price, Qty, Side, VenueId};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::time::{LocalTs, VenueTs};
use engine::venues::exec::open_orders_snapshot_end;

/// The order entry and ack latency of the shipped `execution.sim` block.
const ACK_US: i64 = 15_000;

const ONE: i64 = 100_000_000;

#[derive(Debug, Clone, Copy)]
struct Resting {
    instrument: InstrumentId,
    client_id: ClientOrderId,
    side: Side,
    price: Price,
    qty: Qty,
    /// From its ack on, a print can fill it.
    live_at: i64,
}

pub(crate) struct TestVenue {
    base: AssetId,
    quote: AssetId,
    resting: Vec<Resting>,
    /// Answers in the order they fall due.
    due: Vec<InboundMessage>,
    pub(crate) fills: usize,
}

impl TestVenue {
    pub(crate) fn new(base: AssetId, quote: AssetId) -> Self {
        Self {
            base,
            quote,
            resting: Vec::new(),
            due: Vec::new(),
            fills: 0,
        }
    }

    /// The stream is up, no order rests from before, and the account holds the shipped opening
    /// balances of the simulator.
    pub(crate) fn open(&mut self, instrument: InstrumentId, at: i64) {
        let stream_ready = ExecEvent {
            kind: ExecKind::StreamReady,
            ..event(
                instrument,
                ClientOrderId(0),
                Side::Buy,
                Price::from_mantissa(0),
                at,
            )
        };
        let mut account =
            AccountChunk::new(AccountChunkKind::Snapshot, 1, VenueTs::from_micros(at));
        account.venue = VenueId(0);
        account.push_balances(&[
            AssetBalance {
                asset: self.base,
                free: ONE / 100,
                locked: 0,
            },
            AssetBalance {
                asset: self.quote,
                free: 1_000 * ONE,
                locked: 0,
            },
        ]);
        account.is_last_chunk = true;
        account.venue_update_ts_ms = u64::try_from(at / 1_000).expect("message time is positive");
        let ts = LocalTs::from_micros(at);
        for body in [
            MessageBody::Exec(stream_ready),
            MessageBody::Exec(open_orders_snapshot_end(instrument, ts)),
            MessageBody::Account(account),
        ] {
            self.due.push(InboundMessage::new(ts, body));
        }
    }

    pub(crate) fn on_command(&mut self, stamped: &StampedExecCommand) {
        let at = stamped.issued_ts_us.micros() + ACK_US;
        match stamped.command {
            ExecCommand::Place {
                instrument,
                client_id,
                side,
                price,
                qty,
                ..
            } => {
                self.resting.push(Resting {
                    instrument,
                    client_id,
                    side,
                    price,
                    qty,
                    live_at: at,
                });
                let ack = ExecEvent {
                    kind: ExecKind::AckPlaced,
                    status: Some(VenueOrderStatus::New),
                    qty,
                    ..event(instrument, client_id, side, price, at)
                };
                self.answer(ack, at);
            }
            ExecCommand::Cancel {
                instrument,
                client_id,
            } => self.cancel(instrument, client_id, at),
            ExecCommand::CancelOurs { instrument, .. } => {
                let ours: Vec<ClientOrderId> = self
                    .resting
                    .iter()
                    .filter(|order| order.instrument == instrument)
                    .map(|order| order.client_id)
                    .collect();
                for client_id in ours {
                    self.cancel(instrument, client_id, at);
                }
            }
            _ => {}
        }
    }

    /// A print below a resting bid or above a resting ask fills that order whole, at its price.
    pub(crate) fn on_market(&mut self, message: &InboundMessage) {
        let MessageBody::Trade(trade) = message.body else {
            return;
        };
        let at = message.header.received_ts_us.micros();
        let (filled, resting): (Vec<Resting>, Vec<Resting>) =
            self.resting.iter().partition(|order| {
                let is_through = match order.side {
                    Side::Buy => trade.price < order.price,
                    Side::Sell => trade.price > order.price,
                };
                order.instrument == trade.instrument && order.live_at <= at && is_through
            });
        self.resting = resting;
        for order in filled {
            self.fills += 1;
            let fill = ExecEvent {
                kind: ExecKind::ReportTrade,
                status: Some(VenueOrderStatus::Filled),
                qty: order.qty,
                last_price: order.price,
                last_qty: order.qty,
                cumulative_qty: order.qty,
                cumulative_quote: order.price.notional(order.qty),
                ..event(
                    order.instrument,
                    order.client_id,
                    order.side,
                    order.price,
                    at,
                )
            };
            self.answer(fill, at);
        }
    }

    /// Each answer that falls due at or before `through`, in order.
    pub(crate) fn due_through(&mut self, through: i64) -> Vec<InboundMessage> {
        self.due
            .sort_by_key(|message| message.header.received_ts_us.micros());
        let split = self
            .due
            .partition_point(|message| message.header.received_ts_us.micros() <= through);
        self.due.drain(..split).collect()
    }

    fn cancel(&mut self, instrument: InstrumentId, client_id: ClientOrderId, at: i64) {
        let found = self
            .resting
            .iter()
            .position(|order| order.client_id == client_id);
        let answer = match found {
            Some(index) => {
                let order = self.resting.remove(index);
                ExecEvent {
                    kind: ExecKind::AckCanceled,
                    status: Some(VenueOrderStatus::Canceled),
                    qty: order.qty,
                    ..event(instrument, client_id, order.side, order.price, at)
                }
            }
            // It filled before the cancel came: the venue knows it only as filled.
            None => ExecEvent {
                kind: ExecKind::AckFailed,
                status: Some(VenueOrderStatus::Filled),
                ..event(
                    instrument,
                    client_id,
                    Side::Buy,
                    Price::from_mantissa(0),
                    at,
                )
            },
        };
        self.answer(answer, at);
    }

    fn answer(&mut self, event: ExecEvent, at: i64) {
        self.due.push(InboundMessage::new(
            LocalTs::from_micros(at),
            MessageBody::Exec(event),
        ));
    }
}

/// A zero-valued event of this venue with the identity fields set.
fn event(
    instrument: InstrumentId,
    client_id: ClientOrderId,
    side: Side,
    price: Price,
    at: i64,
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
        price,
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
        exchange_ts_us: VenueTs::from_micros(at),
        request_sent_ts_us: None,
    }
}
