//! The written obligations of a venue execution edge, held for the simulator: its matching model
//! answers through its own wire, behind the Binance codec that a simulated run decodes with.

use engine::msg::exec::{ExecEvent, OrderStyle, VenueOrderStatus};
use engine::msg::ids::{ClientOrderId, InstrumentId, Price, Qty, Side, VenueOrderId};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::time::{DurationUs, LocalTs};
use engine::venues::binance::exec::{
    ResponseContext, StreamEvent, decode_response, decode_stream_event,
};
use engine::venues::exec::ExecRequest;
use engine::venues::sim::core::orders::SimOrder;
use engine::venues::sim::core::{SimEmission, SimVenue, VenueEvent};
use engine::venues::sim::wire::{
    SimBalance, SimFill, VenueWire, response_messages, stream_messages,
};
use serde_json::Value;

use super::lane_order::{INSTRUMENT, LATENCY, at_ms, cents, live_venue, milli_btc, print, stamp};
use crate::venues::binance::fake_venue::{FakeVenue, RUN_NONCE};
use crate::venues::binance::obligations::UNMAPPED_REFUSAL;
use crate::venues::exec::obligations::{
    BalanceReport, EdgeHarness, NOW, own, venue_edge_keeps_each_written_obligation,
};

/// Each step of the stand-in moves the clock of the venue on by this: past each latency of one
/// request, so that one step is done before the next begins.
const STEP: DurationUs = DurationUs::from_secs(1);
const HALF_STEP: DurationUs = DurationUs::from_millis(500);

/// The simulated venue on a book with a bid at 100.00 and an ask at 101.00. It speaks as the
/// identity of the committed Binance payloads, whose codec context decodes each of its words.
pub(crate) struct SimulatorEdge {
    venue: SimVenue,
    wire: VenueWire,
    codec: FakeVenue,
    /// The clock of the venue, apart from the received stamps that the suite passes.
    clock: LocalTs,
    next_print: u64,
}

impl SimulatorEdge {
    fn step(&mut self) -> LocalTs {
        self.clock = self.clock + STEP;
        self.clock
    }

    fn response_context(&self, request: ExecRequest, at: LocalTs) -> ResponseContext<'_> {
        ResponseContext {
            decode: self.codec.decode_context(at),
            request,
            recon_seq: 0,
        }
    }

    /// What the venue emits up to `through`, in the words of its wire, decoded as the edge decodes
    /// them: the answer to `request` first, then the reports of the stream.
    fn emitted(
        &mut self,
        through: LocalTs,
        request: Option<ExecRequest>,
        at: LocalTs,
    ) -> Vec<InboundMessage> {
        let mut due = Vec::new();
        self.venue.advance_to_watermark(through, &mut due);
        due.iter()
            .flat_map(|emission| self.spoken(emission, request, at))
            .collect()
    }

    /// The words of the wire for one emission, as the driver of the simulator says them.
    fn spoken(
        &self,
        emission: &SimEmission,
        request: Option<ExecRequest>,
        at: LocalTs,
    ) -> Vec<InboundMessage> {
        let wire = &self.wire;
        let said = emission.at_ts_us;
        let (answers, reports) = match &emission.event {
            VenueEvent::Rested { snapshot, .. } => (
                vec![wire.place_ack(&snapshot.order, said)],
                vec![wire.new_report(&snapshot.order, said)],
            ),
            VenueEvent::Canceled { snapshot } => (
                vec![wire.cancel_ack(&snapshot.order, said)],
                vec![wire.cancel_report(&snapshot.order, said)],
            ),
            VenueEvent::Filled {
                snapshot,
                trade_id,
                settlement,
                ..
            } => {
                let fill = SimFill {
                    trade_id: *trade_id,
                    settlement,
                    fee_asset: "",
                };
                (
                    Vec::new(),
                    vec![wire.trade_report(&snapshot.order, fill, said)],
                )
            }
            other => panic!("the suite asks the simulator for nothing that emits {other:?}"),
        };
        let mut messages = match request {
            Some(request) => response_messages(&answers, &self.response_context(request, at)),
            None => Vec::new(),
        };
        messages.extend(stream_messages(&reports, self.codec.decode_context(at)));
        messages
    }

    /// The account update that the driver sends after a trade: the balances of the wallet, under
    /// a venue stamp that rises with each one.
    fn account_update(&mut self) -> BalanceReport {
        let at = self.clock;
        let millis =
            u64::try_from(at.micros() / 1_000).expect("the venue clock is after the epoch");
        let settlement = self.venue.wallet_mut().settle(millis);
        let assets = self.codec.assets();
        let balances: Vec<SimBalance<'_>> = settlement
            .balances
            .iter()
            .map(|balance| SimBalance {
                asset: assets
                    .name(balance.asset)
                    .expect("the wallet holds assets of the registry"),
                free: Qty::from_mantissa(balance.free),
                locked: Qty::from_mantissa(balance.locked),
            })
            .collect();
        let payload = self
            .wire
            .account_position(&balances, at, settlement.update_ts_ms);
        match decode_stream_event(&payload, &self.codec.decode_context(NOW))
            .expect("the venue emits only payloads the decoder accepts")
        {
            StreamEvent::Account(chunks) => BalanceReport::Chunks(chunks),
            other => panic!("an account update decoded as {other:?}"),
        }
    }
}

/// An order of `client_id` that rests at the bid of the suite with nothing filled.
fn resting_order(client_id: ClientOrderId) -> SimOrder {
    SimOrder {
        client_id,
        venue_order_id: VenueOrderId(12_510_053_279),
        side: Side::Buy,
        price: SimulatorEdge::RESTING_BID,
        qty: SimulatorEdge::QTY,
        filled: Qty::from_mantissa(0),
        filled_quote: engine::msg::ids::Notional::from_mantissa(0),
    }
}

impl EdgeHarness for SimulatorEdge {
    const VENUE: &'static str = "simulated";
    const INSTRUMENT: InstrumentId = INSTRUMENT;
    const RUN_NONCE: u32 = RUN_NONCE;
    /// Inside the spread, so the order rests first in its level and a print at its price fills it.
    const RESTING_BID: Price = cents(10_050);
    const QTY: Qty = milli_btc(4);
    /// The simulator reports the balances of its wallet whole after each trade, and never a change.
    const SENDS_BALANCE_DELTAS: bool = false;
    /// The edge of the simulator stamps its messages itself, not through the event funnel.
    const QUEUES_THROUGH_FUNNEL: bool = false;

    fn new() -> Self {
        let codec = FakeVenue::new();
        Self {
            venue: live_venue(1_024),
            wire: VenueWire::new(codec.identity()),
            codec,
            clock: stamp(at_ms(1_000)),
            next_print: 1,
        }
    }

    fn answer(&mut self, request: ExecRequest, at: LocalTs) -> Vec<InboundMessage> {
        let issued = self.step();
        self.venue.on_request(&request, LATENCY.arrival(issued));
        self.emitted(issued + HALF_STEP, Some(request), at)
    }

    /// A print that sells `qty` at the price of the order, which rests first in its level.
    fn fill(&mut self, _client_id: ClientOrderId, qty: Qty, at: LocalTs) -> Vec<InboundMessage> {
        let received = self.step();
        let id = self.next_print;
        self.next_print += 1;
        let sold = print(received.micros(), id, Side::Sell, Self::RESTING_BID, qty);
        self.venue
            .on_market_batch(&[sold], LATENCY)
            .expect("the inbox holds one print");
        self.emitted(received + HALF_STEP, None, at)
    }

    fn balance_reports(&mut self) -> Vec<BalanceReport> {
        let client_id = own::<Self>(1);
        let place = ExecRequest::Place {
            instrument: Self::INSTRUMENT,
            client_id,
            side: Side::Buy,
            price: Self::RESTING_BID,
            qty: Self::QTY,
            style: OrderStyle::PostOnly,
        };
        self.answer(place, NOW);
        let quarter = Qty::from_mantissa(Self::QTY.mantissa() / 4);
        (0..2)
            .map(|_| {
                self.fill(client_id, quarter, NOW);
                self.account_update()
            })
            .collect()
    }

    fn unmapped_refusal(&mut self, request: ExecRequest, at: LocalTs) -> Vec<ExecEvent> {
        decode_response(UNMAPPED_REFUSAL, &self.response_context(request, at))
            .expect("an error answer decodes")
            .events
    }

    fn pending_cancel(&mut self, client_id: ClientOrderId, at: LocalTs) -> Option<ExecEvent> {
        let text = self.wire.order_status_as(
            &resting_order(client_id),
            VenueOrderStatus::PendingCancel,
            at,
        );
        let request = ExecRequest::OrderStatus {
            instrument: Self::INSTRUMENT,
            client_id,
        };
        let messages = response_messages(&[text], &self.response_context(request, at));
        let [message] = messages.as_slice() else {
            panic!("one status answer gives one event: {messages:?}");
        };
        match message.body {
            MessageBody::Exec(event) => Some(event),
            ref other => panic!("a status answer decoded as {other:?}"),
        }
    }

    /// The simulator never sends an order that its own codec cannot read, so the text is its own
    /// answer to a read of open orders with the price of the order spoiled.
    fn undecodable_open_order(&mut self, client_id: ClientOrderId) -> (i64, bool) {
        let order = resting_order(client_id);
        let mut answer: Value = serde_json::from_str(&self.wire.open_orders(&[order], NOW))
            .expect("the wire writes json");
        answer["result"][0]["price"] = Value::from("not-a-price");
        let request = ExecRequest::OpenOrders {
            instrument: Self::INSTRUMENT,
        };
        let decoded = decode_response(&answer.to_string(), &self.response_context(request, NOW));
        (order.venue_order_id.0, decoded.is_ok())
    }
}

venue_edge_keeps_each_written_obligation!(SimulatorEdge);
