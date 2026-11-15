//! The simulator speaks the wire of Binance, so each of its answers and reports is decoded by the
//! live codec beside its twin fixture: a simulated venue must never tell the engine something else.

use std::panic::{AssertUnwindSafe, catch_unwind};

use engine::msg::exec::{
    AccountChunkKind, ExecEvent, ExecKind, Liquidity, OrderStyle, Provenance, RejectClass,
    VenueOrderStatus,
};
use engine::msg::ids::{
    AssetId, ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty, Side, TradeId, VenueOrderId,
};
use engine::msg::time::LocalTs;
use engine::venues::binance::exec::{
    ResponseContext, StreamEvent, decode_response, decode_stream_event,
};
use engine::venues::exec::ExecRequest;
use engine::venues::sim::core::orders::SimOrder;
use engine::venues::sim::core::wallet::FillSettlement;
use engine::venues::sim::wire::{Rejection, SimBalance, SimFill, VenueWire};

use crate::venues::binance::fake_venue::{Delivery, FakeVenue, exec_events};

macro_rules! twin {
    ($name:literal) => {
        include_str!(concat!(
            "../../../../fixtures/binance/exec/",
            $name,
            ".json"
        ))
    };
}

const NOW: LocalTs = LocalTs::from_micros(1_785_000_000_700_000);
const INSTRUMENT: InstrumentId = InstrumentId(0);
/// The order that the committed fixtures name, under the identity of the fake venue.
const ORDER_A: ClientOrderId = ClientOrderId(0x6a64_f040_000c_0001);
const VENUE_ORDER: VenueOrderId = VenueOrderId(12_510_053_279);
const TRADE: TradeId = TradeId(778_291);
const PRICE: Price = Price::from_mantissa(118_000 * FIXED_SCALE);
const QTY: Qty = Qty::from_mantissa(10_000);

/// What an event tells the engine. Ids, stamps, and amounts come from each scenario, so they are
/// left out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Shape {
    kind: ExecKind,
    status: Option<VenueOrderStatus>,
    reject: Option<RejectClass>,
    side: Side,
    liquidity: Option<Liquidity>,
    commission_asset: AssetId,
}

impl Shape {
    fn of(event: &ExecEvent) -> Self {
        Self {
            kind: event.kind,
            status: event.status,
            reject: event.reject,
            side: event.side,
            liquidity: event.liquidity,
            commission_asset: event.commission_asset,
        }
    }
}

fn wire(venue: &FakeVenue) -> VenueWire {
    VenueWire::new(venue.identity())
}

fn order(filled: Qty) -> SimOrder {
    SimOrder {
        client_id: ORDER_A,
        venue_order_id: VENUE_ORDER,
        side: Side::Buy,
        price: PRICE,
        qty: QTY,
        filled,
        filled_quote: PRICE.notional(filled),
    }
}

fn place(style: OrderStyle) -> ExecRequest {
    ExecRequest::Place {
        instrument: INSTRUMENT,
        client_id: ORDER_A,
        side: Side::Buy,
        price: PRICE,
        qty: QTY,
        style,
    }
}

fn cancel() -> ExecRequest {
    ExecRequest::Cancel {
        instrument: INSTRUMENT,
        client_id: ORDER_A,
    }
}

fn amend() -> ExecRequest {
    ExecRequest::AmendQty {
        instrument: INSTRUMENT,
        client_id: ORDER_A,
        qty: Qty::from_mantissa(6_000),
    }
}

fn status_query() -> ExecRequest {
    ExecRequest::OrderStatus {
        instrument: INSTRUMENT,
        client_id: ORDER_A,
    }
}

fn answer(text: &str, request: ExecRequest, venue: &FakeVenue) -> Vec<ExecEvent> {
    decode_response(
        text,
        &ResponseContext {
            decode: venue.decode_context(NOW),
            request,
            recon_seq: 7,
        },
    )
    .expect("the text decodes")
    .events
}

fn report(text: &str, venue: &FakeVenue) -> ExecEvent {
    match decode_stream_event(text, &venue.decode_context(NOW)).expect("the text decodes") {
        StreamEvent::Exec(event) => event,
        other => panic!("expected an order event, got {other:?}"),
    }
}

fn shapes(events: &[ExecEvent]) -> Vec<Shape> {
    events.iter().map(Shape::of).collect()
}

/// One fill of `last` that brings `filled` to its total, with a fee of `fee` in the base asset.
fn settlement(filled: &SimOrder, last: Qty, fee: i64) -> FillSettlement {
    let last_quote = PRICE.notional(last);
    FillSettlement {
        last_qty: last,
        last_quote,
        cumulative_qty: filled.filled,
        cumulative_quote: filled.filled_quote,
        debit: last_quote.mantissa(),
        received_gross: last.mantissa(),
        received_net: last.mantissa() - fee,
        fee,
        fee_asset: AssetId::UNKNOWN,
    }
}

/// CONTRACT: each answer and each report of the simulator decodes, through the live codec of
/// Binance, to the kind, status, reject class, side, liquidity, and fee asset of its twin fixture.
/// A variant with no twin is held against the codec table instead. A settlement that agrees with
/// its order gives a partial fill report, and one that disagrees stops the report, because the
/// venue's book and its wallet would then disagree about what moved.
#[test]
fn each_simulator_answer_decodes_as_its_live_fixture() {
    let venue = FakeVenue::new();
    let wire = wire(&venue);
    let resting = order(Qty::from_mantissa(0));
    let partly = order(Qty::from_mantissa(4_000));
    let filled = order(QTY);

    let answers: [(&str, String, &str, ExecRequest); 4] = [
        (
            "place accepted",
            wire.place_ack(&resting, NOW),
            twin!("ack_order_place"),
            place(OrderStyle::PostOnly),
        ),
        (
            "cancel accepted",
            wire.cancel_ack(&resting, NOW),
            twin!("ack_order_cancel"),
            cancel(),
        ),
        (
            "amend accepted",
            wire.amend_ack(&order(Qty::from_mantissa(0)), NOW),
            twin!("ack_order_amend_keep_priority"),
            amend(),
        ),
        (
            "status filled",
            wire.order_status_as(&filled, VenueOrderStatus::Filled, NOW),
            twin!("ack_order_status_filled"),
            status_query(),
        ),
    ];
    for (variant, simulated, fixture, request) in answers {
        assert_eq!(
            shapes(&answer(&simulated, request, &venue)),
            shapes(&answer(fixture, request, &venue)),
            "{variant}: the simulator's answer decodes otherwise than its twin (simulator left)"
        );
    }

    // A status with no twin of its own is the same template with the status replaced, so the
    // codec must read back the status that the simulator meant.
    let status_twin = shapes(&answer(
        twin!("ack_order_status_filled"),
        status_query(),
        &venue,
    ));
    for status in [
        VenueOrderStatus::New,
        VenueOrderStatus::PartiallyFilled,
        VenueOrderStatus::Canceled,
        VenueOrderStatus::Rejected,
    ] {
        let decoded = shapes(&answer(
            &wire.order_status_as(&partly, status, NOW),
            status_query(),
            &venue,
        ));
        assert_eq!(decoded.len(), 1, "status {status:?}");
        assert_eq!(decoded[0].kind, status_twin[0].kind, "status {status:?}");
        assert_eq!(decoded[0].status, Some(status), "status {status:?}");
    }

    // Open orders: the row of this engine's order and the end of the read. The twin also holds
    // an order that a person placed, which the simulator never shows.
    let mine = |events: Vec<ExecEvent>| -> Vec<Shape> {
        events
            .iter()
            .filter(|event| event.provenance != Provenance::Foreign)
            .map(Shape::of)
            .collect()
    };
    let open_request = ExecRequest::OpenOrders {
        instrument: INSTRUMENT,
    };
    assert_eq!(
        mine(answer(
            &wire.open_orders(&[partly], NOW),
            open_request,
            &venue
        )),
        mine(answer(twin!("ack_open_orders"), open_request, &venue)),
        "open orders: the simulator's read decodes otherwise than its twin (simulator left)"
    );
    assert_eq!(
        shapes(&answer(&wire.open_orders(&[], NOW), open_request, &venue)),
        shapes(&answer(
            twin!("ack_open_orders_empty"),
            open_request,
            &venue
        )),
        "an empty read decodes otherwise than its twin (simulator left)"
    );

    // Each refusal with a twin, and the class that the engine acts on.
    let refusals: [(Rejection, &str, ExecRequest, RejectClass); 9] = [
        (
            Rejection::WouldMatchImmediately,
            twin!("error_2010_would_match_immediately"),
            place(OrderStyle::PostOnly),
            RejectClass::Refused,
        ),
        (
            Rejection::InsufficientBalance,
            twin!("error_2010_insufficient_balance"),
            place(OrderStyle::PostOnly),
            RejectClass::Fatal,
        ),
        (
            Rejection::FilterFailure,
            twin!("error_1013_filter_failure"),
            place(OrderStyle::PostOnly),
            RejectClass::Fatal,
        ),
        (
            Rejection::TooManyOrders,
            twin!("error_1013_filter_failure"),
            place(OrderStyle::PostOnly),
            RejectClass::Fatal,
        ),
        (
            Rejection::CancelRejected,
            twin!("error_2011_cancel_rejected"),
            cancel(),
            RejectClass::Ambiguous,
        ),
        (
            Rejection::NoSuchOrder,
            twin!("error_2013_no_such_order"),
            status_query(),
            RejectClass::Gone,
        ),
        (
            Rejection::AmendBudgetSpent,
            twin!("error_2038_amend_budget_spent"),
            amend(),
            RejectClass::StillLive,
        ),
        (
            Rejection::AmendQuantityIncrease,
            twin!("error_2038_amend_quantity_increase"),
            amend(),
            RejectClass::StillLive,
        ),
        (
            Rejection::AmendFilterFailure,
            twin!("error_1013_amend_filter_failure"),
            amend(),
            RejectClass::Fatal,
        ),
    ];
    for (rejection, fixture, request, class) in refusals {
        let simulated = shapes(&answer(&wire.refusal(rejection), request, &venue));
        assert_eq!(
            simulated,
            shapes(&answer(fixture, request, &venue)),
            "{rejection:?}: the simulator's refusal decodes otherwise than its twin (simulator \
             left)"
        );
        assert!(
            simulated.iter().all(|shape| shape.reject == Some(class)),
            "{rejection:?}: class {class:?} expected, decoded {simulated:?}"
        );
    }

    // Each report of the user stream.
    let fee = 6;
    let reports: [(&str, String, &str); 5] = [
        ("new", wire.new_report(&resting, NOW), twin!("report_new")),
        (
            "partial fill",
            wire.trade_report(
                &partly,
                SimFill {
                    trade_id: TRADE,
                    settlement: &settlement(&partly, partly.filled, fee),
                    fee_asset: "BTC",
                },
                NOW,
            ),
            twin!("report_trade_partially_filled"),
        ),
        (
            "fill",
            wire.trade_report(
                &filled,
                SimFill {
                    trade_id: TRADE,
                    settlement: &settlement(&filled, Qty::from_mantissa(6_000), fee),
                    fee_asset: "BTC",
                },
                NOW,
            ),
            twin!("report_trade_filled"),
        ),
        (
            "canceled",
            wire.cancel_report(&resting, NOW),
            twin!("report_canceled"),
        ),
        (
            "rejected post-only cross",
            wire.rejected_report(&resting, NOW),
            twin!("report_rejected_would_match_immediately"),
        ),
    ];
    for (variant, simulated, fixture) in reports {
        assert_eq!(
            Shape::of(&report(&simulated, &venue)),
            Shape::of(&report(fixture, &venue)),
            "{variant}: the simulator's report decodes otherwise than its twin (simulator left)"
        );
    }

    // A fill with no fee names no fee asset, as the venue sends it.
    let free_fill = report(
        &wire.trade_report(
            &filled,
            SimFill {
                trade_id: TRADE,
                settlement: &settlement(&filled, QTY, 0),
                fee_asset: "BTC",
            },
            NOW,
        ),
        &venue,
    );
    assert_eq!(free_fill.commission, 0);
    assert_eq!(free_fill.commission_asset, AssetId::UNKNOWN);

    // A settlement that agrees with its order gives a partial fill report that names its trade.
    let agreeing = wire.trade_report(
        &partly,
        SimFill {
            trade_id: TRADE,
            settlement: &settlement(&partly, partly.filled, 0),
            fee_asset: "",
        },
        NOW,
    );
    assert!(agreeing.contains("PARTIALLY_FILLED"), "{agreeing}");
    assert!(
        agreeing.contains(&format!("\"t\":{}", TRADE.0)),
        "the report names its trade: {agreeing}"
    );
    // One that disagrees stops the report.
    let mut disagreeing = settlement(&partly, partly.filled, 0);
    disagreeing.cumulative_qty = Qty::from_mantissa(disagreeing.cumulative_qty.mantissa() + 1);
    let stopped = catch_unwind(AssertUnwindSafe(|| {
        wire.trade_report(
            &partly,
            SimFill {
                trade_id: TRADE,
                settlement: &disagreeing,
                fee_asset: "",
            },
            NOW,
        )
    }));
    let message = stopped.expect_err("a settlement that disagrees stops the report");
    let message = message
        .downcast_ref::<String>()
        .map(String::as_str)
        .unwrap_or_default();
    assert!(
        message.contains("the venue's book and its wallet disagree"),
        "{message}"
    );

    // The balances after a fill are absolute, as in the twin.
    let account = |text: &str| -> Vec<(AccountChunkKind, bool)> {
        match decode_stream_event(text, &venue.decode_context(NOW)).expect("the text decodes") {
            StreamEvent::Account(chunks) => chunks
                .iter()
                .map(|chunk| (chunk.kind, chunk.is_last_chunk))
                .collect(),
            other => panic!("expected balances, got {other:?}"),
        }
    };
    let balances = [
        SimBalance {
            asset: "BTC",
            free: Qty::from_mantissa(10_000),
            locked: Qty::from_mantissa(0),
        },
        SimBalance {
            asset: "USDT",
            free: Qty::from_mantissa(1_000 * FIXED_SCALE),
            locked: Qty::from_mantissa(0),
        },
    ];
    assert_eq!(
        account(&wire.account_position(&balances, NOW, 1_785_000_000_700)),
        account(twin!("account_position")),
        "the balances decode otherwise than their twin (simulator left)"
    );

    fake_venue_scenarios_decode_through_the_codec();
}

/// The fake venue builds its texts with the simulator's wire, so its own scenarios reach the same
/// decoders: a placement and its fills, cumulative on each one, and the ack and its report in each
/// order the network can deliver them.
fn fake_venue_scenarios_decode_through_the_codec() {
    let mut venue = FakeVenue::new();
    let placed = venue.submit(place(OrderStyle::PostOnly), NOW);
    let events = exec_events(&placed);
    assert_eq!(events.len(), 2, "the ack and the report");
    assert_eq!(events[0].kind, ExecKind::AckPlaced);
    assert_eq!(events[1].kind, ExecKind::ReportNew);
    assert!(venue.is_resting(ORDER_A));

    let first = exec_events(&venue.fill(ORDER_A, Qty::from_mantissa(4_000), NOW));
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].kind, ExecKind::ReportTrade);
    assert_eq!(first[0].status, Some(VenueOrderStatus::PartiallyFilled));
    assert_eq!(first[0].cumulative_qty, Qty::from_mantissa(4_000));

    let rest = exec_events(&venue.fill(ORDER_A, Qty::from_mantissa(6_000), NOW));
    assert_eq!(rest[0].status, Some(VenueOrderStatus::Filled));
    assert_eq!(
        rest[0].cumulative_qty,
        Qty::from_mantissa(10_000),
        "cumulative, so a duplicate folds to nothing"
    );
    assert!(!venue.is_resting(ORDER_A), "a filled order stops resting");

    for (delivery, expected) in [
        (Delivery::AckFirst, 2),
        (Delivery::ReportFirst, 2),
        (Delivery::AckOnly, 1),
        (Delivery::ReportOnly, 1),
        (Delivery::Ambiguous, 0),
        (Delivery::Duplicated, 4),
    ] {
        let mut venue = FakeVenue::new();
        venue.set_delivery(delivery);
        let events = exec_events(&venue.submit(place(OrderStyle::PostOnly), NOW));
        assert_eq!(events.len(), expected, "{delivery:?} message count");
        for event in &events {
            assert_eq!(event.client_id, ORDER_A, "{delivery:?} addresses one order");
            assert_eq!(event.provenance, Provenance::Mine);
        }
        assert!(
            venue.is_resting(ORDER_A),
            "{delivery:?}: the venue's own state never depends on what we were told"
        );
    }
}
