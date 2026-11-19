//! FITNESS: the audit trail's banking rule — a row whenever the state OR the filled totals moved,
//! and nothing at all otherwise.
//!
//! Two failures hide in that one sentence. Keying only on STATE loses the order-level counterpart of
//! every partial fill, because a partial fill leaves an order `Live`; the `fills` table then holds
//! line items whose order the `orders` table cannot explain, which is exactly the shape a
//! reconciliation against a venue statement is trying to resolve. Keying on NEITHER — banking every
//! event — fills the table with rows saying nothing changed, and a redelivered report then reads as a
//! second fill in a table whose whole job is to be the durable record of what happened once.
//!
//! A separate test pins the round trip these rows take to Parquet. This one pins which rows exist.

use engine::hot::dispatch::{ExecWiring, ExposureWiring, HotEngine, HotEngineSetup};
use engine::hot::sink::ExecSink;
use engine::msg::exec::{ExecCommand, ExecEvent, ExecKind, ExecLaneItem, VenueOrderStatus};
use engine::msg::ids::{ClientOrderId, Price, Qty, Side, TradeId, VenueOrderId};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::persist::{AuditRecord, OrderLifecycle, OrderRow, OrderTransition};
use engine::msg::time::Stamper;
use engine::runtime::config::{RecordedTables, TableKind, TrackerSpec};
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    FillPen, Idle, ONE, detached_setup, drain_orders_and_fills, engine_without_warmup, exec_event,
    exposure_ring, instrument_row, persist_ring_for, strategy_log_ring, ts,
};
use crate::exec_support::{
    ASK, BID, CEILING, INSTRUMENT, QUOTE_QTY, TwoSidedQuoter, drain_commands, exec_settings,
    make_ready, reseat_book, row_with_ceiling, spin_at,
};

#[test]
fn a_fill_banks_an_order_row_beside_it_and_a_redelivery_banks_neither() {
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let (persistence, rings) = persist_ring_for(
        1024,
        RecordedTables::new(&[TableKind::Orders, TableKind::Fills]),
    );
    let mut records = rings.audit;
    let (log_sink, _logs) = strategy_log_ring(64);
    let mut engine = engine_without_warmup(&instruments, Box::new(Idle), persistence, log_sink);

    let mut pen = FillPen::new(0);
    let batch = pen.fill(Side::Buy, 100 * ONE, ONE, 10);
    for message in &batch {
        engine.dispatch(message);
    }

    let banked = drain_orders_and_fills(&mut records);
    assert_eq!(
        banked.fills.len(),
        1,
        "one execution must produce exactly one line item, got {:?}",
        banked.fills
    );
    let fill = banked.fills[0];
    assert_eq!(
        fill.booked_qty, fill.last_qty,
        "a clean single delivery books exactly what executed"
    );
    assert_eq!(fill.side, Side::Buy);

    let transitions: Vec<OrderTransition> =
        banked.orders.iter().map(|row| row.transition).collect();
    assert_eq!(
        transitions,
        vec![OrderTransition::SnapshotOrder, OrderTransition::ReportTrade],
        "the adoption moved state and the trade moved the filled totals, so each earned a row"
    );
    let trade_row = banked.orders[1];
    assert_eq!(
        trade_row.state,
        OrderLifecycle::Live,
        "a partial fill leaves the order working — which is why state alone cannot key the banking"
    );
    assert_eq!(
        trade_row.previous_state,
        OrderLifecycle::Live,
        "the state did NOT move, so this row exists only because the filled totals did"
    );
    assert_eq!(
        trade_row.filled_qty, fill.booked_qty,
        "the order-level row carries the running total the fill row's delta contributed to"
    );
    assert!(
        trade_row.style.is_none(),
        "the engine adopted this order rather than sending it, so it cannot claim to know how it \
         was sent — post-only is not a safe guess to record"
    );
    for fill in &banked.fills {
        assert!(
            banked
                .orders
                .iter()
                .any(|row| row.client_id == fill.client_id),
            "a fill with no order-level counterpart: {fill:?}"
        );
    }

    for message in &batch {
        engine.dispatch(message);
    }
    let redelivered = drain_orders_and_fills(&mut records);
    assert!(
        redelivered.orders.is_empty() && redelivered.fills.is_empty(),
        "a redelivered report moved neither state nor totals and must record nothing, got {} \
         order rows and {} fill rows",
        redelivered.orders.len(),
        redelivered.fills.len()
    );
}

/// A quoting engine at the touch, its bid placed and acknowledged, and the read ends of its command
/// lane and its audit ring.
fn resting_bid() -> (
    HotEngine,
    Consumer<ExecLaneItem>,
    Consumer<AuditRecord>,
    ClientOrderId,
) {
    let instruments = [row_with_ceiling(CEILING * 1_000)];
    let tables = RecordedTables::new(&[TableKind::Orders, TableKind::Fills]);
    let (persistence, rings) = persist_ring_for(1_024, tables);
    let (commands_producer, mut commands) = RingBuffer::<ExecLaneItem>::new(1_024);
    let mut engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        exec: vec![ExecWiring {
            sink: ExecSink::new(commands_producer, Stamper::Off),
            settings: exec_settings(),
            run_nonce: 0,
        }],
        exposure: ExposureWiring {
            restored: &[],
            sink: exposure_ring(64, instruments.len()).0,
        },
        ..detached_setup(&instruments, Box::new(TwoSidedQuoter))
    });
    make_ready(&mut engine, 0);
    for message in reseat_book(BID, ASK, 1) {
        engine.dispatch(&message);
    }
    spin_at(&mut engine, 1, 10);
    let bid = drain_commands(&mut commands)
        .into_iter()
        .find_map(|command| match command {
            ExecCommand::Place {
                client_id,
                side: Side::Buy,
                ..
            } => Some(client_id),
            _ => None,
        })
        .expect("the first spin places a bid");
    let ack = ExecEvent {
        kind: ExecKind::AckPlaced,
        status: Some(VenueOrderStatus::New),
        venue_order_id: Some(VenueOrderId(7)),
        ..exec_event(INSTRUMENT, bid, Side::Buy, BID, 11)
    };
    engine.dispatch(&InboundMessage::new(ts(11), MessageBody::Exec(ack)));
    (engine, commands, rings.audit, bid)
}

/// Half the bid, reported by the venue with the stamp `when`.
fn half_fill(bid: ClientOrderId, when: i64) -> InboundMessage {
    let qty = Qty::from_mantissa(QUOTE_QTY.mantissa() / 2);
    let event = ExecEvent {
        kind: ExecKind::ReportTrade,
        status: Some(VenueOrderStatus::PartiallyFilled),
        venue_order_id: Some(VenueOrderId(7)),
        trade_id: Some(TradeId(41)),
        last_price: Price::from_mantissa(BID),
        last_qty: qty,
        cumulative_qty: qty,
        cumulative_quote: Price::from_mantissa(BID).notional(qty),
        ..exec_event(INSTRUMENT, bid, Side::Buy, BID, when)
    };
    InboundMessage::new(ts(when), MessageBody::Exec(event))
}

/// The book moves one unit down, and the next spin sends the cancel of the bid that it left.
fn move_book_and_spin(engine: &mut HotEngine, when: i64) {
    for message in reseat_book(BID - ONE, ASK - ONE, when) {
        engine.dispatch(&message);
    }
    spin_at(engine, 2, when + 10);
}

fn rows_of(records: &mut Consumer<AuditRecord>, client: ClientOrderId) -> Vec<OrderRow> {
    drain_orders_and_fills(records)
        .orders
        .into_iter()
        .filter(|row| row.client_id == client)
        .collect()
}

/// FITNESS: each order row carries the filled totals that the fold holds for its order when the
/// row is written. A fill folded before the cancel is sent gives a `cancel_sent` row with that
/// fill's totals. A fill that the venue stamped before the cancel but that the fold took after it
/// gives a `cancel_sent` row of zero totals and a later `report_trade` row with the earlier stamp:
/// the rows keep the order of the fold, which the stamps of two queues do not give. A cancel row
/// that dropped the totals would read as an order that never filled.
#[test]
fn an_order_row_carries_the_totals_that_the_fold_holds_for_its_order() {
    let half = QUOTE_QTY.mantissa() / 2;

    let (mut engine, _commands, mut records, bid) = resting_bid();
    engine.dispatch(&half_fill(bid, 20));
    move_book_and_spin(&mut engine, 30);
    let rows = rows_of(&mut records, bid);
    let cancel = rows
        .iter()
        .find(|row| row.transition == OrderTransition::CancelSent)
        .expect("the moved book cancels the bid");
    assert_eq!(
        (cancel.filled_qty.mantissa(), cancel.filled_quote),
        (
            half,
            Price::from_mantissa(BID)
                .notional(Qty::from_mantissa(half))
                .mantissa()
        ),
        "a cancel sent after a fill carries the totals of that fill: {rows:?}"
    );

    let (mut engine, _commands, mut records, bid) = resting_bid();
    move_book_and_spin(&mut engine, 30);
    engine.dispatch(&half_fill(bid, 35));
    let rows = rows_of(&mut records, bid);
    let transitions: Vec<(OrderTransition, i64, i64)> = rows
        .iter()
        .map(|row| {
            (
                row.transition,
                row.received_ts_us.micros(),
                row.filled_qty.mantissa(),
            )
        })
        .filter(|(transition, ..)| {
            matches!(
                transition,
                OrderTransition::CancelSent | OrderTransition::ReportTrade
            )
        })
        .collect();
    assert_eq!(
        transitions,
        [
            (OrderTransition::CancelSent, 40, 0),
            (OrderTransition::ReportTrade, 35, half),
        ],
        "the fold sent the cancel first, so the cancel row holds no fill, though the fill's stamp \
         is earlier"
    );
}
