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

use polysim::config::{RecordedTables, TableKind, TrackerSpec};
use polysim::ids::Side;
use polysim::msg::persist::{OrderLifecycle, OrderTransition};

use crate::engine_support::{
    FillPen, Idle, ONE, drain_orders_and_fills, engine_without_warmup, instrument_row,
    metrics_ring, persist_ring_for, pop, strategy_log_ring,
};

#[test]
fn a_fill_banks_an_order_row_beside_it_and_a_redelivery_banks_neither() {
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let (persistence, mut records) = persist_ring_for(
        1024,
        RecordedTables::new(&[TableKind::Orders, TableKind::Fills]),
    );
    let (log_sink, _logs) = strategy_log_ring(64);
    let (metrics, _metrics) = metrics_ring(64);
    let mut engine =
        engine_without_warmup(&instruments, Box::new(Idle), persistence, log_sink, metrics);

    let mut pen = FillPen::new(0);
    let batch = pen.fill(Side::Buy, 100 * ONE, ONE, 10);
    for message in &batch {
        engine.dispatch(pop(0, 0), message);
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
        engine.dispatch(pop(0, 0), message);
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
