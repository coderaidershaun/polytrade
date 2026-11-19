//! The first golden boot: a short session that buys one unit and sells one, with whole-unit prices
//! so that each product is exact and each metric is a number a person checks by hand.

use engine::hot::exec::QuoteLevel;
use engine::msg::exec::{Liquidity, OrderStyle, Provenance, RejectClass};
use engine::msg::ids::{
    AssetId, ClientOrderId, InstrumentId, Notional, Price, Qty, Side, TradeId, VenueOrderId,
};
use engine::msg::kinds::FeatureKind;
use engine::msg::persist::{
    FeatureId, FeatureRow, FillRow, LedgerEntry, LedgerKind, MidRow, NoQuoteReason, NoQuoteRow,
    OrderLifecycle, OrderRow, OrderTransition,
};
use engine::msg::time::{LocalTs, VenueTs};
use engine::runtime::manifest::RunId;
use serde_json::json;

use crate::cells::Run;

pub const RUN_ID: i64 = 1_784_000_000_000_000;
/// One whole unit at the fixed scale.
const UNIT: i64 = 100_000_000;
const INSTRUMENT: InstrumentId = InstrumentId(0);
const QUOTE_ASSET: AssetId = AssetId(1);
const OTHER_ASSET: AssetId = AssetId(2);

/// A stamp `ms` milliseconds after the boot.
fn at(ms: i64) -> LocalTs {
    LocalTs::from_micros(RUN_ID + ms * 1_000)
}

/// The venue stamps its event 10 ms before the engine receives it.
fn venue_at(ms: i64) -> VenueTs {
    VenueTs::from_micros(RUN_ID + (ms - 10) * 1_000)
}

fn units(count: i64) -> i64 {
    count * UNIT
}

/// A placement of one unit by this engine at `price` whole units, as its first row.
fn placed(client: u64, side: Side, price: i64, ms: i64) -> OrderRow {
    OrderRow {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client),
        quote_level: Some(QuoteLevel::ZERO),
        venue_order_id: None,
        transition: OrderTransition::Placed,
        state: OrderLifecycle::PendingNew,
        previous_state: OrderLifecycle::Free,
        provenance: Provenance::Mine,
        side,
        style: Some(OrderStyle::PostOnly),
        price: Price::from_mantissa(units(price)),
        qty: Qty::from_mantissa(units(1)),
        filled_qty: Qty::from_mantissa(0),
        filled_quote: 0,
        reject: None,
        reject_code: 0,
        exchange_ts_us: VenueTs::from_micros(0),
        received_ts_us: at(ms),
        depth_ahead: Some(Qty::from_mantissa(units(1))),
    }
}

/// The next row of the chain of `before`, answered by the venue at `ms`.
fn next(
    before: &OrderRow,
    transition: OrderTransition,
    state: OrderLifecycle,
    venue_order_id: i64,
    ms: i64,
) -> OrderRow {
    OrderRow {
        venue_order_id: Some(VenueOrderId(venue_order_id)),
        transition,
        state,
        previous_state: before.state,
        exchange_ts_us: venue_at(ms),
        received_ts_us: at(ms),
        depth_ahead: None,
        ..*before
    }
}

fn filled(before: &OrderRow, venue_order_id: i64, ms: i64) -> OrderRow {
    OrderRow {
        filled_qty: Qty::from_mantissa(units(1)),
        filled_quote: before.price.mantissa(),
        ..next(
            before,
            OrderTransition::ReportTrade,
            OrderLifecycle::ClosedFilled,
            venue_order_id,
            ms,
        )
    }
}

fn orders(run: &mut Run) {
    // A buys at 101 and fills; B sells at 105 after an amend and fills; C is canceled; D is
    // refused; E expires.
    let a0 = placed(1, Side::Buy, 101, 21_000);
    let a1 = next(
        &a0,
        OrderTransition::AckPlaced,
        OrderLifecycle::Live,
        9_001,
        21_015,
    );
    let a2 = filled(&a1, 9_001, 25_000);
    let b0 = placed(2, Side::Sell, 105, 30_500);
    let b1 = next(
        &b0,
        OrderTransition::ReportNew,
        OrderLifecycle::Live,
        9_002,
        30_600,
    );
    let b2 = next(
        &b1,
        OrderTransition::AmendSent,
        OrderLifecycle::AmendInFlight,
        9_002,
        31_000,
    );
    let b3 = next(
        &b2,
        OrderTransition::AckAmended,
        OrderLifecycle::Live,
        9_002,
        31_010,
    );
    let b4 = next(
        &b3,
        OrderTransition::ReportAmended,
        OrderLifecycle::Live,
        9_002,
        31_020,
    );
    let b5 = filled(&b4, 9_002, 35_000);
    let c0 = placed(3, Side::Buy, 99, 40_000);
    let c1 = next(
        &c0,
        OrderTransition::AckPlaced,
        OrderLifecycle::Live,
        9_003,
        40_030,
    );
    let c2 = next(
        &c1,
        OrderTransition::CancelSent,
        OrderLifecycle::CancelInFlight,
        9_003,
        40_100,
    );
    let c3 = next(
        &c2,
        OrderTransition::AckCanceled,
        OrderLifecycle::ClosedCanceled,
        9_003,
        40_200,
    );
    // Placed past the depth that the book held, so no quantity ahead of it is known.
    let d0 = OrderRow {
        depth_ahead: None,
        ..placed(4, Side::Sell, 110, 41_000)
    };
    let d1 = OrderRow {
        venue_order_id: None,
        reject: Some(RejectClass::Refused),
        reject_code: -2_010,
        ..next(
            &d0,
            OrderTransition::ReportRejected,
            OrderLifecycle::ClosedRejected,
            0,
            41_100,
        )
    };
    let e0 = placed(5, Side::Buy, 98, 42_000);
    let e1 = next(
        &e0,
        OrderTransition::AckPlaced,
        OrderLifecycle::Live,
        9_005,
        42_050,
    );
    let e2 = next(
        &e1,
        OrderTransition::ReportExpired,
        OrderLifecycle::ClosedExpired,
        9_005,
        43_000,
    );
    for row in [
        a0, a1, a2, b0, b1, b2, b3, b4, b5, c0, c1, c2, c3, d0, d1, e0, e1, e2,
    ] {
        run.order(row);
    }
}

fn fills(run: &mut Run) {
    let buy = FillRow {
        instrument: INSTRUMENT,
        trade_id: Some(TradeId(501)),
        venue_order_id: Some(VenueOrderId(9_001)),
        client_id: ClientOrderId(1),
        quote_level: Some(QuoteLevel::ZERO),
        provenance: Provenance::Mine,
        side: Side::Buy,
        liquidity: Some(Liquidity::Maker),
        last_price: Price::from_mantissa(units(101)),
        last_qty: Qty::from_mantissa(units(1)),
        booked_qty: Qty::from_mantissa(units(1)),
        booked_quote: units(101),
        commission: units(1),
        commission_asset: QUOTE_ASSET,
        exchange_ts_us: venue_at(25_000),
        received_ts_us: at(25_000),
    };
    let sell = FillRow {
        trade_id: Some(TradeId(502)),
        venue_order_id: Some(VenueOrderId(9_002)),
        client_id: ClientOrderId(2),
        side: Side::Sell,
        liquidity: Some(Liquidity::Taker),
        last_price: Price::from_mantissa(units(105)),
        booked_quote: units(105),
        commission_asset: OTHER_ASSET,
        exchange_ts_us: venue_at(35_000),
        received_ts_us: at(35_000),
        ..buy
    };
    run.fill(buy);
    run.fill(sell);
}

/// Position, cash, basis, mark and P&L in whole units; realised is cash plus basis, exposure is
/// mark times position, and P&L is cash plus exposure.
fn ledger_row(
    kind: LedgerKind,
    ms: i64,
    position: i64,
    cash: i64,
    basis: i64,
    mark: i64,
) -> LedgerEntry {
    LedgerEntry {
        kind,
        instrument: INSTRUMENT,
        position_base: Qty::from_mantissa(units(position)),
        cash_quote: Notional::from_mantissa(units(cash)),
        basis_quote: Notional::from_mantissa(units(basis)),
        realised_quote: Notional::from_mantissa(units(cash + basis)),
        mark: Some(Price::from_mantissa(units(mark))),
        exposure_quote: Notional::from_mantissa(units(mark * position)),
        pnl_quote: Notional::from_mantissa(units(cash + mark * position)),
        client_id: None,
        trade_id: None,
        received_ts_us: at(ms),
    }
}

fn ledger(run: &mut Run) {
    let fill = |row: LedgerEntry, client: u64, trade: i64| LedgerEntry {
        client_id: Some(ClientOrderId(client)),
        trade_id: Some(TradeId(trade)),
        ..row
    };
    // Boot: 1 unit restored at a basis of 100, marked at 100: P&L 0.
    run.ledger(ledger_row(LedgerKind::Boot, 5_000, 1, -100, 100, 100));
    // Buy 1 at 101 with a fee of 1: cash -202, FIFO basis 201, mark 102: P&L 2.
    run.ledger(fill(
        ledger_row(LedgerKind::Fill, 25_000, 2, -202, 201, 102),
        1,
        501,
    ));
    // Sell 1 at 105, its fee in another asset: cash -97, the lot of 101 left, mark 104: P&L 7.
    run.ledger(fill(
        ledger_row(LedgerKind::Fill, 35_000, 1, -97, 101, 104),
        2,
        502,
    ));
    run.ledger(ledger_row(LedgerKind::Interval, 41_000, 1, -97, 101, 103));
    run.ledger(ledger_row(LedgerKind::Rotation, 45_000, 1, -97, 101, 103));
}

/// One row each 5 s from 5 s to 60 s; a mid only at 10, 20, 30 and 40 s: 100, 102, 104, 103.
fn mids(run: &mut Run) {
    let price = |units_of: i64| Some(Price::from_mantissa(units(units_of)));
    let touch: [(Option<Price>, Option<Price>); 12] = [
        (None, None),
        (price(99), price(101)),
        (None, price(101)),
        (price(101), price(103)),
        (price(101), None),
        (price(103), price(105)),
        (price(103), None),
        (price(102), price(104)),
        (None, None),
        (None, None),
        (None, None),
        (None, None),
    ];
    for (index, (bid, ask)) in touch.into_iter().enumerate() {
        let mid = match (bid, ask) {
            (Some(bid), Some(ask)) => {
                Some(Price::from_mantissa((bid.mantissa() + ask.mantissa()) / 2))
            }
            _ => None,
        };
        run.mid(MidRow {
            instrument: INSTRUMENT,
            spin_seq: index as u64 + 1,
            bid,
            ask,
            mid,
            event_ts_us: at((index as i64 + 1) * 5_000),
        });
    }
}

/// Each reason once, two to a spin, at spins 1 to 10.
fn no_quotes(run: &mut Run) {
    for (index, reason) in NoQuoteReason::ALL.into_iter().enumerate() {
        let spin = index as u64 / 2 + 1;
        run.no_quote(NoQuoteRow {
            instrument: INSTRUMENT,
            spin_seq: spin,
            side: if index % 2 == 0 { Side::Buy } else { Side::Sell },
            reason,
            event_ts_us: at(spin as i64 * 5_000),
        });
    }
}

pub fn run() -> Run {
    let mut run = Run::new(
        RunId::from_micros(RUN_ID),
        &["alpha", "beta"],
        &[FeatureKind::MarketOnly, FeatureKind::ExecutionAware],
    );
    orders(&mut run);
    fills(&mut run);
    ledger(&mut run);
    for (feature, value) in [(0, 0.5), (1, -1.25)] {
        run.feature(FeatureRow {
            instrument: INSTRUMENT,
            feature: FeatureId(feature),
            value,
            event_ts_us: at(10_000),
        });
    }
    mids(&mut run);
    no_quotes(&mut run);
    crate::rollups::one_stage(&mut run, at(0), at(60_000));
    run.metrics = json!({
        "orders.placed": 5,
        "orders.canceled": 1,
        "orders.rejected": 1,
        "orders.filled": 2,
        "orders.ack_round_trip_us": { "p50": 30_000, "p99": 50_000, "max": 50_000 },
        "fills.count": 2,
        "fills.base_bought": units(1),
        "fills.base_sold": units(1),
        "fills.fill_rate": 0.4,
        "fees.quote": units(1),
        "fees.other_asset_fills": 1,
        "pnl.session_quote": units(6),
        "pnl.spread_quote": units(2),
        "pnl.unpriced_fills": 0,
        "pnl.inventory_quote": units(5),
        "pnl.fees_quote": units(-1),
        "pnl.remainder_quote": 0,
        "risk.max_abs_position_base": units(2),
        "risk.max_drawdown_quote": units(1),
        "markout.1s.mean_bps": 243.75294672324375,
        "markout.5s.mean_bps": 243.75294672324375,
        "markout.30s.mean_bps": null,
        "markout.60s.mean_bps": null,
        "markout.dropped": 4,
        "quoting.quoting_side_spins": 4,
        "quoting.not_quoting_spins": crate::cells::not_quoting_spins(1),
        "latency": {
            "trade/venue_to_engine": {
                "p50_ns": 1_000, "p99_ns": 5_000, "p999_ns": 9_000, "max_ns": 12_000,
                "budget_p99_ns": 50_000
            }
        },
    });
    run
}
