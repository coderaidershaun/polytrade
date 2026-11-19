//! The second golden boot, ten minutes after the first: another feature dictionary, so feature id 1
//! names another column, and with the rows of the metrics thread, the rows that give each label of
//! each set and each absent value at least once.

use engine::hot::exec::QuoteLevel;
use engine::msg::exec::{OrderStyle, Provenance, RejectClass};
use engine::msg::ids::{
    ClientOrderId, InstrumentId, Notional, Price, Qty, Side, TradeId, VenueOrderId,
};
use engine::msg::kinds::FeatureKind;
use engine::msg::kinds::KlineInterval;
use engine::msg::persist::{
    BookEventKind, BookEventRow, FeatureId, FeatureRow, FillRow, IncidentKind, IncidentRow,
    KlineRow, LedgerEntry, LedgerKind, LinkFrameRow, LinkRowKind, OrderLifecycle, OrderRow,
    OrderTransition, RotationRow, TradeRow,
};
use engine::msg::time::{LocalTs, VenueTs};
use engine::runtime::manifest::RunId;
use serde_json::json;

use crate::cells::Run;
use crate::cells::not_quoting_spins;

pub const RUN_ID: i64 = 1_784_000_600_000_000;
const UNIT: i64 = 100_000_000;
const INSTRUMENT: InstrumentId = InstrumentId(0);

fn at(ms: i64) -> LocalTs {
    LocalTs::from_micros(RUN_ID + ms * 1_000)
}

fn venue_at(ms: i64) -> VenueTs {
    VenueTs::from_micros(RUN_ID + (ms - 10) * 1_000)
}

fn order(client: u64, transition: OrderTransition, state: OrderLifecycle, ms: i64) -> OrderRow {
    OrderRow {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client),
        quote_level: Some(QuoteLevel::ZERO),
        venue_order_id: None,
        transition,
        state,
        previous_state: OrderLifecycle::Free,
        provenance: Provenance::Mine,
        side: Side::Buy,
        style: Some(OrderStyle::PostOnly),
        price: Price::from_mantissa(100 * UNIT),
        qty: Qty::from_mantissa(UNIT),
        filled_qty: Qty::from_mantissa(0),
        filled_quote: 0,
        reject: None,
        reject_code: 0,
        exchange_ts_us: venue_at(ms),
        received_ts_us: at(ms),
        depth_ahead: (transition == OrderTransition::Placed)
            .then_some(Qty::from_mantissa(UNIT / 2)),
    }
}

/// The next row of a chain: the state before it is the state of `before`.
///
/// A reject belongs to the one row that carried it, as in the engine, which writes no reject on
/// a transition it makes on its own; only `rejected` puts one on a row.
fn then(
    before: &OrderRow,
    transition: OrderTransition,
    state: OrderLifecycle,
    ms: i64,
) -> OrderRow {
    OrderRow {
        transition,
        state,
        previous_state: before.state,
        reject: None,
        reject_code: 0,
        exchange_ts_us: venue_at(ms),
        received_ts_us: at(ms),
        depth_ahead: None,
        ..*before
    }
}

fn rejected(before: &OrderRow, class: RejectClass, code: i32, row: OrderRow) -> OrderRow {
    OrderRow {
        reject: Some(class),
        reject_code: code,
        previous_state: before.state,
        ..row
    }
}

fn orders(run: &mut Run) {
    use OrderLifecycle as State;
    use OrderTransition as Move;
    let not_ours = |client, provenance, ms| OrderRow {
        venue_order_id: Some(VenueOrderId(9_000 + client as i64)),
        provenance,
        quote_level: None,
        style: None,
        side: Side::Sell,
        ..order(client, Move::SnapshotOrder, State::Live, ms)
    };
    let f0 = not_ours(21, Provenance::PriorRun, 1_000);
    let f1 = then(&f0, Move::SweepClosed, State::ClosedReconciledGone, 1_100);
    let g0 = OrderRow {
        style: Some(OrderStyle::Immediate),
        side: Side::Sell,
        ..order(22, Move::Placed, State::PendingNew, 2_000)
    };
    let g1 = then(&g0, Move::SendAbandoned, State::Free, 2_010);
    let h0 = order(23, Move::Placed, State::PendingNew, 4_000);
    let h1 = rejected(
        &h0,
        RejectClass::Ambiguous,
        -2_011,
        then(&h0, Move::AckFailed, State::Unknown, 4_010),
    );
    let h2 = then(&h1, Move::Timeout, State::Unknown, 4_500);
    let h3 = then(&h2, Move::StreamReset, State::Unknown, 4_600);
    let h4 = then(&h3, Move::ReportCanceled, State::ClosedCanceled, 4_700);
    let i0 = not_ours(24, Provenance::Foreign, 5_000);
    let i1 = then(&i0, Move::ReportCanceled, State::ClosedCanceled, 5_100);
    let j0 = order(25, Move::Placed, State::PendingNew, 6_000);
    let j1 = OrderRow {
        venue_order_id: Some(VenueOrderId(9_025)),
        ..then(&j0, Move::AckPlaced, State::Live, 6_020)
    };
    let j2 = then(&j1, Move::CancelSent, State::CancelInFlight, 6_100);
    let j3 = rejected(
        &j2,
        RejectClass::StillLive,
        -2_011,
        then(&j2, Move::AckFailed, State::Live, 6_110),
    );
    let k0 = order(26, Move::Placed, State::PendingNew, 7_000);
    let k1 = rejected(
        &k0,
        RejectClass::Gone,
        -2_013,
        then(&k0, Move::AckFailed, State::ClosedReconciledGone, 7_010),
    );
    let l0 = order(27, Move::Placed, State::PendingNew, 8_000);
    let l1 = rejected(
        &l0,
        RejectClass::Fatal,
        -1_013,
        then(&l0, Move::ReportRejected, State::ClosedRejected, 8_010),
    );
    for row in [
        f0, f1, g0, g1, h0, h1, h2, h3, h4, i0, i1, j0, j1, j2, j3, k0, k1, l0, l1,
    ] {
        run.order(row);
    }
}

/// A fill learned with no trade id, no venue id and no liquidity, of an order of a prior run.
fn fills(run: &mut Run) {
    run.fill(FillRow {
        instrument: INSTRUMENT,
        trade_id: None,
        venue_order_id: None,
        client_id: ClientOrderId(21),
        quote_level: None,
        provenance: Provenance::PriorRun,
        side: Side::Buy,
        liquidity: None,
        last_price: Price::from_mantissa(100 * UNIT),
        last_qty: Qty::from_mantissa(UNIT),
        booked_qty: Qty::from_mantissa(UNIT),
        booked_quote: 100 * UNIT,
        commission: 0,
        commission_asset: engine::msg::ids::AssetId(1),
        exchange_ts_us: venue_at(3_000),
        received_ts_us: at(3_000),
    });
}

fn incidents(run: &mut Run) {
    let base = IncidentRow {
        kind: IncidentKind::FeeGap,
        instrument: INSTRUMENT,
        client_id: Some(ClientOrderId(25)),
        trade_id: Some(TradeId(601)),
        qty: Qty::from_mantissa(UNIT),
        amount: 0,
        window_close_ts_us: None,
        exchange_ts_us: venue_at(9_000),
        received_ts_us: at(9_000),
    };
    let stranded = IncidentRow {
        kind: IncidentKind::StrandedPosition,
        client_id: None,
        trade_id: None,
        amount: 100 * UNIT,
        window_close_ts_us: Some(venue_at(300_000)),
        ..base
    };
    let orphan = IncidentRow {
        kind: IncidentKind::OrphanFill,
        trade_id: Some(TradeId(602)),
        ..base
    };
    let unbooked = IncidentRow {
        kind: IncidentKind::UnbookedFee,
        trade_id: Some(TradeId(603)),
        amount: UNIT / 100,
        ..base
    };
    for row in [base, stranded, orphan, unbooked] {
        run.incident(row);
    }
}

/// One restored instrument with no position and no mark yet.
fn ledger(run: &mut Run) {
    run.ledger(LedgerEntry {
        kind: LedgerKind::Boot,
        instrument: INSTRUMENT,
        position_base: Qty::from_mantissa(0),
        cash_quote: Notional::from_mantissa(0),
        basis_quote: Notional::from_mantissa(0),
        realised_quote: Notional::from_mantissa(0),
        mark: None,
        exposure_quote: Notional::from_mantissa(0),
        pnl_quote: Notional::from_mantissa(0),
        client_id: None,
        trade_id: None,
        received_ts_us: at(500),
    });
}

fn market(run: &mut Run) {
    run.rotation(RotationRow {
        instrument: INSTRUMENT,
        window_open_ts_us: VenueTs::from_micros(RUN_ID),
        window_close_ts_us: VenueTs::from_micros(RUN_ID + 300_000_000),
        token_id_up: "111".into(),
        token_id_down: "222".into(),
        condition_id: "0xabc".into(),
        received_ts_us: at(600),
    });
    for (feature, value) in [(0, 2.0), (1, 0.1)] {
        run.feature(FeatureRow {
            instrument: INSTRUMENT,
            feature: FeatureId(feature),
            value,
            event_ts_us: at(10_000),
        });
    }
    for (side, ms) in [(Side::Buy, 10_100), (Side::Sell, 10_200)] {
        run.trade(TradeRow {
            instrument: INSTRUMENT,
            price: Price::from_mantissa(100 * UNIT),
            qty: Qty::from_mantissa(UNIT / 2),
            side,
            exchange_ts_us: venue_at(ms),
            received_ts_us: at(ms),
        });
    }
    let book = [
        (BookEventKind::Delta, Some(Side::Buy), 99, 2, 41),
        (BookEventKind::Snapshot, Some(Side::Sell), 101, 3, 42),
        (BookEventKind::Reset, None, 0, 0, 0),
    ];
    for (kind, side, price, qty, update_id) in book {
        run.book_event(BookEventRow {
            instrument: INSTRUMENT,
            kind,
            side,
            price: Price::from_mantissa(price * UNIT),
            qty: Qty::from_mantissa(qty * UNIT),
            update_id,
            received_ts_us: at(11_000),
        });
    }
    klines(run);
    let frames = [
        (LinkRowKind::Payload, 16, 0.25),
        (LinkRowKind::RunRunning, 1, 0.0),
        (LinkRowKind::RunIdle, 1, 0.0),
    ];
    for (seq, (kind, topic, value)) in (1..).zip(frames) {
        run.link_frame(LinkFrameRow {
            kind,
            sender_te_hash: 0x00c0_ffee,
            topic,
            seq,
            slot: 0,
            count: 1,
            value,
            event_ts_us: venue_at(12_000),
            received_ts_us: at(12_000),
        });
    }
}

fn klines(run: &mut Run) {
    let intervals = [
        KlineInterval::OneMinute,
        KlineInterval::ThreeMinutes,
        KlineInterval::FiveMinutes,
        KlineInterval::FifteenMinutes,
        KlineInterval::ThirtyMinutes,
        KlineInterval::OneHour,
        KlineInterval::TwoHours,
        KlineInterval::FourHours,
        KlineInterval::SixHours,
        KlineInterval::EightHours,
        KlineInterval::TwelveHours,
        KlineInterval::OneDay,
        KlineInterval::ThreeDays,
        KlineInterval::OneWeek,
        KlineInterval::OneMonth,
    ];
    for (index, interval) in intervals.into_iter().enumerate() {
        run.kline(KlineRow {
            instrument: INSTRUMENT,
            interval,
            open_ts_us: VenueTs::from_micros(RUN_ID),
            open: Price::from_mantissa(100 * UNIT),
            high: Price::from_mantissa(102 * UNIT),
            low: Price::from_mantissa(99 * UNIT),
            close: Price::from_mantissa(101 * UNIT),
            base_volume: Qty::from_mantissa(3 * UNIT),
            quote_volume: 303 * UNIT,
            trade_count: 7,
            is_closed: index % 2 == 0,
            exchange_ts_us: venue_at(13_000),
            received_ts_us: at(13_000),
        });
    }
}

pub fn run() -> Run {
    let mut run = Run::new(
        RunId::from_micros(RUN_ID),
        &["alpha", "gamma", "beta"],
        &[
            FeatureKind::MarketOnly,
            FeatureKind::MarketOnly,
            FeatureKind::ExecutionAware,
        ],
    );
    orders(&mut run);
    fills(&mut run);
    incidents(&mut run);
    ledger(&mut run);
    market(&mut run);
    let latency = crate::rollups::coverage(&mut run, at(0), at(60_000));
    run.metrics = json!({
        "orders.placed": 5,
        "orders.canceled": 2,
        "orders.rejected": 4,
        "orders.filled": 0,
        "orders.ack_round_trip_us": { "p50": 20_000, "p99": 20_000, "max": 20_000 },
        "fills.count": 1,
        "fills.base_bought": UNIT,
        "fills.base_sold": 0,
        "fills.fill_rate": 0.2,
        "fees.quote": 0,
        "fees.other_asset_fills": 0,
        "pnl.session_quote": 0,
        "pnl.spread_quote": 0,
        "pnl.unpriced_fills": 1,
        "pnl.inventory_quote": 0,
        "pnl.fees_quote": 0,
        "pnl.remainder_quote": 0,
        "risk.max_abs_position_base": 0,
        "risk.max_drawdown_quote": 0,
        "markout.1s.mean_bps": null,
        "markout.5s.mean_bps": null,
        "markout.30s.mean_bps": null,
        "markout.60s.mean_bps": null,
        "markout.dropped": 4,
        "quoting.quoting_side_spins": 0,
        "quoting.not_quoting_spins": not_quoting_spins(0),
        "latency": latency,
    });
    run
}
