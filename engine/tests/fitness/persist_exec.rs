//! The audit trail must survive the round trip. `orders` and `fills` are the only durable record of
//! what this engine did with real money, and the only thing a venue statement can be reconciled
//! against — so every column has to come back out of a real Parquet file byte-identical, and the
//! columns that answer "which asset was the fee in" and "did the venue ever accept this order" have
//! to come back as themselves rather than as a zero.
//!
//! Three properties, each pinning a way the trail goes silently wrong:
//!  - the round trip, cell for cell, over rows chosen to cover every absent-value path;
//!  - `strategy.tables` governing these two exactly as it governs every other table, so a live run
//!    cannot record orders it was never configured to record (or, worse, believe it did);
//!  - the enum spellings, which are a DATA FORMAT: a rename silently re-labels history, and two
//!    variants sharing one spelling merges two causes nobody can separate afterwards.

use std::path::{Path, PathBuf};

use polysim::config::{RecordedTables, TableKind};
use polysim::hot::exec::QuoteLevel;
use polysim::ids::{
    AssetId, ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty, Side, TradeId, VenueOrderId,
};
use polysim::msg::exec::{Liquidity, OrderStyle, Provenance, RejectClass};
use polysim::msg::persist::{FillRow, OrderLifecycle, OrderRow, OrderTransition, PersistRecord};
use polysim::time::TsUs;

use crate::parquet_readback::{
    BASE_TS_US, Cell, FileData, TempDir, drain_blocking, parquet_files, read_parquet_file, run_dir,
    spawn_writer,
};

const QUOTE_ASSET: AssetId = AssetId(1);

/// One BTC at $60,000 in 1e-8 mantissas, so a scale slip anywhere shows as a factor of 1e8 rather
/// than as a plausible-looking number.
const PRICE: Price = Price(60_000 * FIXED_SCALE);
const QTY: Qty = Qty(FIXED_SCALE / 10_000);

fn ts(offset: i64) -> TsUs {
    TsUs::from_micros(BASE_TS_US + offset)
}

/// The order's life as a live run records it: sent, acknowledged, partially filled, then rejected on
/// the way out. Deliberately includes the two rows a naive schema loses — the send, which has no
/// venue order id and no venue clock, and the reject, which carries a code and a class.
fn order_rows() -> Vec<OrderRow> {
    let sent = OrderRow {
        instrument: InstrumentId(0),
        client_id: ClientOrderId(0x1234_5678_9abc_def0),
        quote_level: Some(QuoteLevel::ZERO),
        venue_order_id: None,
        transition: OrderTransition::Placed,
        state: OrderLifecycle::PendingNew,
        previous_state: OrderLifecycle::Free,
        provenance: Provenance::Mine,
        side: Side::Buy,
        style: Some(OrderStyle::PostOnly),
        price: PRICE,
        qty: QTY,
        filled_qty: Qty(0),
        filled_quote: 0,
        reject: None,
        reject_code: 0,
        exchange_ts_us: TsUs::from_micros(0),
        received_ts_us: ts(1),
    };
    vec![
        sent,
        OrderRow {
            venue_order_id: Some(VenueOrderId(9_001)),
            transition: OrderTransition::AckPlaced,
            state: OrderLifecycle::Live,
            previous_state: OrderLifecycle::PendingNew,
            exchange_ts_us: ts(2),
            received_ts_us: ts(3),
            ..sent
        },
        OrderRow {
            venue_order_id: Some(VenueOrderId(9_001)),
            transition: OrderTransition::ReportTrade,
            state: OrderLifecycle::Live,
            previous_state: OrderLifecycle::Live,
            filled_qty: Qty(QTY.0 / 2),
            filled_quote: PRICE.notional(Qty(QTY.0 / 2)),
            exchange_ts_us: ts(4),
            received_ts_us: ts(5),
            ..sent
        },
        OrderRow {
            client_id: ClientOrderId(u64::MAX),
            venue_order_id: Some(VenueOrderId(-1)),
            transition: OrderTransition::AckFailed,
            state: OrderLifecycle::Unknown,
            previous_state: OrderLifecycle::CancelInFlight,
            provenance: Provenance::PriorRun,
            quote_level: None,
            side: Side::Sell,
            // An order this engine never sent: it was adopted from a snapshot, and no venue field
            // reports what a prior run asked for.
            style: None,
            reject: Some(RejectClass::Ambiguous),
            reject_code: -2011,
            exchange_ts_us: ts(6),
            received_ts_us: ts(7),
            ..sent
        },
    ]
}

/// Three fills covering the paths an accountant's line item can take: an ordinary maker fill, a
/// taker fill whose fee is charged in an asset the config never named, and a fill learned from a
/// cancel acknowledgement — no trade id, no liquidity, and a booked quantity larger than the last
/// executed one because a report in between was lost.
fn fill_rows() -> Vec<FillRow> {
    let maker = FillRow {
        instrument: InstrumentId(0),
        trade_id: Some(TradeId(77)),
        venue_order_id: Some(VenueOrderId(9_001)),
        client_id: ClientOrderId(0x1234_5678_9abc_def0),
        quote_level: Some(QuoteLevel::ZERO),
        provenance: Provenance::Mine,
        side: Side::Buy,
        liquidity: Some(Liquidity::Maker),
        last_price: PRICE,
        last_qty: Qty(QTY.0 / 2),
        booked_qty: Qty(QTY.0 / 2),
        booked_quote: PRICE.notional(Qty(QTY.0 / 2)),
        commission: 4_720,
        commission_asset: QUOTE_ASSET,
        exchange_ts_us: ts(4),
        received_ts_us: ts(5),
    };
    vec![
        maker,
        FillRow {
            trade_id: Some(TradeId(78)),
            provenance: Provenance::Foreign,
            quote_level: None,
            side: Side::Sell,
            liquidity: Some(Liquidity::Taker),
            commission: 9_440,
            commission_asset: AssetId::UNKNOWN,
            exchange_ts_us: ts(8),
            received_ts_us: ts(9),
            ..maker
        },
        FillRow {
            trade_id: None,
            venue_order_id: None,
            liquidity: None,
            last_qty: Qty(QTY.0 / 4),
            booked_qty: QTY,
            booked_quote: PRICE.notional(QTY),
            commission: 0,
            exchange_ts_us: ts(10),
            received_ts_us: ts(11),
            ..maker
        },
    ]
}

#[test]
fn orders_and_fills_round_trip_through_the_arrow_reader() {
    let root = TempDir::new("persist-exec-round-trip");
    let orders = order_rows();
    let fills = fill_rows();
    let records = orders
        .iter()
        .copied()
        .map(PersistRecord::Order)
        .chain(fills.iter().copied().map(PersistRecord::Fill))
        .collect();
    drain(root.path(), RecordedTables::new(&EXEC_TABLES), records);

    let orders_file = read_one_file(&table_dir(root.path(), TableKind::Orders));
    assert_eq!(
        orders_file.field_names,
        [
            "exchange_ts_us",
            "received_ts_us",
            "instrument_id",
            "client_order_id",
            "quote_level",
            "venue_order_id",
            "transition",
            "state",
            "previous_state",
            "provenance",
            "side",
            "style",
            "price",
            "qty",
            "filled_qty",
            "filled_quote",
            "reject_class",
            "reject_code",
        ]
    );
    assert_eq!(
        orders_file.rows,
        orders.iter().map(order_cells).collect::<Vec<_>>(),
        "every orders column survives the parquet round trip unchanged"
    );

    let fills_file = read_one_file(&table_dir(root.path(), TableKind::Fills));
    assert_eq!(
        fills_file.field_names,
        [
            "exchange_ts_us",
            "received_ts_us",
            "instrument_id",
            "trade_id",
            "venue_order_id",
            "client_order_id",
            "quote_level",
            "provenance",
            "side",
            "liquidity",
            "last_price",
            "last_qty",
            "booked_qty",
            "booked_quote",
            "commission",
            "commission_asset_id",
        ]
    );
    assert_eq!(
        fills_file.rows,
        fills.iter().map(fill_cells).collect::<Vec<_>>(),
        "every fills column survives the parquet round trip unchanged"
    );

    let dictionary = footer_value(&fills_file.footer, "asset_dictionary");
    assert_eq!(
        dictionary, r#"["btc","usdt"]"#,
        "the fills commission asset is a dense index, so the footer must carry the names it indexes"
    );
    assert_eq!(
        footer_value(&fills_file.footer, "fixed_scale"),
        FIXED_SCALE.to_string(),
        "the money columns come back as mantissas above, which a reader cannot interpret without \
         the scale they are in"
    );
}

#[test]
fn an_execution_table_the_config_never_named_gets_no_sink() {
    let orders_only = TempDir::new("persist-exec-orders-only");
    drain(
        orders_only.path(),
        RecordedTables::new(&[TableKind::Orders]),
        both_kinds(),
    );
    assert!(
        table_dir(orders_only.path(), TableKind::Orders).is_dir(),
        "a named execution table opens its file on the first row"
    );
    assert!(
        !table_dir(orders_only.path(), TableKind::Fills).exists(),
        "naming orders says nothing about fills — each table is named or it does not exist"
    );

    let neither = TempDir::new("persist-exec-neither");
    drain(neither.path(), RecordedTables::new(&[]), both_kinds());
    assert!(
        !run_dir(neither.path()).exists(),
        "with neither named there is no sink to open a file, so not even the run root appears"
    );
}

/// Every variant's spelling, pinned. The `match` is what makes this immortal: a new variant fails to
/// compile here until somebody decides what it is called on disk, which is the moment to decide it —
/// afterwards it is a migration.
#[test]
fn every_transition_and_lifecycle_column_value_is_distinct() {
    let transitions = [
        OrderTransition::Placed,
        OrderTransition::CancelSent,
        OrderTransition::AmendSent,
        OrderTransition::SendAbandoned,
        OrderTransition::Timeout,
        OrderTransition::SweepClosed,
        OrderTransition::StreamReset,
        OrderTransition::AckPlaced,
        OrderTransition::AckCanceled,
        OrderTransition::AckAmended,
        OrderTransition::AckFailed,
        OrderTransition::ReportNew,
        OrderTransition::ReportTrade,
        OrderTransition::ReportCanceled,
        OrderTransition::ReportExpired,
        OrderTransition::ReportRejected,
        OrderTransition::ReportAmended,
        OrderTransition::SnapshotOrder,
    ];
    for transition in transitions {
        assert_eq!(transition.as_str(), expected_transition_str(transition));
    }
    assert_distinct(&transitions.map(OrderTransition::as_str));

    let lifecycles = [
        OrderLifecycle::Free,
        OrderLifecycle::PendingNew,
        OrderLifecycle::Live,
        OrderLifecycle::CancelInFlight,
        OrderLifecycle::AmendInFlight,
        OrderLifecycle::Unknown,
        OrderLifecycle::ClosedFilled,
        OrderLifecycle::ClosedCanceled,
        OrderLifecycle::ClosedRejected,
        OrderLifecycle::ClosedExpired,
        OrderLifecycle::ClosedReconciledGone,
    ];
    for lifecycle in lifecycles {
        assert_eq!(lifecycle.as_str(), expected_lifecycle_str(lifecycle));
    }
    assert_distinct(&lifecycles.map(OrderLifecycle::as_str));
}

fn expected_transition_str(transition: OrderTransition) -> &'static str {
    match transition {
        OrderTransition::Placed => "placed",
        OrderTransition::CancelSent => "cancel_sent",
        OrderTransition::AmendSent => "amend_sent",
        OrderTransition::SendAbandoned => "send_abandoned",
        OrderTransition::Timeout => "timeout",
        OrderTransition::SweepClosed => "sweep_closed",
        OrderTransition::StreamReset => "stream_reset",
        OrderTransition::AckPlaced => "ack_placed",
        OrderTransition::AckCanceled => "ack_canceled",
        OrderTransition::AckAmended => "ack_amended",
        OrderTransition::AckFailed => "ack_failed",
        OrderTransition::ReportNew => "report_new",
        OrderTransition::ReportTrade => "report_trade",
        OrderTransition::ReportCanceled => "report_canceled",
        OrderTransition::ReportExpired => "report_expired",
        OrderTransition::ReportRejected => "report_rejected",
        OrderTransition::ReportAmended => "report_amended",
        OrderTransition::SnapshotOrder => "snapshot_order",
    }
}

fn expected_lifecycle_str(lifecycle: OrderLifecycle) -> &'static str {
    match lifecycle {
        OrderLifecycle::Free => "free",
        OrderLifecycle::PendingNew => "pending_new",
        OrderLifecycle::Live => "live",
        OrderLifecycle::CancelInFlight => "cancel_in_flight",
        OrderLifecycle::AmendInFlight => "amend_in_flight",
        OrderLifecycle::Unknown => "unknown",
        OrderLifecycle::ClosedFilled => "closed_filled",
        OrderLifecycle::ClosedCanceled => "closed_canceled",
        OrderLifecycle::ClosedRejected => "closed_rejected",
        OrderLifecycle::ClosedExpired => "closed_expired",
        OrderLifecycle::ClosedReconciledGone => "closed_reconciled_gone",
    }
}

fn assert_distinct(spellings: &[&'static str]) {
    let mut sorted = spellings.to_vec();
    sorted.sort_unstable();
    let before = sorted.len();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        before,
        "two variants sharing one column value merges two causes nobody can separate afterwards"
    );
}

const EXEC_TABLES: [TableKind; 2] = [TableKind::Orders, TableKind::Fills];

fn both_kinds() -> Vec<PersistRecord> {
    vec![
        PersistRecord::Order(order_rows()[0]),
        PersistRecord::Fill(fill_rows()[0]),
    ]
}

fn order_cells(row: &OrderRow) -> Vec<Cell> {
    vec![
        Cell::I64(row.exchange_ts_us.micros()),
        Cell::I64(row.received_ts_us.micros()),
        Cell::U16(row.instrument.0),
        Cell::U64(row.client_id.0),
        row.quote_level
            .map_or(Cell::Null, |level| Cell::U8(level.get())),
        row.venue_order_id.map_or(Cell::Null, |id| Cell::I64(id.0)),
        Cell::Str(row.transition.as_str().to_owned()),
        Cell::Str(row.state.as_str().to_owned()),
        Cell::Str(row.previous_state.as_str().to_owned()),
        Cell::Str(provenance_str(row.provenance).to_owned()),
        Cell::Str(side_str(row.side).to_owned()),
        Cell::Str(style_str(row.style).to_owned()),
        Cell::I64(row.price.0),
        Cell::I64(row.qty.0),
        Cell::I64(row.filled_qty.0),
        Cell::I64(row.filled_quote),
        Cell::Str(reject_str(row.reject).to_owned()),
        Cell::I32(row.reject_code),
    ]
}

fn fill_cells(row: &FillRow) -> Vec<Cell> {
    vec![
        Cell::I64(row.exchange_ts_us.micros()),
        Cell::I64(row.received_ts_us.micros()),
        Cell::U16(row.instrument.0),
        row.trade_id.map_or(Cell::Null, |id| Cell::I64(id.0)),
        row.venue_order_id.map_or(Cell::Null, |id| Cell::I64(id.0)),
        Cell::U64(row.client_id.0),
        row.quote_level
            .map_or(Cell::Null, |level| Cell::U8(level.get())),
        Cell::Str(provenance_str(row.provenance).to_owned()),
        Cell::Str(side_str(row.side).to_owned()),
        Cell::Str(liquidity_str(row.liquidity).to_owned()),
        Cell::I64(row.last_price.0),
        Cell::I64(row.last_qty.0),
        Cell::I64(row.booked_qty.0),
        Cell::I64(row.booked_quote),
        Cell::I64(row.commission),
        Cell::U16(row.commission_asset.0),
    ]
}

fn provenance_str(provenance: Provenance) -> &'static str {
    match provenance {
        Provenance::Mine => "mine",
        Provenance::PriorRun => "prior_run",
        Provenance::Foreign => "foreign",
    }
}

fn reject_str(reject: Option<RejectClass>) -> &'static str {
    match reject {
        None => "none",
        Some(RejectClass::StillLive) => "still_live",
        Some(RejectClass::Refused) => "refused",
        Some(RejectClass::Gone) => "gone",
        Some(RejectClass::Ambiguous) => "ambiguous",
        Some(RejectClass::Fatal) => "fatal",
    }
}

fn style_str(style: Option<OrderStyle>) -> &'static str {
    match style {
        None => "unknown",
        Some(OrderStyle::PostOnly) => "post_only",
        Some(OrderStyle::Immediate) => "immediate",
    }
}

fn liquidity_str(liquidity: Option<Liquidity>) -> &'static str {
    match liquidity {
        None => "none",
        Some(Liquidity::Maker) => "maker",
        Some(Liquidity::Taker) => "taker",
    }
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

/// Spawn the real actor over preloaded records and drain it exactly as the runtime does, so the only
/// variable between cases is the configured table set.
fn drain(dir_root: &Path, tables: RecordedTables, records: Vec<PersistRecord>) {
    drain_blocking(spawn_writer(dir_root, tables, records))
        .expect("a row with no sink to land in is a drop, never a drain failure");
}

fn read_one_file(dir: &Path) -> FileData {
    let files = parquet_files(dir);
    assert_eq!(
        files.len(),
        1,
        "one hour of rows seals into one file, so a second means the rotation moved"
    );
    read_parquet_file(&files[0])
}

fn footer_value(footer: &[(String, Option<String>)], key: &str) -> String {
    footer
        .iter()
        .find(|(name, _)| name == key)
        .and_then(|(_, value)| value.clone())
        .unwrap_or_else(|| panic!("footer carries {key}"))
}

fn table_dir(dir_root: &Path, table: TableKind) -> PathBuf {
    run_dir(dir_root).join(table.as_str())
}
