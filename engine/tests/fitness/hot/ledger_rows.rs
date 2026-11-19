//! The ledger table is the only record of what the engine held and what it was worth between two
//! fills, so the fold writes a ledger row at the boot of each restored position, at each fill that
//! the ledger books, while a position is open, and before a rotation clears a row.

use std::num::NonZeroU64;

use engine::hot::dispatch::{ExecWiring, ExposureWiring, HotEngine, HotEngineSetup};
use engine::hot::sink::ExecSink;
use engine::msg::exec::ExecLaneItem;
use engine::msg::ids::{InstrumentId, Notional, Price, Qty, Side};
use engine::msg::inbound::InboundMessage;
use engine::msg::persist::{
    AuditRecord, LedgerEntry, LedgerKind, MidRow, NoQuoteReason, ResearchRecord,
};
use engine::msg::time::{LocalTs, Stamper};
use engine::runtime::config::{RecordedTables, TableKind, TrackerSpec};
use engine::runtime::exposure::InstrumentExposure;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    FillPen, Idle, ONE, PersistRings, detached_setup, exposure_ring, instrument_row,
    persist_ring_for, rotation, spin, ts,
};
use crate::exec_support::{exec_settings, reseat_book};

const START: i64 = 1_000_000;
const PRICE: i64 = 100 * ONE;

/// One unit restored at a basis of 100.
fn restored() -> InstrumentExposure {
    InstrumentExposure {
        instrument: InstrumentId(0),
        position_base: Qty::from_mantissa(ONE),
        cash_quote: -100 * ONE,
        basis_quote: 100 * ONE,
    }
}

struct Ledgered {
    engine: HotEngine,
    rings: PersistRings,
    _commands: Consumer<ExecLaneItem>,
}

fn engine(restored: &[InstrumentExposure], ledger_interval: Option<NonZeroU64>) -> Ledgered {
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let tables = RecordedTables::new(&[TableKind::Orders, TableKind::Fills]);
    let (persistence, rings) = persist_ring_for(4_096, tables);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(1_024);
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        exec: vec![ExecWiring {
            sink: ExecSink::new(commands_producer, Stamper::Off),
            settings: exec_settings(),
            run_nonce: 0,
        }],
        exposure: ExposureWiring {
            restored,
            sink: exposure_ring(64, instruments.len()).0,
        },
        ..detached_setup(&instruments, Box::new(Idle))
    });
    let engine = match ledger_interval {
        Some(interval) => engine.writing_ledger_every(interval),
        None => engine,
    };
    Ledgered {
        engine,
        rings,
        _commands: commands,
    }
}

impl Ledgered {
    fn dispatch(&mut self, messages: &[InboundMessage]) -> Vec<AuditRecord> {
        for message in messages {
            self.engine.dispatch(message);
        }
        self.rings.audit()
    }
}

fn ledger_rows(records: &[AuditRecord]) -> Vec<LedgerEntry> {
    records
        .iter()
        .filter_map(|record| match record {
            AuditRecord::Ledger(row) => Some(*row),
            _ => None,
        })
        .collect()
}

/// The row that a position, cash and basis give at `mark`: realised is cash plus basis, exposure
/// is mark times position, and P&L is cash plus exposure.
fn entry(
    kind: LedgerKind,
    position: i64,
    cash: i64,
    basis: i64,
    mark: i64,
    at: i64,
) -> LedgerEntry {
    let mark = Price::from_mantissa(mark);
    let exposure = mark.notional(Qty::from_mantissa(position)).mantissa();
    LedgerEntry {
        kind,
        instrument: InstrumentId(0),
        position_base: Qty::from_mantissa(position),
        cash_quote: Notional::from_mantissa(cash),
        basis_quote: Notional::from_mantissa(basis),
        realised_quote: Notional::from_mantissa(cash + basis),
        mark: Some(mark),
        exposure_quote: Notional::from_mantissa(exposure),
        pnl_quote: Notional::from_mantissa(cash + exposure),
        client_id: None,
        trade_id: None,
        received_ts_us: ts(at),
    }
}

fn booted() -> Result<Ledgered, TestCaseError> {
    booted_with(None)
}

/// The first message folds the boot row of the restored position, before any other row.
fn booted_with(ledger_interval: Option<NonZeroU64>) -> Result<Ledgered, TestCaseError> {
    let mut ledgered = engine(&[restored()], ledger_interval);
    let first = ledgered.dispatch(&reseat_book(99 * ONE, 101 * ONE, START));
    let boot = LedgerEntry {
        mark: None,
        exposure_quote: Notional::ZERO,
        pnl_quote: Notional::from_mantissa(-100 * ONE),
        ..entry(LedgerKind::Boot, ONE, -100 * ONE, 100 * ONE, PRICE, START)
    };
    proptest::prop_assert_eq!(
        first.first(),
        Some(&AuditRecord::Ledger(boot)),
        "the restored position gives one boot row, first, stamped with the first message"
    );
    Ok(ledgered)
}

fn fills_give_ledger_rows(steps: &[i64], copies: &[usize]) -> Result<(), TestCaseError> {
    let mut ledgered = booted()?;
    let mut pen = FillPen::new(0);
    let (mut position, mut cash) = (ONE, -100 * ONE);
    let mut when = START;
    for (index, step) in steps.iter().enumerate() {
        when += 1_000;
        let messages = pen.fill(Side::Buy, PRICE, *step, when);
        let rows = ledgered.dispatch(&messages);
        position += step;
        cash -= Price::from_mantissa(PRICE)
            .notional(Qty::from_mantissa(*step))
            .mantissa();
        let ledger = ledger_rows(&rows);
        proptest::prop_assert_eq!(ledger.len(), 1, "one fill gives one ledger row: {:?}", rows);
        let row = ledger[0];
        proptest::prop_assert_eq!(
            row,
            LedgerEntry {
                client_id: row.client_id,
                trade_id: row.trade_id,
                ..entry(LedgerKind::Fill, position, cash, -cash, PRICE, when)
            },
            "the row holds the ledger after the fill"
        );
        let fill = rows.iter().find_map(|record| match record {
            AuditRecord::Fill(fill) => Some(*fill),
            _ => None,
        });
        proptest::prop_assert_eq!(
            fill.map(|fill| (Some(fill.client_id), fill.trade_id)),
            Some((row.client_id, row.trade_id)),
            "the ledger row names the order and the trade of its fill row: {:?}",
            rows
        );
        if copies.contains(&index) {
            let again = ledgered.dispatch(&messages[messages.len() - 1..]);
            proptest::prop_assert!(
                ledger_rows(&again).is_empty(),
                "a redelivered report moves no money and gives no row"
            );
        }
    }
    Ok(())
}

/// FITNESS: the ledger table follows the money. The first message gives a boot row for each
/// restored position; each report that the ledger books gives one row of kind `fill`, named by
/// the order and the trade of its fill row, with the position, cash, basis, mark, exposure and P&L
/// that the ledger holds after it; a redelivered report gives none. A table that missed a fill would show a P&L that the run
/// never had.
#[test]
fn fill_gives_ledger_row() {
    fills_give_ledger_rows(&[ONE / 10, ONE / 4, ONE], &[1]).expect("the fixed case");
    let mut runner = TestRunner::new(Config {
        cases: 64,
        failure_persistence: None,
        ..Config::default()
    });
    runner
        .run(
            &(
                proptest::collection::vec(1..=ONE, 1..12),
                proptest::collection::vec(0..12_usize, 0..4),
            ),
            |(steps, copies)| fills_give_ledger_rows(&steps, &copies),
        )
        .expect("each booked fill gives its ledger row");
}

/// FITNESS: a rotation gives the slot a new window and clears its ledger row, so the row as it
/// stood goes to the tape first, with the rotation's stamp. After it the row starts from nothing:
/// the next fill shows its own quantity alone. A rotation that cleared the row first would lose the
/// end of the old window's session.
#[test]
fn rotation_writes_ledger_row_before_reset() {
    let mut ledgered = booted().expect("the boot row comes first");
    let at = START + 5_000;
    let rows = ledgered.dispatch(&[rotation(0, at, at + 300_000_000, at)]);
    let ledger = ledger_rows(&rows);
    assert_eq!(
        ledger,
        [entry(
            LedgerKind::Rotation,
            ONE,
            -100 * ONE,
            100 * ONE,
            PRICE,
            at
        )],
        "the row before the reset, with the rotation's stamp"
    );

    let mut pen = FillPen::new(0);
    let after = ledger_rows(&ledgered.dispatch(&pen.fill(Side::Buy, PRICE, ONE / 2, at + 1_000)));
    let cash = -Price::from_mantissa(PRICE)
        .notional(Qty::from_mantissa(ONE / 2))
        .mantissa();
    let fill = after
        .first()
        .copied()
        .expect("the fill after the rotation gives a row");
    assert_eq!(
        (
            fill.kind,
            fill.position_base,
            fill.cash_quote,
            fill.received_ts_us
        ),
        (
            LedgerKind::Fill,
            Qty::from_mantissa(ONE / 2),
            Notional::from_mantissa(cash),
            LocalTs::from_micros(at + 1_000)
        ),
        "the row starts again from nothing after the rotation"
    );
}

/// FITNESS: each spin writes the touch of each book, and after its execution pass each traded side
/// that holds no quote, with the reason the pass recorded; at the first spin past each interval the
/// ledger of each open position goes to the tape too. Quoting time and inventory P&L are read off
/// these rows, so a spin that wrote none would make a quiet run look like one that never quoted.
#[test]
fn spin_writes_interval_mid_and_no_quote_rows() {
    let interval = NonZeroU64::new(10).expect("not zero");
    let mut ledgered = booted_with(Some(interval)).expect("the boot row comes first");
    let at = START + 9_000;
    let audit = ledgered.dispatch(&[spin(10, at)]);
    assert_eq!(
        ledger_rows(&audit),
        [entry(
            LedgerKind::Interval,
            ONE,
            -100 * ONE,
            100 * ONE,
            PRICE,
            at
        )],
        "the first spin past the interval writes the open position"
    );
    let audit = ledgered.dispatch(&[spin(11, at + 1)]);
    assert!(ledger_rows(&audit).is_empty(), "one row for each interval");
    ledgered.engine.dispatch(&spin(12, at + 2));

    let research = ledgered.rings.research();
    let mids: Vec<MidRow> = research
        .iter()
        .filter_map(|record| match record {
            ResearchRecord::Mid(row) => Some(*row),
            _ => None,
        })
        .collect();
    assert_eq!(
        mids.len(),
        3,
        "one mid row for each instrument at each spin"
    );
    assert_eq!(
        (mids[0].bid, mids[0].ask, mids[0].mid, mids[0].spin_seq),
        (
            Some(Price::from_mantissa(99 * ONE)),
            Some(Price::from_mantissa(101 * ONE)),
            Some(Price::from_mantissa(PRICE)),
            10
        ),
        "the touch and its exact mid"
    );
    let unquoted: Vec<(u64, Side, NoQuoteReason)> = research
        .iter()
        .filter_map(|record| match record {
            ResearchRecord::NoQuote(row) => Some((row.spin_seq, row.side, row.reason)),
            _ => None,
        })
        .collect();
    assert_eq!(
        unquoted[..2],
        [
            (10, Side::Buy, NoQuoteReason::NotReadyStream),
            (10, Side::Sell, NoQuoteReason::NotReadyStream),
        ],
        "an engine that waits for its stream quotes neither side, and says why"
    );
}
