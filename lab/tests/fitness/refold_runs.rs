//! A re-fold of a run's tape from its first message gives the run's tables and the state of its
//! fold: with orders and fills, from a restored position, across a halt the run recorded, and with
//! research rows that the run dropped.

use engine::hot::exec::{ClientIdLayout, side_base};
use engine::hot::sink::AuditBank;
use engine::hot::strategy::{Strategy, StrategyConfig};
use engine::msg::exec::{ExecEvent, ExecKind, Provenance, VenueOrderStatus};
use engine::msg::ids::{InstrumentId, Price, Qty, Side, VenueId};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::persist::{BootExposure, BootVenue};
use engine::runtime::FoldRings;
use engine::runtime::config::Config;
use engine::runtime::manifest::{InstrumentTerms, MANIFEST_FILE, RunKind, RunManifest};
use engine::tape::TapePayload;
use lab::backtest::{RefoldDiff, RefoldOutcome, RefoldPlan, TapeReader, check_refold, refold};

use crate::fixtures::{
    BINANCE_CHANGES, BINANCE_CONFIG, NO_PARAMS, POLYMARKET_CONFIG, TouchQuoter, binance_tape,
    config_text, polymarket_tape,
};
use crate::micro_strategy::MicroRecorder;
use crate::poly_strategy::PolyUpPublisher;
use crate::runs::{DropReport, Pumping, Recorded, RunPlan, TempRoot, record};

const ONE: i64 = 100_000_000;
const RUN_NONCE: u32 = 0x2a;

/// The nonce of an earlier run, so the venue's report on its order is one this run did not place.
const PRIOR_NONCE: u32 = 0xabcd;

/// The terms that the preflight of the Binance spot engine reads from its venue.
fn binance_terms(terms: &mut InstrumentTerms) {
    terms.tick_size = Some(ONE / 100);
    terms.lot_size = Some(1_000);
    terms.min_qty = Some(1_000);
    terms.min_notional = Some(5 * ONE);
    terms.max_num_orders = Some(200);
    terms.max_num_order_amends = Some(10);
}

/// The terms that the preflight of the Polymarket engine stamps on each slot.
fn polymarket_terms(terms: &mut InstrumentTerms) {
    terms.tick_size = Some(ONE / 100);
    terms.lot_size = Some(ONE / 100);
    terms.min_qty = Some(5 * ONE);
    terms.max_price = Some(ONE - ONE / 100);
    terms.max_num_orders = Some(32);
}

fn plan<'a>(config_text: &'a str, tape: Vec<InboundMessage>) -> RunPlan<'a> {
    RunPlan {
        config_text,
        tape,
        run_nonce: Some(RUN_NONCE),
        exposures: Vec::new(),
        rings: FoldRings::SHIPPED,
        pumping: Pumping::EachStep,
        drops: DropReport::AsCounted,
        terms: binance_terms,
    }
}

fn refold_of<S>(run: &Recorded, config_text: &str) -> RefoldOutcome
where
    S: Strategy + StrategyConfig + 'static,
{
    let root = run
        .run_dir
        .ancestors()
        .nth(4)
        .expect("a run folder sits four folders below its root");
    refold::<S>(RefoldPlan {
        source_run: run.run_dir.clone(),
        config: Config::from_yaml(config_text).expect("the config of the run parses"),
        config_text: config_text.to_owned(),
        out_root: root.to_path_buf(),
    })
    .unwrap_or_else(|error| panic!("the re-fold of {} failed: {error}", run.run_dir.display()))
}

/// The re-fold holds each table of its run and the state the run ended in, writes no tape, and
/// names the run it folded.
fn assert_equal(case: &str, run: &Recorded, replay: &RefoldOutcome) {
    if let Err(diff) = check_refold(&run.run_dir, &replay.run_dir) {
        panic!("{case}: the re-fold parts from its run: {diff}");
    }
    assert_eq!(
        replay.state_digest, run.digest,
        "{case}: the fold of the re-fold ends in another state than its run"
    );
    assert!(
        !replay.run_dir.join("tape").exists(),
        "{case}: a re-fold writes no tape of its own"
    );
    let text = std::fs::read_to_string(replay.run_dir.join(MANIFEST_FILE)).expect("run.json");
    let manifest: RunManifest = serde_json::from_str(&text).expect("a manifest");
    let unwritten: Vec<&String> = manifest
        .tables
        .iter()
        .filter(|table| *table == "tape" || table.starts_with("metrics_"))
        .collect();
    assert!(
        unwritten.is_empty(),
        "{case}: the run.json of a re-fold lists only tables it writes, and it writes no tape and no \
         metrics table, yet it lists {unwritten:?}"
    );
    let source = manifest.source.expect("a replay names its source");
    assert_eq!(
        (manifest.kind, source.run_id.as_str()),
        (
            RunKind::Replay,
            run.run_dir
                .file_name()
                .and_then(|name| name.to_str())
                .expect("an id")
        ),
        "{case}: the manifest of the re-fold"
    );
}

/// FITNESS (X1): a simulated run and the re-fold of its tape give the same tables, cell for cell,
/// and the same state at the end. Three cases: the Binance engine with a probe that quotes the
/// touch, so orders and fills exist (its answers come from a lab test venue; through the real
/// simulator it is re-run by S4 when VS-25 lifts the stepper), the shipped Binance recorder, and the
/// shipped Polymarket engine with execution off. A re-fold that drifts from its run would judge a
/// run that never traded, and every later stage judges runs this way.
#[test]
fn sim_run_equals_its_refold() {
    let root = TempRoot::new("x1-touch");
    let mut changes = BINANCE_CHANGES.to_vec();
    changes.push(NO_PARAMS);
    let text = config_text(BINANCE_CONFIG, root.path(), &changes);
    let touch = record::<TouchQuoter>(plan(&text, binance_tape()));
    println!(
        "case 1: {} orders placed, {} fills, over {} messages",
        touch.places,
        touch.fills,
        binance_tape().len()
    );
    assert!(
        touch.places > 0 && touch.fills > 0,
        "case 1 must trade to prove anything about orders: {} orders placed, {} fills",
        touch.places,
        touch.fills
    );
    assert_equal("case 1", &touch, &refold_of::<TouchQuoter>(&touch, &text));

    let root = TempRoot::new("x1-recorder");
    let text = config_text(BINANCE_CONFIG, root.path(), &BINANCE_CHANGES);
    let recorder = record::<MicroRecorder>(plan(&text, binance_tape()));
    assert_equal(
        "case 2",
        &recorder,
        &refold_of::<MicroRecorder>(&recorder, &text),
    );

    let root = TempRoot::new("x1-polymarket");
    let text = config_text(
        POLYMARKET_CONFIG,
        root.path(),
        &[("strategy:\n", "strategy:\n  tables: [features]\n")],
    );
    let polymarket = record::<PolyUpPublisher>(RunPlan {
        run_nonce: None,
        terms: polymarket_terms,
        ..plan(&text, polymarket_tape())
    });
    assert_eq!(polymarket.places, 0, "case 3 runs with execution off");
    assert_equal(
        "case 3",
        &polymarket,
        &refold_of::<PolyUpPublisher>(&polymarket, &text),
    );
}

/// A report on an order that a run of nonce [`PRIOR_NONCE`] left resting: first the report that
/// seats it, then a fill of `qty`.
fn prior_run_order(at: i64, qty: i64, price: i64) -> [InboundMessage; 2] {
    let client_id = ClientIdLayout {
        run_nonce: PRIOR_NONCE,
    }
    .encode(side_base(InstrumentId(0), Side::Buy) + 1, 1);
    let report = |kind, status, filled: i64| ExecEvent {
        venue: VenueId(0),
        instrument: Some(InstrumentId(0)),
        client_id,
        venue_order_id: None,
        trade_id: None,
        kind,
        status: Some(status),
        reject: None,
        provenance: Provenance::PriorRun,
        side: Side::Buy,
        liquidity: None,
        price: Price::from_mantissa(price),
        qty: Qty::from_mantissa(4 * qty),
        last_price: Price::from_mantissa(if filled > 0 { price } else { 0 }),
        last_qty: Qty::from_mantissa(filled),
        cumulative_qty: Qty::from_mantissa(filled),
        cumulative_quote: Price::from_mantissa(price).notional(Qty::from_mantissa(filled)),
        commission: 0,
        commission_asset: engine::msg::ids::AssetId::UNKNOWN,
        reject_code: 0,
        amends_remaining: ExecEvent::AMENDS_UNKNOWN,
        recon_seq: 0,
        exchange_ts_us: engine::msg::time::VenueTs::from_micros(at),
        request_sent_ts_us: None,
    };
    let message = |event| {
        InboundMessage::new(
            engine::msg::time::LocalTs::from_micros(at),
            MessageBody::Exec(event),
        )
    };
    [
        message(report(ExecKind::ReportNew, VenueOrderStatus::New, 0)),
        message(report(
            ExecKind::ReportTrade,
            VenueOrderStatus::PartiallyFilled,
            qty,
        )),
    ]
}

/// FITNESS: the start state of a fold is no message, so the run records it in its boot row. A run
/// that starts from a restored position, under its own nonce, and folds a fill of an order that an
/// earlier run left resting, re-folds to the same tables and state: a re-fold that started flat
/// would book every later fill against another position.
#[test]
fn refold_of_run_with_boot_position_and_prior_nonce_equals_run() {
    let root = TempRoot::new("boot");
    let mut changes = BINANCE_CHANGES.to_vec();
    changes.push(NO_PARAMS);
    let text = config_text(BINANCE_CONFIG, root.path(), &changes);
    let mut tape = binance_tape();
    let third = tape.len() / 3;
    let at = tape[third].header.received_ts_us.micros();
    let inherited = prior_run_order(at, ONE / 10_000, 64_682 * ONE);
    tape.splice(third..third, inherited);
    let boot_position = BootExposure {
        instrument: InstrumentId(0),
        position_base: Qty::from_mantissa(ONE / 1_000),
        cash_quote: -64_680 * ONE / 1_000,
        basis_quote: 64_680 * ONE / 1_000,
    };
    let run = record::<TouchQuoter>(RunPlan {
        exposures: vec![boot_position],
        ..plan(&text, tape)
    });

    let reader = TapeReader::open(&run.run_dir).expect("the tape of the run opens");
    assert_eq!(
        (
            reader.boot().venues.clone(),
            reader.boot().exposures.clone()
        ),
        (
            vec![BootVenue {
                venue: VenueId(0),
                run_nonce: RUN_NONCE
            }],
            vec![boot_position]
        ),
        "the boot row holds the nonce of the run and the position it started from"
    );
    drop(reader);
    assert_equal("boot", &run, &refold_of::<TouchQuoter>(&run, &text));
}

/// FITNESS (F5r): a run whose audit bank reached its high mark halted quoting at one message of its
/// input, and that message is on its tape. The re-fold pumps its writer after each message, so its
/// own bank never fills: it halts at the recorded message, places nothing after it, and holds the
/// run's orders, the sweep's cancels included. A halt from the state of a ring would leave the
/// re-fold quoting on.
#[test]
fn refold_halts_at_recorded_bank_message() {
    let root = TempRoot::new("bank");
    let mut changes = BINANCE_CHANGES.to_vec();
    changes.push(NO_PARAMS);
    let text = config_text(BINANCE_CONFIG, root.path(), &changes);
    let high_mark = 12;
    let run = record::<TouchQuoter>(RunPlan {
        rings: FoldRings {
            audit: 4,
            bank: AuditBank {
                rows: 4_096,
                high_mark,
            },
            ..FoldRings::SHIPPED
        },
        pumping: Pumping::AfterBankPost,
        ..plan(&text, binance_tape())
    });
    assert!(
        run.counters.audit_banked_peak >= high_mark as u64,
        "the bank of the run reached {} rows, below its high mark of {high_mark}",
        run.counters.audit_banked_peak
    );
    let posts: Vec<u64> = TapeReader::open(&run.run_dir)
        .expect("the tape of the run opens")
        .map(|row| row.expect("each tape row decodes"))
        .filter(|row| {
            matches!(
                row.payload,
                TapePayload::Message(MessageBody::AuditBankFull(_))
            )
        })
        .map(|row| row.seq)
        .collect();
    assert_eq!(
        posts.len(),
        1,
        "the bank posts once for each run, and the tape holds it: seqs {posts:?}"
    );
    assert_equal("bank", &run, &refold_of::<TouchQuoter>(&run, &text));
}

/// FITNESS: a research row that the run dropped on a full ring is in the re-fold, which pumps its
/// writer after each message. The re-fold check takes such rows as no breach while each table holds
/// no more of them than the run counted dropped in `metrics_drops`, and as a breach past that: a
/// check that took any extra row would hide a re-fold that writes rows of its own.
#[test]
fn refold_check_takes_dropped_research_rows_by_count() {
    let mut changes = BINANCE_CHANGES.to_vec();
    changes.push(NO_PARAMS);
    changes.push(("persistence_capacity: 65536", "persistence_capacity: 16"));
    for (report, case) in [
        (DropReport::AsCounted, "counted"),
        (DropReport::OneShort, "short"),
    ] {
        let root = TempRoot::new(case);
        let text = config_text(BINANCE_CONFIG, root.path(), &changes);
        let run = record::<TouchQuoter>(RunPlan {
            pumping: Pumping::EveryNth(400),
            drops: report,
            ..plan(&text, binance_tape())
        });
        let dropped = run.counters.research_dropped.total();
        assert!(
            dropped > 0,
            "{case}: the run must drop research rows to test the check"
        );
        let replay = refold_of::<TouchQuoter>(&run, &text);
        let verdict = check_refold(&run.run_dir, &replay.run_dir);
        match report {
            DropReport::AsCounted => {
                if let Err(diff) = verdict {
                    panic!(
                        "{case}: {dropped} rows dropped and counted, yet the check refused: {diff}"
                    );
                }
                assert_eq!(
                    replay.state_digest, run.digest,
                    "{case}: drops change no state"
                );
            }
            DropReport::OneShort => assert!(
                matches!(verdict, Err(RefoldDiff::OnlyInReplay { .. })),
                "{case}: a drop count one short must be a breach, got {verdict:?}"
            ),
        }
    }
}

/// FITNESS: the reader holds two row groups at most, the one the re-fold folds and the one a second
/// thread decodes next, however long the tape. A seal every 5 spins cuts the tape into a file for
/// each border, so it has many groups; a reader that read ahead with no bound would hold a long
/// tape whole.
#[test]
fn tape_reader_holds_two_row_groups_at_most() {
    let root = TempRoot::new("groups");
    let mut changes = BINANCE_CHANGES.to_vec();
    changes.push(NO_PARAMS);
    changes.push((
        "  retention_hours: 6",
        "  retention_hours: 6\n  seal_interval_spins: 5",
    ));
    let text = config_text(BINANCE_CONFIG, root.path(), &changes);
    let run = record::<TouchQuoter>(plan(&text, binance_tape()));
    let mut reader = TapeReader::open(&run.run_dir).expect("the tape of the run opens");
    let mut snapshots = 0;
    for row in reader.by_ref() {
        let row = row.expect("each tape row decodes");
        snapshots += usize::from(matches!(row.payload, TapePayload::BookSnapshot(_)));
    }
    assert!(
        snapshots >= 2 * 5,
        "the tape opens at least 5 files with the two sides of the book, saw {snapshots} snapshots"
    );
    assert!(
        reader.most_groups_held() <= 2,
        "the reader held {} row groups at once",
        reader.most_groups_held()
    );
    assert_equal("groups", &run, &refold_of::<TouchQuoter>(&run, &text));
}

/// Keeps the first half of the rows of the one file of `table` in `run`, its footer as it was, and
/// names the table in `stopped_tables` when `is_stopped` says so.
fn stop_table_halfway(run: &std::path::Path, table: &str, stopped: Stopped) {
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;

    let date = std::fs::read_dir(run.join(table))
        .expect("a table folder")
        .next()
        .expect("a date folder")
        .expect("an entry")
        .path();
    let file = std::fs::read_dir(date)
        .expect("files")
        .next()
        .expect("one file")
        .expect("an entry")
        .path();
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&file).expect("open"))
            .expect("a footer");
    let schema = std::sync::Arc::clone(builder.schema());
    let footer = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned();
    let batches: Vec<_> = builder
        .build()
        .expect("a reader")
        .map(|batch| batch.expect("a batch"))
        .collect();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    let kept = arrow_select_head(&batches, rows / 2);
    let properties = WriterProperties::builder()
        .set_key_value_metadata(footer)
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&file).expect("create"),
        schema,
        Some(properties),
    )
    .expect("a writer");
    for batch in &kept {
        writer.write(batch).expect("write");
    }
    writer.close().expect("close");
    if stopped == Stopped::Named {
        let path = run.join(MANIFEST_FILE);
        let mut manifest: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("run.json")).expect("json");
        manifest["stopped_tables"] = serde_json::json!([table]);
        std::fs::write(&path, manifest.to_string()).expect("run.json is writable");
    }
}

/// Whether `run.json` names the table that lost its rows as stopped by a writer fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stopped {
    Named,
    NotNamed,
}

/// The first `rows` rows of `batches`, in order.
fn arrow_select_head(
    batches: &[arrow_array::RecordBatch],
    rows: usize,
) -> Vec<arrow_array::RecordBatch> {
    let mut left = rows;
    let mut kept = Vec::new();
    for batch in batches {
        let take = left.min(batch.num_rows());
        kept.push(batch.slice(0, take));
        left -= take;
    }
    kept
}

/// FITNESS: a writer fault stops one table, and `run.json` names it in `stopped_tables`: the run
/// holds that table's rows up to the stop, and its re-fold, whose writer did not fault, holds each
/// row. The re-fold check takes the rows past the stop of a stopped table as no difference, and the
/// same rows of a table that no fault stopped as a breach.
#[test]
fn refold_check_takes_rows_past_the_stop_of_a_stopped_table() {
    let mut changes = BINANCE_CHANGES.to_vec();
    changes.push(NO_PARAMS);
    for (stopped, case) in [
        (Stopped::Named, "stopped"),
        (Stopped::NotNamed, "not-stopped"),
    ] {
        let root = TempRoot::new(case);
        let text = config_text(BINANCE_CONFIG, root.path(), &changes);
        let run = record::<TouchQuoter>(plan(&text, binance_tape()));
        let replay = refold_of::<TouchQuoter>(&run, &text);
        stop_table_halfway(&run.run_dir, "mids", stopped);

        let verdict = check_refold(&run.run_dir, &replay.run_dir);

        match stopped {
            Stopped::Named => {
                if let Err(diff) = verdict {
                    panic!("{case}: the rows past the stop of mids are no difference: {diff}");
                }
            }
            Stopped::NotNamed => assert!(
                matches!(verdict, Err(RefoldDiff::OnlyInReplay { table: "mids", .. })),
                "{case}: rows of mids that only the re-fold holds, with no stop and no drops, are \
                 a breach, got {verdict:?}"
            ),
        }
    }
}
