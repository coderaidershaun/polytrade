//! A re-fold refuses a tape that cannot give back its run, before it folds a message and before it
//! writes a folder: a tape with a gap, a tape of another layout, or a config of another digest.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use engine::msg::failure::{FailureClass, Halt};
use engine::runtime::FoldRings;
use engine::runtime::config::{Config, NoParams};
use engine::runtime::manifest::InstrumentTerms;
use engine::tape::TAPE_KINDS;
use lab::backtest::{RefoldPlan, ReplayError, refold};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

use crate::fixtures::{
    BINANCE_CHANGES, BINANCE_CONFIG, NO_PARAMS, TouchQuoter, binance_tape, config_text,
};
use crate::runs::{DropReport, Pumping, RunPlan, TempRoot, record};

const ONE: i64 = 100_000_000;

fn terms(terms: &mut InstrumentTerms) {
    terms.tick_size = Some(ONE / 100);
    terms.lot_size = Some(1_000);
    terms.min_qty = Some(1_000);
    terms.min_notional = Some(5 * ONE);
    terms.max_num_orders = Some(200);
    terms.max_num_order_amends = Some(10);
}

/// A run of the probe that does not trade, its tape ring `tape_ring` slots deep, its writer pumped
/// after each `pumped_every` steps.
fn run(root: &Path, tape_ring: usize, pumped_every: usize) -> (PathBuf, String) {
    let mut changes = BINANCE_CHANGES.to_vec();
    changes.push(NO_PARAMS);
    let text = config_text(BINANCE_CONFIG, root, &changes);
    let run = record::<TouchQuoter>(RunPlan {
        config_text: &text,
        tape: binance_tape(),
        run_nonce: None,
        exposures: Vec::new(),
        rings: FoldRings {
            tape: tape_ring,
            ..FoldRings::SHIPPED
        },
        pumping: Pumping::EveryNth(pumped_every),
        drops: DropReport::AsCounted,
        terms,
    });
    (run.run_dir, text)
}

fn try_refold(root: &Path, source_run: &Path, config_text: &str) -> Result<(), ReplayError> {
    refold::<TouchQuoter>(RefoldPlan {
        source_run: source_run.to_path_buf(),
        config: Config::<NoParams>::from_yaml(config_text).expect("the config parses"),
        config_text: config_text.to_owned(),
        out_root: root.to_path_buf(),
    })
    .map(|_| ())
}

fn assert_no_replay_folder(root: &Path) {
    let replay = root
        .join("strat-micro-recorder")
        .join("te-refold")
        .join("replay");
    assert!(
        !replay.exists(),
        "a refused re-fold writes no folder, and {} exists",
        replay.display()
    );
}

/// CONTRACT (X2): a tape ring of 8 that the writer does not drain for 12 messages loses seq 9 to
/// 12, and the tape holds one gap row for them. A re-fold refuses that tape before its first fold,
/// names the lost seqs, and writes no folder: a re-fold that crossed the gap would fold a book that
/// the run never had.
#[test]
fn replay_refuses_to_cross_a_gap() {
    let root = TempRoot::new("gap");
    let (source_run, text) = run(root.path(), 8, 12);
    let refused = try_refold(root.path(), &source_run, &text);
    let Err(error) = refused else {
        panic!("the re-fold crossed the gap of {}", source_run.display());
    };
    assert!(
        matches!(
            error,
            ReplayError::Gap {
                first_lost_seq: 9,
                last_lost_seq: 12
            }
        ),
        "the refusal names the lost seqs, got {error:?}"
    );
    assert_eq!(error.failure_class(), FailureClass::Halt(Halt::Start));
    assert!(
        error.to_string().contains("seq 9 to 12"),
        "the text names the gap: {error}"
    );
    assert_no_replay_folder(root.path());
}

/// Rewrites the footer of `file`, giving tape kind 1 the digest `digest`.
fn give_trade_digest(file: &Path, digest: &str) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(file).expect("open"))
        .expect("a footer");
    let schema = Arc::clone(builder.schema());
    let mut footer: Vec<KeyValue> = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned()
        .unwrap_or_default();
    let batches: Vec<_> = builder
        .build()
        .expect("a reader")
        .map(|batch| batch.expect("a batch"))
        .collect();
    let kinds = footer
        .iter_mut()
        .find(|pair| pair.key == "tape_kinds")
        .expect("a tape file names its kinds");
    let mut entries: Vec<serde_json::Value> =
        serde_json::from_str(kinds.value.as_deref().expect("a value")).expect("json");
    for entry in &mut entries {
        if entry["kind"] == 1 {
            entry["digest"] = serde_json::Value::from(digest);
        }
    }
    kinds.value = Some(serde_json::to_string(&entries).expect("json"));
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(footer))
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(file).expect("create"),
        schema,
        Some(properties),
    )
    .expect("a writer");
    for batch in &batches {
        writer.write(batch).expect("write");
    }
    writer.close().expect("close");
}

fn first_tape_file(run: &Path) -> PathBuf {
    let date = std::fs::read_dir(run.join("tape"))
        .expect("a tape folder")
        .next()
        .expect("a date folder")
        .expect("an entry")
        .path();
    let mut files: Vec<PathBuf> = std::fs::read_dir(date)
        .expect("files")
        .map(|entry| entry.expect("an entry").path())
        .collect();
    files.sort();
    files.remove(0)
}

/// CONTRACT (F11): a tape whose footer gives a kind another digest than this build's was written by
/// another layout, and the re-fold rejects it whole, naming both digests and the file. A config of
/// another digest than the run's is rejected too, naming both. Neither folds a message nor writes a
/// folder: a tape read by the wrong layout decodes to other messages with no error.
#[test]
fn replay_refuses_other_layout() {
    let root = TempRoot::new("layout");
    let (source_run, text) = run(root.path(), FoldRings::SHIPPED.tape, 1);
    let file = first_tape_file(&source_run);
    give_trade_digest(&file, "0x0000000000000001");

    let error = try_refold(root.path(), &source_run, &text).expect_err("the layout differs");
    let expected = TAPE_KINDS[0].digest;
    match &error {
        ReplayError::LayoutMismatch {
            kind: 1,
            name,
            found: 0x1,
            expected: built,
            file: named,
        } if &**name == "trade" && *built == expected && *named == file => {}
        other => {
            panic!("the refusal names kind 1 `trade`, both digests and the file, got {other:?}")
        }
    }
    assert_eq!(error.failure_class(), FailureClass::Reject);
    assert_no_replay_folder(root.path());

    let other_config = format!("{text}\n# another config\n");
    let error =
        try_refold(root.path(), &source_run, &other_config).expect_err("the config differs");
    assert!(
        matches!(error, ReplayError::ConfigMismatch { found, expected } if found != expected),
        "the refusal names both config digests, got {error:?}"
    );
    assert_eq!(error.failure_class(), FailureClass::Reject);
    assert_no_replay_folder(root.path());
}

/// CONTRACT (R18): a source run whose `run.json` still reads `open` may still be writing its tape,
/// so a file sealed after the check would be folded unchecked. The re-fold refuses it before its
/// first fold and writes no folder.
#[test]
fn replay_refuses_a_source_run_still_open() {
    let root = TempRoot::new("open");
    let (source_run, text) = run(root.path(), FoldRings::SHIPPED.tape, 1);
    let manifest_path = source_run.join("run.json");
    let mut manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).expect("run.json"))
            .expect("json");
    manifest["end_state"] = serde_json::Value::from("open");
    std::fs::write(&manifest_path, manifest.to_string()).expect("run.json is writable");

    let refused = try_refold(root.path(), &source_run, &text);

    let Err(error) = refused else {
        panic!(
            "the re-fold folded {}, whose run.json still reads open",
            source_run.display()
        );
    };
    assert!(
        error.to_string().contains("still open"),
        "the refusal says why: {error}"
    );
    assert_eq!(error.failure_class(), FailureClass::Halt(Halt::Start));
    assert_no_replay_folder(root.path());
}
