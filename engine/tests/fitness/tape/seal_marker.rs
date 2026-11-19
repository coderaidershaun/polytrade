//! A tape file ends where message time says, so a replay of a run cuts its files at the same rows
//! as the run did.

use std::num::NonZeroU64;
use std::path::Path;
use std::time::Duration;

use rtrb::RingBuffer;

use engine::hot::ingress::IngressQueues;
use engine::hot::sink::TapeSink;
use engine::hot::spawn::{HotDriver, Step};
use engine::hot::strategy::StrategyConfig;
use engine::msg::ids::Side;
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::persist::{BootRecord, TapeRecord};
use engine::runtime::config::{RecordedTables, TableKind, TrackerSpec};
use engine::tape::{PersistConfig, PersistWriter, TapeInput};

use crate::engine_support::{
    ONE, engine_view, engine_without_warmup, instrument_row, recorder_spec, sealing_persist_rings,
    spin, strategy_log_ring, trade, ts,
};
use crate::parquet_readback::{
    BASE_TS_US, Cell, TempDir, drain_blocking, read_parquet_file, run_meta, table_files,
};
use crate::raw_recorder::RecorderStrategy;
use crate::tape::tape_rows::{TapeCells, tape_files};

const SPIN_US: i64 = 1_000_000;
const SEAL_INTERVAL_SPINS: u64 = 300;
const SPINS: u64 = 1_000;

/// One spin each second and one trade half a second after it, all inside one hour, so no file
/// ends at the hour cross.
fn inputs(skipped: &[u64]) -> Vec<InboundMessage> {
    let mut messages = Vec::new();
    for seq in 0..SPINS {
        let at = BASE_TS_US + seq as i64 * SPIN_US;
        if !skipped.contains(&seq) {
            messages.push(spin(seq, at));
        }
        let when = at + SPIN_US / 2;
        let event = trade(0, 100 * ONE + seq as i64, ONE, Side::Buy, when);
        messages.push(InboundMessage::new(ts(when), MessageBody::Trade(event)));
    }
    messages
}

/// Each file of the trades table: its name with the boot stamp of its writer masked, and its rows.
type TapeFiles = Vec<(String, Vec<Vec<Cell>>)>;

/// Folds the inputs through a driver with the tee on into a writer thread that drains the rings
/// while the fold runs, as a live run does, and reads back each file of the trades table.
fn fold_with_writer(inputs: &[InboundMessage], dir: &Path) -> TapeFiles {
    let tables = RecordedTables::new(&[TableKind::Trades]);
    let interval = NonZeroU64::new(SEAL_INTERVAL_SPINS).expect("not zero");
    let (persistence, rings) = sealing_persist_rings(inputs.len() * 2, tables, interval);
    let (tape_producer, tape_records) = RingBuffer::<TapeRecord>::new(inputs.len() * 2);
    let writer = PersistWriter::on_disk()
        .recording_tape(TapeInput {
            records: tape_records,
            boot: BootRecord::default(),
        })
        .spawn(
            PersistConfig {
                dir: dir.to_path_buf(),
                tables,
                retention: Duration::from_hours(6),
            },
            run_meta(),
            rings.into_writer_inputs(),
        );
    let strategy = RecorderStrategy::from_spec(
        &recorder_spec(vec![TableKind::Trades]),
        engine_view(engine::msg::time::DurationUs::from_micros(SPIN_US)),
    );
    let engine = engine_without_warmup(
        &[instrument_row(0, TrackerSpec::default(), 64)],
        Box::new(strategy),
        persistence,
        strategy_log_ring(64).0,
    );
    let (mut queue, consumer) = RingBuffer::<InboundMessage>::new(inputs.len() + 1);
    for message in inputs {
        queue
            .push(*message)
            .expect("the queue has room for the inputs");
    }
    let mut driver = HotDriver::new(IngressQueues::new(vec![consumer]), engine, None)
        .with_tape(TapeSink::new(tape_producer));
    while driver.step() == Step::Folded {}
    assert_eq!(
        driver.engine().dropped_research_records().total(),
        0,
        "the ring had room: {:?}",
        driver.engine().dropped_research_records()
    );
    drop(driver);
    drain_blocking(writer).expect("the writer seals each file");
    table_files(dir, "trades")
        .iter()
        .map(|path| (masked_name(path), read_parquet_file(path).rows))
        .collect()
}

/// A name is `{hour}-{boot stamp}-{sequence}.parquet`. The boot stamp is the wall clock at the
/// start of the writer, which keeps two runs from writing over each other's files.
fn masked_name(path: &Path) -> String {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("a file name");
    let parts: Vec<&str> = name.split('-').collect();
    let [hour, _boot_stamp, sequence] = parts.as_slice() else {
        panic!("a tape file name has three parts: {name}");
    };
    format!("{hour}-*-{sequence}")
}

/// The seconds after the first spin of the trades in a file, from the first to the last.
fn trade_seconds(rows: &[Vec<Cell>]) -> (i64, i64) {
    let second = |row: &Vec<Cell>| match row[1] {
        Cell::I64(received_ts_us) => (received_ts_us - BASE_TS_US) / SPIN_US,
        ref other => panic!("received_ts_us is an i64, got {other:?}"),
    };
    let first = rows.first().map(second).expect("a file holds rows");
    let last = rows.last().map(second).expect("a file holds rows");
    (first, last)
}

/// FITNESS: a seal border comes from message time only. A run folds 1,000 spins with a writer
/// thread, and the fold seals each open file at slots 300, 600 and 900. A second fold of the same
/// inputs gives files with the same names and the same rows. A seal paced by the clock of the
/// writer cuts a replay at other rows than the run, so the files of the two never line up.
#[test]
fn run_and_second_run_seal_at_equal_borders() {
    let recorded = inputs(&[]);
    let (first, second) = (TempDir::new("seal-run"), TempDir::new("seal-replay"));
    let run = fold_with_writer(&recorded, first.path());
    let replay = fold_with_writer(&recorded, second.path());

    let borders: Vec<(i64, i64)> = run.iter().map(|(_, rows)| trade_seconds(rows)).collect();
    assert_eq!(
        borders,
        vec![(0, 299), (300, 599), (600, 899), (900, 999)],
        "the fold seals each open file at slots 300, 600 and 900, ahead of the rows of that slot"
    );
    assert_eq!(
        run, replay,
        "the replay gives the files of the run, name for name and row for row"
    );
    tape_seals_at_the_borders_of_the_fold(first.path(), second.path());

    // A late timer skips slot 600. The next spin crosses the border, so the seal goes there.
    let late = TempDir::new("seal-late");
    let skipped = fold_with_writer(&inputs(&[600]), late.path());
    let borders: Vec<(i64, i64)> = skipped
        .iter()
        .map(|(_, rows)| trade_seconds(rows))
        .collect();
    assert_eq!(
        borders,
        vec![(0, 299), (300, 600), (601, 899), (900, 999)],
        "a skipped slot moves the border to the next spin, and no border is lost"
    );
}

/// The tape seals at the spins where the fold seals: each file ends before spin 300, 600 or 900,
/// and each later file opens with a snapshot of the book, then that spin. The rows after the boot
/// row are the same in a second fold.
fn tape_seals_at_the_borders_of_the_fold(run: &Path, replay: &Path) {
    let run = tape_files(run);
    let replay = tape_files(replay);
    let spin_seq =
        |row: &TapeCells| u64::from_le_bytes(row.payload[..8].try_into().expect("8 bytes"));
    let opening: Vec<(u16, u16, u16, u64)> = run[1..]
        .iter()
        .map(|rows| (rows[0].kind, rows[1].kind, rows[2].kind, spin_seq(&rows[2])))
        .collect();
    assert_eq!(
        (run.len(), opening),
        (
            4,
            vec![(257, 257, 6, 300), (257, 257, 6, 600), (257, 257, 6, 900)]
        ),
        "each tape file after the first opens with the bid and ask snapshot of the book, then the \
         spin of its border"
    );
    for rows in &run[..3] {
        let last = rows.last().expect("a tape file holds rows");
        assert_eq!(
            last.kind, 1,
            "a tape file ends with the trade before its border spin"
        );
    }
    assert_eq!(
        run.concat()[1..],
        replay.concat()[1..],
        "a second fold writes the same tape rows after its boot row"
    );
}
