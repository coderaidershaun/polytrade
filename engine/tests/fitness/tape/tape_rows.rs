//! The tape table: what the tee of the hot driver writes, read back row by row. A replay folds
//! these rows, so a row that is missing, out of order, or decodes to another message gives a
//! re-fold that differs from its run with no error anywhere.

use std::path::Path;
use std::time::Duration;

use proptest::prelude::*;
use rtrb::{Consumer, RingBuffer};

use engine::hot::book::{Book, SnapshotOutcome};
use engine::hot::ingress::IngressQueues;
use engine::hot::sink::TapeSink;
use engine::hot::spawn::{HotDriver, Step};
use engine::msg::exec::ExecKind;
use engine::msg::ids::{
    AggregateTradeId, AssetId, ClientOrderId, InstrumentId, Qty, QueueId, RawTradeId, Side,
    StreamEpoch, VenueId,
};
use engine::msg::inbound::{
    AuditBankFull, FileWriter, InboundMessage, MessageBody, TradeEvent, WriterFailure,
};
use engine::msg::link::{LinkFrame, LinkHash, LinkOrigin, LinkPayload, TopicId};
use engine::msg::persist::{BootExposure, BootRecord, BootVenue, TapeGap, TapeRecord};
use engine::msg::time::LocalTs;
use engine::runtime::config::{KlineInterval, RecordedTables, TableKind, TrackerSpec};
use engine::tape::{
    GapSpan, PersistConfig, PersistWriter, RunMeta, TapeInput, TapePayload, decode_payload,
};

use crate::engine_support::{
    Idle, ONE, account_chunk, book_reset, delta_chunk, engine_without_warmup, exec_event,
    instrument_row, kline, persist_ring_for, rotation, run_control, running_at, snapshot_pair,
    spin, strategy_log_ring, trade, ts, venue_ts,
};
use crate::parquet_readback::{
    BASE_TS_US, Cell, TempDir, drain_blocking, read_parquet_file, run_meta, table_files,
};

pub(crate) const BOOT_KIND: u16 = 256;
pub(crate) const SNAPSHOT_KIND: u16 = 257;
pub(crate) const GAP_KIND: u16 = 258;

/// The kinds of message, in the order of their tape kind ids 1 to 12.
pub(crate) const MESSAGE_KINDS: u8 = 12;

/// One row of a tape file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TapeCells {
    pub(crate) seq: u64,
    pub(crate) kind: u16,
    pub(crate) queue: Option<u8>,
    pub(crate) received_us: i64,
    pub(crate) queued_us: i64,
    pub(crate) payload: Vec<u8>,
}

/// The rows of each tape file below `dir`, file by file in seal order. The boot row takes the
/// boot stamp and the test messages a stamp of long ago, so the date folders of two files of one
/// run do not sort in seal order: the sequence number at the end of each name does.
pub(crate) fn tape_files(dir: &Path) -> Vec<Vec<TapeCells>> {
    let mut paths = table_files(dir, "tape");
    paths.sort_by_key(|path| {
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.rsplit('-').next())
            .map(str::to_owned)
    });
    paths
        .iter()
        .map(|path| {
            read_parquet_file(path)
                .rows
                .into_iter()
                .map(|row| match row.as_slice() {
                    [
                        Cell::U64(seq),
                        Cell::U16(kind),
                        queue,
                        Cell::I64(received_us),
                        Cell::I64(queued_us),
                        Cell::Bytes(payload),
                    ] => TapeCells {
                        seq: *seq,
                        kind: *kind,
                        queue: match queue {
                            Cell::U8(queue) => Some(*queue),
                            Cell::Null => None,
                            other => panic!("queue_id is a nullable u8, got {other:?}"),
                        },
                        received_us: *received_us,
                        queued_us: *queued_us,
                        payload: payload.clone(),
                    },
                    other => panic!(
                        "{}: a tape row of unexpected cells {other:?}",
                        path.display()
                    ),
                })
                .collect()
        })
        .collect()
}

/// A message of kind `kind` (0 to 11, in the order of the tape kind ids) that `n` varies.
pub(crate) fn sample(kind: u8, n: u64, when: i64) -> InboundMessage {
    let small = n as i64;
    let body = match kind {
        0 => MessageBody::Trade(TradeEvent {
            aggregate_id: AggregateTradeId(1_000 + n),
            first_trade_id: RawTradeId(5_000 + n),
            last_trade_id: RawTradeId(5_001 + n),
            stream_epoch: StreamEpoch::default(),
            exchange_sent_ts_us: Some(venue_ts(when - 2)),
            ..trade(0, 100 * ONE + small, ONE / 10, Side::Sell, when - 5)
        }),
        1 => MessageBody::Book(delta_chunk(
            0,
            Side::Buy,
            &[(99 * ONE - small, ONE), (98 * ONE, 2 * ONE)],
            when - 4,
        )),
        2 => return book_reset(0, when),
        3 => return rotation(0, when + 1_000, when + 301_000, when),
        4 => MessageBody::Kline(kline(
            0,
            KlineInterval::FiveMinutes,
            (100 * ONE, 101 * ONE, 99 * ONE, 100 * ONE + small),
            n.is_multiple_of(2),
            when - 3,
        )),
        5 => return spin(n, when),
        6 => MessageBody::Link(LinkFrame {
            origin: LinkOrigin {
                sender_te_hash: LinkHash(0x00c0_ffee),
                boot_ts_us: venue_ts(1_000),
                topic: TopicId(16),
                seq: n,
            },
            payload: LinkPayload::new(LinkHash(0x5eed), venue_ts(when - 1), &[1.5, small as f64]),
        }),
        7 => return run_control(running_at(n + 1), when),
        8 => MessageBody::Exec(engine::msg::exec::ExecEvent {
            kind: ExecKind::ReportNew,
            qty: Qty::from_mantissa(ONE),
            ..exec_event(
                InstrumentId(0),
                ClientOrderId(n),
                Side::Buy,
                100 * ONE,
                when - 6,
            )
        }),
        9 => {
            let mut chunk = account_chunk(&[(AssetId(0), small * ONE), (AssetId(1), ONE)], when);
            chunk.venue = VenueId(0);
            MessageBody::Account(chunk)
        }
        10 => MessageBody::WriterFailed(WriterFailure {
            writer: FileWriter::Exposure,
            consecutive_failures: n as u32,
        }),
        11 => MessageBody::AuditBankFull(AuditBankFull {
            banked: n as u32,
            capacity: 65_536,
        }),
        other => panic!("no message kind {other}"),
    };
    InboundMessage::new(ts(when), body)
}

/// The writer of one run with a tape, its boot row `boot`, reading the tape ring `records`.
pub(crate) fn tape_writer(
    dir: &Path,
    meta: RunMeta,
    records: Consumer<TapeRecord>,
    boot: BootRecord,
) -> engine::tape::PersistHandle {
    let tables = RecordedTables::new(&[TableKind::Trades]);
    let (_wiring, rings) = persist_ring_for(8, tables);
    PersistWriter::on_disk()
        .recording_tape(TapeInput { records, boot })
        .spawn(
            PersistConfig {
                dir: dir.to_path_buf(),
                tables,
                retention: Duration::from_hours(6),
            },
            meta,
            rings.into_writer_inputs(),
        )
}

/// Folds each message through a hot driver with the tee on, three queues deep, while a writer
/// thread drains the tape, and returns the boot stamp of the run.
fn fold_through_driver(dir: &Path, messages: &[(usize, InboundMessage)]) -> i64 {
    let (tape_producer, tape_records) = RingBuffer::<TapeRecord>::new(messages.len() + 1);
    let meta = run_meta();
    let boot_us = meta.run_id.micros();
    let writer = tape_writer(dir, meta, tape_records, BootRecord::default());
    let tables = RecordedTables::new(&[TableKind::Trades]);
    let (persistence, _rings) = persist_ring_for(messages.len() * 4 + 16, tables);
    let engine = engine_without_warmup(
        &[instrument_row(0, TrackerSpec::default(), 64)],
        Box::new(Idle),
        persistence,
        strategy_log_ring(64).0,
    );
    let (mut producers, consumers): (Vec<_>, Vec<_>) = (0..3)
        .map(|_| RingBuffer::<InboundMessage>::new(messages.len() + 1))
        .unzip();
    for (queue, message) in messages {
        producers[*queue]
            .push(*message)
            .expect("each queue has room for the sequence");
    }
    let mut driver = HotDriver::new(IngressQueues::new(consumers), engine, None)
        .with_tape(TapeSink::new(tape_producer));
    while driver.step() == Step::Folded {}
    drop(driver);
    drain_blocking(writer).expect("the writer seals the tape");
    boot_us
}

proptest! {
    // Each case writes and reads a Parquet file, so fewer cases than a pure property test.
    #![proptest_config(ProptestConfig {
        cases: if std::env::var("CI_NIGHTLY").is_ok() { 256 } else { 24 },
        ..ProptestConfig::default()
    })]

    /// FITNESS: the tape holds each message that the driver popped, in pop order, with the queue
    /// it came from, and each payload decodes to its message bit for bit. A re-fold of a tape with
    /// a hole, a reordered row or a payload that decodes to another value folds another run.
    #[test]
    fn tape_holds_each_consumed_message_in_order(
        steps in prop::collection::vec((0..3usize, 0..MESSAGE_KINDS, 0..1_000u64), 1..240),
    ) {
        let dir = TempDir::new("tape-order");
        let messages: Vec<(usize, InboundMessage)> = steps
            .iter()
            .enumerate()
            .map(|(index, &(queue, kind, n))| {
                (queue, sample(kind, n, BASE_TS_US + index as i64 * 10))
            })
            .collect();
        let boot_us = fold_through_driver(dir.path(), &messages);

        let files = tape_files(dir.path());
        prop_assert_eq!(files.len(), 1, "no seal border, so one tape file");
        let rows = &files[0];
        prop_assert_eq!(rows.len(), messages.len() + 1, "the boot row and one row for each pop");
        let boot = &rows[0];
        prop_assert_eq!(
            (boot.seq, boot.kind, boot.queue, boot.received_us),
            (0, BOOT_KIND, None, boot_us),
            "the boot row comes first"
        );
        for (index, ((queue, message), row)) in messages.iter().zip(&rows[1..]).enumerate() {
            let kind = message_kind(message);
            prop_assert_eq!(
                (row.seq, row.kind, row.queue, row.received_us, row.queued_us),
                (
                    index as u64 + 1,
                    kind,
                    Some(*queue as u8),
                    message.header.received_ts_us.micros(),
                    message.header.queued_ts_us.micros(),
                ),
                "row {} is the pop of message {}", index + 1, index
            );
            let decoded = decode_payload(row.kind, &row.payload)
                .map_err(|error| TestCaseError::fail(format!("row {}: {error}", index + 1)))?;
            prop_assert_eq!(decoded, TapePayload::Message(message.body), "row {}", index + 1);
        }
    }
}

pub(crate) fn message_kind(message: &InboundMessage) -> u16 {
    match message.body {
        MessageBody::Trade(_) => 1,
        MessageBody::Book(_) => 2,
        MessageBody::BookReset(_) => 3,
        MessageBody::MarketRotation(_) => 4,
        MessageBody::Kline(_) => 5,
        MessageBody::SpinTick(_) => 6,
        MessageBody::Link(_) => 7,
        MessageBody::RunControl(_) => 8,
        MessageBody::Exec(_) => 9,
        MessageBody::Account(_) => 10,
        MessageBody::WriterFailed(_) => 11,
        MessageBody::AuditBankFull(_) => 12,
    }
}

/// CONTRACT: the first row of each tape is the boot row: sequence number 0, no queue, the boot
/// stamp twice, and the start state of the fold that no message carries. A re-fold that started
/// from no boot row would fold a run with no position and the wrong nonce.
#[test]
fn tape_starts_with_boot_record() {
    let dir = TempDir::new("tape-boot");
    let boot = BootRecord {
        venues: vec![BootVenue {
            venue: VenueId(0),
            run_nonce: 41,
        }],
        exposures: vec![BootExposure {
            instrument: InstrumentId(0),
            position_base: Qty::from_mantissa(3 * ONE),
            cash_quote: -300 * ONE,
            basis_quote: -290 * ONE,
        }],
    };
    let (mut producer, records) = RingBuffer::<TapeRecord>::new(4);
    let message = sample(0, 1, BASE_TS_US);
    producer
        .push(TapeRecord::Message {
            seq: 1,
            queue: QueueId::new(0).expect("queue 0"),
            message,
        })
        .expect("room");
    let meta = run_meta();
    let boot_us = meta.run_id.micros();
    let writer = tape_writer(dir.path(), meta, records, boot.clone());
    drop(producer);
    drain_blocking(writer).expect("the writer seals the tape");

    let files = tape_files(dir.path());
    let first = files
        .first()
        .and_then(|rows| rows.first())
        .expect("a tape row");
    assert_eq!(
        (
            first.seq,
            first.kind,
            first.queue,
            first.received_us,
            first.queued_us
        ),
        (0, BOOT_KIND, None, boot_us, boot_us),
        "the boot row opens the tape, stamped with the boot stamp"
    );
    assert_eq!(
        decode_payload(first.kind, &first.payload),
        Ok(TapePayload::Boot(boot)),
        "the boot row holds the nonce of each venue and each restored position"
    );
    assert_eq!(files[0][1].seq, 1, "the first record of the tee follows it");
}

/// One book of three levels on each side.
fn three_level_book() -> Book {
    let mut book = Book::new(16);
    let (bids, asks) = snapshot_pair(
        0,
        &[(99 * ONE, ONE), (98 * ONE, ONE), (97 * ONE, ONE)],
        &[(101 * ONE, ONE), (102 * ONE, ONE), (103 * ONE, ONE)],
        BASE_TS_US,
    );
    for chunk in [bids, asks] {
        assert_eq!(book.apply_snapshot_chunk(&chunk), SnapshotOutcome::Clean);
    }
    book
}

fn record_seq(record: &TapeRecord) -> Option<u64> {
    match record {
        TapeRecord::Message { seq, .. } | TapeRecord::BookSnapshot { seq, .. } => Some(*seq),
        TapeRecord::Gap(_) | TapeRecord::Seal => None,
    }
}

/// CONTRACT: a full tape ring loses records and latches one gap. Each lost record keeps its
/// sequence number; when the ring has room again, the gap row, a seal and a snapshot of each book
/// go ahead of the next record, so the file after the gap opens with whole books. A tape that
/// skipped the gap row would let a replay fold straight across the hole.
#[test]
fn tape_ring_full_latches_one_gap_record() {
    let books = [three_level_book()];
    let queue = QueueId::new(0).expect("queue 0");
    let (producer, mut ring) = RingBuffer::<TapeRecord>::new(8);
    let mut sink = TapeSink::new(producer);
    let at = |index: u64| BASE_TS_US + index as i64 * 1_000;
    for index in 1..=12 {
        sink.record(queue, &sample(0, index, at(index)), &books);
    }
    assert_eq!(
        (sink.lost(), sink.gaps()),
        (4, 1),
        "records 9 to 12 are lost in one gap"
    );
    let mut records: Vec<TapeRecord> = std::iter::from_fn(|| ring.pop().ok()).collect();
    assert_eq!(
        records.iter().map(record_seq).collect::<Vec<_>>(),
        (1..=8).map(Some).collect::<Vec<_>>(),
        "the ring holds records 1 to 8"
    );

    sink.record(queue, &sample(0, 13, at(13)), &books);
    let after: Vec<TapeRecord> = std::iter::from_fn(|| ring.pop().ok()).collect();
    let [
        TapeRecord::Gap(gap),
        TapeRecord::Seal,
        TapeRecord::BookSnapshot {
            seq: 13,
            chunk: bids,
            ..
        },
        TapeRecord::BookSnapshot {
            seq: 14,
            chunk: asks,
            ..
        },
        TapeRecord::Message { seq: 15, .. },
    ] = after.as_slice()
    else {
        panic!("the gap, a seal, the snapshot and the record follow the room: {after:?}");
    };
    assert_eq!(
        *gap,
        TapeGap {
            first_lost_seq: 9,
            last_lost_seq: 12,
            received_ts_us: LocalTs::from_micros(at(9)),
        }
    );
    assert_eq!(
        (
            bids.side,
            bids.active_levels().len(),
            asks.side,
            asks.is_last_chunk
        ),
        (Side::Buy, 3, Side::Sell, true),
        "one chunk for each side of the book"
    );
    assert_eq!((sink.lost(), sink.gaps()), (4, 1), "the gap is one gap");

    records.extend(after);
    let dir = TempDir::new("tape-gap");
    let (mut producer, tape) = RingBuffer::<TapeRecord>::new(records.len());
    for record in records {
        producer.push(record).expect("room");
    }
    let writer = tape_writer(dir.path(), run_meta(), tape, BootRecord::default());
    drop(producer);
    drain_blocking(writer).expect("the writer seals the tape");
    let files: Vec<Vec<(u64, u16)>> = tape_files(dir.path())
        .iter()
        .map(|rows| rows.iter().map(|row| (row.seq, row.kind)).collect())
        .collect();
    let mut before_gap = vec![(0, BOOT_KIND)];
    before_gap.extend((1..=8).map(|seq| (seq, 1)));
    before_gap.push((9, GAP_KIND));
    assert_eq!(
        files,
        vec![
            before_gap,
            vec![(13, SNAPSHOT_KIND), (14, SNAPSHOT_KIND), (15, 1)]
        ],
        "the file before the gap ends with the gap row; the file after opens with the books"
    );
    let gap_row = &tape_files(dir.path())[0][9];
    assert_eq!(
        decode_payload(gap_row.kind, &gap_row.payload),
        Ok(TapePayload::Gap(GapSpan {
            first_lost_seq: 9,
            last_lost_seq: 12,
        })),
    );
}
