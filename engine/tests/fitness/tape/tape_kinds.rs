//! The kinds of tape row that recorded tapes hold. A tape outlives the build that wrote it, so each
//! kind keeps its id, its digest and the bytes of its payload, and a new kind only appends.

use std::fmt::Write as _;

use rtrb::RingBuffer;

use engine::msg::ids::{QueueId, VenueId};
use engine::msg::inbound::InboundMessage;
use engine::msg::persist::{BootRecord, BootVenue, TapeGap, TapeRecord};
use engine::tape::{GapSpan, TAPE_KINDS, TapePayload, decode_payload};

use crate::engine_support::{ONE, snapshot_pair, ts};
use crate::parquet_readback::{BASE_TS_US, TempDir, drain_blocking, run_meta};
use crate::tape::tape_rows::{
    BOOT_KIND, GAP_KIND, MESSAGE_KINDS, SNAPSHOT_KIND, message_kind, sample, tape_files,
    tape_writer,
};

/// The kinds of the build as recorded tapes know them, and a payload of each that a tape of that
/// build holds. A line is appended, never changed.
const KINDS_FILE: &str = "tests/fitness/tape/tape_kinds.txt";
const BLESS_VARIABLE: &str = "POLYTRADE_BLESS_TAPE_KINDS";

/// The sample payload of each kind, as the test builds it today.
fn kind_samples() -> Vec<(u16, TapePayload)> {
    let mut samples: Vec<(u16, TapePayload)> = (0..MESSAGE_KINDS)
        .map(|kind| {
            let message = sample(kind, 3, BASE_TS_US);
            (message_kind(&message), TapePayload::Message(message.body))
        })
        .collect();
    let (bids, _) = snapshot_pair(0, &[(99 * ONE, ONE)], &[], BASE_TS_US);
    samples.push((
        BOOT_KIND,
        TapePayload::Boot(BootRecord {
            venues: vec![BootVenue {
                venue: VenueId(0),
                run_nonce: 7,
            }],
            exposures: Vec::new(),
        }),
    ));
    samples.push((SNAPSHOT_KIND, TapePayload::BookSnapshot(bids)));
    samples.push((
        GAP_KIND,
        TapePayload::Gap(GapSpan {
            first_lost_seq: 4,
            last_lost_seq: 6,
        }),
    ));
    samples
}

/// The payload bytes of each sample, as the writer of this build writes them.
fn written_samples(samples: &[(u16, TapePayload)]) -> Vec<(u16, Vec<u8>)> {
    let dir = TempDir::new("tape-kinds");
    let (mut producer, tape) = RingBuffer::<TapeRecord>::new(samples.len() + 1);
    let queue = QueueId::new(0).expect("queue 0");
    let mut boot = None;
    for (seq, (_, payload)) in samples.iter().enumerate() {
        let seq = seq as u64 + 1;
        let record = match payload {
            TapePayload::Message(body) => TapeRecord::Message {
                seq,
                queue,
                message: InboundMessage::new(ts(BASE_TS_US), *body),
            },
            TapePayload::BookSnapshot(chunk) => TapeRecord::BookSnapshot {
                seq,
                received_ts_us: ts(BASE_TS_US),
                chunk: *chunk,
            },
            TapePayload::Gap(span) => TapeRecord::Gap(TapeGap {
                first_lost_seq: span.first_lost_seq,
                last_lost_seq: span.last_lost_seq,
                received_ts_us: ts(BASE_TS_US),
            }),
            TapePayload::Boot(record) => {
                boot = Some(record.clone());
                continue;
            }
        };
        producer.push(record).expect("room");
    }
    let boot = boot.expect("a boot sample");
    let writer = tape_writer(dir.path(), run_meta(), tape, boot);
    drop(producer);
    drain_blocking(writer).expect("the writer seals the tape");
    tape_files(dir.path())
        .concat()
        .into_iter()
        .map(|row| (row.kind, row.payload))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex digits"))
        .collect()
}

/// FITNESS: a tape recorded by an earlier build stays readable. Each kind that the kinds file
/// records keeps its id, its name and its digest, and its recorded payload decodes to the same
/// value, while a new kind only appends a line. A changed layout under an old id would make each
/// recorded tape either refused or, worse, misread.
#[test]
fn old_tape_stays_readable_after_new_variant() {
    let text = std::fs::read_to_string(KINDS_FILE)
        .unwrap_or_else(|error| panic!("{KINDS_FILE}: {error}. Run with {BLESS_VARIABLE}=1"));
    let samples = kind_samples();
    let mut recorded = Vec::new();
    for line in text
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
    {
        let [id, name, digest, payload] = line.split(' ').collect::<Vec<_>>()[..] else {
            panic!("{KINDS_FILE}: {line:?} is not `<id> <name> <digest> <payload hex>`");
        };
        let id: u16 = id.parse().expect("a kind id");
        let build = TAPE_KINDS
            .iter()
            .find(|kind| kind.id == id)
            .unwrap_or_else(|| panic!("kind {id} ({name}) of a recorded tape is gone"));
        assert_eq!(
            (build.name, build.digest_text().as_str()),
            (name, digest),
            "kind {id}: its layout changed, so each tape recorded with it is refused. Give the \
             changed layout a new kind id and keep the old one"
        );
        let (_, want) = samples
            .iter()
            .find(|(kind, _)| *kind == id)
            .unwrap_or_else(|| panic!("no sample of kind {id}"));
        assert_eq!(
            decode_payload(id, &unhex(payload)).as_ref(),
            Ok(want),
            "kind {id} ({name}): the payload recorded by an earlier build reads back as its value"
        );
        recorded.push(id);
    }
    let written = written_samples(&samples);
    let mut appended = String::new();
    for kind in TAPE_KINDS {
        let (_, bytes) = written
            .iter()
            .find(|(written_kind, _)| *written_kind == kind.id)
            .unwrap_or_else(|| panic!("the writer wrote no row of kind {}", kind.id));
        let line = format!(
            "{} {} {} {}",
            kind.id,
            kind.name,
            kind.digest_text(),
            hex(bytes)
        );
        if recorded.contains(&kind.id) {
            assert!(
                text.lines().any(|recorded_line| recorded_line == line),
                "kind {}: the writer of this build writes other bytes than the recorded ones",
                kind.id
            );
        } else {
            appended.push_str(&line);
            appended.push('\n');
        }
    }
    if appended.is_empty() {
        return;
    }
    assert!(
        std::env::var(BLESS_VARIABLE).is_ok(),
        "the build writes kinds that {KINDS_FILE} does not record. Rerun with {BLESS_VARIABLE}=1 \
         to append them:\n{appended}"
    );
    std::fs::write(KINDS_FILE, format!("{text}{appended}")).expect("the kinds file is writable");
}
