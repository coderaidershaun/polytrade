//! The wire carries a digest of its own layout. Each frame kind round-trips bit for bit, each
//! refused frame names its own cause, and the bytes of a frame change only with the digest.

use std::net::{Ipv4Addr, UdpSocket};

use engine::hot::exec::{CloseReason, OrderState};
use engine::link::{
    LINK_LAYOUT, LINK_MAGIC, LINK_MAX_FIELDS, LinkBody, LinkDatagram, LinkDecodeError, LinkHash,
    LinkPayload, TopicId,
};
use engine::msg::ids::InstrumentId;
use engine::msg::persist::FeatureId;
use engine::msg::time::VenueTs;
use engine::msg::ui::{
    UI_BOOK_LEVELS, UI_ORDER_SNAPSHOT_CAPACITY, UI_ORDER_SNAPSHOT_MAX_TOTAL, UiEvent,
    UiWorkingOrder,
};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

use super::loopback::LoopbackActor;
use super::wire_frames::{
    FRAMES, FRAMES_PATH, SCHEMA, STRATEGY, TOKEN, arb_datagram, arb_f64_bits, book_datagram,
    decode, encode, envelope, event_datagram, first_difference, from_hex, golden_frames,
    order_snapshot, payload_datagram, round_trip, to_hex, working_order,
};

/// The envelope that opens each datagram. The topic is the field after the magic and the layout
/// digest.
const ENVELOPE_LEN: usize = 86;
const TOPIC_OFFSET: usize = 4 + 8;
/// The count of a payload follows its 8-byte schema hash and its 8-byte event stamp.
const PAYLOAD_COUNT_OFFSET: usize = ENVELOPE_LEN + 8 + 8;

struct Refusal {
    name: &'static str,
    bytes: Vec<u8>,
    cause: LinkDecodeError,
}

fn snapshot_bytes(detail_len: u8, total_working: u16, details: &[UiWorkingOrder]) -> Vec<u8> {
    encode(&event_datagram(order_snapshot(
        detail_len,
        total_working,
        details,
    )))
}

fn forged_book(bid_len: u16, ask_len: u16) -> Vec<u8> {
    let mut datagram = book_datagram();
    let LinkBody::Book(book) = &mut datagram.body else {
        unreachable!("the book datagram holds a book");
    };
    book.bid_len = bid_len;
    book.ask_len = ask_len;
    encode(&datagram)
}

/// One frame for each cause that the decoder can refuse a frame of this layout for. A peer from
/// another run and a peer with a forged length present the same symptom, nothing decodes, so each
/// cause must be named or an operator cannot tell them apart. The forged lengths matter most: the
/// workstation slices its fixed arrays by them, so an unrefused one lets any peer crash it.
fn refusals() -> Vec<Refusal> {
    let other_run = LinkHash::of_name("someone-elses-run");
    let other_strategy = LinkHash::of_name("strat-something-else");
    let drifted = LinkHash::of_fields(&["microprice", "mid", "imbalance"]);
    let book_bytes = encode(&book_datagram());
    let unknown_topic = TopicId(TopicId::HISTORY.0 + 1);
    let forged_field_count = LINK_MAX_FIELDS as u8 + 1;
    let forged_book_len = UI_BOOK_LEVELS as u16 + 1;
    let over_capacity = (UI_ORDER_SNAPSHOT_CAPACITY + 1) as u8;
    let terminal = OrderState::Closed(CloseReason::Filled);
    let live = working_order(7, OrderState::Live);
    let pair = [live, working_order(8, OrderState::Live)];
    vec![
        Refusal {
            name: "magic mismatch",
            bytes: {
                let mut datagram = book_datagram();
                datagram.envelope.magic = LINK_MAGIC ^ 1;
                encode(&datagram)
            },
            cause: LinkDecodeError::MagicMismatch {
                found: LINK_MAGIC ^ 1,
            },
        },
        Refusal {
            name: "token mismatch",
            bytes: {
                let mut datagram = book_datagram();
                datagram.envelope.token_hash = other_run;
                encode(&datagram)
            },
            cause: LinkDecodeError::TokenMismatch {
                found: other_run,
                expected: TOKEN,
            },
        },
        Refusal {
            name: "strategy mismatch",
            bytes: {
                let mut datagram = book_datagram();
                datagram.envelope.strategy_hash = other_strategy;
                encode(&datagram)
            },
            cause: LinkDecodeError::StrategyMismatch {
                found: other_strategy,
                expected: STRATEGY,
            },
        },
        Refusal {
            name: "schema mismatch (drifted link fields)",
            bytes: encode(&payload_datagram(drifted)),
            cause: LinkDecodeError::SchemaMismatch {
                found: drifted,
                expected: SCHEMA,
            },
        },
        Refusal {
            name: "truncated below the envelope",
            bytes: book_bytes[..8].to_vec(),
            cause: LinkDecodeError::Truncated { found: 8 },
        },
        Refusal {
            name: "length does not match its kind",
            bytes: book_bytes[..book_bytes.len() - 1].to_vec(),
            cause: LinkDecodeError::LengthMismatch {
                topic: TopicId::BOOKS,
                expected: book_bytes.len(),
                found: book_bytes.len() - 1,
            },
        },
        Refusal {
            name: "unknown engine topic",
            bytes: {
                let mut bytes = book_bytes.clone();
                bytes[TOPIC_OFFSET..TOPIC_OFFSET + 2]
                    .copy_from_slice(&unknown_topic.0.to_le_bytes());
                bytes
            },
            cause: LinkDecodeError::UnknownTopic {
                topic: unknown_topic,
            },
        },
        Refusal {
            name: "payload count beyond capacity",
            bytes: {
                let mut bytes = encode(&payload_datagram(SCHEMA));
                bytes[PAYLOAD_COUNT_OFFSET] = forged_field_count;
                bytes
            },
            cause: LinkDecodeError::FieldCountExceeded {
                count: forged_field_count,
                capacity: LINK_MAX_FIELDS,
            },
        },
        Refusal {
            name: "book bid_len beyond capacity",
            bytes: forged_book(forged_book_len, 1),
            cause: LinkDecodeError::BookLevelsExceeded {
                count: forged_book_len,
                capacity: UI_BOOK_LEVELS,
            },
        },
        Refusal {
            name: "book ask_len beyond capacity",
            bytes: forged_book(1, forged_book_len),
            cause: LinkDecodeError::BookLevelsExceeded {
                count: forged_book_len,
                capacity: UI_BOOK_LEVELS,
            },
        },
        Refusal {
            name: "a snapshot detailing more orders than it can carry",
            bytes: snapshot_bytes(over_capacity, 9, &pair),
            cause: LinkDecodeError::OrderSnapshotCountsInvalid {
                detail_len: over_capacity,
                total_working: 9,
                detail_capacity: UI_ORDER_SNAPSHOT_CAPACITY,
                total_capacity: UI_ORDER_SNAPSHOT_MAX_TOTAL,
            },
        },
        Refusal {
            name: "a snapshot detailing more orders than it says are working",
            bytes: snapshot_bytes(2, 1, &pair),
            cause: LinkDecodeError::OrderSnapshotCountsInvalid {
                detail_len: 2,
                total_working: 1,
                detail_capacity: UI_ORDER_SNAPSHOT_CAPACITY,
                total_capacity: UI_ORDER_SNAPSHOT_MAX_TOTAL,
            },
        },
        Refusal {
            name: "a snapshot claiming more working orders than the engine can hold",
            bytes: snapshot_bytes(0, UI_ORDER_SNAPSHOT_MAX_TOTAL + 1, &pair),
            cause: LinkDecodeError::OrderSnapshotCountsInvalid {
                detail_len: 0,
                total_working: UI_ORDER_SNAPSHOT_MAX_TOTAL + 1,
                detail_capacity: UI_ORDER_SNAPSHOT_CAPACITY,
                total_capacity: UI_ORDER_SNAPSHOT_MAX_TOTAL,
            },
        },
        Refusal {
            name: "a snapshot carrying a closed order",
            bytes: snapshot_bytes(1, 1, &[working_order(7, terminal)]),
            cause: LinkDecodeError::OrderSnapshotTerminalState { state: terminal },
        },
        Refusal {
            name: "a snapshot repeating a client id",
            bytes: snapshot_bytes(2, 2, &[live, live]),
            cause: LinkDecodeError::OrderSnapshotDuplicate {
                client_id: live.client_id,
            },
        },
    ]
}

fn payload_bits_survive(bits: &[u64]) -> Result<(), TestCaseError> {
    let values: Vec<f64> = bits.iter().copied().map(f64::from_bits).collect();
    let datagram = LinkDatagram {
        envelope: envelope(TopicId::FIRST_STRATEGY, 3),
        body: LinkBody::Payload(LinkPayload::new(SCHEMA, VenueTs::from_micros(1), &values)),
    };
    let decoded = round_trip(&datagram).expect("a canonical payload decodes");
    let LinkBody::Payload(payload) = decoded.body else {
        panic!("payload topic decoded as {:?}", decoded.body);
    };
    let decoded_bits: Vec<u64> = payload.values().iter().copied().map(f64::to_bits).collect();
    prop_assert_eq!(decoded_bits, bits);
    Ok(())
}

/// A NaN is unequal to itself, so a structural round trip cannot see a codec that changes one.
/// The bits of the event floats are compared instead.
fn event_bits_survive(
    feature_bits: u64,
    exposure_bits: u64,
    pnl_bits: u64,
) -> Result<(), TestCaseError> {
    let decoded = round_trip(&event_datagram(UiEvent::Feature {
        instrument: InstrumentId(3),
        seq: 8,
        event_ts_us: VenueTs::from_micros(11),
        feature: FeatureId(2),
        value: f64::from_bits(feature_bits),
    }))
    .expect("a canonical event decodes");
    let LinkBody::Event(UiEvent::Feature { value, .. }) = decoded.body else {
        panic!("the events topic decoded as {:?}", decoded.body);
    };
    prop_assert_eq!(value.to_bits(), feature_bits);

    let decoded = round_trip(&event_datagram(UiEvent::Position {
        instrument: InstrumentId(3),
        seq: 9,
        event_ts_us: VenueTs::from_micros(12),
        exposure_quote: f64::from_bits(exposure_bits),
        pnl_quote: f64::from_bits(pnl_bits),
    }))
    .expect("a canonical event decodes");
    let LinkBody::Event(UiEvent::Position {
        exposure_quote,
        pnl_quote,
        ..
    }) = decoded.body
    else {
        panic!("the events topic decoded as {:?}", decoded.body);
    };
    prop_assert_eq!(exposure_quote.to_bits(), exposure_bits);
    prop_assert_eq!(pnl_quote.to_bits(), pnl_bits);
    Ok(())
}

/// FITNESS: a decoded frame equals the frame that was encoded, for each frame kind, its float
/// slots keep their exact bits, and each refused frame names its own cause. A drift here corrupts a
/// book, a peer's signal or a subscription with no crash: the numbers are only wrong.
#[test]
fn each_frame_kind_round_trips() {
    for refusal in refusals() {
        assert_eq!(
            decode(&refusal.bytes),
            Err(refusal.cause),
            "case {}: wrong cause of the refusal",
            refusal.name
        );
    }
    assert!(
        round_trip(&payload_datagram(SCHEMA)).is_ok(),
        "the correct schema still round-trips"
    );

    let frames = (
        arb_datagram(),
        prop::collection::vec(any::<u64>(), 1..=LINK_MAX_FIELDS),
        (arb_f64_bits(), arb_f64_bits(), arb_f64_bits()),
    );
    let mut runner = TestRunner::new(Config {
        cases: 256,
        failure_persistence: None,
        ..Config::default()
    });
    let outcome = runner.run(
        &frames,
        |(datagram, payload_bits, (feature, exposure, pnl))| {
            let decoded = round_trip(&datagram).expect("a canonical frame decodes");
            prop_assert_eq!(decoded, datagram);
            payload_bits_survive(&payload_bits)?;
            event_bits_survive(feature, exposure, pnl)
        },
    );
    if let Err(failure) = outcome {
        panic!("{failure}");
    }
}

/// The digest of the layout that this build puts on the wire.
fn build_layout() -> u64 {
    LINK_LAYOUT.0
}

/// The first line of the golden file: the layout digest that its bytes were written under.
const LAYOUT_KEY: &str = "layout";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bless {
    Off,
    On,
}

impl Bless {
    fn from_env() -> Self {
        if std::env::var_os("POLYSIM_BLESS_LINK").is_some() { Bless::On } else { Bless::Off }
    }
}

/// What the check of the golden file decides for one build.
#[derive(Debug, PartialEq, Eq)]
enum Golden {
    Holds,
    Rewrite(String),
    Red(String),
}

type Frames<'a> = Vec<(&'a str, Vec<u8>)>;

fn encoded_golden_frames() -> Frames<'static> {
    golden_frames()
        .iter()
        .map(|(kind, datagram)| (*kind, encode(datagram)))
        .collect()
}

fn golden_text(layout: u64, frames: &[(&str, Vec<u8>)]) -> String {
    let mut text = format!("{LAYOUT_KEY} {layout:#018x}\n");
    for (kind, bytes) in frames {
        text.push_str(&format!("{kind} {}\n", to_hex(bytes)));
    }
    text
}

/// The layout digest of the header line, if the file has one, and each frame line.
fn parse_golden(text: &str) -> (Option<u64>, Frames<'_>) {
    let mut layout = None;
    let mut frames = Vec::new();
    for line in text.lines() {
        let (key, value) = line
            .split_once(' ')
            .unwrap_or_else(|| panic!("{FRAMES_PATH}: line {line:?} is not `<kind> <hex>`"));
        if key == LAYOUT_KEY {
            let digits = value.trim_start_matches("0x");
            layout = Some(u64::from_str_radix(digits, 16).unwrap_or_else(|_| {
                panic!("{FRAMES_PATH}: the layout line {line:?} holds no hex digest")
            }));
        } else {
            frames.push((key, from_hex(key, value)));
        }
    }
    (layout, frames)
}

fn differences(encoded: &[(&str, Vec<u8>)], checked_in: &[(&str, Vec<u8>)]) -> Vec<String> {
    let encoded_kinds: Vec<&str> = encoded.iter().map(|(kind, _)| *kind).collect();
    let checked_in_kinds: Vec<&str> = checked_in.iter().map(|(kind, _)| *kind).collect();
    if encoded_kinds != checked_in_kinds {
        return vec![format!(
            "the frame kinds are {encoded_kinds:?}, the file holds {checked_in_kinds:?}"
        )];
    }
    encoded
        .iter()
        .zip(checked_in)
        .filter_map(|((kind, bytes), (_, expected))| {
            first_difference(bytes, expected).map(|offset| {
                format!(
                    "{kind} first differs at byte {offset} ({} bytes encoded, {} checked in)",
                    bytes.len(),
                    expected.len()
                )
            })
        })
        .collect()
}

/// Bytes that differ under an equal digest are red in each mode: a layout input is missing from the
/// digest, and a peer of this digest would misread the frame. Only a new digest may be blessed.
fn golden_verdict(
    checked_in: &str,
    layout: u64,
    frames: &[(&str, Vec<u8>)],
    bless: Bless,
) -> Golden {
    let (checked_in_layout, checked_in_frames) = parse_golden(checked_in);
    let drifted = differences(frames, &checked_in_frames);
    if checked_in_layout == Some(layout) {
        if drifted.is_empty() {
            return Golden::Holds;
        }
        return Golden::Red(format!(
            "the frames differ from {FRAMES_PATH} under the equal layout digest {layout:#018x}: \
             {drifted:#?}; a field, width or tag of the layout is missing from the digest"
        ));
    }
    match bless {
        Bless::On => Golden::Rewrite(golden_text(layout, frames)),
        Bless::Off => Golden::Red(format!(
            "the layout digest is {layout:#018x}, and {FRAMES_PATH} holds {checked_in_layout:#x?} \
             with these differences: {drifted:#?}; if the layout change is intended, rerun with \
             POLYSIM_BLESS_LINK=1 and read the diff"
        )),
    }
}

/// One changed byte of one frame, which the digest of the layout does not see.
fn arb_changed_byte(frames: usize) -> impl Strategy<Value = (usize, prop::sample::Index, u8)> {
    (0..frames, any::<prop::sample::Index>(), 1_u8..)
}

/// FITNESS: the bytes of a frame change only with the layout digest. A peer of another layout is
/// refused by its digest, but a change of bytes under an equal digest would be misread in silence,
/// so the golden file is red for it, in bless mode too. A new digest with new bytes is red until
/// the file is blessed, and then it holds.
#[test]
fn changed_frame_bytes_need_a_changed_digest() {
    let layout = build_layout();
    let frames = encoded_golden_frames();
    let checked_in = golden_text(layout, &frames);

    let mut runner = TestRunner::new(Config {
        cases: 256,
        failure_persistence: None,
        ..Config::default()
    });
    let outcome = runner.run(&arb_changed_byte(frames.len()), |(frame, at, flip)| {
        let mut changed = frames.clone();
        let bytes = &mut changed[frame].1;
        let offset = at.index(bytes.len());
        bytes[offset] ^= flip;
        for bless in [Bless::Off, Bless::On] {
            let verdict = golden_verdict(&checked_in, layout, &changed, bless);
            prop_assert!(
                matches!(verdict, Golden::Red(_)),
                "byte {offset} of {} changed under an equal digest, and bless {bless:?} did not \
                 refuse it",
                changed[frame].0
            );
        }
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("case 2: {failure}");
    }

    let new_layout = layout ^ 0x5a5a;
    let mut widened = frames.clone();
    let lifecycle = widened
        .iter_mut()
        .find(|(kind, _)| *kind == "lifecycle")
        .expect("the golden frames hold a lifecycle frame");
    lifecycle.1.extend_from_slice(&[0, 0]);
    assert!(
        matches!(
            golden_verdict(&checked_in, new_layout, &widened, Bless::Off),
            Golden::Red(_)
        ),
        "case 1: a new digest with new bytes passed before the file was blessed"
    );
    let Golden::Rewrite(blessed) = golden_verdict(&checked_in, new_layout, &widened, Bless::On)
    else {
        panic!("case 1: bless mode did not rewrite the file for a new digest");
    };
    assert_eq!(
        golden_verdict(&blessed, new_layout, &widened, Bless::Off),
        Golden::Holds,
        "case 1: the blessed file does not hold the new digest and bytes"
    );

    match golden_verdict(FRAMES, layout, &frames, Bless::from_env()) {
        Golden::Holds => {}
        Golden::Rewrite(text) => {
            std::fs::write(FRAMES_PATH, text).expect("the golden file is writable")
        }
        Golden::Red(why) => panic!("{why}"),
    }
}

/// CONTRACT: an engine refuses a frame of another layout digest before it reads the length or the
/// body of that frame, names both digests, counts the refusal, and hands nothing on. A frame of its
/// own layout from the same peer still passes, so the refusal is the digest and not the peer.
#[test]
fn digest_mismatch_is_refused_before_the_body() {
    let foreign = LinkHash(LINK_LAYOUT.0 ^ 0x0101);
    let refused = LinkDecodeError::LayoutMismatch {
        found: foreign,
        expected: LINK_LAYOUT,
    };
    let mut foreign_payload = payload_datagram(SCHEMA);
    foreign_payload.envelope.layout = foreign;
    let foreign_bytes = encode(&foreign_payload);
    let mut forged_count = foreign_bytes.clone();
    forged_count[PAYLOAD_COUNT_OFFSET] = LINK_MAX_FIELDS as u8 + 1;
    let bodies: [(&str, &[u8]); 4] = [
        ("a payload of this length", &foreign_bytes),
        ("a payload whose count is past capacity", &forged_count),
        (
            "a body one byte short",
            &foreign_bytes[..foreign_bytes.len() - 1],
        ),
        ("no body at all", &foreign_bytes[..ENVELOPE_LEN]),
    ];
    for (name, bytes) in bodies {
        assert_eq!(
            decode(bytes),
            Err(refused),
            "case {name}: wrong cause of the refusal"
        );
    }

    let mut actor = LoopbackActor::start();
    let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a loopback port binds");
    peer.send_to(&foreign_bytes, actor.address)
        .expect("the foreign frame is sent");
    let mut own = payload_datagram(SCHEMA);
    own.envelope.seq = 2;
    peer.send_to(&encode(&own), actor.address)
        .expect("the frame of this layout is sent");
    let LinkBody::Payload(own_payload) = own.body else {
        unreachable!("the payload datagram holds a payload");
    };
    assert_eq!(
        actor.peer_frames(1),
        vec![own_payload],
        "only the frame of this layout reaches the hot thread"
    );
    let counts = actor.counts_after(1);
    assert_eq!(
        counts.decode_failed, 1,
        "the foreign frame is counted once: {counts:?}"
    );
}
