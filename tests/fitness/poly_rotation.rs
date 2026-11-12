//! Rotation FSM replay: the slot lifecycle must fold a fixed input sequence into fixed actions and a
//! fixed final state. These synthetic sequences pin the load-bearing behaviours —
//! per-instrument rotation/reset fan-out, probe-fallback teardown that never needs the collapse
//! burst, armed-cadence probing, and force-teardown on an occupied slot.

use std::sync::Arc;

use polysim::adapters::polymarket::discovery::PolySchedule;
use polysim::adapters::polymarket::rotation::{
    ForceTeardownFacts, OutcomeLeg, ProbeOutcome, RotationFacts, SlotAction, SlotInput,
    SlotMachine, SlotState, TokenId, WindowAssignment, WindowTokens,
};
use polysim::ids::InstrumentId;
use polysim::time::DurationUs;

use crate::engine_support::ts_secs;

const SECOND_US: i64 = 1_000_000;
const OPEN: i64 = 1_784_439_000;
const CLOSE: i64 = OPEN + 300;
const UP: InstrumentId = InstrumentId(6);
const DOWN: InstrumentId = InstrumentId(7);

fn tokens(tag: &str) -> WindowTokens {
    WindowTokens {
        up: TokenId::from(format!("{tag}-up")),
        down: TokenId::from(format!("{tag}-down")),
    }
}

fn window(open_secs: i64, tag: &str) -> WindowAssignment {
    WindowAssignment {
        up: OutcomeLeg {
            instrument: UP,
            token: TokenId::from(format!("{tag}-up")),
        },
        down: OutcomeLeg {
            instrument: DOWN,
            token: TokenId::from(format!("{tag}-down")),
        },
        window_open_ts_us: ts_secs(open_secs),
        window_close_ts_us: ts_secs(open_secs + 300),
        condition_id: Arc::from(format!("cond-{tag}").as_str()),
    }
}

fn feed(machine: &mut SlotMachine, now_secs: i64, input: SlotInput) -> Vec<SlotAction> {
    let mut actions = Vec::new();
    machine.on_input(ts_secs(now_secs), input, &mut |action| actions.push(action));
    actions
}

fn not_found(tag: &str) -> SlotInput {
    SlotInput::ProbeResult {
        outcome: ProbeOutcome::NotFound,
        tokens: tokens(tag),
    }
}

fn book_exists(tag: &str) -> SlotInput {
    SlotInput::ProbeResult {
        outcome: ProbeOutcome::BookExists,
        tokens: tokens(tag),
    }
}

/// Subscribe fans a `Subscribe` op plus a per-instrument `Rotation → BookReset` for both legs; each
/// `Rotation` carries the window's shared token pair + condition id (the lineage side-channel feed).
fn subscribe_actions(assignment: &WindowAssignment) -> Vec<SlotAction> {
    let mut actions = vec![SlotAction::Subscribe(assignment.clone())];
    for instrument in [assignment.up.instrument, assignment.down.instrument] {
        actions.push(SlotAction::Rotation(RotationFacts {
            instrument,
            window_open_ts_us: assignment.window_open_ts_us,
            window_close_ts_us: assignment.window_close_ts_us,
            tokens: assignment.tokens(),
            condition_id: assignment.condition_id.clone(),
        }));
        actions.push(SlotAction::BookReset(instrument));
    }
    actions
}

/// Teardown unsubscribes both tokens and resets both instruments' books.
fn teardown_actions(tag: &str) -> Vec<SlotAction> {
    vec![
        SlotAction::Unsubscribe(tokens(tag)),
        SlotAction::BookReset(UP),
        SlotAction::BookReset(DOWN),
    ]
}

#[test]
fn normal_rotation_reaches_active_then_grace() {
    let mut machine = SlotMachine::new(PolySchedule::BTC_5M);
    let assignment = window(OPEN, "n");

    let actions = feed(
        &mut machine,
        OPEN - 60,
        SlotInput::Assign(assignment.clone()),
    );
    assert_eq!(actions, subscribe_actions(&assignment));
    assert_eq!(machine.state(), SlotState::Subscribed);

    assert!(feed(&mut machine, OPEN, SlotInput::Frame).is_empty());
    assert_eq!(machine.state(), SlotState::Active);

    assert!(feed(&mut machine, CLOSE, SlotInput::Frame).is_empty());
    assert_eq!(machine.state(), SlotState::Grace);
}

#[test]
fn revived_book_after_false_collapse_does_not_fast_teardown() {
    let mut machine = SlotMachine::new(PolySchedule::BTC_5M);
    feed(
        &mut machine,
        OPEN - 60,
        SlotInput::Assign(window(OPEN, "n")),
    );
    feed(&mut machine, OPEN, SlotInput::Frame);
    feed(&mut machine, CLOSE, SlotInput::Frame);

    // A momentary blip latches the detector, but the book keeps trading (revival frames).
    feed(&mut machine, CLOSE + 10, SlotInput::Frame);
    feed(&mut machine, CLOSE + 10, SlotInput::Collapsed);
    assert!(feed(&mut machine, CLOSE + 11, SlotInput::Frame).is_empty());

    // The latch is cleared, so the later silence does not fast-tear-down; it falls back to a probe.
    assert_eq!(
        feed(&mut machine, CLOSE + 14, SlotInput::Tick),
        vec![SlotAction::Probe(tokens("n"))]
    );
    assert_eq!(machine.state(), SlotState::Grace);
}

#[test]
fn missed_burst_recovers_via_silence_probe_and_404() {
    let mut machine = SlotMachine::new(PolySchedule::BTC_5M);
    feed(
        &mut machine,
        OPEN - 60,
        SlotInput::Assign(window(OPEN, "n")),
    );
    feed(&mut machine, OPEN, SlotInput::Frame);
    feed(&mut machine, CLOSE, SlotInput::Frame);

    // A reconnect missed the collapse burst — no Collapsed ever arrives. Silence alone arms a probe.
    let actions = feed(&mut machine, CLOSE + 3, SlotInput::Tick);
    assert_eq!(actions, vec![SlotAction::Probe(tokens("n"))]);

    // A live book (200) does NOT tear down.
    assert!(feed(&mut machine, CLOSE + 4, book_exists("n")).is_empty());
    assert_eq!(machine.state(), SlotState::Grace);

    // The definitive 404 does, without the machine ever having seen the burst.
    let actions = feed(&mut machine, CLOSE + 9, not_found("n"));
    assert_eq!(actions, teardown_actions("n"));
    assert_eq!(machine.state(), SlotState::TornDown);
}

#[test]
fn armed_cadence_probes_are_rate_limited_then_404_tears_down() {
    let mut machine = SlotMachine::new(PolySchedule::BTC_5M);
    feed(
        &mut machine,
        OPEN - 60,
        SlotInput::Assign(window(OPEN, "n")),
    );
    feed(&mut machine, OPEN, SlotInput::Frame);
    feed(&mut machine, CLOSE, SlotInput::Frame);

    // The path every real window takes: the burst lands ~100s late, so frames keep the feed alive
    // until the grace probe arms at nominal_end+60s. Frames refresh liveness, so these fire the
    // ARMED path, not the silence path.
    assert!(feed(&mut machine, CLOSE + 30, SlotInput::Frame).is_empty());
    assert_eq!(
        feed(&mut machine, CLOSE + 61, SlotInput::Frame),
        vec![SlotAction::Probe(tokens("n"))]
    );
    // Within the 5s cadence — rate-limited to nothing.
    assert!(feed(&mut machine, CLOSE + 63, SlotInput::Frame).is_empty());
    // Cadence elapsed — the next probe fires.
    assert_eq!(
        feed(&mut machine, CLOSE + 66, SlotInput::Frame),
        vec![SlotAction::Probe(tokens("n"))]
    );

    let actions = feed(&mut machine, CLOSE + 67, not_found("n"));
    assert_eq!(actions, teardown_actions("n"));
}

#[test]
fn force_teardown_when_next_subscribe_finds_slot_occupied() {
    let mut machine = SlotMachine::new(PolySchedule::BTC_5M);
    feed(
        &mut machine,
        OPEN - 60,
        SlotInput::Assign(window(OPEN, "n")),
    );
    feed(&mut machine, OPEN, SlotInput::Frame);
    feed(&mut machine, CLOSE, SlotInput::Frame);

    // The venue never tore the window down; arming still emits a probe, and its book answers 200.
    assert_eq!(
        feed(&mut machine, CLOSE + 61, SlotInput::Frame),
        vec![SlotAction::Probe(tokens("n"))]
    );
    feed(&mut machine, CLOSE + 62, book_exists("n"));
    assert_eq!(machine.state(), SlotState::Grace);

    // The next slot-A window (N+2) opens at OPEN+600; its subscribe falls due at OPEN+540 — a 240s
    // tail. Handing it over evicts the wedged window and brings the new one up in one step.
    let next = window(OPEN + 600, "n2");
    let actions = feed(&mut machine, OPEN + 540, SlotInput::Assign(next.clone()));

    let mut expected = vec![
        SlotAction::Unsubscribe(tokens("n")),
        SlotAction::BookReset(UP),
        SlotAction::BookReset(DOWN),
        SlotAction::ForceTeardown(ForceTeardownFacts {
            up_instrument: UP,
            down_instrument: DOWN,
            window_open_ts_us: ts_secs(OPEN),
            grace_age: DurationUs::from_micros(240 * SECOND_US),
            tokens: tokens("n"),
        }),
    ];
    expected.extend(subscribe_actions(&next));
    assert_eq!(actions, expected);
    assert_eq!(machine.state(), SlotState::Subscribed);
}
