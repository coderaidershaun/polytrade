//! The rotation of Polymarket windows: a slot subscribes and tears down each window once, the grid
//! and the slot parity hold, and a fixed sequence of events gives fixed effects.

use std::sync::Arc;

use engine::msg::ids::{InstrumentId, Price, Qty};
use engine::msg::inbound::{BookChunkKind, Level, MessageBody};
use engine::msg::time::DurationUs;
use engine::venues::polymarket::actor::{DriverEffect, PolyDriverCore, SlotLegs};
use engine::venues::polymarket::discovery::{PolySchedule, Slot};
use engine::venues::polymarket::parse::{PolyBook, PolyFrame};
use engine::venues::polymarket::rotation::{
    ForceTeardownFacts, OutcomeLeg, ProbeOutcome, RotationFacts, SlotAction, SlotInput,
    SlotMachine, SlotState, TokenId, WindowAssignment, WindowTokens,
};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use crate::engine_support::{ts_secs, venue_ts_secs};

const SECOND_US: i64 = 1_000_000;
const WINDOW_SECS: i64 = 300;
/// An even window index, so slot A; the window after it is slot B.
const OPEN: i64 = 1_784_439_000;
const CLOSE: i64 = OPEN + WINDOW_SECS;
const UP: InstrumentId = InstrumentId(6);
const DOWN: InstrumentId = InstrumentId(7);
const ONE_SLOT: SlotLegs = SlotLegs { up: UP, down: DOWN };

fn window_tokens(tag: &str) -> WindowTokens {
    WindowTokens {
        up: TokenId::from(format!("{tag}-up")),
        down: TokenId::from(format!("{tag}-down")),
    }
}

/// The window that opens at `open_secs`, with the tokens `{tag}-up` and `{tag}-down` on `legs`.
fn assignment(open_secs: i64, tag: &str, legs: SlotLegs) -> WindowAssignment {
    let WindowTokens { up, down } = window_tokens(tag);
    WindowAssignment {
        legs: [
            OutcomeLeg {
                instrument: legs.up,
                token: up,
            },
            OutcomeLeg {
                instrument: legs.down,
                token: down,
            },
        ],
        window_open_ts_us: venue_ts_secs(open_secs),
        window_close_ts_us: venue_ts_secs(open_secs + WINDOW_SECS),
        condition_id: Arc::from(format!("cond-{tag}").as_str()),
    }
}

fn forced_facts(window: &WindowAssignment, grace_age: DurationUs) -> ForceTeardownFacts {
    ForceTeardownFacts {
        instruments: [UP, DOWN],
        window_open_ts_us: window.window_open_ts_us,
        grace_age,
        tokens: window.tokens(),
    }
}

fn feed(machine: &mut SlotMachine, now_secs: i64, input: SlotInput) -> Vec<SlotAction> {
    let mut actions = Vec::new();
    machine.on_input(ts_secs(now_secs), input, &mut |action| actions.push(action));
    actions
}

fn probe_result(tag: &str, outcome: ProbeOutcome) -> SlotInput {
    SlotInput::ProbeResult {
        outcome,
        tokens: window_tokens(tag),
    }
}

/// A subscribe fans out a `Rotation` and a `BookReset` for each leg, and each `Rotation` carries
/// the token pair and the condition id of the window.
fn subscribe_actions(window: &WindowAssignment) -> Vec<SlotAction> {
    let mut actions = vec![SlotAction::Subscribe(window.clone())];
    for instrument in [UP, DOWN] {
        actions.push(SlotAction::Rotation(RotationFacts {
            instrument,
            window_open_ts_us: window.window_open_ts_us,
            window_close_ts_us: window.window_close_ts_us,
            tokens: window.tokens(),
            condition_id: window.condition_id.clone(),
        }));
        actions.push(SlotAction::BookReset(instrument));
    }
    actions
}

fn teardown_actions(tokens: WindowTokens) -> Vec<SlotAction> {
    vec![
        SlotAction::Unsubscribe(tokens),
        SlotAction::BookReset(UP),
        SlotAction::BookReset(DOWN),
    ]
}

/// One input of the slot property, with the seconds that pass before it.
#[derive(Debug, Clone)]
enum Step {
    Tick,
    Assign(i64),
    Frame,
    Collapsed,
    Probe { window: i64, is_found: bool },
}

/// The window `n` of slot A, two windows after `n - 1`.
fn slot_window(n: i64) -> WindowAssignment {
    assignment(OPEN + 2 * WINDOW_SECS * n, &format!("w{n}"), ONE_SLOT)
}

impl Step {
    fn input(&self) -> SlotInput {
        match *self {
            Step::Tick => SlotInput::Tick,
            Step::Assign(n) => SlotInput::Assign(slot_window(n)),
            Step::Frame => SlotInput::Frame,
            Step::Collapsed => SlotInput::Collapsed,
            Step::Probe { window, is_found } => SlotInput::ProbeResult {
                outcome: if is_found { ProbeOutcome::BookExists } else { ProbeOutcome::NotFound },
                tokens: slot_window(window).tokens(),
            },
        }
    }
}

fn steps() -> impl Strategy<Value = Vec<(i64, Step)>> {
    let step = prop_oneof![
        Just(Step::Tick),
        (0..6i64).prop_map(Step::Assign),
        Just(Step::Frame),
        Just(Step::Collapsed),
        (0..6i64, any::<bool>()).prop_map(|(window, is_found)| Step::Probe { window, is_found }),
    ];
    prop::collection::vec((0..120i64, step), 0..80)
}

/// Replays `steps` into one slot and reads its actions as a ledger of the window it holds: a
/// subscribe takes the slot, and a teardown gives it back.
fn one_window_at_a_time(steps: &[(i64, Step)]) -> Result<(), TestCaseError> {
    let mut machine = SlotMachine::new(PolySchedule::BTC_5M);
    let mut now = OPEN - 2 * WINDOW_SECS;
    let mut held: Option<WindowAssignment> = None;
    let (mut subscribes, mut teardowns) = (0usize, 0usize);
    for (advance, step) in steps {
        now += advance;
        let actions = feed(&mut machine, now, step.input());
        let mut rest = actions.as_slice();
        while let Some((action, tail)) = rest.split_first() {
            rest = match action {
                SlotAction::Subscribe(window) => {
                    prop_assert!(
                        held.is_none(),
                        "{:?} is subscribed while {:?} is held",
                        window,
                        held
                    );
                    let fan_out = &subscribe_actions(window)[1..];
                    prop_assert!(
                        tail.starts_with(fan_out),
                        "the subscribe of {:?} fans out to both legs: {:?}",
                        window,
                        tail
                    );
                    held = Some(window.clone());
                    subscribes += 1;
                    &tail[fan_out.len()..]
                }
                SlotAction::Unsubscribe(tokens) => {
                    let Some(window) = held.take() else {
                        return Err(TestCaseError::fail(format!(
                            "{tokens:?} torn down while no window is held"
                        )));
                    };
                    prop_assert_eq!(
                        tokens,
                        &window.tokens(),
                        "the teardown names the held window"
                    );
                    let resets = &teardown_actions(window.tokens())[1..];
                    prop_assert!(
                        tail.starts_with(resets),
                        "the teardown resets both legs: {:?}",
                        tail
                    );
                    teardowns += 1;
                    let tail = &tail[resets.len()..];
                    match tail.first() {
                        Some(SlotAction::ForceTeardown(facts)) => {
                            prop_assert_eq!(facts, &forced_facts(&window, facts.grace_age));
                            &tail[1..]
                        }
                        _ => tail,
                    }
                }
                SlotAction::Probe(tokens) => {
                    let probed = held.as_ref().map(WindowAssignment::tokens);
                    prop_assert_eq!(
                        Some(tokens),
                        probed.as_ref(),
                        "a probe names the held window"
                    );
                    tail
                }
                other => {
                    return Err(TestCaseError::fail(format!(
                        "{other:?} outside a subscribe or a teardown"
                    )));
                }
            };
        }
        let is_occupied = matches!(
            machine.state(),
            SlotState::Subscribed | SlotState::Active | SlotState::Grace
        );
        prop_assert_eq!(is_occupied, held.is_some(), "state {:?}", machine.state());
    }
    prop_assert_eq!(subscribes - teardowns, usize::from(held.is_some()));
    Ok(())
}

fn a_normal_rotation_reaches_grace() {
    let mut machine = SlotMachine::new(PolySchedule::BTC_5M);
    let window = assignment(OPEN, "n", ONE_SLOT);
    let actions = feed(&mut machine, OPEN - 60, SlotInput::Assign(window.clone()));
    assert_eq!(actions, subscribe_actions(&window));
    assert_eq!(machine.state(), SlotState::Subscribed);
    assert!(feed(&mut machine, OPEN, SlotInput::Frame).is_empty());
    assert_eq!(machine.state(), SlotState::Active);
    assert!(feed(&mut machine, CLOSE, SlotInput::Frame).is_empty());
    assert_eq!(machine.state(), SlotState::Grace);
}

/// A slot of window `n` driven to its grace tail.
fn in_grace() -> SlotMachine {
    let mut machine = SlotMachine::new(PolySchedule::BTC_5M);
    feed(
        &mut machine,
        OPEN - 60,
        SlotInput::Assign(assignment(OPEN, "n", ONE_SLOT)),
    );
    feed(&mut machine, OPEN, SlotInput::Frame);
    feed(&mut machine, CLOSE, SlotInput::Frame);
    machine
}

fn a_false_collapse_revives() {
    let mut machine = in_grace();
    feed(&mut machine, CLOSE + 10, SlotInput::Frame);
    feed(&mut machine, CLOSE + 10, SlotInput::Collapsed);
    assert!(feed(&mut machine, CLOSE + 11, SlotInput::Frame).is_empty());
    assert_eq!(
        feed(&mut machine, CLOSE + 14, SlotInput::Tick),
        vec![SlotAction::Probe(window_tokens("n"))],
        "a frame after the collapse clears it, so the silence that follows probes"
    );
    assert_eq!(machine.state(), SlotState::Grace);
}

fn a_missed_burst_tears_down_by_probe_and_404() {
    let mut machine = in_grace();
    assert_eq!(
        feed(&mut machine, CLOSE + 3, SlotInput::Tick),
        vec![SlotAction::Probe(window_tokens("n"))],
        "silence alone arms a probe"
    );
    assert!(
        feed(
            &mut machine,
            CLOSE + 4,
            probe_result("n", ProbeOutcome::BookExists)
        )
        .is_empty()
    );
    assert_eq!(machine.state(), SlotState::Grace);
    assert_eq!(
        feed(
            &mut machine,
            CLOSE + 9,
            probe_result("n", ProbeOutcome::NotFound)
        ),
        teardown_actions(window_tokens("n"))
    );
    assert_eq!(machine.state(), SlotState::TornDown);
}

fn the_probe_cadence_holds() {
    let mut machine = in_grace();
    assert!(feed(&mut machine, CLOSE + 30, SlotInput::Frame).is_empty());
    assert_eq!(
        feed(&mut machine, CLOSE + 61, SlotInput::Frame),
        vec![SlotAction::Probe(window_tokens("n"))],
        "the probe arms 60 s after the close"
    );
    assert!(
        feed(&mut machine, CLOSE + 63, SlotInput::Frame).is_empty(),
        "within the 5 s cadence"
    );
    assert_eq!(
        feed(&mut machine, CLOSE + 66, SlotInput::Frame),
        vec![SlotAction::Probe(window_tokens("n"))]
    );
    assert_eq!(
        feed(
            &mut machine,
            CLOSE + 67,
            probe_result("n", ProbeOutcome::NotFound)
        ),
        teardown_actions(window_tokens("n"))
    );
}

fn a_new_assignment_forces_the_teardown_of_a_held_window() {
    let mut machine = in_grace();
    assert_eq!(
        feed(&mut machine, CLOSE + 61, SlotInput::Frame),
        vec![SlotAction::Probe(window_tokens("n"))]
    );
    feed(
        &mut machine,
        CLOSE + 62,
        probe_result("n", ProbeOutcome::BookExists),
    );
    assert_eq!(machine.state(), SlotState::Grace);

    let next = assignment(OPEN + 600, "n2", ONE_SLOT);
    let actions = feed(&mut machine, OPEN + 540, SlotInput::Assign(next.clone()));
    let mut expected = teardown_actions(window_tokens("n"));
    expected.push(SlotAction::ForceTeardown(forced_facts(
        &assignment(OPEN, "n", ONE_SLOT),
        DurationUs::from_micros(240 * SECOND_US),
    )));
    expected.extend(subscribe_actions(&next));
    assert_eq!(actions, expected);
    assert_eq!(machine.state(), SlotState::Subscribed);
}

/// FITNESS: a slot holds one window at a time, subscribes it with both legs, and tears it down
/// once with both legs, whatever comes in. A teardown that reset one leg, or a subscribe on a slot
/// that still held a window, would leave a book of the old window under the new one with no error.
#[test]
fn each_subscribe_has_one_teardown_and_no_slot_holds_two_windows() {
    TestRunner::new(Config {
        cases: 256,
        failure_persistence: None,
        ..Config::default()
    })
    .run(&steps(), |steps| one_window_at_a_time(&steps))
    .unwrap_or_else(|error| panic!("{error}"));

    a_normal_rotation_reaches_grace();
    a_false_collapse_revives();
    a_missed_burst_tears_down_by_probe_and_404();
    the_probe_cadence_holds();
    a_new_assignment_forces_the_teardown_of_a_held_window();
}

/// FITNESS: each instant lies in one window of the 300 s grid, the next window starts where it
/// closes, and the slot of a window is the parity of its index. A drift here would subscribe the
/// wrong 5-minute market, or put two live windows in one slot.
#[test]
fn window_grid_and_slot_parity_hold() {
    let schedule = PolySchedule::BTC_5M;
    let len = schedule.window_len.micros();
    let runner = || {
        TestRunner::new(Config {
            cases: 256,
            failure_persistence: None,
            ..Config::default()
        })
    };
    runner()
        .run(&(0i64..4_000_000_000i64), |now_secs| {
            let now = venue_ts_secs(now_secs);
            let window = schedule.current_window(now);
            prop_assert_eq!(window.window_start_ts_us.micros() % len, 0);
            prop_assert!(window.window_start_ts_us <= now);
            prop_assert!(now < window.window_close_ts_us);
            prop_assert_eq!(
                window.window_close_ts_us.micros() - window.window_start_ts_us.micros(),
                len
            );
            prop_assert_eq!(
                window.window_start_ts_us.micros() / SECOND_US % WINDOW_SECS,
                0
            );
            let next = schedule.next_window(now);
            prop_assert_eq!(next.window_start_ts_us, window.window_close_ts_us);
            prop_assert_eq!(next.index, window.index + 1);
            prop_assert_ne!(
                next.slot,
                window.slot,
                "the next window is in the other slot"
            );
            Ok(())
        })
        .unwrap_or_else(|error| panic!("the grid: {error}"));
    runner()
        .run(&(0i64..20_000_000i64), |index| {
            let window = schedule.window_at(venue_ts_secs(index * WINDOW_SECS));
            prop_assert_eq!(window.index, index);
            let parity = if window.index % 2 == 0 { Slot::A } else { Slot::B };
            prop_assert_eq!(window.slot, parity, "the slot is the parity of the index");
            Ok(())
        })
        .unwrap_or_else(|error| panic!("the parity: {error}"));

    let now = venue_ts_secs(1_784_439_123);
    let current = schedule.current_window(now);
    let next = schedule.next_window(now);
    assert_eq!(
        schedule.subscribe_at(next.window_start_ts_us),
        current.window_close_ts_us - schedule.subscribe_lead,
        "the next window subscribes 60 s before the current one closes"
    );
    assert_eq!(next.window_start_ts_us, current.window_close_ts_us);
}

/// A grid-aligned even index is slot A, and the window after it is slot B.
const OPEN_A: i64 = OPEN;
const OPEN_B: i64 = OPEN_A + WINDOW_SECS;
const CLOSE_A: i64 = OPEN_A + WINDOW_SECS;
const SLOT_A: SlotLegs = SlotLegs {
    up: InstrumentId(0),
    down: InstrumentId(1),
};
const SLOT_B: SlotLegs = SlotLegs {
    up: InstrumentId(2),
    down: InstrumentId(3),
};

fn core() -> PolyDriverCore {
    PolyDriverCore::new([SLOT_A, SLOT_B], PolySchedule::BTC_5M)
}

/// The window that opens at `open_secs`, on the legs of the slot of its index.
fn driver_window(open_secs: i64) -> WindowAssignment {
    let legs = if (open_secs / WINDOW_SECS).rem_euclid(2) == 0 { SLOT_A } else { SLOT_B };
    assignment(open_secs, &open_secs.to_string(), legs)
}

fn driver_tokens(open_secs: i64) -> WindowTokens {
    window_tokens(&open_secs.to_string())
}

/// A book of two levels on each side, bids best first as the parser gives them. One level a side
/// would read the same in either order, which would hide a driver that stopped reversing the bids.
fn book_frame(token: &str, now_secs: i64) -> PolyFrame {
    let level = |price, qty| Level {
        price: Price::from_mantissa(price),
        qty: Qty::from_mantissa(qty),
    };
    PolyFrame::Book(PolyBook {
        asset_id: token.into(),
        bids: vec![level(50_000_000, 100), level(49_000_000, 200)],
        asks: vec![level(51_000_000, 150), level(52_000_000, 250)],
        exchange_ts_us: venue_ts_secs(now_secs),
        received_ts_us: ts_secs(now_secs),
    })
}

fn on_tick(core: &mut PolyDriverCore, now_secs: i64) -> Vec<DriverEffect> {
    let mut effects = Vec::new();
    core.on_tick(ts_secs(now_secs), &mut |effect| effects.push(effect));
    effects
}

fn on_resolved(core: &mut PolyDriverCore, now_secs: i64, open_secs: i64) -> Vec<DriverEffect> {
    let mut effects = Vec::new();
    core.on_window_resolved(ts_secs(now_secs), driver_window(open_secs), &mut |effect| {
        effects.push(effect)
    });
    effects
}

fn on_frame(core: &mut PolyDriverCore, now_secs: i64, frame: &PolyFrame) -> Vec<DriverEffect> {
    let mut effects = Vec::new();
    core.on_frame(ts_secs(now_secs), frame, &mut |effect| effects.push(effect));
    effects
}

fn on_book(core: &mut PolyDriverCore, now_secs: i64, open_secs: i64) -> Vec<DriverEffect> {
    on_frame(
        core,
        now_secs,
        &book_frame(&format!("{open_secs}-up"), now_secs),
    )
}

fn on_probe(
    core: &mut PolyDriverCore,
    now_secs: i64,
    open_secs: i64,
    outcome: ProbeOutcome,
) -> Vec<DriverEffect> {
    let mut effects = Vec::new();
    core.on_probe_result(
        ts_secs(now_secs),
        driver_tokens(open_secs),
        outcome,
        &mut |effect| effects.push(effect),
    );
    effects
}

/// An effect as a test compares it: the kind, with its instrument or the up token of its window.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Tag {
    Resolve(i64),
    Subscribe(String),
    Unsubscribe(String),
    Probe(String),
    Rotation(u16),
    PersistRotation(u16),
    BookReset(u16),
    Snapshot(u16),
    Delta(u16),
    Trade(u16),
    ForcedTeardown(i64),
    Diverged(u16),
    TickSize,
    Suspect(u16),
    /// The window handed to the execution edge, by its up token, so a sequence shows that it names
    /// the market of the subscribe that follows it.
    BindExecution(String),
}

fn tag(effect: &DriverEffect) -> Tag {
    match effect {
        DriverEffect::Resolve(start) => Tag::Resolve(start.micros() / SECOND_US),
        DriverEffect::Subscribe(tokens) => Tag::Subscribe(tokens.up.as_str().to_owned()),
        DriverEffect::Unsubscribe(tokens) => Tag::Unsubscribe(tokens.up.as_str().to_owned()),
        DriverEffect::Probe(tokens) => Tag::Probe(tokens.up.as_str().to_owned()),
        DriverEffect::ForcedTeardown(facts) => {
            Tag::ForcedTeardown(facts.window_open_ts_us.micros() / SECOND_US)
        }
        DriverEffect::Diverged { instrument, .. } => Tag::Diverged(instrument.0),
        DriverEffect::TickSizeChange { .. } => Tag::TickSize,
        DriverEffect::Suspect { instrument, .. } => Tag::Suspect(instrument.0),
        DriverEffect::BindExecution(window) => {
            Tag::BindExecution(window.tokens().up.as_str().to_owned())
        }
        DriverEffect::PersistRotation(row) => Tag::PersistRotation(row.instrument.0),
        DriverEffect::Emit(message) => match &message.body {
            MessageBody::MarketRotation(rotation) => Tag::Rotation(rotation.instrument.0),
            MessageBody::BookReset(reset) => Tag::BookReset(reset.instrument.0),
            MessageBody::Book(chunk) => match chunk.kind {
                BookChunkKind::Snapshot => Tag::Snapshot(chunk.instrument.0),
                BookChunkKind::Delta => Tag::Delta(chunk.instrument.0),
            },
            MessageBody::Trade(trade) => Tag::Trade(trade.instrument.0),
            other => panic!("unexpected emitted message: {other:?}"),
        },
    }
}

fn tags(effects: &[DriverEffect]) -> Vec<Tag> {
    effects.iter().map(tag).collect()
}

/// The subscribe of a window: the binding of the execution edge first, because the edge must
/// know the token of each leg before any frame on that token reaches it, then the subscribe, then
/// `Rotation`, `PersistRotation`, and `BookReset` for each leg.
fn subscribe_tags(open_secs: i64, legs: SlotLegs) -> Vec<Tag> {
    let mut tags = vec![
        Tag::BindExecution(format!("{open_secs}-up")),
        Tag::Subscribe(format!("{open_secs}-up")),
    ];
    for instrument in [legs.up, legs.down] {
        tags.extend([
            Tag::Rotation(instrument.0),
            Tag::PersistRotation(instrument.0),
            Tag::BookReset(instrument.0),
        ]);
    }
    tags
}

/// The fixed sequence, with the effects of each event: start in window A, prefetch, a first book,
/// the handover to window B, and the teardown of window A by its 404.
fn fixed_sequence() -> Vec<Vec<DriverEffect>> {
    let mut core = core();
    vec![
        on_resolved(&mut core, OPEN_A + 10, OPEN_A),
        on_tick(&mut core, OPEN_A + 10),
        on_book(&mut core, OPEN_A + 11, OPEN_A),
        on_resolved(&mut core, OPEN_A + 15, OPEN_B),
        on_tick(&mut core, OPEN_B - 60),
        on_tick(&mut core, CLOSE_A + 1),
        on_probe(&mut core, CLOSE_A + 2, OPEN_A, ProbeOutcome::NotFound),
    ]
}

fn startup_prefetches_then_hands_over_before_the_old_window_tears_down() {
    let mut core = core();
    assert_eq!(
        tags(&on_resolved(&mut core, OPEN_A + 10, OPEN_A)),
        subscribe_tags(OPEN_A, SLOT_A),
        "a window resolved after its subscribe instant subscribes at once"
    );
    assert_eq!(
        tags(&on_tick(&mut core, OPEN_A + 10)),
        vec![Tag::Resolve(OPEN_A + 2 * WINDOW_SECS), Tag::Resolve(OPEN_B)],
        "the first tick prefetches the next window of each slot"
    );
    assert!(tags(&on_book(&mut core, OPEN_A + 11, OPEN_A)).contains(&Tag::Snapshot(0)));
    assert!(
        on_resolved(&mut core, OPEN_A + 15, OPEN_B).is_empty(),
        "the next window waits for its own subscribe instant"
    );
    let handover = tags(&on_tick(&mut core, OPEN_B - 60));
    for expected in subscribe_tags(OPEN_B, SLOT_B) {
        assert!(
            handover.contains(&expected),
            "missing {expected:?} in {handover:?}"
        );
    }
    assert!(tags(&on_book(&mut core, OPEN_B + 1, OPEN_B)).contains(&Tag::Snapshot(2)));
    on_tick(&mut core, CLOSE_A + 1);
    assert_eq!(
        tags(&on_probe(
            &mut core,
            CLOSE_A + 2,
            OPEN_A,
            ProbeOutcome::NotFound
        )),
        vec![
            Tag::Unsubscribe(format!("{OPEN_A}-up")),
            Tag::BookReset(0),
            Tag::BookReset(1),
        ],
        "only the 404 tears window A down, after window B is live"
    );
}

fn a_reconnect_resets_each_live_leg() {
    let mut core = core();
    on_resolved(&mut core, OPEN_A + 10, OPEN_A);
    on_book(&mut core, OPEN_A + 11, OPEN_A);
    let mut effects = Vec::new();
    core.on_reconnect(ts_secs(OPEN_A + 20), &mut |effect| effects.push(effect));
    assert_eq!(tags(&effects), vec![Tag::BookReset(0), Tag::BookReset(1)]);
    assert_eq!(
        core.live_tokens(),
        vec![
            TokenId::from(format!("{OPEN_A}-up")),
            TokenId::from(format!("{OPEN_A}-down"))
        ]
    );
    assert!(
        tags(&on_book(&mut core, OPEN_A + 21, OPEN_A)).contains(&Tag::Snapshot(0)),
        "the book after a reconnect forwards as a new snapshot"
    );
}

fn an_occupied_slot_is_forced_down_at_the_next_subscribe() {
    let mut core = core();
    on_resolved(&mut core, OPEN_A + 10, OPEN_A);
    on_tick(&mut core, CLOSE_A + 1);
    let next_open = OPEN_A + 2 * WINDOW_SECS;
    let effect_tags = tags(&on_resolved(&mut core, next_open - 60, next_open));
    assert!(effect_tags.contains(&Tag::ForcedTeardown(OPEN_A)));
    assert_eq!(core.force_teardown_count(), 1);
    let evict = effect_tags
        .iter()
        .position(|tag| *tag == Tag::Unsubscribe(format!("{OPEN_A}-up")))
        .expect("the old window is unsubscribed");
    let subscribe = effect_tags
        .iter()
        .position(|tag| *tag == Tag::Subscribe(format!("{next_open}-up")))
        .expect("the new window is subscribed");
    assert!(
        evict < subscribe,
        "the eviction comes before the new subscribe"
    );
}

/// The shadow book compares levels in the venue's ascending order, and the parser gives bids best
/// first, so the driver reverses them first. A driver that stopped would see a mismatch at each
/// repeat of a deep book, and reset it every third frame.
fn a_repeated_deep_book_validates_with_no_reset() {
    let mut core = core();
    on_resolved(&mut core, OPEN_A + 10, OPEN_A);
    assert!(tags(&on_book(&mut core, OPEN_A + 11, OPEN_A)).contains(&Tag::Snapshot(0)));
    let frame = book_frame(&format!("{OPEN_A}-up"), OPEN_A + 11);
    for repeat in 1..=3 {
        let effect_tags = tags(&on_frame(&mut core, OPEN_A + 11 + repeat, &frame));
        assert!(
            !effect_tags.iter().any(|tag| matches!(
                tag,
                Tag::Diverged(_) | Tag::BookReset(_) | Tag::Snapshot(_) | Tag::Delta(_)
            )),
            "repeat {repeat} validates silently, got {effect_tags:?}"
        );
    }
}

/// CONTRACT: the driver core turns a fixed sequence of windows, ticks, frames, and probe answers
/// into a fixed list of effects, so a recorded rotation replays with no socket, the handover and
/// the teardown included. The core and its slot machines and legs are the real ones.
#[test]
fn rotation_driver_gives_fixed_effects_for_a_fixed_event_sequence() {
    let effects = fixed_sequence();
    assert_eq!(effects, fixed_sequence(), "a replay gives the same effects");
    let next_a = OPEN_A + 2 * WINDOW_SECS;
    let next_b = OPEN_B + 2 * WINDOW_SECS;
    let expected = [
        subscribe_tags(OPEN_A, SLOT_A),
        vec![Tag::Resolve(next_a), Tag::Resolve(OPEN_B)],
        vec![Tag::Snapshot(0), Tag::Snapshot(0)],
        vec![],
        [vec![Tag::Resolve(next_a)], subscribe_tags(OPEN_B, SLOT_B)].concat(),
        vec![
            Tag::Resolve(next_a),
            Tag::Probe(format!("{OPEN_A}-up")),
            Tag::Resolve(next_b),
        ],
        vec![
            Tag::Unsubscribe(format!("{OPEN_A}-up")),
            Tag::BookReset(0),
            Tag::BookReset(1),
        ],
    ];
    assert_eq!(effects.len(), expected.len());
    for (event, (effects, expected)) in effects.iter().zip(expected).enumerate() {
        assert_eq!(tags(effects), expected, "the effects of event {event}");
    }

    startup_prefetches_then_hands_over_before_the_old_window_tears_down();
    a_reconnect_resets_each_live_leg();
    an_occupied_slot_is_forced_down_at_the_next_subscribe();
    a_repeated_deep_book_validates_with_no_reset();
}
