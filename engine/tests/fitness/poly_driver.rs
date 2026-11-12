//! Driver-level replay: the pure `PolyDriverCore` must fold a fixed event sequence into fixed
//! effects, so a recorded rotation replays without sockets. These synthetic sequences pin
//! the load-bearing driver behaviours — the subscribe-time rotation slot sequence, the zero-gap
//! handover (the next window subscribes + snapshots while the old one still streams), reconnect
//! re-baseline, and force-teardown of an occupied slot.

use std::sync::Arc;

use polysim::adapters::polymarket::actor::{DriverEffect, PolyDriverCore, SlotLegs};
use polysim::adapters::polymarket::discovery::PolySchedule;
use polysim::adapters::polymarket::parse::{PolyBook, PolyFrame};
use polysim::adapters::polymarket::rotation::{
    OutcomeLeg, ProbeOutcome, TokenId, WindowAssignment, WindowTokens,
};
use polysim::ids::{InstrumentId, Price, Qty};
use polysim::msg::inbound::{BookChunkKind, InboundMessage, Level};

use crate::engine_support::ts_secs;

const SECOND_US: i64 = 1_000_000;
const WINDOW_SECS: i64 = 300;
/// A grid-aligned even index maps to slot A; the next window (odd) maps to slot B.
const OPEN_A: i64 = 1_784_439_000;
const OPEN_B: i64 = OPEN_A + WINDOW_SECS;
const CLOSE_A: i64 = OPEN_A + WINDOW_SECS;

/// Slot A hosts `btc-updown-5m-a-{up,down}` (instruments 0/1); slot B the `-b-` pair (2/3).
fn core() -> PolyDriverCore {
    PolyDriverCore::new(
        [
            SlotLegs {
                up: InstrumentId(0),
                down: InstrumentId(1),
            },
            SlotLegs {
                up: InstrumentId(2),
                down: InstrumentId(3),
            },
        ],
        PolySchedule::BTC_5M,
    )
}

fn tokens(open_secs: i64) -> WindowTokens {
    WindowTokens {
        up: TokenId::from(format!("{open_secs}-up")),
        down: TokenId::from(format!("{open_secs}-down")),
    }
}

fn assignment(open_secs: i64) -> WindowAssignment {
    let WindowTokens {
        up: up_token,
        down: down_token,
    } = tokens(open_secs);
    let (up, down) = if (open_secs / WINDOW_SECS).rem_euclid(2) == 0 {
        (InstrumentId(0), InstrumentId(1))
    } else {
        (InstrumentId(2), InstrumentId(3))
    };
    WindowAssignment {
        up: OutcomeLeg {
            instrument: up,
            token: up_token,
        },
        down: OutcomeLeg {
            instrument: down,
            token: down_token,
        },
        window_open_ts_us: ts_secs(open_secs),
        window_close_ts_us: ts_secs(open_secs + WINDOW_SECS),
        condition_id: Arc::from(format!("cond-{open_secs}").as_str()),
    }
}

/// A two-level-per-side book frame in the order parse emits: bids best-first (descending), asks
/// ascending. Depth matters — a one-level side reads the same in either order, which is
/// exactly what the bid-reversal regression below needs to rule out.
fn book_frame(token: &str, now_secs: i64) -> PolyFrame {
    PolyFrame::Book(PolyBook {
        asset_id: token.into(),
        bids: vec![
            Level {
                price: Price(50_000_000),
                qty: Qty(100),
            },
            Level {
                price: Price(49_000_000),
                qty: Qty(200),
            },
        ],
        asks: vec![
            Level {
                price: Price(51_000_000),
                qty: Qty(150),
            },
            Level {
                price: Price(52_000_000),
                qty: Qty(250),
            },
        ],
        exchange_ts_us: ts_secs(now_secs),
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
    core.on_window_resolved(ts_secs(now_secs), assignment(open_secs), &mut |effect| {
        effects.push(effect)
    });
    effects
}

fn on_frame(core: &mut PolyDriverCore, now_secs: i64, frame: &PolyFrame) -> Vec<DriverEffect> {
    let mut effects = Vec::new();
    core.on_frame(ts_secs(now_secs), frame, &mut |effect| effects.push(effect));
    effects
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
        tokens(open_secs),
        outcome,
        &mut |effect| effects.push(effect),
    );
    effects
}

/// A comparable projection of an effect — collapses noisy book-chunk internals to (kind, instrument).
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
    /// The window handed to the execution edge. Tagged by the UP token so the sequence proves it
    /// names the same market the subscribe that follows it does.
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
        DriverEffect::BindExecution(assignment) => {
            Tag::BindExecution(assignment.up.token.as_str().to_owned())
        }
        DriverEffect::PersistRotation(row) => Tag::PersistRotation(row.instrument.0),
        DriverEffect::Emit(message) => match message {
            InboundMessage::MarketRotation(rotation) => Tag::Rotation(rotation.instrument.0),
            InboundMessage::BookReset(reset) => Tag::BookReset(reset.instrument.0),
            InboundMessage::Book(chunk) => match chunk.kind {
                BookChunkKind::Snapshot => Tag::Snapshot(chunk.instrument.0),
                BookChunkKind::Delta => Tag::Delta(chunk.instrument.0),
            },
            InboundMessage::Trade(trade) => Tag::Trade(trade.instrument.0),
            other => panic!("unexpected emitted message: {other:?}"),
        },
    }
}

fn tags(effects: &[DriverEffect]) -> Vec<Tag> {
    effects.iter().map(tag).collect()
}

/// A window's subscribe fans the execution binding, a `Subscribe` op, and a per-leg
/// `Rotation → PersistRotation → BookReset` for both legs (the lineage side-channel row rides
/// beside each rotation message).
///
/// The binding leads deliberately: the execution edge must know which token a leg is trading before
/// any frame on that token can reach it.
fn subscribe_tags(open_secs: i64, up: u16, down: u16) -> Vec<Tag> {
    vec![
        Tag::BindExecution(format!("{open_secs}-up")),
        Tag::Subscribe(format!("{open_secs}-up")),
        Tag::Rotation(up),
        Tag::PersistRotation(up),
        Tag::BookReset(up),
        Tag::Rotation(down),
        Tag::PersistRotation(down),
        Tag::BookReset(down),
    ]
}

#[test]
fn startup_prefetches_neighbours_then_hands_over_before_the_old_window_tears_down() {
    let mut core = core();

    // Boot mid-window A: the resolved current window subscribes at once (past its T-60s instant).
    let effects = on_resolved(&mut core, OPEN_A + 10, OPEN_A);
    assert_eq!(tags(&effects), subscribe_tags(OPEN_A, 0, 1));

    // The first tick prefetches slot A's next window (index+2) and slot B's upcoming window.
    let effects = on_tick(&mut core, OPEN_A + 10);
    assert_eq!(
        tags(&effects),
        vec![Tag::Resolve(OPEN_A + 2 * WINDOW_SECS), Tag::Resolve(OPEN_B)]
    );

    // The first venue book forwards as a Snapshot on the slot-A up instrument.
    let effects = on_frame(
        &mut core,
        OPEN_A + 11,
        &book_frame(&format!("{OPEN_A}-up"), OPEN_A + 11),
    );
    assert!(tags(&effects).contains(&Tag::Snapshot(0)));

    // Slot B's window resolves early but must NOT subscribe before its own T-60s instant.
    let held = on_resolved(&mut core, OPEN_A + 15, OPEN_B);
    assert!(
        held.is_empty(),
        "next window holds until its subscribe time"
    );

    // At B's subscribe instant (A still Active) the handover fires — this is the zero-gap moment.
    let effects = on_tick(&mut core, OPEN_B - 60);
    let effect_tags = tags(&effects);
    for expected in subscribe_tags(OPEN_B, 2, 3) {
        assert!(effect_tags.contains(&expected), "missing {expected:?}");
    }

    // Slot B's book reaches Valid (Snapshot) while slot A has not yet been unsubscribed.
    let effects = on_frame(
        &mut core,
        OPEN_B + 1,
        &book_frame(&format!("{OPEN_B}-up"), OPEN_B + 1),
    );
    assert!(tags(&effects).contains(&Tag::Snapshot(2)));

    // Only now does slot A tear down, on the definitive /book 404.
    on_tick(&mut core, CLOSE_A + 1);
    let effects = on_probe(&mut core, CLOSE_A + 2, OPEN_A, ProbeOutcome::NotFound);
    assert_eq!(
        tags(&effects),
        vec![
            Tag::Unsubscribe(format!("{OPEN_A}-up")),
            Tag::BookReset(0),
            Tag::BookReset(1),
        ]
    );
}

#[test]
fn reconnect_re_baselines_every_live_leg() {
    let mut core = core();
    on_resolved(&mut core, OPEN_A + 10, OPEN_A);
    on_frame(
        &mut core,
        OPEN_A + 11,
        &book_frame(&format!("{OPEN_A}-up"), OPEN_A + 11),
    );

    // A fresh socket resets both live legs and emits a BookReset per instrument.
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

    // The already-seen up book now forwards as a fresh Snapshot again, not a silent validation.
    let effects = on_frame(
        &mut core,
        OPEN_A + 21,
        &book_frame(&format!("{OPEN_A}-up"), OPEN_A + 21),
    );
    assert!(tags(&effects).contains(&Tag::Snapshot(0)));
}

#[test]
fn occupied_slot_is_force_torn_down_at_the_next_subscribe() {
    let mut core = core();
    on_resolved(&mut core, OPEN_A + 10, OPEN_A);
    // Drive window A into its grace tail but never confirm teardown, so slot A stays occupied.
    on_tick(&mut core, CLOSE_A + 1);

    // Slot A's next window (index+2) resolves; its subscribe instant finds the slot still occupied.
    let next_open = OPEN_A + 2 * WINDOW_SECS;
    let effects = on_resolved(&mut core, next_open - 60, next_open);
    let effect_tags = tags(&effects);

    assert!(effect_tags.contains(&Tag::ForcedTeardown(OPEN_A)));
    assert_eq!(core.force_teardown_count(), 1);
    // The eviction (unsubscribe + reset of both legs) precedes the new window's subscribe.
    let evict = effect_tags
        .iter()
        .position(|t| *t == Tag::Unsubscribe(format!("{OPEN_A}-up")))
        .expect("old window unsubscribed");
    let resubscribe = effect_tags
        .iter()
        .position(|t| *t == Tag::Subscribe(format!("{next_open}-up")))
        .expect("new window subscribed");
    assert!(evict < resubscribe, "eviction precedes the new subscribe");
}

/// The shadow comparison is order-sensitive (venue-native ascending), while parse emits bids
/// best-first — the driver must reverse them before validating. If it stopped, every repeated
/// multi-level cut would mismatch and the three-cut confirm would churn BookReset + resnapshot
/// ~every third venue frame. So: an identical deep book repeated past the confirm threshold must
/// validate silently — no Diverged, no BookReset, nothing re-forwarded.
#[test]
fn a_repeated_deep_book_validates_without_reset_churn() {
    let mut core = core();
    on_resolved(&mut core, OPEN_A + 10, OPEN_A);
    let frame = book_frame(&format!("{OPEN_A}-up"), OPEN_A + 11);
    let effects = on_frame(&mut core, OPEN_A + 11, &frame);
    assert!(
        tags(&effects).contains(&Tag::Snapshot(0)),
        "first deep book forwards as Snapshot"
    );

    for repeat in 1..=3 {
        let effects = on_frame(&mut core, OPEN_A + 11 + repeat, &frame);
        let effect_tags = tags(&effects);
        assert!(
            !effect_tags.iter().any(|tag| matches!(
                tag,
                Tag::Diverged(_) | Tag::BookReset(_) | Tag::Snapshot(_) | Tag::Delta(_)
            )),
            "repeat {repeat} must validate silently, got {effect_tags:?}"
        );
    }
}

#[test]
fn identical_event_sequence_replays_identical_effects() {
    let replay = || {
        let mut core = core();
        let mut log = Vec::new();
        log.extend(on_resolved(&mut core, OPEN_A + 10, OPEN_A));
        log.extend(on_tick(&mut core, OPEN_A + 10));
        log.extend(on_frame(
            &mut core,
            OPEN_A + 11,
            &book_frame(&format!("{OPEN_A}-up"), OPEN_A + 11),
        ));
        log.extend(on_resolved(&mut core, OPEN_A + 15, OPEN_B));
        log.extend(on_tick(&mut core, OPEN_B - 60));
        log.extend(on_tick(&mut core, CLOSE_A + 1));
        log.extend(on_probe(
            &mut core,
            CLOSE_A + 2,
            OPEN_A,
            ProbeOutcome::NotFound,
        ));
        log
    };
    assert_eq!(replay(), replay());
}
