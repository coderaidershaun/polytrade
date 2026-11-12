//! Link bar fitness: what the bar shows at each width, and when its run button sends. The bar is
//! the one strip that says whether the engine is answering and lets the operator stop it, and both
//! of its jobs fail silently. A chip that is dropped, or painted over its neighbour, at a narrow
//! width leaves an operator reading a bar that hides the connection or the run state. A confirm
//! that survives a change of engine or run state sends START or STOP to something the operator
//! never looked at. Driven only through the public API, with every expected value written out as a
//! literal.

use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

use polysim::link::{RunPhase, RunState};
use polysim::time::DurationUs;
use polysim::workstation::link_model::{ConnectionState, ControlVerdict, LinkStatus};
use polysim::workstation::link_view::{ChipKind, LinkBarLayout, PlacedChip, RunConfirm};
use proptest::prelude::*;

/// The bar's monospace font, Hack at 11 points, advances this far per character, so a page width
/// divides into whole columns.
const COLUMN_PT: f32 = 6.62;

const fn columns_at(width_pt: f32) -> usize {
    (width_pt / COLUMN_PT) as usize
}

/// The narrowest page the workstation supports, in columns.
const NARROWEST_PAGE: usize = AT_320;
const BUTTON_COLUMNS: usize = 15;
const LEFT_PAD: usize = 2;

/// Most important first. The bar shortens and then drops from the end of this list.
const PRIORITY: [ChipKind; 5] = [
    ChipKind::Peer,
    ChipKind::Connection,
    ChipKind::Run,
    ChipKind::Control,
    ChipKind::Phase,
];

const DISPLAY: [ChipKind; 5] = [
    ChipKind::Peer,
    ChipKind::Connection,
    ChipKind::Phase,
    ChipKind::Run,
    ChipKind::Control,
];

fn on_loopback(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn engine() -> LinkStatus {
    LinkStatus {
        peer: on_loopback(9310),
        peer_index: 0,
        peer_count: 1,
        session: 0,
        connection: ConnectionState::Live,
        phase: None,
        reported_state: None,
        asserted_state: None,
        control: ControlVerdict::NoOpinion,
    }
}

fn two_engines() -> LinkStatus {
    LinkStatus {
        peer_count: 2,
        ..engine()
    }
}

const AT_320: usize = columns_at(320.0);
const AT_480: usize = columns_at(480.0);
const AT_800: usize = columns_at(800.0);
const AT_1600: usize = columns_at(1600.0);

/// What the bar should show for a status at a width: each chip's text in display order, joined by
/// " | ".
struct Row {
    name: &'static str,
    status: LinkStatus,
    columns: usize,
    shows: &'static str,
}

fn row(name: &'static str, status: LinkStatus, columns: usize, shows: &'static str) -> Row {
    Row {
        name,
        status,
        columns,
        shows,
    }
}

fn waiting() -> LinkStatus {
    LinkStatus {
        connection: ConnectionState::Connecting,
        ..engine()
    }
}

fn running_ready() -> LinkStatus {
    LinkStatus {
        phase: Some(RunPhase::Ready),
        reported_state: Some(RunState::Running),
        ..two_engines()
    }
}

fn asserting_idle() -> LinkStatus {
    LinkStatus {
        asserted_state: Some(RunState::Idle),
        control: ControlVerdict::Pending,
        ..running_ready()
    }
}

fn lost_control() -> LinkStatus {
    LinkStatus {
        control: ControlVerdict::Lost {
            holder_epoch: 1_761_508_800_123_457,
        },
        ..asserting_idle()
    }
}

fn stale_stopped() -> LinkStatus {
    LinkStatus {
        connection: ConnectionState::Stale,
        phase: Some(RunPhase::Stopped),
        reported_state: Some(RunState::Idle),
        asserted_state: Some(RunState::Idle),
        control: ControlVerdict::Applied,
        ..two_engines()
    }
}

fn starting_idle() -> LinkStatus {
    LinkStatus {
        phase: Some(RunPhase::Starting),
        reported_state: Some(RunState::Idle),
        ..engine()
    }
}

fn draining_running() -> LinkStatus {
    LinkStatus {
        phase: Some(RunPhase::Draining),
        reported_state: Some(RunState::Running),
        asserted_state: Some(RunState::Idle),
        control: ControlVerdict::Pending,
        ..engine()
    }
}

fn third_of_three_on_ipv6() -> LinkStatus {
    LinkStatus {
        peer: "[::1]:9311".parse().expect("the fixture peer is a literal"),
        peer_index: 2,
        peer_count: 3,
        reported_state: Some(RunState::Running),
        ..engine()
    }
}

fn rows() -> Vec<Row> {
    vec![
        row("waiting", waiting(), AT_320, ":9310 | CONN | run --"),
        row(
            "waiting",
            waiting(),
            AT_480,
            "engine 127.0.0.1:9310 | CONN | run -- | control none",
        ),
        row(
            "waiting",
            waiting(),
            AT_800,
            "engine 127.0.0.1:9310 | CONNECTING | run -- | control none",
        ),
        row(
            "waiting",
            waiting(),
            AT_1600,
            "engine 127.0.0.1:9310 | CONNECTING | run -- | control none",
        ),
        row(
            "running, no opinion",
            running_ready(),
            AT_320,
            ":9310 [1/2] | LIVE | RUNNING",
        ),
        row(
            "running, no opinion",
            running_ready(),
            60,
            ":9310 [1/2] | LIVE | RUNNING | control none",
        ),
        row(
            "running, no opinion",
            running_ready(),
            AT_480,
            ":9310 [1/2] | LIVE | ready | RUNNING | control none",
        ),
        row(
            "running, no opinion",
            running_ready(),
            AT_800,
            "engine 127.0.0.1:9310 [1/2] | LIVE | ready | run RUNNING | control none",
        ),
        row(
            "running, no opinion",
            running_ready(),
            AT_1600,
            "engine 127.0.0.1:9310 [1/2] | LIVE | ready | run RUNNING | control none",
        ),
        row(
            "asserting idle",
            asserting_idle(),
            AT_320,
            ":9310 [1/2] | LIVE | RUNNING",
        ),
        row(
            "asserting idle",
            asserting_idle(),
            AT_480,
            ":9310 [1/2] | LIVE | ready | RUNNING | asserting IDLE",
        ),
        row(
            "asserting idle",
            asserting_idle(),
            AT_800,
            "engine 127.0.0.1:9310 [1/2] | LIVE | ready | run RUNNING | control asserting IDLE",
        ),
        row(
            "asserting idle",
            asserting_idle(),
            AT_1600,
            "engine 127.0.0.1:9310 [1/2] | LIVE | ready | run RUNNING | control asserting IDLE",
        ),
        row(
            "control lost",
            lost_control(),
            AT_320,
            ":9310 [1/2] | LIVE | RUNNING",
        ),
        row(
            "control lost",
            lost_control(),
            AT_480,
            ":9310 [1/2] | LIVE | ready | RUNNING | control LOST",
        ),
        row(
            "control lost",
            lost_control(),
            AT_800,
            "engine 127.0.0.1:9310 [1/2] | LIVE | ready | run RUNNING | \
             control LOST to epoch 1761508800123457",
        ),
        row(
            "control lost",
            lost_control(),
            AT_1600,
            "engine 127.0.0.1:9310 [1/2] | LIVE | ready | run RUNNING | \
             control LOST to epoch 1761508800123457",
        ),
        row(
            "stale and stopped",
            stale_stopped(),
            AT_320,
            ":9310 [1/2] | STALE | IDLE",
        ),
        row(
            "stale and stopped",
            stale_stopped(),
            AT_480,
            ":9310 [1/2] | STALE | stopped | IDLE | applied IDLE",
        ),
        row(
            "stale and stopped",
            stale_stopped(),
            AT_800,
            "engine 127.0.0.1:9310 [1/2] | STALE | stopped | run IDLE | control applied IDLE",
        ),
        row(
            "stale and stopped",
            stale_stopped(),
            AT_1600,
            "engine 127.0.0.1:9310 [1/2] | STALE | stopped | run IDLE | control applied IDLE",
        ),
        row("starting", starting_idle(), AT_320, ":9310 | LIVE | IDLE"),
        row(
            "starting",
            starting_idle(),
            AT_480,
            ":9310 | LIVE | starting | IDLE | control none",
        ),
        row(
            "starting",
            starting_idle(),
            AT_800,
            "engine 127.0.0.1:9310 | LIVE | starting | run IDLE | control none",
        ),
        row(
            "draining",
            draining_running(),
            AT_320,
            ":9310 | LIVE | RUNNING",
        ),
        row(
            "draining",
            draining_running(),
            AT_480,
            ":9310 | LIVE | draining | RUNNING | asserting IDLE",
        ),
        row(
            "draining",
            draining_running(),
            AT_800,
            "engine 127.0.0.1:9310 | LIVE | draining | run RUNNING | control asserting IDLE",
        ),
        row(
            "third of three on ipv6",
            third_of_three_on_ipv6(),
            AT_320,
            ":9311 [3/3] | LIVE | RUNNING",
        ),
        row(
            "third of three on ipv6",
            third_of_three_on_ipv6(),
            AT_800,
            "engine [::1]:9311 [3/3] | LIVE | run RUNNING | control none",
        ),
    ]
}

#[test]
fn the_bar_shows_these_chips_at_these_widths() {
    for row in rows() {
        let layout = LinkBarLayout::fit(&row.status, row.columns);
        let shown: Vec<&str> = layout.chips.iter().map(|chip| chip.text.as_str()).collect();
        assert_eq!(
            shown.join(" | "),
            row.shows,
            "{} at {} columns",
            row.name,
            row.columns
        );
    }
}

#[test]
fn the_bar_places_each_chip_on_its_own_columns() {
    let layout = LinkBarLayout::fit(&running_ready(), AT_480);
    let placed: Vec<(ChipKind, &str, usize)> = layout
        .chips
        .iter()
        .map(|chip| (chip.kind, chip.text.as_str(), chip.column))
        .collect();
    assert_eq!(
        placed,
        [
            (ChipKind::Peer, ":9310 [1/2]", 2),
            (ChipKind::Connection, "LIVE", 15),
            (ChipKind::Phase, "ready", 21),
            (ChipKind::Run, "RUNNING", 28),
            (ChipKind::Control, "control none", 37),
        ]
    );
    assert_eq!(layout.button_column, 57);
}

fn at(millis: i64) -> DurationUs {
    DurationUs::from_millis(millis)
}

/// A running engine, so the press stands for STOP.
fn stoppable() -> LinkStatus {
    LinkStatus {
        reported_state: Some(RunState::Running),
        ..engine()
    }
}

/// An idle engine, so the press stands for START.
fn startable() -> LinkStatus {
    LinkStatus {
        reported_state: Some(RunState::Idle),
        ..engine()
    }
}

#[test]
fn the_first_press_arms_and_sends_nothing() {
    let mut confirm = RunConfirm::default();
    assert_eq!(confirm.press(&stoppable(), at(1_000)), None);
}

#[test]
fn a_second_press_inside_the_window_stops_a_running_engine() {
    let mut confirm = RunConfirm::default();
    confirm.press(&stoppable(), at(1_000));
    assert_eq!(confirm.press(&stoppable(), at(3_900)), Some(RunState::Idle));
}

#[test]
fn a_second_press_inside_the_window_starts_an_idle_engine() {
    let mut confirm = RunConfirm::default();
    confirm.press(&startable(), at(1_000));
    assert_eq!(
        confirm.press(&startable(), at(3_900)),
        Some(RunState::Running)
    );
}

#[test]
fn a_press_after_a_send_arms_again_instead_of_sending_twice() {
    let mut confirm = RunConfirm::default();
    confirm.press(&stoppable(), at(0));
    assert_eq!(confirm.press(&stoppable(), at(500)), Some(RunState::Idle));
    assert_eq!(confirm.press(&stoppable(), at(1_000)), None);
}

#[test]
fn a_second_press_just_inside_the_window_sends() {
    let mut confirm = RunConfirm::default();
    confirm.press(&stoppable(), at(0));
    assert_eq!(confirm.press(&stoppable(), at(2_999)), Some(RunState::Idle));
}

#[test]
fn a_second_press_at_the_end_of_the_window_arms_again() {
    let mut confirm = RunConfirm::default();
    confirm.press(&stoppable(), at(0));
    assert_eq!(confirm.press(&stoppable(), at(3_000)), None);
    assert_eq!(
        confirm.press(&stoppable(), at(3_000 + 2_900)),
        Some(RunState::Idle)
    );
}

#[test]
fn a_press_long_after_the_window_arms_again() {
    let mut confirm = RunConfirm::default();
    confirm.press(&stoppable(), at(0));
    assert_eq!(confirm.press(&stoppable(), at(60_000)), None);
}

#[test]
fn a_reading_earlier_than_the_arming_does_not_send() {
    let mut confirm = RunConfirm::default();
    confirm.press(&stoppable(), at(5_000));
    assert_eq!(confirm.press(&stoppable(), at(4_000)), None);
}

#[test]
fn a_press_after_the_run_state_changed_arms_for_the_new_target() {
    let mut confirm = RunConfirm::default();
    confirm.press(&stoppable(), at(0));
    assert_eq!(confirm.press(&startable(), at(1_000)), None);
    assert_eq!(
        confirm.press(&startable(), at(2_000)),
        Some(RunState::Running)
    );
}

#[test]
fn a_press_on_another_engine_does_not_send() {
    let other = LinkStatus {
        peer: on_loopback(9311),
        ..stoppable()
    };
    let mut confirm = RunConfirm::default();
    confirm.press(&stoppable(), at(0));
    assert_eq!(confirm.press(&other, at(1_000)), None);
}

#[test]
fn a_press_in_a_new_session_does_not_send() {
    let moved = LinkStatus {
        session: 1,
        ..stoppable()
    };
    let mut confirm = RunConfirm::default();
    confirm.press(&stoppable(), at(0));
    assert_eq!(confirm.press(&moved, at(1_000)), None);
}

#[test]
fn a_press_sends_when_only_the_reports_changed_and_the_target_did_not() {
    let reported_idle_after_asserting_running = LinkStatus {
        reported_state: Some(RunState::Idle),
        asserted_state: Some(RunState::Running),
        ..engine()
    };
    let mut confirm = RunConfirm::default();
    confirm.press(&stoppable(), at(0));
    assert_eq!(
        confirm.press(&reported_idle_after_asserting_running, at(1_000)),
        Some(RunState::Idle)
    );
}

fn arb_peer() -> impl Strategy<Value = SocketAddr> {
    prop_oneof![
        (any::<[u8; 4]>(), any::<u16>())
            .prop_map(|(octets, port)| SocketAddr::from((octets, port))),
        (any::<[u16; 8]>(), any::<u16>(), any::<u32>()).prop_map(|(segments, port, scope)| {
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(segments), port, 0, scope))
        }),
    ]
}

fn arb_connection() -> impl Strategy<Value = ConnectionState> {
    prop_oneof![
        Just(ConnectionState::Connecting),
        Just(ConnectionState::Live),
        Just(ConnectionState::Stale),
    ]
}

fn arb_run_state() -> impl Strategy<Value = RunState> {
    prop_oneof![Just(RunState::Running), Just(RunState::Idle)]
}

fn arb_phase() -> impl Strategy<Value = RunPhase> {
    prop_oneof![
        Just(RunPhase::Starting),
        Just(RunPhase::Ready),
        Just(RunPhase::Draining),
        Just(RunPhase::Stopped),
    ]
}

fn arb_control() -> impl Strategy<Value = ControlVerdict> {
    prop_oneof![
        Just(ControlVerdict::NoOpinion),
        Just(ControlVerdict::Pending),
        Just(ControlVerdict::Applied),
        any::<u64>().prop_map(|holder_epoch| ControlVerdict::Lost { holder_epoch }),
    ]
}

/// An engine reports its phase and its run state together, or not at all.
fn arb_lifecycle() -> impl Strategy<Value = (Option<RunPhase>, Option<RunState>)> {
    prop_oneof![
        Just((None, None)),
        (arb_phase(), arb_run_state()).prop_map(|(phase, state)| (Some(phase), Some(state))),
    ]
}

/// At most nine engines. With ten or more, a five-digit port and a stale running engine, the peer,
/// connection and run chips need 30 columns, and the narrowest page leaves only 29.
fn arb_status() -> impl Strategy<Value = LinkStatus> {
    (
        arb_peer(),
        1_usize..=9,
        0_usize..9,
        any::<u64>(),
        arb_connection(),
        arb_lifecycle(),
        prop::option::of(arb_run_state()),
        arb_control(),
    )
        .prop_map(
            |(
                peer,
                peer_count,
                peer_slot,
                session,
                connection,
                (phase, reported_state),
                asserted_state,
                control,
            )| LinkStatus {
                peer,
                peer_index: peer_slot % peer_count,
                peer_count,
                session,
                connection,
                phase,
                reported_state,
                asserted_state,
                control,
            },
        )
}

fn is_present(kind: ChipKind, status: &LinkStatus) -> bool {
    kind != ChipKind::Phase || status.phase.is_some()
}

fn shown(layout: &LinkBarLayout, kind: ChipKind) -> Option<&PlacedChip> {
    layout.chips.iter().find(|chip| chip.kind == kind)
}

fn width(chip: &PlacedChip) -> usize {
    chip.text.chars().count()
}

proptest! {
    #[test]
    fn the_peer_is_always_shown_and_the_button_takes_the_right_edge(
        status in arb_status(),
        columns in 0_usize..=700,
    ) {
        let layout = LinkBarLayout::fit(&status, columns);
        prop_assert!(shown(&layout, ChipKind::Peer).is_some());
        prop_assert_eq!(layout.button_column, columns.saturating_sub(BUTTON_COLUMNS));
    }

    #[test]
    fn from_the_narrowest_page_up_no_chip_overlaps_another_or_the_button(
        status in arb_status(),
        columns in NARROWEST_PAGE..=700,
    ) {
        let layout = LinkBarLayout::fit(&status, columns);
        let kinds: Vec<ChipKind> = layout.chips.iter().map(|chip| chip.kind).collect();
        let in_display_order: Vec<ChipKind> =
            DISPLAY.into_iter().filter(|kind| kinds.contains(kind)).collect();
        prop_assert_eq!(kinds, in_display_order);
        prop_assert_eq!(layout.chips.first().map(|chip| chip.column), Some(LEFT_PAD));
        for pair in layout.chips.windows(2) {
            prop_assert!(pair[0].column + width(&pair[0]) < pair[1].column, "{pair:?}");
        }
        let last = layout.chips.last().expect("the peer is always shown");
        prop_assert!(last.column + width(last) < layout.button_column, "{last:?}");
        for kind in [ChipKind::Peer, ChipKind::Connection, ChipKind::Run] {
            prop_assert!(shown(&layout, kind).is_some(), "{kind:?} was dropped");
        }
    }

    #[test]
    fn a_chip_is_shown_only_while_every_more_important_chip_is_shown(
        status in arb_status(),
        columns in 0_usize..=700,
    ) {
        let layout = LinkBarLayout::fit(&status, columns);
        for (rank, kind) in PRIORITY.iter().enumerate() {
            if shown(&layout, *kind).is_none() {
                continue;
            }
            for more_important in &PRIORITY[..rank] {
                prop_assert!(
                    !is_present(*more_important, &status) || shown(&layout, *more_important).is_some(),
                    "{kind:?} is shown but {more_important:?} is not"
                );
            }
        }
    }

    #[test]
    fn chips_shorten_before_any_chip_is_dropped(
        status in arb_status(),
        columns in 0_usize..=700,
    ) {
        let layout = LinkBarLayout::fit(&status, columns);
        let is_any_dropped = DISPLAY
            .iter()
            .any(|kind| is_present(*kind, &status) && shown(&layout, *kind).is_none());
        if is_any_dropped {
            let peer = shown(&layout, ChipKind::Peer).expect("the peer is always shown");
            prop_assert!(!peer.text.starts_with("engine "), "{peer:?} is still in full");
        }
    }

    #[test]
    fn a_wider_bar_never_hides_or_shortens_a_chip(
        status in arb_status(),
        columns in 0_usize..=700,
        extra in 0_usize..=300,
    ) {
        let narrow = LinkBarLayout::fit(&status, columns);
        let wide = LinkBarLayout::fit(&status, columns + extra);
        for chip in &narrow.chips {
            let is_kept = shown(&wide, chip.kind).is_some_and(|wider| width(wider) >= width(chip));
            prop_assert!(is_kept, "{chip:?} shrank or vanished at {extra} more columns");
        }
    }

    #[test]
    fn a_wide_enough_bar_shows_every_chip_in_full(status in arb_status()) {
        let layout = LinkBarLayout::fit(&status, AT_1600);
        prop_assert_eq!(&layout.chips, &LinkBarLayout::fit(&status, 100_000).chips);
        let present = DISPLAY.iter().filter(|kind| is_present(**kind, &status)).count();
        prop_assert_eq!(layout.chips.len(), present);
        let peer = shown(&layout, ChipKind::Peer).expect("the peer is always shown");
        prop_assert!(peer.text.starts_with("engine "), "{peer:?} is not in full");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Triple {
    state: RunState,
    peer_port: u16,
    session: u64,
}

#[derive(Debug, Clone, Copy)]
struct Press {
    triple: Triple,
    millis: i64,
    is_sent: bool,
}

/// Statuses with the target each one stands for, written out by hand. Several share a target while
/// differing in what they report, and some differ in state, engine or session.
fn variants() -> [(LinkStatus, Triple); 6] {
    let idle_on_9310 = Triple {
        state: RunState::Idle,
        peer_port: 9310,
        session: 0,
    };
    [
        (stoppable(), idle_on_9310),
        (
            LinkStatus {
                reported_state: Some(RunState::Idle),
                asserted_state: Some(RunState::Running),
                ..engine()
            },
            idle_on_9310,
        ),
        (engine(), idle_on_9310),
        (
            startable(),
            Triple {
                state: RunState::Running,
                ..idle_on_9310
            },
        ),
        (
            LinkStatus {
                peer: on_loopback(9311),
                ..stoppable()
            },
            Triple {
                peer_port: 9311,
                ..idle_on_9310
            },
        ),
        (
            LinkStatus {
                session: 1,
                ..stoppable()
            },
            Triple {
                session: 1,
                ..idle_on_9310
            },
        ),
    ]
}

proptest! {
    #[test]
    fn a_press_sends_only_to_confirm_the_press_just_before_it(
        presses in prop::collection::vec((0_usize..6, 0_i64..=40), 1..=30),
    ) {
        let variants = variants();
        let mut confirm = RunConfirm::default();
        let mut before: Option<Press> = None;
        for (variant, quarter_seconds) in presses {
            let (status, triple) = &variants[variant];
            let millis = quarter_seconds * 250;
            let sent = confirm.press(status, at(millis));
            let is_confirming = before.is_some_and(|earlier| {
                !earlier.is_sent
                    && earlier.triple == *triple
                    && (0..3_000).contains(&(millis - earlier.millis))
            });
            prop_assert_eq!(sent, is_confirming.then_some(triple.state));
            before = Some(Press { triple: *triple, millis, is_sent: sent.is_some() });
        }
    }
}
