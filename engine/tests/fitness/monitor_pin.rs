//! Monitor pin fitness: a tap pins one detail, and a second tap on it, or a tap on nothing, lets go.
//! A pin that stuck, stacked or leaked into the tab and scroll state would leave a touch operator
//! with a record they cannot dismiss. The pin is private, so these rules are observable only
//! through the state's equality.

use polysim::ids::Side;
use polysim::msg::persist::FeatureId;
use polysim::workstation::monitor::view::{Channel, MonitorDetail, MonitorUiState};

use crate::workstation_support::NamedCase;

/// One of each kind, and pairs that differ in only one part: the side, the feature, the ordinal and
/// the channel.
const DETAILS: [MonitorDetail; 7] = [
    MonitorDetail::Delta(Side::Buy),
    MonitorDetail::Delta(Side::Sell),
    MonitorDetail::Feature(FeatureId(2)),
    MonitorDetail::Feature(FeatureId(3)),
    MonitorDetail::Row {
        channel: Channel::Orders,
        ordinal: 7,
    },
    MonitorDetail::Row {
        channel: Channel::Orders,
        ordinal: 8,
    },
    MonitorDetail::Row {
        channel: Channel::PubTrades,
        ordinal: 7,
    },
];

fn tapped(targets: &[Option<MonitorDetail>]) -> MonitorUiState {
    tapped_from(MonitorUiState::new(), targets)
}

fn tapped_from(mut state: MonitorUiState, targets: &[Option<MonitorDetail>]) -> MonitorUiState {
    for target in targets {
        state.tap(*target);
    }
    state
}

#[test]
fn a_tap_pins_one_detail_and_a_second_tap_lets_go() {
    fn one_tap_pins_and_a_second_tap_on_it_restores_the_fresh_state(name: &str) {
        for detail in DETAILS {
            assert_ne!(
                tapped(&[Some(detail)]),
                MonitorUiState::new(),
                "{name}: a tap on {detail:?} pins it"
            );
            assert_eq!(
                tapped(&[Some(detail), Some(detail)]),
                MonitorUiState::new(),
                "{name}: a second tap on {detail:?} lets go"
            );
        }
    }

    fn distinct_details_pin_distinct_states(name: &str) {
        for (first_index, first) in DETAILS.iter().enumerate() {
            for second in &DETAILS[first_index + 1..] {
                assert_ne!(
                    tapped(&[Some(*first)]),
                    tapped(&[Some(*second)]),
                    "{name}: {first:?} and {second:?} must not look the same"
                );
            }
        }
    }

    fn a_tap_on_another_detail_moves_the_pin_without_stacking(name: &str) {
        for first in DETAILS {
            for second in DETAILS.into_iter().filter(|detail| *detail != first) {
                assert_eq!(
                    tapped(&[Some(first), Some(second)]),
                    tapped(&[Some(second)]),
                    "{name}: {first:?} then {second:?} leaves only {second:?}"
                );
            }
        }
    }

    fn a_tap_on_nothing_clears_and_clearing_nothing_changes_nothing(name: &str) {
        for detail in DETAILS {
            assert_eq!(
                tapped(&[Some(detail), None]),
                MonitorUiState::new(),
                "{name}: a tap elsewhere clears {detail:?}"
            );
        }
        assert_eq!(
            tapped(&[None]),
            MonitorUiState::new(),
            "{name}: nothing was pinned"
        );
        assert_eq!(
            tapped(&[None, None]),
            MonitorUiState::new(),
            "{name}: still nothing"
        );
    }

    fn a_pin_leaves_the_tab_and_follow_state_alone(name: &str) {
        let mut scrolled = MonitorUiState::new();
        scrolled.active_tab = Channel::Orders;
        scrolled.set_scrolled_away(Channel::Orders, 5, 40.0);
        for detail in DETAILS {
            assert_ne!(
                tapped_from(scrolled.clone(), &[Some(detail)]),
                scrolled,
                "{name}: {detail:?} is pinned on the scrolled state"
            );
            assert_eq!(
                tapped_from(scrolled.clone(), &[Some(detail), Some(detail)]),
                scrolled,
                "{name}: letting go of {detail:?} restores the tab and scroll exactly"
            );
            assert_eq!(
                tapped_from(scrolled.clone(), &[Some(detail), None]),
                scrolled,
                "{name}: clearing {detail:?} restores the tab and scroll exactly"
            );
        }
    }

    let cases: &[NamedCase] = &[
        (
            "one tap pins and a second tap on it restores the fresh state",
            one_tap_pins_and_a_second_tap_on_it_restores_the_fresh_state,
        ),
        (
            "distinct details pin distinct states",
            distinct_details_pin_distinct_states,
        ),
        (
            "a tap on another detail moves the pin without stacking",
            a_tap_on_another_detail_moves_the_pin_without_stacking,
        ),
        (
            "a tap on nothing clears, and clearing nothing changes nothing",
            a_tap_on_nothing_clears_and_clearing_nothing_changes_nothing,
        ),
        (
            "a pin leaves the tab and follow state alone",
            a_pin_leaves_the_tab_and_follow_state_alone,
        ),
    ];
    for (name, case) in cases {
        case(name);
    }
}
