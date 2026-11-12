//! Monitor fit fitness: which columns a channel row shows, and how tall each band is, for any pane
//! the shell can hand the monitor. A rule that let a column overlap its neighbour, or starved a
//! band of rows, would break only at the pane sizes nobody tried by hand, and the painter would
//! hide it behind a clipped or empty band.

use proptest::prelude::*;

use polysim::workstation::monitor::fit::{
    BandHeights, BandSizes, ColumnPlan, ColumnWidths, TimeColumn, content_width, fit_bands,
    fit_columns,
};
use polysim::workstation::monitor::view::Channel;

use crate::workstation_support::NamedCase;

/// The column widths the browser workstation passes in. They mirror its theme metrics, which exist
/// only in the wasm build.
fn workstation_widths() -> ColumnWidths {
    ColumnWidths {
        pad: 8.0,
        time: 84.0,
        short_time: 58.0,
        liquidity: 30.0,
        side: 42.0,
        price: 72.0,
        qty: 72.0,
        fee: 113.0,
        system_description: 120.0,
        order_description: 200.0,
    }
}

/// The band sizes the browser workstation passes in, mirroring its theme metrics the same way.
fn workstation_sizes() -> BandSizes {
    BandSizes {
        status: 26.0,
        account: 98.0,
        summary: 56.0,
        tabs: 36.0,
        feature_pitch: 22.0,
        channel_pitch: 18.0,
        feature_fraction: 0.43,
    }
}

const fn plan(time: TimeColumn, has_liquidity: bool, has_fee: bool) -> ColumnPlan {
    ColumnPlan {
        time,
        has_liquidity,
        has_fee,
    }
}

/// What a row of that channel shows when nothing is dropped.
fn fullest(channel: Channel) -> ColumnPlan {
    let is_fill = channel == Channel::TradeFills;
    plan(TimeColumn::Full, is_fill, is_fill)
}

/// How much of each optional part a plan keeps, so a part that is kept more compares higher.
fn kept(plan: ColumnPlan) -> [u8; 3] {
    let time = match plan.time {
        TimeColumn::Hidden => 0,
        TimeColumn::Short => 1,
        TimeColumn::Full => 2,
    };
    [time, u8::from(plan.has_liquidity), u8::from(plan.has_fee)]
}

struct ColumnOutcome {
    pane: f32,
    system: ColumnPlan,
    orders: ColumnPlan,
    pub_trades: ColumnPlan,
    trade_fills: ColumnPlan,
}

impl ColumnOutcome {
    fn plan_for(&self, channel: Channel) -> ColumnPlan {
        match channel {
            Channel::System => self.system,
            Channel::Orders => self.orders,
            Channel::PubTrades => self.pub_trades,
            Channel::TradeFills => self.trade_fills,
        }
    }
}

#[test]
fn the_row_fits_its_columns_to_the_pane() {
    fn the_content_column_is_capped(name: &str) {
        for (pane, expected) in [
            (300.0, 300.0),
            (480.0, 480.0),
            (600.0, 480.0),
            (2400.0, 480.0),
        ] {
            assert_eq!(content_width(pane), expected, "{name}: a {pane} pane");
        }
    }

    fn columns_drop_by_priority_as_the_pane_narrows(name: &str) {
        use TimeColumn::{Full, Hidden, Short};

        let outcomes = [
            ColumnOutcome {
                pane: 300.0,
                system: plan(Full, false, false),
                orders: plan(Hidden, false, false),
                pub_trades: plan(Full, false, false),
                trade_fills: plan(Full, false, false),
            },
            ColumnOutcome {
                pane: 320.0,
                system: plan(Full, false, false),
                orders: plan(Short, false, false),
                pub_trades: plan(Full, false, false),
                trade_fills: plan(Full, true, false),
            },
            ColumnOutcome {
                pane: 400.0,
                system: plan(Full, false, false),
                orders: plan(Full, false, false),
                pub_trades: plan(Full, false, false),
                trade_fills: plan(Full, true, false),
            },
            // The widest row, a fill with its fee, needs 413 points, and a pane adds 16 of padding.
            ColumnOutcome {
                pane: 428.0,
                system: plan(Full, false, false),
                orders: plan(Full, false, false),
                pub_trades: plan(Full, false, false),
                trade_fills: plan(Full, true, false),
            },
            ColumnOutcome {
                pane: 429.0,
                system: plan(Full, false, false),
                orders: plan(Full, false, false),
                pub_trades: plan(Full, false, false),
                trade_fills: plan(Full, true, true),
            },
            ColumnOutcome {
                pane: 600.0,
                system: plan(Full, false, false),
                orders: plan(Full, false, false),
                pub_trades: plan(Full, false, false),
                trade_fills: plan(Full, true, true),
            },
            ColumnOutcome {
                pane: 1200.0,
                system: plan(Full, false, false),
                orders: plan(Full, false, false),
                pub_trades: plan(Full, false, false),
                trade_fills: plan(Full, true, true),
            },
            ColumnOutcome {
                pane: 2400.0,
                system: plan(Full, false, false),
                orders: plan(Full, false, false),
                pub_trades: plan(Full, false, false),
                trade_fills: plan(Full, true, true),
            },
        ];
        let widths = workstation_widths();
        for outcome in &outcomes {
            for channel in Channel::ALL {
                assert_eq!(
                    fit_columns(channel, outcome.pane, &widths),
                    outcome.plan_for(channel),
                    "{name}: {} in a {} pane",
                    channel.label(),
                    outcome.pane
                );
            }
        }
    }

    let cases: &[NamedCase] = &[
        ("the content column is capped", the_content_column_is_capped),
        (
            "columns drop by priority as the pane narrows",
            columns_drop_by_priority_as_the_pane_narrows,
        ),
    ];
    for (name, case) in cases {
        case(name);
    }
}

proptest! {
    /// FITNESS: a wider pane never shows fewer columns. A row that lost a column as the pane grew
    /// would make a detail appear and vanish while the operator resizes, and the lost column is
    /// the one they were reading.
    #[test]
    fn a_wider_pane_never_shows_fewer_columns(first in 300.0f32..=2400.0, second in 300.0f32..=2400.0) {
        let (narrow, wide) = if first <= second { (first, second) } else { (second, first) };
        let widths = workstation_widths();
        for channel in Channel::ALL {
            let before = kept(fit_columns(channel, narrow, &widths));
            let after = kept(fit_columns(channel, wide, &widths));
            for (part, (&was, &now)) in before.iter().zip(&after).enumerate() {
                prop_assert!(
                    now >= was,
                    "{} lost part {part} going from a {narrow} to a {wide} pane: {before:?} then {after:?}",
                    channel.label()
                );
            }
        }
    }

    /// FITNESS: from a 430 pane every channel shows every column. The cap on the content column
    /// must stay above the widest row, or a big screen would hide the fee it has room for.
    #[test]
    fn a_roomy_pane_shows_every_column(pane in 430.0f32..=2400.0) {
        let widths = workstation_widths();
        for channel in Channel::ALL {
            prop_assert_eq!(
                fit_columns(channel, pane, &widths),
                fullest(channel),
                "{} in a {} pane",
                channel.label(),
                pane
            );
        }
    }
}

/// The shortest pane that shows every band whole: the status strip, the account, the summary and
/// the tabs (26 + 98 + 56 + 36) plus three feature rows of 22 and four channel rows of 18.
const WHOLE_MIN: f32 = 354.0;

/// The bands are f32 sums, so a total can miss its pane by rounding. This is far below a pixel.
const TOLERANCE: f32 = 1e-3;

const FEATURE_ROW: f32 = 22.0;
const CHANNEL_ROW: f32 = 18.0;

fn is_close(actual: f32, expected: f32) -> bool {
    (actual - expected).abs() <= TOLERANCE
}

struct HeightCase {
    pane: f32,
    is_scrolled: bool,
    features: f32,
    channel: f32,
}

fn heights_at(pane: f32, feature_count: usize) -> BandHeights {
    fit_bands(pane, feature_count, &workstation_sizes())
}

/// The least that any outer scroll offset leaves on screen of the bands that are not lists. A
/// touch drag over a list scrolls the list, so a touch user can move the outer content only by
/// dragging account, summary or tabs. Checking only the offsets where a window edge meets a band
/// edge is exact, because the visible amount changes in straight lines between them.
fn least_outer_band_on_screen(heights: &BandHeights, viewport: f32) -> f32 {
    let account = (0.0, heights.account);
    let summary = (account.1, account.1 + heights.summary);
    let features = (summary.1, summary.1 + heights.features);
    let tabs = (features.1, features.1 + heights.tabs);
    let content = tabs.1 + heights.channel;
    let max_offset = content - viewport;
    let edges = [account.0, account.1, summary.1, features.1, tabs.1, content];
    let offsets = edges
        .into_iter()
        .flat_map(|edge| [edge, edge - viewport])
        .map(|offset| offset.clamp(0.0, max_offset));
    let overlap = |window: (f32, f32), band: (f32, f32)| {
        (window.1.min(band.1) - window.0.max(band.0)).max(0.0)
    };
    offsets
        .map(|offset| {
            let window = (offset, offset + viewport);
            [account, summary, tabs]
                .into_iter()
                .map(|band| overlap(window, band))
                .sum::<f32>()
        })
        .fold(f32::INFINITY, f32::min)
}

#[test]
fn the_bands_share_the_pane_height() {
    fn heights_follow_the_rule_at_fixed_panes(name: &str) {
        // The 120 pane is below the 220 floor the shell promises. The channel still keeps its four
        // rows there.
        let cases = [
            HeightCase {
                pane: 120.0,
                is_scrolled: true,
                features: 58.0,
                channel: 72.0,
            },
            HeightCase {
                pane: 220.0,
                is_scrolled: true,
                features: 158.0,
                channel: 158.0,
            },
            HeightCase {
                pane: 353.0,
                is_scrolled: true,
                features: 264.0,
                channel: 291.0,
            },
            HeightCase {
                pane: 354.0,
                is_scrolled: false,
                features: 66.0,
                channel: 72.0,
            },
            HeightCase {
                pane: 360.0,
                is_scrolled: false,
                features: 66.0,
                channel: 78.0,
            },
            HeightCase {
                pane: 600.0,
                is_scrolled: false,
                features: 165.12,
                channel: 218.88,
            },
            HeightCase {
                pane: 900.0,
                is_scrolled: false,
                features: 264.0,
                channel: 420.0,
            },
            HeightCase {
                pane: 1000.0,
                is_scrolled: false,
                features: 264.0,
                channel: 520.0,
            },
            HeightCase {
                pane: 1600.0,
                is_scrolled: false,
                features: 264.0,
                channel: 1120.0,
            },
        ];
        for case in cases {
            let heights = heights_at(case.pane, 12);
            let pane = case.pane;
            assert_eq!(
                heights.is_scrolled, case.is_scrolled,
                "{name}: scrolling at {pane}"
            );
            assert!(
                is_close(heights.status, 26.0),
                "{name}: status at {pane}: {heights:?}"
            );
            assert!(
                is_close(heights.account, 98.0),
                "{name}: account at {pane}: {heights:?}"
            );
            assert!(
                is_close(heights.summary, 56.0),
                "{name}: summary at {pane}: {heights:?}"
            );
            assert!(
                is_close(heights.tabs, 36.0),
                "{name}: tabs at {pane}: {heights:?}"
            );
            assert!(
                is_close(heights.features, case.features),
                "{name}: features at {pane}: {heights:?}"
            );
            assert!(
                is_close(heights.channel, case.channel),
                "{name}: channel at {pane}: {heights:?}"
            );
        }
    }

    fn few_features_take_only_the_rows_they_need(name: &str) {
        // At 1000 the share of the free height is 337.12, so 15 rows (330) fit under it and 16
        // (352) hit it. No features, or one, still holds a row for the empty note.
        let cases = [
            (0, 22.0),
            (1, 22.0),
            (2, 44.0),
            (3, 66.0),
            (15, 330.0),
            (16, 337.12),
        ];
        for (count, features) in cases {
            let heights = heights_at(1000.0, count);
            assert!(
                is_close(heights.features, features),
                "{name}: {count} features: {heights:?}"
            );
            assert!(
                is_close(heights.channel, 784.0 - features),
                "{name}: the channel takes the rest with {count} features: {heights:?}"
            );
        }
    }

    fn the_bands_show_whole_from_exactly_the_smallest_whole_pane(name: &str) {
        for whole_points in 220..=1600 {
            let pane = whole_points as f32;
            for count in [0, 1, 3, 12, 40] {
                assert_eq!(
                    heights_at(pane, count).is_scrolled,
                    pane < WHOLE_MIN,
                    "{name}: a {pane} pane with {count} features"
                );
            }
        }
    }

    let cases: &[NamedCase] = &[
        (
            "heights follow the rule at fixed panes",
            heights_follow_the_rule_at_fixed_panes,
        ),
        (
            "few features take only the rows they need",
            few_features_take_only_the_rows_they_need,
        ),
        (
            "the bands show whole from exactly the smallest whole pane",
            the_bands_show_whole_from_exactly_the_smallest_whole_pane,
        ),
    ];
    for (name, case) in cases {
        case(name);
    }
}

proptest! {
    /// FITNESS: with room, the bands fill the pane exactly, the features keep their floor and never
    /// take more than they need, and the channel keeps its floor whatever share of the free height
    /// the features are offered. A gap would show as dead space under the channel, and an overlap
    /// would paint one band over another.
    #[test]
    fn whole_bands_fill_the_pane(
        pane in WHOLE_MIN..=1600.0f32,
        feature_count in 0usize..=60,
        feature_fraction in 0.0f32..=1.0,
    ) {
        let sizes = BandSizes { feature_fraction, ..workstation_sizes() };
        let heights = fit_bands(pane, feature_count, &sizes);
        let needed = feature_count.max(1) as f32 * FEATURE_ROW;
        prop_assert!(!heights.is_scrolled, "a {pane} pane scrolls: {heights:?}");
        let total = heights.status + heights.content_height();
        prop_assert!(is_close(total, pane), "the bands total {total} in a {pane} pane: {heights:?}");
        prop_assert!(
            heights.features <= needed + TOLERANCE,
            "{feature_count} features take {} of the {needed} they need",
            heights.features
        );
        prop_assert!(
            heights.features >= needed.min(3.0 * FEATURE_ROW) - TOLERANCE,
            "{feature_count} features keep fewer than three rows: {heights:?}"
        );
        prop_assert!(
            heights.channel >= 4.0 * CHANNEL_ROW - TOLERANCE,
            "the channel keeps fewer than four rows: {heights:?}"
        );
    }

    /// FITNESS: a short pane scrolls, its lists never fill the whole view, and the channel keeps
    /// its floor. Were a list as tall as the view, a touch drag anywhere would scroll the list, and
    /// the operator could not reach the bands past it.
    #[test]
    fn scrolled_bands_always_leave_something_to_drag(
        pane in 220.0f32..WHOLE_MIN,
        feature_count in 0usize..=60,
    ) {
        let sizes = workstation_sizes();
        let heights = heights_at(pane, feature_count);
        let viewport = pane - sizes.status;
        prop_assert!(heights.is_scrolled, "a {pane} pane does not scroll: {heights:?}");
        prop_assert!(
            heights.features <= viewport - sizes.tabs + TOLERANCE,
            "features take {} of {viewport}: {heights:?}",
            heights.features
        );
        prop_assert!(
            heights.channel >= 4.0 * CHANNEL_ROW - TOLERANCE,
            "the channel keeps fewer than four rows: {heights:?}"
        );
        prop_assert!(
            heights.content_height() > viewport,
            "the content fits the {viewport} view, so nothing scrolls: {heights:?}"
        );
        let least = least_outer_band_on_screen(&heights, viewport);
        prop_assert!(
            least >= sizes.tabs - TOLERANCE,
            "an offset leaves {least} of the draggable bands on screen: {heights:?}"
        );
    }
}
