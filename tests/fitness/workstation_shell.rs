//! Shell fitness: what the operator sees must follow from the canvas and the selection alone, and
//! never from the painters. A pane that lands outside its size envelope, two panes that overlap, or
//! a safety badge that is squeezed out of the top bar fails silently on a screen the author never
//! opened, so the arrangement is pinned as pure geometry. The navigation half pins what the URL and
//! the keys can say.

use polysim::hot::exec::{ExecHalt, HaltReason};
use polysim::time::TsUs;
use polysim::workstation::shell::{
    Area, Arrangement, Navigation, Pane, Selection, SizeClass, View, zoom_to_fit,
};
use proptest::prelude::*;

const HEIGHT: f32 = 800.0;

fn canvas(width: f32, height: f32) -> Area {
    Area {
        left: 0.0,
        top: 0.0,
        width,
        height,
    }
}

fn latched() -> Option<ExecHalt> {
    Some(ExecHalt::Halted {
        reason: HaltReason::RejectStreak,
        halted_ts_us: TsUs::from_micros(1),
    })
}

fn arrange(width: f32, height: f32, selection: Selection) -> Arrangement {
    Arrangement::new(canvas(width, height), &Navigation::new(selection), None)
}

/// Left, top, right and bottom of a pane with its header, which is what it takes from the content.
fn extent(pane: Pane) -> [f32; 4] {
    let top = pane.header.map_or(pane.body.top, |header| header.top);
    [
        pane.body.left,
        top,
        pane.body.left + pane.body.width,
        pane.body.top + pane.body.height,
    ]
}

fn shown(arrangement: &Arrangement, view: View) -> Pane {
    arrangement
        .pane(view)
        .unwrap_or_else(|| panic!("{view:?} is not shown"))
}

#[test]
fn the_key_map_is_the_position_in_the_switcher() {
    assert_eq!(
        Selection::ALL,
        [
            Selection::Desk,
            Selection::View(View::Chart),
            Selection::View(View::Ladder),
            Selection::View(View::Monitor),
        ]
    );
}

#[test]
fn a_fragment_round_trips_and_anything_unknown_means_the_desk() {
    for selection in Selection::ALL {
        assert_eq!(Selection::from_fragment(selection.fragment()), selection);
    }
    let cases = [
        ("#chart", Selection::View(View::Chart)),
        ("ladder", Selection::View(View::Ladder)),
        ("#monitor", Selection::View(View::Monitor)),
        ("#desk", Selection::Desk),
        ("#foo", Selection::Desk),
        ("#Chart", Selection::Desk),
        ("##chart", Selection::Desk),
        ("", Selection::Desk),
        ("#", Selection::Desk),
    ];
    for (fragment, expected) in cases {
        assert_eq!(
            Selection::from_fragment(fragment),
            expected,
            "fragment {fragment:?}"
        );
    }
}

#[test]
fn the_size_class_and_the_bar_heights_follow_the_canvas_width() {
    use SizeClass::{Compact, Medium, Wide};
    // width, class, tab bar, link bar, pane header
    let cases = [
        (0.0, Compact, Some(48.0), 36.0, 36.0),
        (719.99, Compact, Some(48.0), 36.0, 36.0),
        (720.0, Medium, None, 24.0, 30.0),
        (1_199.99, Medium, None, 24.0, 30.0),
        (1_200.0, Wide, None, 24.0, 30.0),
        (4_000.0, Wide, None, 24.0, 30.0),
    ];
    for (width, class, tab_bar, link_bar, header) in cases {
        let arrangement = arrange(width, HEIGHT, Selection::View(View::Chart));
        assert_eq!(arrangement.class, class, "class at {width}");
        assert_eq!(arrangement.top_bar.area.height, 40.0, "top bar at {width}");
        assert_eq!(
            arrangement.tab_bar.map(|area| area.height),
            tab_bar,
            "tab bar at {width}"
        );
        assert_eq!(arrangement.link_bar.height, link_bar, "link bar at {width}");
        let chart = arrangement
            .pane(View::Chart)
            .expect("a shown view has a pane");
        assert_eq!(
            chart.header.map(|area| area.height),
            Some(header),
            "chart header at {width}"
        );
    }
}

#[test]
fn the_wide_desk_splits_the_content_fifty_twenty_five_twenty_five() {
    let arrangement = arrange(1_280.0, HEIGHT, Selection::Desk);
    assert_eq!(arrangement.shown, Selection::Desk);
    assert_eq!(arrangement.top_bar.area.top, 0.0);
    assert_eq!(arrangement.link_bar.top, 776.0);
    assert_eq!(
        extent(shown(&arrangement, View::Chart)),
        [0.0, 40.0, 640.0, 776.0]
    );
    assert_eq!(
        extent(shown(&arrangement, View::Monitor)),
        [640.0, 40.0, 960.0, 776.0]
    );
    assert_eq!(
        extent(shown(&arrangement, View::Ladder)),
        [960.0, 40.0, 1_280.0, 776.0]
    );
}

#[test]
fn the_wide_desk_splits_at_whole_points_so_no_column_falls_under_the_floor() {
    // Rounding the 75% split instead leaves a 299.9 ladder at 1200.9.
    for (width, chart, monitor) in [(1_200.0, 600.0, 300.0), (1_200.9, 600.0, 300.0)] {
        let arrangement = arrange(width, HEIGHT, Selection::Desk);
        let [wide_chart, wide_monitor, wide_ladder] = [View::Chart, View::Monitor, View::Ladder]
            .map(|view| shown(&arrangement, view).body.width);
        assert_eq!(
            (wide_chart, wide_monitor),
            (chart, monitor),
            "columns at {width}"
        );
        assert!(wide_ladder >= 300.0, "ladder {wide_ladder} at {width}");
    }
}

#[test]
fn the_medium_desk_gives_the_ladder_forty_percent_clamped_to_320_and_440() {
    // canvas width, ladder width
    let cases = [
        (720.0, 320.0),
        (900.0, 360.0),
        (1_000.0, 400.0),
        (1_100.0, 440.0),
        (1_199.0, 440.0),
    ];
    for (width, ladder) in cases {
        let arrangement = arrange(width, HEIGHT, Selection::Desk);
        assert_eq!(arrangement.class, SizeClass::Medium, "class at {width}");
        assert_eq!(
            arrangement.pane(View::Monitor),
            None,
            "the monitor has no place at {width}"
        );
        let [chart_left, _, chart_right, _] = extent(shown(&arrangement, View::Chart));
        let [ladder_left, _, ladder_right, _] = extent(shown(&arrangement, View::Ladder));
        assert_eq!(
            (chart_left, chart_right),
            (0.0, width - ladder),
            "chart at {width}"
        );
        assert_eq!(
            (ladder_left, ladder_right),
            (width - ladder, width),
            "ladder at {width}"
        );
    }
}

#[test]
fn a_single_view_fills_the_content_and_hides_the_others_in_every_class() {
    for width in [390.0, 900.0, 1_280.0] {
        for view in [View::Chart, View::Ladder, View::Monitor] {
            let arrangement = arrange(width, HEIGHT, Selection::View(view));
            let content_top = 40.0;
            let content_bottom = arrangement
                .tab_bar
                .map_or(arrangement.link_bar.top, |bar| bar.top);
            assert_eq!(
                extent(shown(&arrangement, view)),
                [0.0, content_top, width, content_bottom],
                "{view:?} at {width}"
            );
            for other in [View::Chart, View::Ladder, View::Monitor] {
                assert_eq!(
                    arrangement.pane(other).is_some(),
                    other == view,
                    "{other:?} at {width}"
                );
            }
        }
    }
}

#[test]
fn chart_and_ladder_panes_carry_a_header_directly_above_the_body_and_the_monitor_none() {
    for width in [390.0, 900.0, 1_280.0] {
        for (view, has_header) in [
            (View::Chart, true),
            (View::Ladder, true),
            (View::Monitor, false),
        ] {
            let pane = shown(&arrange(width, HEIGHT, Selection::View(view)), view);
            let Some(header) = pane.header else {
                assert!(!has_header, "{view:?} at {width} lost its header");
                continue;
            };
            assert!(has_header, "{view:?} at {width} grew a header");
            assert_eq!(
                header.top + header.height,
                pane.body.top,
                "{view:?} at {width}"
            );
            assert_eq!(
                (header.left, header.width),
                (pane.body.left, pane.body.width)
            );
        }
    }
}

#[test]
fn a_pane_is_capped_at_2400_by_1600_and_a_wider_slot_centres_it_at_the_top() {
    let arrangement = arrange(3_840.0, 2_160.0, Selection::View(View::Chart));
    let chart = shown(&arrangement, View::Chart);
    let header = chart.header.expect("the chart has a header");
    assert_eq!(header.top, 40.0);
    assert_eq!((header.left, header.width), (720.0, 2_400.0));
    assert_eq!(
        chart.body,
        Area {
            left: 720.0,
            top: 70.0,
            width: 2_400.0,
            height: 1_600.0
        }
    );
}

#[test]
fn compact_falls_back_to_the_last_single_view_and_refuses_the_desk() {
    let compact =
        |navigation: &Navigation| Arrangement::new(canvas(390.0, 844.0), navigation, None).shown;
    let wide =
        |navigation: &Navigation| Arrangement::new(canvas(1_280.0, HEIGHT), navigation, None).shown;
    let chart = Selection::View(View::Chart);
    let monitor = Selection::View(View::Monitor);

    let desk_first = Navigation::new(Selection::Desk);
    assert_eq!(
        compact(&desk_first),
        chart,
        "a desk with no history shows the chart"
    );

    let mut navigation = Navigation::new(Selection::Desk);
    navigation.select(monitor, SizeClass::Wide);
    navigation.select(Selection::Desk, SizeClass::Wide);
    assert_eq!(compact(&navigation), monitor, "the last view comes back");
    assert_eq!(
        wide(&navigation),
        Selection::Desk,
        "the wide canvas keeps the desk"
    );

    let before = navigation;
    navigation.select(Selection::Desk, SizeClass::Compact);
    assert_eq!(navigation, before, "key 0 does nothing on a compact canvas");

    navigation.select(chart, SizeClass::Compact);
    assert_eq!(compact(&navigation), chart);
    assert_eq!(
        wide(&navigation),
        chart,
        "a view chosen in compact stays chosen when it grows"
    );
}

/// Left and width of each top bar slot, which is all the drop order decides.
type Cell = Option<(f32, f32)>;

fn cell(area: Option<Area>) -> Cell {
    area.map(|area| (area.left, area.width))
}

#[test]
fn the_top_bar_drops_the_strategy_first_and_never_the_badges() {
    // width, latched, picker, switcher, strategy, halt, mode
    let cases: [(f32, bool, Cell, Cell, Cell, Cell, Cell); 9] = [
        (
            1_280.0,
            false,
            Some((0.0, 220.0)),
            Some((220.0, 256.0)),
            Some((476.0, 744.0)),
            None,
            Some((1_220.0, 60.0)),
        ),
        (
            1_280.0,
            true,
            Some((0.0, 220.0)),
            Some((220.0, 256.0)),
            Some((476.0, 672.0)),
            Some((1_148.0, 72.0)),
            Some((1_220.0, 60.0)),
        ),
        (
            720.0,
            false,
            Some((0.0, 220.0)),
            Some((220.0, 256.0)),
            Some((476.0, 184.0)),
            None,
            Some((660.0, 60.0)),
        ),
        (
            720.0,
            true,
            Some((0.0, 220.0)),
            Some((220.0, 256.0)),
            Some((476.0, 112.0)),
            Some((588.0, 72.0)),
            Some((660.0, 60.0)),
        ),
        (
            390.0,
            false,
            Some((0.0, 220.0)),
            None,
            Some((220.0, 110.0)),
            None,
            Some((330.0, 60.0)),
        ),
        (
            390.0,
            true,
            Some((0.0, 258.0)),
            None,
            None,
            Some((258.0, 72.0)),
            Some((330.0, 60.0)),
        ),
        (
            320.0,
            false,
            Some((0.0, 260.0)),
            None,
            None,
            None,
            Some((260.0, 60.0)),
        ),
        (
            320.0,
            true,
            Some((0.0, 188.0)),
            None,
            None,
            Some((188.0, 72.0)),
            Some((260.0, 60.0)),
        ),
        (
            100.0,
            true,
            Some((0.0, 0.0)),
            None,
            None,
            Some((0.0, 40.0)),
            Some((40.0, 60.0)),
        ),
    ];
    for (width, is_latched, picker, switcher, strategy, halt, mode) in cases {
        let halt_state = if is_latched { latched() } else { None };
        let arrangement = Arrangement::new(
            canvas(width, HEIGHT),
            &Navigation::new(Selection::Desk),
            halt_state,
        );
        let bar = arrangement.top_bar;
        let case = format!("{width} wide, latched {is_latched}");
        assert_eq!(cell(Some(bar.picker)), picker, "picker, {case}");
        assert_eq!(cell(bar.switcher), switcher, "switcher, {case}");
        assert_eq!(cell(bar.strategy), strategy, "strategy, {case}");
        assert_eq!(cell(bar.halt), halt, "halt, {case}");
        assert_eq!(cell(Some(bar.mode)), mode, "mode, {case}");
    }
}

#[test]
fn a_page_of_320_by_400_or_more_is_never_zoomed_out_and_a_smaller_one_fits_down_to_half() {
    // css width, css height, current zoom, zoom to switch to
    let cases = [
        (320.0, 400.0, 1.0, None),
        (390.0, 844.0, 1.0, None),
        (600.0, 900.0, 1.0, None),
        (1_920.0, 1_080.0, 1.0, None),
        (300.0, 844.0, 1.0, Some(0.9375)),
        (390.0, 300.0, 1.0, Some(0.75)),
        (100.0, 100.0, 1.0, Some(0.5)),
        (300.0, 844.0, 0.9375, None),
        (300.0, 844.0, 0.9385, None),
        (300.0, 844.0, 0.941, Some(0.9375)),
        (320.0, 400.0, 0.5, Some(1.0)),
        (0.0, 500.0, 1.0, None),
        (500.0, 0.0, 1.0, None),
    ];
    for (width, height, current, expected) in cases {
        assert_eq!(
            zoom_to_fit(width, height, current),
            expected,
            "{width}x{height} from {current}"
        );
    }
}

#[test]
fn a_canvas_shorter_than_its_bars_keeps_the_link_bar_then_the_top_bar_then_the_tab_bar() {
    let arrangement = arrange(390.0, 50.0, Selection::View(View::Ladder));
    assert_eq!(
        arrangement.link_bar,
        Area {
            left: 0.0,
            top: 14.0,
            width: 390.0,
            height: 36.0
        }
    );
    assert_eq!(
        arrangement.top_bar.area,
        Area {
            left: 0.0,
            top: 0.0,
            width: 390.0,
            height: 14.0
        }
    );
    assert_eq!(arrangement.tab_bar.map(|bar| bar.height), Some(0.0));
    let ladder = shown(&arrangement, View::Ladder);
    assert_eq!(
        (
            ladder.header.map(|header| header.height),
            ladder.body.height
        ),
        (Some(0.0), 0.0)
    );

    let arrangement = arrange(390.0, 20.0, Selection::View(View::Ladder));
    assert_eq!(arrangement.link_bar.height, 20.0);
    assert_eq!(arrangement.top_bar.area.height, 0.0);
}

const EPSILON: f32 = 1e-3;

fn right(area: Area) -> f32 {
    area.left + area.width
}

fn bottom(area: Area) -> f32 {
    area.top + area.height
}

fn is_overlapping(a: Area, b: Area) -> bool {
    let is_empty = |area: Area| area.width <= EPSILON || area.height <= EPSILON;
    !is_empty(a)
        && !is_empty(b)
        && a.left < right(b) - EPSILON
        && b.left < right(a) - EPSILON
        && a.top < bottom(b) - EPSILON
        && b.top < bottom(a) - EPSILON
}

fn is_inside(inner: Area, outer: Area) -> bool {
    inner.left >= outer.left - EPSILON
        && inner.top >= outer.top - EPSILON
        && right(inner) <= right(outer) + EPSILON
        && bottom(inner) <= bottom(outer) + EPSILON
}

fn span(pane: Pane) -> Area {
    let [left, top, right, bottom] = extent(pane);
    Area {
        left,
        top,
        width: right - left,
        height: bottom - top,
    }
}

fn panes(arrangement: &Arrangement) -> Vec<(View, Pane)> {
    [View::Chart, View::Monitor, View::Ladder]
        .into_iter()
        .filter_map(|view| arrangement.pane(view).map(|pane| (view, pane)))
        .collect()
}

fn top_bar_slots(arrangement: &Arrangement) -> Vec<(&'static str, Area)> {
    let bar = arrangement.top_bar;
    let named = [
        ("picker", Some(bar.picker)),
        ("switcher", bar.switcher),
        ("strategy", bar.strategy),
        ("halt", bar.halt),
        ("mode", Some(bar.mode)),
    ];
    named
        .into_iter()
        .filter_map(|(name, slot)| slot.map(|slot| (name, slot)))
        .collect()
}

fn every_area(arrangement: &Arrangement) -> Vec<(String, Area)> {
    let mut areas = vec![
        ("top bar".to_owned(), arrangement.top_bar.area),
        ("link bar".to_owned(), arrangement.link_bar),
    ];
    areas.extend(arrangement.tab_bar.map(|bar| ("tab bar".to_owned(), bar)));
    areas.extend(
        top_bar_slots(arrangement)
            .into_iter()
            .map(|(name, slot)| (name.to_owned(), slot)),
    );
    for (view, pane) in panes(arrangement) {
        areas.push((format!("{view:?} body"), pane.body));
        areas.extend(
            pane.header
                .map(|header| (format!("{view:?} header"), header)),
        );
    }
    areas
}

fn envelope(view: View) -> (std::ops::RangeInclusive<f32>, std::ops::RangeInclusive<f32>) {
    match view {
        View::Chart => (300.0..=2_400.0, 200.0..=1_600.0),
        View::Ladder => (300.0..=2_400.0, 160.0..=1_600.0),
        View::Monitor => (300.0..=2_400.0, 220.0..=1_600.0),
    }
}

proptest! {
    /// Whatever the canvas, a pane must land in the envelope its painter was written for, and the
    /// bars and panes must tile the canvas without overlap. A pane outside its envelope paints
    /// clipped or cramped, and the author never sees it because no one tries that exact size.
    #[test]
    fn every_arrangement_keeps_each_pane_in_its_envelope_and_the_bars_in_place(
        width in 320.0f32..4_000.0,
        height in 400.0f32..2_400.0,
        index in 0usize..4,
        is_latched in any::<bool>(),
    ) {
        let selection = Selection::ALL[index];
        let canvas = canvas(width, height);
        let halt = if is_latched { latched() } else { None };
        let arrangement = Arrangement::new(canvas, &Navigation::new(selection), halt);

        let top = arrangement.top_bar.area;
        let link = arrangement.link_bar;
        prop_assert!((top.left, top.top, top.width) == (0.0, 0.0, width), "top bar {top:?}");
        prop_assert!((link.left, link.width) == (0.0, width), "link bar {link:?}");
        prop_assert!((bottom(link) - height).abs() <= EPSILON, "link bar {link:?}");
        match arrangement.tab_bar {
            Some(tab) => {
                prop_assert!(arrangement.class == SizeClass::Compact, "tab bar outside compact");
                prop_assert!((bottom(tab) - link.top).abs() <= EPSILON, "tab bar {tab:?}");
                prop_assert!((tab.left, tab.width) == (0.0, width), "tab bar {tab:?}");
            }
            None => prop_assert!(arrangement.class != SizeClass::Compact, "no tab bar in compact"),
        }

        let content_top = bottom(top);
        let content_bottom = arrangement.tab_bar.map_or(link.top, |tab| tab.top);
        let content = Area { left: 0.0, top: content_top, width, height: content_bottom - content_top };
        let shown = panes(&arrangement);
        prop_assert!(!shown.is_empty(), "nothing is shown");
        for (view, pane) in &shown {
            let (widths, heights) = envelope(*view);
            prop_assert!(widths.contains(&pane.body.width), "{view:?} body {:?}", pane.body);
            prop_assert!(heights.contains(&pane.body.height), "{view:?} body {:?}", pane.body);
            prop_assert!(is_inside(span(*pane), content), "{view:?} leaves the content");
        }
        for (first, (_, a)) in shown.iter().enumerate() {
            for (_, b) in &shown[first + 1..] {
                prop_assert!(!is_overlapping(span(*a), span(*b)), "panes overlap: {a:?} {b:?}");
            }
        }
        if selection == Selection::Desk {
            let mut spans: Vec<Area> = shown.iter().map(|(_, pane)| span(*pane)).collect();
            spans.sort_by(|a, b| a.left.total_cmp(&b.left));
            prop_assert!((spans[0].left - content.left).abs() <= EPSILON, "desk starts late");
            for pair in spans.windows(2) {
                prop_assert!((right(pair[0]) - pair[1].left).abs() <= EPSILON, "desk gap {pair:?}");
            }
            let last = spans[spans.len() - 1];
            prop_assert!((right(last) - right(content)).abs() <= EPSILON, "desk ends early");
        }

        let bar = arrangement.top_bar;
        prop_assert!(bar.mode.width == 60.0 && (right(bar.mode) - width).abs() <= EPSILON);
        prop_assert!(bar.halt.is_some() == is_latched, "halt slot {:?}", bar.halt);
        prop_assert!(bar.switcher.is_some() == (arrangement.class != SizeClass::Compact));
        if bar.picker.width < 220.0 {
            prop_assert!(bar.strategy.is_none(), "the strategy id outlives the picker");
        }
        let first_badge = bar.halt.unwrap_or(bar.mode);
        let last_before_badges = bar.strategy.or(bar.switcher).unwrap_or(bar.picker);
        prop_assert!(
            (right(last_before_badges) - first_badge.left).abs() <= EPSILON,
            "a gap before the badges: {last_before_badges:?} then {first_badge:?}"
        );
        let slots = top_bar_slots(&arrangement);
        for (first, (name, a)) in slots.iter().enumerate() {
            prop_assert!(is_inside(*a, top), "{name} leaves the top bar");
            for (other, b) in &slots[first + 1..] {
                prop_assert!(!is_overlapping(*a, *b), "{name} overlaps {other}");
            }
        }
    }

    /// A hidden frame reports a canvas of nothing, and a tiny one a canvas shorter than its bars. The
    /// shell must hand out areas that are never negative and never overlap, so a painter cannot be
    /// asked to draw into a rectangle turned inside out.
    #[test]
    fn no_canvas_makes_an_area_negative_or_two_areas_overlap(
        width in 0.0f32..5_000.0,
        height in prop_oneof![0.0f32..200.0, 0.0f32..5_000.0],
        index in 0usize..4,
        is_latched in any::<bool>(),
    ) {
        let halt = if is_latched { latched() } else { None };
        let arrangement = Arrangement::new(canvas(width, height), &Navigation::new(Selection::ALL[index]), halt);
        let areas = every_area(&arrangement);
        for (name, area) in &areas {
            prop_assert!(area.width >= 0.0 && area.height >= 0.0, "{name} is {area:?}");
        }
        // The top bar's slots sit inside the top bar, so they are checked against each other below.
        let regions: Vec<&(String, Area)> = areas
            .iter()
            .filter(|(name, _)| name.ends_with("body") || name.ends_with("header") || name.ends_with("bar"))
            .collect();
        for (first, (name, a)) in regions.iter().enumerate() {
            for (other, b) in &regions[first + 1..] {
                prop_assert!(!is_overlapping(*a, *b), "{name} overlaps {other}");
            }
        }
        if width > 0.0 {
            let slots = top_bar_slots(&arrangement);
            for (first, (name, a)) in slots.iter().enumerate() {
                for (other, b) in &slots[first + 1..] {
                    prop_assert!(!is_overlapping(*a, *b), "{name} overlaps {other}");
                }
            }
        }
    }

    /// The zoom rule must never shrink a page that fits, and never ask for less than half. A
    /// target above the floor must leave the page at least as large as the layout needs.
    #[test]
    fn the_zoom_target_stays_between_half_and_one_and_fits_what_it_can(
        width in 0.01f32..5_000.0,
        height in 0.01f32..5_000.0,
    ) {
        let target = zoom_to_fit(width, height, 0.0).expect("a page with a size always has a target");
        prop_assert!((0.5..=1.0).contains(&target), "target {target}");
        if width >= 320.0 && height >= 400.0 {
            prop_assert!(zoom_to_fit(width, height, 1.0).is_none(), "a page that fits was zoomed");
        }
        if target > 0.5 {
            prop_assert!(width / target >= 320.0 - EPSILON, "{width} wide at {target}");
            prop_assert!(height / target >= 400.0 - EPSILON, "{height} high at {target}");
        }
    }
}
