//! Axis and crosshair projection fitness: the nice-numbers tick generator both charts share, the
//! exact label strings it feeds the painter, and the pointer-to-bucket inverse the shared crosshair
//! rides on. ONE generator serves the mid chart and the exposure/PnL chart, so its failure mode
//! is a tick drawn outside the plotted range or a label that quietly rounds — a lie painted over live
//! data rather than a crash, which is exactly what a fitness pin exists to catch.

use polysim::workstation::chart::view::{
    ChartBounds, ChartDomain, bucket_at_fraction, value_ticks, x_fraction, y_fraction,
};
use polysim::workstation::format::{
    axis_ticks, legible_tick_ceiling, quote_axis_decimals, write_quote_amount,
};
use proptest::prelude::*;

/// `axis_ticks` raises any non-zero ceiling to this, so a property over ceilings must compare against
/// the FLOORED value rather than the one it passed in.
const MIN_TICK_CEILING: usize = 5;

/// Spans a chart actually asks for, spread across decades rather than drawn uniformly. A flat
/// `0..4_000_000_000_000` puts essentially all its mass in the top decade — P(span < 1e9) is ~2.5e-4 —
/// so the ranges a real price window covers would go effectively unsampled.
fn arb_span(minimum: i64) -> impl Strategy<Value = i64> {
    prop_oneof![
        minimum..1_000i64,
        minimum..1_000_000i64,
        minimum..1_000_000_000i64,
        minimum..4_000_000_000_000i64,
    ]
}

/// The widest window the model can ask for: the chart model's own ceiling on retained buckets, so
/// the round trip is pinned across every window shape a run can produce, not just the shipped one.
const MAX_WINDOW_SLOTS: u64 = 3_000;

/// Whether `step` sits on the {1,2,5}·10^k ladder, checked independently of the generator so a
/// change of ladder cannot quietly agree with itself.
fn is_nice_step(step: i64) -> bool {
    let mut value = step;
    while value >= 10 && value % 10 == 0 {
        value /= 10;
    }
    matches!(value, 1 | 2 | 5)
}

/// The shipped axis font, and the two pitches the painter derives from it: 2.0 font heights is the
/// spacing the gutter AIMS for, 1.2 the tightest it stays readable at. Both here so the bands this
/// crosses are the bands the real gutter crosses.
const AXIS_FONT: f32 = 11.0;
const IDEAL_PITCH: f32 = AXIS_FONT * 2.0;
const MINIMUM_PITCH: f32 = AXIS_FONT * 1.2;

/// The exposure chart's plot at the smallest pane that shows the whole latency grid: a 360pt pane
/// leaves its chart 100pt, less the frame's 10pt of padding above and below. This chart must show a
/// y-axis, so the geometry it actually ships at is pinned rather than left to follow from the
/// arithmetic.
const EXPOSURE_PLOT_HEIGHT: f32 = 80.0;

struct TickCase {
    name: &'static str,
    low: i64,
    high: i64,
    ceiling: usize,
    expect_step: Option<i64>,
    expect_ticks: Vec<i64>,
}

/// FITNESS: the generator climbs the {1,2,5}·10^k ladder to the finest step under the ceiling and
/// stays safe on degenerate ranges (inverted, zero-ceiling, single-value, and past i64's own width);
/// the axis never hands a painter more labels than a plot can show legibly nor withholds one it could
/// show, pinned both generally and at the exposure chart's own shipped height (that chart must show
/// an axis there: raise the plot or lower the legibility pitch before ever deleting that assertion);
/// the label writers render exact strings from the step's own chosen decimals; and the crosshair's
/// fraction-to-bucket inverse clamps to an edge for a pointer that has left the plot.
#[test]
fn axis_tick_generation_legibility_and_label_rendering() {
    let cases = [
        TickCase {
            name: "finest step under a 6-tick ceiling",
            low: 0,
            high: 100,
            ceiling: 6,
            expect_step: Some(20),
            expect_ticks: vec![0, 20, 40, 60, 80, 100],
        },
        TickCase {
            name: "finest step across zero at a 5-tick ceiling",
            low: -7,
            high: 23,
            ceiling: 5,
            expect_step: Some(10),
            expect_ticks: vec![0, 10, 20],
        },
        TickCase {
            name: "an inverted range yields an empty axis with a safe step",
            low: 100,
            high: 0,
            ceiling: 8,
            expect_step: Some(1),
            expect_ticks: vec![],
        },
        TickCase {
            name: "a zero ceiling draws nothing but keeps a safe step",
            low: 0,
            high: 100,
            ceiling: 0,
            expect_step: Some(1),
            expect_ticks: vec![],
        },
        TickCase {
            name: "a range holding one value gets one tick, at it",
            low: 42,
            high: 42,
            ceiling: 8,
            expect_step: None,
            expect_ticks: vec![42],
        },
        TickCase {
            name: "no rung divides the whole of i64, so the axis gives up",
            low: i64::MIN,
            high: i64::MAX,
            ceiling: 8,
            expect_step: None,
            expect_ticks: vec![],
        },
    ];
    for case in cases {
        let ticks = axis_ticks(case.low, case.high, case.ceiling);
        if let Some(step) = case.expect_step {
            assert_eq!(ticks.step(), step, "{}: step", case.name);
        }
        assert_eq!(
            ticks.collect::<Vec<_>>(),
            case.expect_ticks,
            "{}: ticks",
            case.name
        );
    }

    // an_axis_is_drawn_exactly_when_its_labels_would_be_legible: the axis never hands a painter more
    // labels than the plot can show them legibly, and never withholds one a plot could show. 0..400pt
    // at a 13.2pt minimum pitch spans room for 0 through 30 labels, so the 66pt boundary and the
    // broken 22-55pt band are both swept rather than straddled.
    for tenths in 0..4_000u32 {
        let height = tenths as f32 / 10.0;
        let legible = (height / MINIMUM_PITCH) as usize;
        let ceiling = legible_tick_ceiling(height, IDEAL_PITCH, MINIMUM_PITCH);
        let count = axis_ticks(0, 100_000, ceiling).count();
        assert!(
            count <= legible,
            "a {height}pt plot fits {legible} legible label(s) and was handed {count}"
        );
        match legible >= MIN_TICK_CEILING {
            true => assert!(
                count >= 2,
                "a {height}pt plot fits {legible} legible label(s) and must still get an axis"
            ),
            false => assert_eq!(
                count, 0,
                "a {height}pt plot cannot show the floor's five legibly and must get a bare gutter"
            ),
        }
    }

    // the_exposure_chart_shows_an_axis_at_its_default_height: the exposure chart at its shipped size
    // carries the axis it is required to show.
    let exposure_bounds = ChartBounds {
        low: -5_000_000,
        high: 5_000_000,
    };
    let ticks: Vec<i64> = value_ticks(exposure_bounds, EXPOSURE_PLOT_HEIGHT, AXIS_FONT).collect();
    assert!(
        ticks.len() >= 3,
        "a {EXPOSURE_PLOT_HEIGHT}pt exposure plot drew {} label(s). This axis was ASKED FOR — \
         \"the y axis for this should show, along with the latest number\" — so reddening this is \
         removing a requested feature, not adjusting a constant. Raise the plot or lower the \
         legibility pitch; do not delete the assertion.",
        ticks.len()
    );
    assert!(
        EXPOSURE_PLOT_HEIGHT / ticks.len() as f32 >= MINIMUM_PITCH,
        "and they are {}pt apart, under the {MINIMUM_PITCH}pt they need to be read",
        EXPOSURE_PLOT_HEIGHT / ticks.len() as f32
    );

    // axis_labels_render_their_ticks_exactly: the generator's step chooses the decimals and the
    // writer renders the tick at them. An exposure axis from -2.5 to +7.5 quote units.
    let label_ticks = axis_ticks(-250_000_000, 750_000_000, 6);
    assert_eq!(label_ticks.step(), 200_000_000, "two whole quote units");
    let decimals = quote_axis_decimals(label_ticks.step());
    assert_eq!(decimals, 0, "a whole-unit step needs no fractional digit");
    let mut label = String::new();
    let mut labels = Vec::new();
    for tick in label_ticks {
        write_quote_amount(&mut label, tick, decimals);
        labels.push(label.clone());
    }
    assert_eq!(labels, ["-2", "0", "2", "4", "6"]);

    // a_fraction_outside_the_plot_clamps_to_an_edge_bucket: the pointer leaves the plot rect
    // constantly — the operator drags across the panel edge, or hovers the gutter — and the crosshair
    // must answer with a bucket the chart actually holds.
    let plot = ChartDomain {
        first: 100,
        last: 400,
    };
    assert_eq!(bucket_at_fraction(-3.0, plot), 100);
    assert_eq!(bucket_at_fraction(4.0, plot), 400);
    assert_eq!(
        bucket_at_fraction(f32::NAN, plot),
        100,
        "a pointer with no position reads as the window's start, never a panic"
    );
    assert_eq!(
        bucket_at_fraction(0.5, ChartDomain { first: 7, last: 7 }),
        7,
        "a one-slot window is its own answer"
    );
}

/// Every ceiling in the band a real chart asks for, pinned by literal rather than left to a sampler.
/// Below five the {1,2,5} ladder can jump clean over the two-to-four band: over `[-1194, -1186]` a
/// step of 2 wants five ticks and the next rung, 5, yields exactly one, so an unfloored ceiling of 3
/// or 4 would paint a SINGLE-label axis over live data. The exposure/PnL chart's default geometry
/// lands precisely there — ~90 pt of plot at an 11 pt font asks for 4.
#[test]
fn a_ceiling_below_the_floor_still_draws_a_real_axis() {
    // The two ranges an exhaustive sweep found in the broken band, plus two ordinary ones.
    let ranges = [(-1194i64, -1186i64), (-1200, -600), (0, 100), (11, 14)];
    for ceiling in 1..=5usize {
        for (low, high) in ranges {
            let count = axis_ticks(low, high, ceiling).count();
            assert!(
                count >= 2,
                "ceiling {ceiling} over [{low}, {high}] drew {count} tick(s); an axis with one \
                 number on it reads as a broken chart"
            );
        }
    }
}

/// The plot height below which `value_ticks` draws no labels, computed with the painter's own
/// expression: the f32 product is 79.200005, not the 79.2 a literal would give.
const LEGIBLE_AXIS_HEIGHT: f32 = AXIS_FONT * 1.2 * 6.0;

/// The mid and risk plot heights the pane budget gives at a spread of real pane sizes.
const PANE_PLOT_HEIGHTS: [f32; 14] = [
    20.0, 79.0, 80.0, 97.0, 110.0, 114.0, 179.0, 187.0, 277.0, 337.0, 397.0, 547.0, 757.0, 1037.0,
];

/// A half-tick mid window, a one-spread window, a whole-unit exposure window and the narrow window
/// where a five-label axis spans nearly six steps.
const AXIS_RANGES: [ChartBounds; 4] = [
    ChartBounds {
        low: 174,
        high: 306,
    },
    ChartBounds {
        low: 201,
        high: 203,
    },
    ChartBounds {
        low: -250_000_000,
        high: 750_000_000,
    },
    ChartBounds {
        low: -95,
        high: 495,
    },
];

/// How far apart, in points, the painter puts each pair of adjacent labels on a plot `height` tall.
fn label_spacing(ticks: &[i64], bounds: ChartBounds, height: f32) -> Vec<f32> {
    ticks
        .windows(2)
        .map(|pair| (y_fraction(pair[1], bounds) - y_fraction(pair[0], bounds)) * height)
        .collect()
}

/// Draws the axis of a `height` plot over `bounds`, asserts the rules that hold at every height,
/// and returns how many labels it drew.
fn assert_axis_is_legible(bounds: ChartBounds, height: f32) -> usize {
    let ticks: Vec<i64> = value_ticks(bounds, height, AXIS_FONT).collect();
    if height < LEGIBLE_AXIS_HEIGHT {
        assert!(
            ticks.is_empty(),
            "a {height}pt plot is under {LEGIBLE_AXIS_HEIGHT}pt and drew {ticks:?} over {bounds:?}"
        );
        return 0;
    }
    assert!(
        ticks.len() >= 2,
        "a {height}pt plot drew {} label(s) over {bounds:?}; one number reads as a broken axis",
        ticks.len()
    );
    for spacing in label_spacing(&ticks, bounds, height) {
        assert!(
            spacing >= MINIMUM_PITCH - 1e-3,
            "labels {spacing}pt apart on a {height}pt plot over {bounds:?}, under the \
             {MINIMUM_PITCH}pt they need to be read"
        );
    }
    ticks.len()
}

/// FITNESS: the value axis never crowds its labels and thins out as the plot shrinks. Spacing is
/// measured the way the painter places a label (through `y_fraction`), at the plot heights the pane
/// budget really produces and across a 0..1600pt sweep. Below six legible pitches the generator's
/// floor of five labels could overlap, so the axis draws none; from there up it draws at least two
/// and never fewer as the plot grows.
#[test]
fn value_axis_density_at_the_pane_plot_sizes() {
    for bounds in AXIS_RANGES {
        for height in PANE_PLOT_HEIGHTS {
            assert_axis_is_legible(bounds, height);
        }
        let mut previous_count = 0;
        for half_points in 0..=3_200u32 {
            let height = half_points as f32 / 2.0;
            let count = assert_axis_is_legible(bounds, height);
            assert!(
                count >= previous_count,
                "a {height}pt plot drew {count} label(s) after a shorter one drew {previous_count} \
                 over {bounds:?}"
            );
            previous_count = count;
        }
    }

    let crowded = ChartBounds {
        low: -95,
        high: 495,
    };
    let generator_alone = axis_ticks(
        crowded.low,
        crowded.high,
        legible_tick_ceiling(79.0, IDEAL_PITCH, MINIMUM_PITCH),
    );
    assert_eq!(
        generator_alone.count(),
        5,
        "the range must be one where the generator alone would put five labels on a 79pt plot"
    );
    assert_eq!(
        value_ticks(crowded, 79.0, AXIS_FONT).count(),
        0,
        "five labels on 79pt can sit 13pt apart or less, so the axis draws none"
    );
    assert!(
        value_ticks(crowded, 80.0, AXIS_FONT).count() >= 2,
        "80pt is the risk chart's plot at the smallest pane that shows the latency grid"
    );
}

proptest! {
    /// The crosshair reads a pointer position back to the bucket the painter drew there, and both
    /// charts must resolve the SAME slot from it. An inverse off by one slot puts a neighbouring
    /// spin's value under the hairline — which reads as data, not as an error.
    ///
    /// The round trip alone does not pin the forward transform: one that mapped the window BACKWARDS,
    /// or off the plot rect entirely, would still invert perfectly. So the fractions are also held
    /// inside `0.0..=1.0` and strictly increasing — the first keeps every point inside the rect it is
    /// clipped to, the second is what stops a window painting right to left.
    #[test]
    fn every_bucket_in_the_domain_survives_the_fraction_round_trip(
        // Epoch scale, as a real index is: ~1.75e9 at the shipped one-second spin. The `f32` fraction
        // carries 24 mantissa bits, so the inverse is exact while the window stays under 2^23 slots —
        // and the model clamps it three orders of magnitude below that.
        first in 0u64..2_000_000_000_000,
        width in 1u64..=MAX_WINDOW_SLOTS,
    ) {
        let domain = ChartDomain { first, last: first + width - 1 };
        prop_assert_eq!(domain.width(), width);

        let mut previous = -1.0f32;
        for index in first..=domain.last {
            let fraction = x_fraction(index, domain);
            prop_assert!((0.0..=1.0).contains(&fraction), "slot {} left the plot rect", index);
            prop_assert!(fraction > previous, "slot {} did not advance past the last", index);
            previous = fraction;
            prop_assert_eq!(bucket_at_fraction(fraction, domain), index);
        }
    }

    /// Whatever range the charts hand it, the generator stays inside the plot: never a tick past an
    /// edge, never more labels than the gutter was measured for, always on the shared ladder and
    /// evenly spaced. A tick outside the range paints a price the chart never reached. The ceiling
    /// generator starts at 1 because starting it at 6 is exactly what hid the one-label axis until
    /// an exhaustive sweep found it.
    ///
    /// Five is the bound, and the derivation is why [`MIN_TICK_CEILING`] is five: take the coarsest
    /// step `s` with `2s <= span`. Its successor exceeds `span / 2`, and a successor is at most `2.5s`,
    /// so `s > span / 5`; therefore `count <= span / s + 1 <= 5`. A ceiling of five always admits that
    /// step, and that step spans the range at least twice.
    #[test]
    fn every_tick_lands_on_the_ladder_inside_the_range(
        low in -2_000_000_000_000i64..2_000_000_000_000,
        span in arb_span(0),
        max_ticks in 1usize..64,
    ) {
        let high = low + span;
        let ticks = axis_ticks(low, high, max_ticks);
        let step = ticks.step();
        prop_assert!(is_nice_step(step), "step {} is off the ladder", step);

        let values: Vec<i64> = ticks.collect();
        prop_assert!(values.len() <= max_ticks.max(MIN_TICK_CEILING));
        prop_assert!(span == 0 || values.len() >= 2, "a real range drew {} tick(s)", values.len());
        for (position, &value) in values.iter().enumerate() {
            prop_assert!((low..=high).contains(&value));
            prop_assert_eq!(value % step, 0, "a tick off its own step breaks chart-to-chart alignment");
            if position > 0 {
                prop_assert_eq!(value - values[position - 1], step);
            }
        }
    }
}
