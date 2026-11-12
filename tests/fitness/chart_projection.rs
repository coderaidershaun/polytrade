//! Chart projection fitness: the pure map from the ordered book and event lanes to the
//! rolling five-minute mid series, and the screen transform over it. Buckets are folded in event time
//! alone, so a spin carrying no trustworthy mid stays a real hole the line splits across rather
//! than an invented sample, and replay reproduces the series exactly. Prices never leave exact
//! half-tick integers until the final `0..=1` fraction. The domain grows from left to right before it
//! slides; the bounds cover fills as well as mids, so an off-mid marker can never be invisible.

use polysim::ids::{AssetId, InstrumentId, Price, Qty, Side};
use polysim::msg::exec::Liquidity;
use polysim::msg::ui::{UiBookSnapshot, UiBookState, UiEvent};
use polysim::time::{DurationUs, TsUs};
use polysim::workstation::chart::model::{
    BookContinuity, ChartBucket, ChartFill, ChartModel, bucket_open_ts,
};
use polysim::workstation::chart::view::{
    Candle, CandleGrouping, ChartBounds, ChartDomain, bounds, domain, segment_points,
    visible_fills, x_fraction, y_fraction,
};
use proptest::prelude::*;

use crate::workstation_support::{
    CAPACITY, SPIN, TICK, chart_book, chart_model, chart_with_ticks, rotation_event, valid_book,
};

/// Buckets committed past a full window, so eviction and sliding are exercised by an amount rather
/// than by a literal that would silently mean something else if the window span moved.
const OVERRUN: u64 = 100;

/// Fold one two-sided commit into `bucket` with no snapshot loss before it — the ordinary case.
fn commit(chart: &mut ChartModel, instrument: u16, bucket: u64, bid: i64, ask: i64) {
    chart.apply_book(
        &valid_book(instrument, bucket, bid, ask),
        BookContinuity::Continuous,
    );
}

fn fill_event(instrument: u16, bucket: u64, side: Side, price: i64) -> UiEvent {
    UiEvent::Fill {
        instrument: InstrumentId(instrument),
        seq: 0,
        event_ts_us: TsUs::from_micros(bucket as i64 * SPIN.micros()),
        quote_level: None,
        side,
        price: Price(price),
        qty: Qty(1),
        commission: 0,
        commission_asset: AssetId(0),
        liquidity: Some(Liquidity::Maker),
    }
}

fn buckets(chart: &ChartModel, instrument: u16) -> Vec<ChartBucket> {
    chart.buckets(InstrumentId(instrument)).copied().collect()
}

fn fills(chart: &ChartModel, instrument: u16) -> Vec<ChartFill> {
    chart.fills(InstrumentId(instrument)).copied().collect()
}

fn indices(chart: &ChartModel, instrument: u16) -> Vec<u64> {
    chart
        .buckets(InstrumentId(instrument))
        .map(|b| b.index)
        .collect()
}

/// The `is_run_start` flag of every visible bucket, in paint order — where the painted line breaks.
fn run_starts(chart: &ChartModel, instrument: u16) -> Vec<bool> {
    let instrument = InstrumentId(instrument);
    let domain = domain(chart, instrument).expect("a series to project");
    segment_points(chart, instrument, domain)
        .map(|point| point.is_run_start)
        .collect()
}

/// FITNESS: the instant a bucket opened folds back into that same bucket, four same-spin commits fold
/// into one open/high/low/close, and the next spin opens a fresh bucket flat on its first mid. Late
/// event time for an already-settled bucket is dropped rather than folded backwards. The crosshair's
/// time label is the inverse of the model's bucketing, and the two are separate functions, so nothing
/// but the first case here keeps them describing one axis.
#[test]
fn bucket_folding_and_boundary_invariants() {
    for index in [0u64, 1, 7, CAPACITY - 1, 1_000_000, 1_750_000_000] {
        let at = bucket_open_ts(index, SPIN).expect("the shipped cadence has an instant");
        let mut chart = chart_model(1);
        let mut snapshot = valid_book(0, 0, 100, 102);
        snapshot.event_ts_us = at;
        chart.apply_book(&snapshot, BookContinuity::Continuous);
        assert_eq!(
            indices(&chart, 0),
            vec![index],
            "bucket {index} opened at {}us and the model bucketed that instant elsewhere",
            at.micros()
        );
    }
    assert_eq!(
        bucket_open_ts(1, DurationUs::ZERO),
        None,
        "a cadence the model never took has no instant to report rather than a fabricated one"
    );
    assert_eq!(
        bucket_open_ts(u64::MAX, SPIN),
        None,
        "and an index whose instant leaves i64 reports none rather than wrapping"
    );

    let mut chart = chart_model(1);
    for (bid, ask) in [(100, 102), (105, 107), (95, 97), (101, 103)] {
        commit(&mut chart, 0, 1, bid, ask);
    }
    assert_eq!(
        buckets(&chart, 0),
        vec![ChartBucket {
            index: 1,
            open_half_ticks: 202,
            high_half_ticks: 212,
            low_half_ticks: 192,
            close_half_ticks: 204,
            has_gap_before: false,
        }],
        "four commits in one spin are one bucket: first mid opens, extremes bound it, last closes"
    );

    // Still inside spin 1: 1_999_999 µs floors to bucket 1, not 2.
    chart.apply_book(
        &UiBookSnapshot {
            event_ts_us: TsUs::from_micros(1_999_999),
            ..valid_book(0, 1, 124, 126)
        },
        BookContinuity::Continuous,
    );
    commit(&mut chart, 0, 2, 110, 112);
    let banked = buckets(&chart, 0);
    assert_eq!(indices(&chart, 0), vec![1, 2]);
    assert_eq!(
        banked[0].high_half_ticks, 250,
        "the last microsecond of a spin is its own bucket's"
    );
    assert_eq!(banked[0].close_half_ticks, 250);
    assert_eq!(
        (
            banked[1].open_half_ticks,
            banked[1].high_half_ticks,
            banked[1].low_half_ticks,
            banked[1].close_half_ticks
        ),
        (222, 222, 222, 222),
        "a fresh bucket opens flat on its first mid"
    );

    let mut late_chart = chart_model(1);
    commit(&mut late_chart, 0, 2, 100, 102);
    commit(&mut late_chart, 0, 3, 149, 151);
    let settled = buckets(&late_chart, 0);
    // A snapshot stamped back inside bucket 2, with an extreme mid that would be visible if folded.
    commit(&mut late_chart, 0, 2, 500, 500);
    assert_eq!(
        buckets(&late_chart, 0),
        settled,
        "a late snapshot for a settled bucket is dropped, never folded backwards"
    );
}

struct DiscontinuityCase {
    name: &'static str,
    setup: fn(&mut ChartModel),
    expect_indices: Vec<u64>,
    expect_gap_flags: Vec<bool>,
    expect_run_starts: Vec<bool>,
}

fn missing_spin_case(chart: &mut ChartModel) {
    commit(chart, 0, 1, 100, 102);
    commit(chart, 0, 2, 101, 103);
    // Spin 3 commits, but the book is awaiting its snapshot: nothing is banked for it.
    chart.apply_book(
        &chart_book(0, 3, UiBookState::AwaitingSnapshot, &[], &[]),
        BookContinuity::Continuous,
    );
    commit(chart, 0, 4, 105, 107);
}

fn lost_snapshot_run_case(chart: &mut ChartModel) {
    commit(chart, 0, 1, 100, 102);
    commit(chart, 0, 2, 101, 103);
    // The lane dropped snapshots, revealed on a one-sided commit that banks nothing itself.
    chart.apply_book(
        &chart_book(0, 3, UiBookState::Valid, &[100], &[]),
        BookContinuity::GapBefore,
    );
    commit(chart, 0, 3, 104, 106);
}

fn loss_inside_open_bucket_case(chart: &mut ChartModel) {
    commit(chart, 0, 1, 100, 102);
    commit(chart, 0, 2, 101, 103);
    // The ordinary case: the book commits many times per spin, so the commit that reveals the loss
    // folds into the already-open bucket instead of pushing a new one.
    chart.apply_book(&valid_book(0, 2, 104, 106), BookContinuity::GapBefore);
    commit(chart, 0, 3, 105, 107);
}

/// FITNESS: a missing spin, a lost-snapshot run revealed on the next bucket, and a loss revealed
/// inside the still-open bucket all split the painted line at the boundary of what was actually lost —
/// never before it, never after it.
#[test]
fn line_breaks_across_discontinuities() {
    let cases = [
        DiscontinuityCase {
            name: "a spin without a trustworthy mid stays a hole",
            setup: missing_spin_case,
            expect_indices: vec![1, 2, 4],
            expect_gap_flags: vec![false, false, false],
            expect_run_starts: vec![true, false, true],
        },
        DiscontinuityCase {
            name: "a lost snapshot run splits the line at the next bucket",
            setup: lost_snapshot_run_case,
            expect_indices: vec![1, 2, 3],
            expect_gap_flags: vec![false, false, true],
            expect_run_starts: vec![true, false, true],
        },
        DiscontinuityCase {
            name: "a loss inside the open bucket splits the line before that bucket",
            setup: loss_inside_open_bucket_case,
            expect_indices: vec![1, 2, 3],
            expect_gap_flags: vec![false, true, false],
            expect_run_starts: vec![true, true, false],
        },
    ];
    for case in cases {
        let mut chart = chart_model(1);
        (case.setup)(&mut chart);
        assert_eq!(
            indices(&chart, 0),
            case.expect_indices,
            "{}: bucket indices",
            case.name
        );
        let gap_flags: Vec<bool> = buckets(&chart, 0)
            .iter()
            .map(|b| b.has_gap_before)
            .collect();
        assert_eq!(
            gap_flags, case.expect_gap_flags,
            "{}: has_gap_before flags",
            case.name
        );
        assert_eq!(
            run_starts(&chart, 0),
            case.expect_run_starts,
            "{}: run_starts",
            case.name
        );
    }
}

/// Buckets retained past the visible window stay in the series (eviction is by capacity, visibility by
/// the five-minute domain); a rotation clears only its own instrument; only a valid two-sided,
/// on-grid book banks a mid, and an instrument with no tick grid at all charts nothing; the domain
/// grows from left to right while filling then slides once full; and the whole projection is replay
/// deterministic.
#[test]
fn domain_retention_rotation_grid_and_replay_determinism() {
    let mut chart = chart_model(1);
    commit(&mut chart, 0, 0, 100, 102);
    commit(&mut chart, 0, 5_000, 110, 112);
    assert_eq!(
        indices(&chart, 0),
        vec![0, 5_000],
        "two buckets is far under capacity, so nothing was evicted"
    );
    let domain_far = domain(&chart, InstrumentId(0)).expect("a series to project");
    assert_eq!(
        domain_far,
        ChartDomain {
            first: 5_000 - CAPACITY + 1,
            last: 5_000
        }
    );
    let visible: Vec<u64> = segment_points(&chart, InstrumentId(0), domain_far)
        .map(|point| point.bucket.index)
        .collect();
    assert_eq!(
        visible,
        vec![5_000],
        "eviction is by capacity, visibility by the five-minute window — the old bucket falls out of \
         the second, not the first"
    );

    let mut rotation_chart = chart_model(2);
    commit(&mut rotation_chart, 0, 1, 100, 102);
    commit(&mut rotation_chart, 1, 1, 300, 302);
    rotation_chart.apply_event(&fill_event(0, 1, Side::Buy, 101));
    rotation_chart.apply_event(&fill_event(1, 1, Side::Buy, 301));
    rotation_chart.apply_event(&rotation_event(0, 2));
    assert!(
        buckets(&rotation_chart, 0).is_empty(),
        "a new window is a new distribution"
    );
    assert!(
        fills(&rotation_chart, 0).is_empty(),
        "its markers go with it"
    );
    assert_eq!(
        indices(&rotation_chart, 1),
        vec![1],
        "the neighbour is untouched"
    );
    assert_eq!(fills(&rotation_chart, 1).len(), 1);

    let mut grid_chart = chart_model(1);
    grid_chart.apply_book(
        &chart_book(0, 1, UiBookState::AwaitingSnapshot, &[100], &[102]),
        BookContinuity::Continuous,
    );
    grid_chart.apply_book(
        &chart_book(0, 2, UiBookState::Valid, &[100], &[]),
        BookContinuity::Continuous,
    );
    grid_chart.apply_book(
        &chart_book(0, 3, UiBookState::Valid, &[], &[102]),
        BookContinuity::Continuous,
    );
    assert!(
        buckets(&grid_chart, 0).is_empty(),
        "an awaiting or one-sided book has no mid to bank"
    );

    // A ten-mantissa grid: 105 is not a multiple of the tick, so the mid does not exist.
    let mut off_grid = chart_with_ticks(&[Some(Price(10))]);
    commit(&mut off_grid, 0, 1, 100, 105);
    assert!(
        buckets(&off_grid, 0).is_empty(),
        "an off-grid best price is invalid, never rounded onto the grid"
    );
    commit(&mut off_grid, 0, 2, 100, 110);
    assert_eq!(
        buckets(&off_grid, 0)[0].close_half_ticks,
        21,
        "on-grid, the mid is the exact sum of the two tick indices"
    );

    let mut no_grid = chart_with_ticks(&[None]);
    commit(&mut no_grid, 0, 1, 100, 102);
    no_grid.apply_event(&fill_event(0, 1, Side::Buy, 101));
    assert!(buckets(&no_grid, 0).is_empty());
    assert!(fills(&no_grid, 0).is_empty());
    assert_eq!(
        domain(&no_grid, InstrumentId(0)),
        None,
        "no grid, no series — an honest empty, never a fabricated one"
    );

    let mut capacity_chart = chart_model(1);
    assert_eq!(capacity_chart.capacity() as u64, CAPACITY);
    for bucket in 1..=3 {
        commit(&mut capacity_chart, 0, bucket, 100, 102);
    }
    let growing = domain(&capacity_chart, InstrumentId(0)).expect("a series to project");
    assert_eq!(
        growing,
        ChartDomain {
            first: 1,
            last: CAPACITY
        },
        "the window is anchored at run start while it fills, so the line grows left → right"
    );
    assert_eq!(growing.width(), CAPACITY);

    for bucket in 4..=CAPACITY + OVERRUN {
        commit(&mut capacity_chart, 0, bucket, 100, 102);
    }
    let sliding = domain(&capacity_chart, InstrumentId(0)).expect("a series to project");
    assert_eq!(
        sliding,
        ChartDomain {
            first: OVERRUN + 1,
            last: CAPACITY + OVERRUN
        },
        "once full, the window slides so the newest bucket holds the right edge"
    );
    assert_eq!(sliding.width(), CAPACITY);

    let banked = buckets(&capacity_chart, 0);
    assert_eq!(banked.len(), CAPACITY as usize);
    assert_eq!(
        (banked[0].index, banked[banked.len() - 1].index),
        (OVERRUN + 1, CAPACITY + OVERRUN),
        "the series retains exactly its capacity, oldest evicted first, iterated oldest → newest"
    );

    let run = || {
        let mut chart = chart_model(2);
        commit(&mut chart, 0, 1, 100, 102);
        commit(&mut chart, 1, 1, 300, 302);
        commit(&mut chart, 0, 1, 105, 107);
        chart.apply_book(
            &chart_book(0, 2, UiBookState::AwaitingSnapshot, &[], &[]),
            BookContinuity::GapBefore,
        );
        commit(&mut chart, 0, 3, 98, 100);
        chart.apply_event(&fill_event(0, 3, Side::Buy, 99));
        chart.apply_event(&rotation_event(1, 3));
        let instrument = InstrumentId(0);
        let domain = domain(&chart, instrument);
        (
            buckets(&chart, 0),
            fills(&chart, 0),
            buckets(&chart, 1),
            domain,
            domain.and_then(|domain| bounds(&chart, instrument, domain)),
            domain.map(|domain| {
                visible_fills(&chart, instrument, domain)
                    .copied()
                    .collect::<Vec<_>>()
            }),
        )
    };
    assert_eq!(
        run(),
        run(),
        "the same input sequence yields the same series, domain and bounds"
    );
}

/// A fill marks its own price, never the mid, and a whole window of fills at the run's real rate is
/// never evicted out from under a still-drawn bucket, which is what a fill ring sized independently
/// of the bucket ring would do. Bounds span the data's extremes with 5% air —
/// swallowing fills as well as mids, and never collapsing to zero height on a flat series — and the
/// x/y fractions built over those bounds stay in `0.0..=1.0`, clamping outside the window and reading
/// as their own centre on a zero-height range.
#[test]
fn chart_view_fills_bounds_and_fractions() {
    let mut chart = chart_model(1);
    commit(&mut chart, 0, 1, 100, 102);
    chart.apply_event(&fill_event(0, 1, Side::Sell, 150));
    assert_eq!(
        fills(&chart, 0),
        vec![ChartFill {
            index: 1,
            half_ticks: 300,
            side: Side::Sell,
        }],
        "the marker sits at the fill's own price in half-ticks, in its own bucket"
    );
    assert_eq!(
        buckets(&chart, 0)[0].close_half_ticks,
        202,
        "and the fill leaves the mid series alone"
    );

    // The 100 ms spin `MAX_BUCKETS_PER_INSTRUMENT` is sized for — the cadence at which a fill ring
    // unrelated to the bucket ring starts evicting markers from under a line that is still drawn.
    const FAST_SPIN: DurationUs = DurationUs::from_micros(100_000);
    let mut fast = ChartModel::with_capacity(1, FAST_SPIN);
    fast.configure(&[Some(TICK)], FAST_SPIN);
    let at = |bucket: u64| TsUs::from_micros(bucket as i64 * FAST_SPIN.micros());
    // Two fills per bucket is the run's real ceiling: the recorder arms one quote per side per spin
    // and a markout side disarms itself on fill.
    let window = fast.capacity() as u64;
    for bucket in 0..window {
        fast.apply_book(
            &UiBookSnapshot {
                event_ts_us: at(bucket),
                ..valid_book(0, 0, 100, 102)
            },
            BookContinuity::Continuous,
        );
        for side in [Side::Buy, Side::Sell] {
            fast.apply_event(&UiEvent::Fill {
                instrument: InstrumentId(0),
                seq: 0,
                event_ts_us: at(bucket),
                quote_level: None,
                side,
                price: Price(101),
                qty: Qty(1),
                commission: 0,
                commission_asset: AssetId(0),
                liquidity: Some(Liquidity::Maker),
            });
        }
    }
    let fast_domain = domain(&fast, InstrumentId(0)).expect("a series to project");
    let visible_fast: Vec<ChartFill> = visible_fills(&fast, InstrumentId(0), fast_domain)
        .copied()
        .collect();
    assert_eq!(
        visible_fast.len() as u64,
        2 * window,
        "a whole window of fills at the run's real rate, none of them evicted"
    );
    assert_eq!(
        visible_fast[0].index,
        buckets(&fast, 0)[0].index,
        "so no bucket is ever drawn with its own markers already gone — which would read as a \
         strategy that did not trade"
    );

    let mut extremes_chart = chart_model(1);
    for (bid, ask) in [(100, 102), (109, 111), (89, 91), (99, 101)] {
        commit(&mut extremes_chart, 0, 1, bid, ask);
    }
    extremes_chart.apply_event(&fill_event(0, 1, Side::Sell, 150));
    let extremes_domain = domain(&extremes_chart, InstrumentId(0)).expect("a series to project");
    let extremes =
        bounds(&extremes_chart, InstrumentId(0), extremes_domain).expect("visible data to bound");
    assert_eq!(
        extremes,
        ChartBounds {
            low: 174,
            high: 306
        },
        "low 180 / high 300 (the fill above every mid), then 5% air each side"
    );
    assert!(
        extremes.low < 200 && extremes.high > 220,
        "extremes bound the axis, never the closes — so switching Line ↔ Candles cannot rescale y"
    );

    let mut flat_chart = chart_model(1);
    commit(&mut flat_chart, 0, 1, 100, 102);
    let flat_domain = domain(&flat_chart, InstrumentId(0)).expect("a series to project");
    let flat = bounds(&flat_chart, InstrumentId(0), flat_domain).expect("visible data to bound");
    assert_eq!(
        flat,
        ChartBounds {
            low: 201,
            high: 203
        },
        "an equal min and max expand by one whole tick, so the transform is never zero-height"
    );
    assert_eq!(
        y_fraction(202, flat),
        0.5,
        "the constant line sits mid-plot"
    );

    let wide = ChartDomain {
        first: 100,
        last: 700,
    };
    assert_eq!(x_fraction(100, wide), 0.0);
    assert_eq!(x_fraction(700, wide), 1.0);
    assert_eq!(x_fraction(400, wide), 0.5);
    assert_eq!(
        x_fraction(50, wide),
        0.0,
        "an index below the window clamps"
    );
    assert_eq!(x_fraction(9_999, wide), 1.0, "and above it clamps too");

    let range = ChartBounds {
        low: 100,
        high: 200,
    };
    assert_eq!(y_fraction(100, range), 0.0, "0.0 is the low bound");
    assert_eq!(y_fraction(200, range), 1.0);
    assert_eq!(y_fraction(150, range), 0.5);
    assert_eq!(y_fraction(-5_000, range), 0.0);
    assert_eq!(y_fraction(5_000, range), 1.0);
    assert_eq!(
        y_fraction(5, ChartBounds { low: 5, high: 5 }),
        0.5,
        "a zero-height window reads as its own centre rather than dividing by zero"
    );
}

/// Mirrors the chart view's narrowest candle, in points.
const MIN_CANDLE_PITCH: f64 = 4.0;

/// One bucket of the candle walk: its commits in order as (bid, ask), and whether its first commit
/// reveals a lost snapshot.
struct WalkBucket {
    index: u64,
    commits: &'static [(i64, i64)],
    has_gap_before: bool,
}

/// Three buckets to a candle at a 450 pt plot. The walk starts off a group boundary and ends on
/// the window's last slot, so both end candles are clamped. It holds a missing spin inside a group
/// (3004), one on a group boundary (3006), a lost snapshot revealed inside a group (3010) and a
/// long hole before 3299.
const WALK: [WalkBucket; 11] = [
    WalkBucket {
        index: 3001,
        commits: &[(100, 102)],
        has_gap_before: false,
    },
    WalkBucket {
        index: 3002,
        commits: &[(101, 103)],
        has_gap_before: false,
    },
    WalkBucket {
        index: 3003,
        commits: &[(100, 102), (104, 106)],
        has_gap_before: false,
    },
    WalkBucket {
        index: 3005,
        commits: &[(95, 97), (99, 101)],
        has_gap_before: false,
    },
    WalkBucket {
        index: 3007,
        commits: &[(110, 112)],
        has_gap_before: false,
    },
    WalkBucket {
        index: 3008,
        commits: &[(111, 113)],
        has_gap_before: false,
    },
    WalkBucket {
        index: 3009,
        commits: &[(112, 114)],
        has_gap_before: false,
    },
    WalkBucket {
        index: 3010,
        commits: &[(108, 110), (109, 111)],
        has_gap_before: true,
    },
    WalkBucket {
        index: 3011,
        commits: &[(110, 112)],
        has_gap_before: false,
    },
    WalkBucket {
        index: 3299,
        commits: &[(90, 92)],
        has_gap_before: false,
    },
    WalkBucket {
        index: 3300,
        commits: &[(91, 93)],
        has_gap_before: false,
    },
];

const WALK_PLOT_WIDTH: f32 = 450.0;

fn walked_chart() -> ChartModel {
    let mut chart = chart_model(1);
    for bucket in &WALK {
        for (position, &(bid, ask)) in bucket.commits.iter().enumerate() {
            let is_revealing = bucket.has_gap_before && position == 0;
            let continuity =
                if is_revealing { BookContinuity::GapBefore } else { BookContinuity::Continuous };
            chart.apply_book(&valid_book(0, bucket.index, bid, ask), continuity);
        }
    }
    chart
}

fn candles_at(chart: &ChartModel, domain: ChartDomain, plot_width: f32) -> Vec<Candle> {
    CandleGrouping::for_plot(domain, plot_width).candles(chart, InstrumentId(0))
}

/// FITNESS: grouping buckets into candles keeps each candle at least four points wide, and no
/// wider than it must be. Past that, a candle is the extremes and the ends of its buckets, a break
/// is drawn before it exactly where the line would have broken, and sliding the window never
/// changes a candle it still holds whole. A grouping that smeared candles into dots, or one that
/// redrew history as the window slid, would paint a price the market never made.
#[test]
fn candles_keep_their_pitch_and_their_extremes() {
    for width in [1u64, 2, 3, 7, 50, 299, 300, 301, 1_000, 3_000] {
        let domain = ChartDomain {
            first: 1_000,
            last: 1_000 + width - 1,
        };
        for whole_points in 100..=2_400u32 {
            let plot_width = whole_points as f32;
            let per_candle = CandleGrouping::for_plot(domain, plot_width).buckets_per_candle();
            let pitch = |buckets: u64| buckets as f64 * f64::from(plot_width) / width as f64;
            assert!(
                pitch(per_candle) >= MIN_CANDLE_PITCH,
                "{per_candle} bucket(s) per candle gives {} pt at {plot_width}pt over {width} slots",
                pitch(per_candle)
            );
            if pitch(1) >= MIN_CANDLE_PITCH {
                assert_eq!(
                    per_candle, 1,
                    "one bucket already has the room at {plot_width}pt over {width}"
                );
            } else {
                assert!(
                    pitch(per_candle - 1) < MIN_CANDLE_PITCH,
                    "{per_candle} per candle is more than {plot_width}pt over {width} slots needs"
                );
            }
        }
    }

    let chart = walked_chart();
    let window = domain(&chart, InstrumentId(0)).expect("a series to project");
    assert_eq!(
        window,
        ChartDomain {
            first: 3_001,
            last: 3_300
        }
    );
    let grouping = CandleGrouping::for_plot(window, WALK_PLOT_WIDTH);
    assert_eq!(grouping.buckets_per_candle(), 3);

    let candles = grouping.candles(&chart, InstrumentId(0));
    let shapes: Vec<(u64, u64, bool)> = candles
        .iter()
        .map(|candle| (candle.first, candle.last, candle.has_break_before))
        .collect();
    assert_eq!(
        shapes,
        vec![
            (3_001, 3_002, false),
            (3_003, 3_005, false),
            (3_006, 3_008, true),
            (3_009, 3_011, true),
            (3_297, 3_299, true),
            (3_300, 3_300, false),
        ],
        "the window's first candle never breaks; a missing spin inside a group is absorbed; one on \
         a group boundary, a lost snapshot inside a group and a long hole each break before it"
    );
    let prices: Vec<(i64, i64, i64, i64)> = candles
        .iter()
        .map(|candle| {
            (
                candle.open_half_ticks,
                candle.high_half_ticks,
                candle.low_half_ticks,
                candle.close_half_ticks,
            )
        })
        .collect();
    assert_eq!(
        prices,
        vec![
            (202, 204, 202, 204),
            (202, 210, 192, 200),
            (222, 224, 222, 224),
            (226, 226, 218, 222),
            (182, 182, 182, 182),
            (184, 184, 184, 184),
        ],
        "open from the first bucket present, close from the last, extremes from all of them"
    );

    let visible: Vec<_> = segment_points(&chart, InstrumentId(0), window).collect();
    for point in &visible {
        let owners = candles
            .iter()
            .filter(|candle| (candle.first..=candle.last).contains(&point.bucket.index))
            .count();
        assert_eq!(
            owners, 1,
            "bucket {} must sit in exactly one candle",
            point.bucket.index
        );
    }
    for candle in &candles {
        assert!(
            candle.first % 3 == 0 || candle.first == window.first,
            "a candle starts on a multiple of three or at the window's edge, got {}",
            candle.first
        );
    }

    let single = candles_at(&chart, window, 2_308.0);
    assert_eq!(
        single.len(),
        visible.len(),
        "with room to spare every bucket is its own candle"
    );
    for (position, (candle, point)) in single.iter().zip(&visible).enumerate() {
        let bucket = point.bucket;
        assert_eq!((candle.first, candle.last), (bucket.index, bucket.index));
        assert_eq!(
            (
                candle.open_half_ticks,
                candle.high_half_ticks,
                candle.low_half_ticks,
                candle.close_half_ticks
            ),
            (
                bucket.open_half_ticks,
                bucket.high_half_ticks,
                bucket.low_half_ticks,
                bucket.close_half_ticks
            )
        );
        assert_eq!(
            candle.has_break_before,
            position > 0 && point.is_run_start,
            "bucket {}: one bucket per candle breaks where the line breaks",
            bucket.index
        );
    }

    for slide in 1..=7u64 {
        let slid = ChartDomain {
            first: window.first + slide,
            last: window.last + slide,
        };
        let after = candles_at(&chart, slid, WALK_PLOT_WIDTH);
        let mut compared = 0;
        for before in candles.iter().filter(|candle| candle.first >= slid.first) {
            let whole = before.first % 3 == 0 && before.last == before.first + 2;
            let Some(same) = after.iter().find(|candle| candle.first == before.first) else {
                assert!(
                    !whole,
                    "a whole candle at {} vanished after sliding {slide}",
                    before.first
                );
                continue;
            };
            if !whole {
                continue;
            }
            compared += 1;
            assert_eq!(
                (
                    same.last,
                    same.open_half_ticks,
                    same.high_half_ticks,
                    same.low_half_ticks,
                    same.close_half_ticks
                ),
                (
                    before.last,
                    before.open_half_ticks,
                    before.high_half_ticks,
                    before.low_half_ticks,
                    before.close_half_ticks
                ),
                "the candle at {} changed when the window slid {slide}",
                before.first
            );
        }
        assert!(
            compared >= 2,
            "sliding {slide} compared only {compared} whole candle(s)"
        );
    }
}

proptest! {
    /// A series that projects always bounds. The painter reads the two together, so a domain without
    /// bounds paints "no mid samples yet" over live data — the chart's own honest-empty state turned
    /// into a lie.
    #[test]
    fn a_series_with_a_domain_always_has_bounds(
        steps in prop::collection::vec(0u64..2_000, 1..40),
    ) {
        let mut chart = chart_model(1);
        let mut bucket = 0u64;
        for step in steps {
            bucket += step;
            commit(&mut chart, 0, bucket, 100, 102);
        }

        let domain = domain(&chart, InstrumentId(0)).expect("a commit banks a bucket");
        prop_assert!(bounds(&chart, InstrumentId(0), domain).is_some());
    }

    /// Prices round-trip through the y transform, monotonically and inside `0..=1`.
    #[test]
    fn prices_round_trip_through_the_y_transform(
        low in -1_000_000i64..1_000_000,
        span in 1i64..10_000,
        offset in 0i64..10_000,
    ) {
        let bounds = ChartBounds { low, high: low + span };
        let price = low + offset.min(span);
        let fraction = y_fraction(price, bounds);
        prop_assert!((0.0..=1.0).contains(&fraction));
        prop_assert!(fraction >= y_fraction(price - 1, bounds));
        prop_assert_eq!(low + (f64::from(fraction) * span as f64).round() as i64, price);
    }
}
