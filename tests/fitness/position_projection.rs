//! Risk-chart projection fitness: the map from the ordered event lane to the rolling exposure/PnL
//! series, and the quote-unit window over it. The lower chart is stacked ON the mid chart and reads
//! under one shared crosshair, so its failure mode is not a crash but a value shown against the wrong
//! spin — or an axis whose labels imply a position nobody holds. Three properties carry that weight:
//! both series bucket one event time identically, the window is never derived here, and a wire value
//! becomes the exact quote mantissa the engine's ledger held.

use polysim::ids::{FIXED_SCALE, InstrumentId};
use polysim::msg::ui::UiEvent;
use polysim::time::{DurationUs, TsUs};
use polysim::workstation::chart::model::ChartModel;
use polysim::workstation::chart::position_model::{PositionBucket, PositionModel};
use polysim::workstation::chart::position_view::{RiskSeries, bounds, visible_buckets};
use polysim::workstation::chart::view::{ChartDomain, domain, x_fraction};
use proptest::prelude::*;

use crate::workstation_support::{
    CAPACITY, SPIN, TICK, at, chart_model, commit_at, rotation_event,
};

fn positions(instruments: usize) -> PositionModel {
    PositionModel::with_capacity(instruments, SPIN)
}

/// The frame as the engine emits it: absolute state, its exact quote mantissas divided down to the
/// `f64` the link carries — so every test here drives the real conversion rather than a value
/// chosen to survive it.
fn position(instrument: u16, bucket: u64, exposure_mantissa: i64, pnl_mantissa: i64) -> UiEvent {
    wire_position(
        instrument,
        bucket,
        exposure_mantissa as f64 / FIXED_SCALE as f64,
        pnl_mantissa as f64 / FIXED_SCALE as f64,
    )
}

/// A frame carrying arbitrary wire values, for the ones no ledger could have produced.
fn wire_position(instrument: u16, bucket: u64, exposure_quote: f64, pnl_quote: f64) -> UiEvent {
    UiEvent::Position {
        instrument: InstrumentId(instrument),
        seq: 0,
        event_ts_us: at(bucket),
        exposure_quote,
        pnl_quote,
    }
}

fn banked(positions: &PositionModel, instrument: u16) -> Vec<PositionBucket> {
    positions
        .buckets(InstrumentId(instrument))
        .copied()
        .collect()
}

/// A window over the whole retained series, supplied the way the composer supplies it: from outside.
fn window(first: u64) -> ChartDomain {
    ChartDomain {
        first,
        last: first + CAPACITY - 1,
    }
}

/// A spin banks the engine's LATEST absolute state, never an average or a first sample: the frames
/// are re-sent every spin precisely so the freshest one is the truth. A frame stamped
/// back inside a settled bucket is dropped rather than rewriting a bucket the painter has drawn.
#[test]
fn the_last_state_inside_a_spin_is_the_one_its_bucket_keeps() {
    let mut positions = positions(1);
    for event in [
        position(0, 5, 100, -20),
        position(0, 5, 300, -60),
        position(0, 4, 999, 999),
        position(0, 6, 400, -80),
    ] {
        positions.apply_event(&event);
    }

    assert_eq!(
        banked(&positions, 0),
        vec![
            PositionBucket {
                index: 5,
                exposure_quote: 300,
                pnl_quote: -60,
            },
            PositionBucket {
                index: 6,
                exposure_quote: 400,
                pnl_quote: -80,
            },
        ],
        "the settled bucket keeps its last state and the late frame changes nothing"
    );
    assert_eq!(
        positions.latest(InstrumentId(0)).map(|b| b.exposure_quote),
        Some(400),
        "the readout beside the toggle shows the newest bucket"
    );
}

/// The honesty pin for this view. A rotation is a new market in the slot, and the engine then goes
/// SILENT for it: `Position` rides only while the row holds a mark, and rotation clears the mark
/// until a fresh two-sided book re-marks it. No zero frame announces the reset, so the rotation event
/// is the whole of the signal — the spins of silence behind it must leave the series EMPTY, never
/// holding the pre-rotation value. Holding it would show live risk in a market that no longer exists;
/// clearing the wrong instrument would erase risk the engine really does hold. The same fabrication
/// risk shows up in the window: no visible bucket must mean NO window, never an invented one — a
/// chart drawn for an instrument holding no position would be indistinguishable from a genuinely flat
/// one.
#[test]
fn a_window_never_fabricates_a_value_it_cannot_support() {
    {
        let mut positions = positions(2);
        positions.apply_event(&position(0, 1, 500, 10));
        positions.apply_event(&position(1, 1, 700, 20));

        positions.apply_event(&rotation_event(0, 2));

        assert!(banked(&positions, 0).is_empty());
        assert_eq!(positions.latest(InstrumentId(0)), None);

        // Minutes of spins pass with the rotated row emitting nothing, because it holds no mark.
        for bucket in 3..40 {
            positions.apply_event(&position(1, bucket, 700, 20));
        }

        assert!(
            banked(&positions, 0).is_empty(),
            "silence is the absence of an honest valuation, never a value to carry forward"
        );
        assert_eq!(
            banked(&positions, 1).first(),
            Some(&PositionBucket {
                index: 1,
                exposure_quote: 700,
                pnl_quote: 20,
            }),
            "the neighbour's pre-rotation history is untouched"
        );
    }

    let empty = positions(1);
    assert_eq!(
        bounds(&empty, InstrumentId(0), RiskSeries::Exposure, window(0)),
        None,
        "an instrument that has banked nothing has no window"
    );

    let mut one_bucket = positions(1);
    one_bucket.apply_event(&position(0, 5, 250_000_000, -125_000_000));
    assert!(
        bounds(
            &one_bucket,
            InstrumentId(0),
            RiskSeries::Exposure,
            window(0)
        )
        .is_some(),
        "one banked bucket inside the window does bound"
    );
    assert_eq!(
        bounds(
            &one_bucket,
            InstrumentId(0),
            RiskSeries::Exposure,
            window(CAPACITY + 1)
        ),
        None,
        "and a window the whole series falls outside has nothing to bound either"
    );
}

/// The two series' extents genuinely differ, so a risk chart deriving its own window would put the
/// same bucket somewhere else on screen: the mid banks on every valid two-sided book from run start, a
/// position banks once per spin and only once the engine holds a mark. Built here in that production
/// shape — the mid strictly earlier and denser — so the assertion has something to bite on; built from
/// a stream where both start together it would pass while pinning nothing. What it pins is that
/// sharing the mid's domain is load-bearing rather than incidental, not that no other window could be
/// computed.
#[test]
fn a_self_derived_window_would_put_the_same_bucket_somewhere_else() {
    let mut chart = chart_model(1);
    let mut positions = positions(1);
    for bucket in 0..=400 {
        commit_at(&mut chart, 0, at(bucket));
    }
    let position_buckets = [250, 300, 350, 400];
    for bucket in position_buckets {
        positions.apply_event(&position(0, bucket, 1_000, 500));
    }

    let mid = domain(&chart, InstrumentId(0)).expect("a mid series to project");
    assert_eq!(
        mid,
        window(101),
        "a full window ending at the newest bucket"
    );

    // What a window derived from the position series' own extents would be, built through the same
    // public projection so the comparison cannot drift from the real formula.
    let mut shadow = chart_model(1);
    for bucket in position_buckets {
        commit_at(&mut shadow, 0, at(bucket));
    }
    let self_derived = domain(&shadow, InstrumentId(0)).expect("a shadow series to project");

    assert_ne!(mid, self_derived);
    assert_eq!(
        x_fraction(400, mid),
        1.0,
        "the newest spin is the right edge"
    );
    assert!(
        x_fraction(400, self_derived) < 0.6,
        "the SAME spin would sit near the middle of a self-derived window: got {}",
        x_fraction(400, self_derived)
    );

    assert_eq!(
        visible_buckets(&positions, InstrumentId(0), mid).count(),
        position_buckets.len(),
        "every banked position is inside the mid chart's window, so injecting it loses nothing"
    );
    // A series outlives the window that slid past it: the ring still holds bucket 250 long after the
    // mid chart stopped drawing it, and a painter handed the whole ring would draw off its own rect.
    let slid_on = ChartDomain {
        first: 300,
        last: 400,
    };
    assert_eq!(
        visible_buckets(&positions, InstrumentId(0), slid_on)
            .map(|bucket| bucket.index)
            .collect::<Vec<_>>(),
        vec![300, 350, 400]
    );
}

/// A hostile peer can send a stamp in no bucket, or a value that cannot be a quote mantissa — the
/// link is an UNTRUSTED remote producer and decode validates a frame's count and schema hash, never
/// its float values. `inf`, `NaN` and an absurd finite magnitude all saturate the cast to `i64::MAX`,
/// and one such bucket crushes every honest sample in the five-minute window flat against the axis —
/// a display-denial primitive for anyone who can reach the port. Every such frame must be dropped and
/// COUNTED, never silently: the counter is the only signal that a peer is sending frames the ledger
/// cannot have produced. Drop and count, never panic: this is external and expected.
#[test]
fn frames_the_ledger_could_not_have_produced_are_dropped_and_counted() {
    let cases: [(&str, UiEvent); 6] = [
        (
            "timestamp before the epoch",
            UiEvent::Position {
                instrument: InstrumentId(0),
                seq: 0,
                event_ts_us: TsUs::from_micros(-1),
                exposure_quote: 2.5,
                pnl_quote: 1.0,
            },
        ),
        ("exposure is +inf", wire_position(0, 1, f64::INFINITY, 0.0)),
        (
            "exposure is -inf",
            wire_position(0, 2, f64::NEG_INFINITY, 0.0),
        ),
        ("exposure is NaN", wire_position(0, 3, f64::NAN, 0.0)),
        (
            "exposure is an absurd finite magnitude",
            wire_position(0, 4, 1e300, 0.0),
        ),
        ("pnl is +inf", wire_position(0, 5, 12.5, f64::INFINITY)),
    ];

    let mut positions = positions(1);
    for (name, event) in &cases {
        positions.apply_event(event);
        assert!(
            banked(&positions, 0).is_empty(),
            "case {name}: a hostile frame must not reach the series"
        );
    }
    assert_eq!(
        positions.rejected_frames(),
        cases.len() as u64,
        "every case above was counted as a drop, not silent"
    );

    positions.apply_event(&position(0, 9, 250_000_000, -125_000_000));
    assert_eq!(
        banked(&positions, 0),
        vec![PositionBucket {
            index: 9,
            exposure_quote: 250_000_000,
            pnl_quote: -125_000_000,
        }],
        "an honest frame behind them still banks"
    );
    assert_eq!(
        positions.rejected_frames(),
        cases.len() as u64,
        "the count moves only on a rejection"
    );
}

/// The measured case behind the conversion. `1e8 = 2^8·5^8` is not a power of two, so dividing a
/// mantissa down for the wire and multiplying it back lands a hair short: truncation loses a whole
/// mantissa on 7.99% of values, and `-1_999_999` is the first. A cent silently missing from a PnL
/// readout is exactly the kind of lie no crash ever reports.
#[test]
fn the_first_mantissa_truncation_would_lose_survives_the_wire() {
    let mut positions = positions(1);
    positions.apply_event(&position(0, 0, -1_999_999, -1_999_999));

    assert_eq!(
        banked(&positions, 0).first().map(|b| b.exposure_quote),
        Some(-1_999_999)
    );
}

proptest! {
    /// Alignment is a property of construction: both models must bucket an event time through the
    /// same arithmetic at the same cadence, and retain the same number of slots, or the shared window
    /// the two charts stack under is a slot wrong for one of them.
    #[test]
    fn both_models_bucket_one_event_time_alike(
        // Epoch-scale µs at every cadence the model accepts, so the overwhelming majority of draws
        // land strictly inside a bucket rather than on its boundary — an off-by-one in either model's
        // arithmetic is inside this grid, and so is the boundary case itself.
        micros in 0i64..2_000_000_000_000,
        spin_micros in 1i64..=2_000_000,
    ) {
        let spin = DurationUs::from_micros(spin_micros);
        let mut chart = ChartModel::with_capacity(1, spin);
        chart.configure(&[Some(TICK)], spin);
        let mut positions = PositionModel::with_capacity(1, spin);

        let stamp = TsUs::from_micros(micros);
        commit_at(&mut chart, 0, stamp);
        positions.apply_event(&UiEvent::Position {
            instrument: InstrumentId(0),
            seq: 0,
            event_ts_us: stamp,
            exposure_quote: 1.0,
            pnl_quote: 1.0,
        });

        prop_assert_eq!(
            chart.buckets(InstrumentId(0)).next().map(|bucket| bucket.index),
            positions.buckets(InstrumentId(0)).next().map(|bucket| bucket.index)
        );
        prop_assert_eq!(chart.capacity(), positions.capacity());
    }

    /// Zero stays in view whatever the series does — including the all-positive and all-negative runs
    /// a directional strategy produces, which are the draws that would expose bounds seeded from the
    /// data alone. The two series are fed OPPOSITE values, so the window also proves the toggle scales
    /// the series it names: showing a PnL window under an EXPOSURE label is a silent lie, not a crash.
    #[test]
    fn zero_is_inside_the_window_of_whichever_series_the_toggle_names(
        // ±$1M in mantissas, spanning sub-cent to well past any position the recorder takes. Signs
        // are unconstrained, so all-positive, all-negative and zero-crossing runs are all drawn.
        exposures in prop::collection::vec(-100_000_000_000_000i64..100_000_000_000_000, 1..24),
        series in prop_oneof![Just(RiskSeries::Exposure), Just(RiskSeries::Pnl)],
    ) {
        let mut positions = PositionModel::with_capacity(1, SPIN);
        for (bucket, &exposure) in exposures.iter().enumerate() {
            positions.apply_event(&position(0, bucket as u64, exposure, -exposure));
        }
        // Transcribed from what the stream carried, never read back through the selector under test.
        let shown: Vec<i64> = match series {
            RiskSeries::Exposure => exposures.clone(),
            RiskSeries::Pnl => exposures.iter().map(|exposure| -exposure).collect(),
        };

        let quote_window = bounds(&positions, InstrumentId(0), series, window(0))
            .expect("a banked series to bound")
            .as_chart_bounds();
        prop_assert!(quote_window.low <= 0 && quote_window.high >= 0);
        for value in shown {
            prop_assert!(quote_window.low <= value && value <= quote_window.high);
        }
    }

    /// The wire is `f64` by ratified design, so the projection's job is to give back the exact
    /// mantissa the engine's ledger held. Exact recovery is provable while `|mantissa|` stays far
    /// below `2^53`: the two roundings carry ~2.3e-16 relative error, which at 1e14 is 0.02 — an
    /// order of magnitude inside the half-mantissa `.round()` needs. Above that band the wire itself
    /// is lossy (~$90M) and no conversion can help.
    #[test]
    fn a_quote_mantissa_survives_the_wire_round_trip_exactly(
        // ±$1M in mantissas. The measured 7.99% of truncation failures are spread across this whole
        // grid, and `-1_999_999` — the smallest of them — is pinned deterministically above.
        exposure in -100_000_000_000_000i64..100_000_000_000_000,
        pnl in -100_000_000_000_000i64..100_000_000_000_000,
    ) {
        let mut positions = PositionModel::with_capacity(1, SPIN);
        positions.apply_event(&position(0, 0, exposure, pnl));

        prop_assert_eq!(
            positions.buckets(InstrumentId(0)).next().copied(),
            Some(PositionBucket { index: 0, exposure_quote: exposure, pnl_quote: pnl })
        );
    }
}
