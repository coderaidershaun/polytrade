//! Where one Kyle observation ends and the next begins: a wrong boundary halves or inverts the
//! price impact that each sizing model reads, with no error.

use engine::hot::quant::liquidity::{KyleEstimate, KyleFeed, KylesLambdaSpec};

fn feed() -> KyleFeed {
    KyleFeed::new(KylesLambdaSpec {
        window: 100,
        min_observations: 2,
        // One-sided flow is the point of the sequence below; the sign gate would refuse it.
        min_sign_fraction: 0.0,
        ..KylesLambdaSpec::default()
    })
}

/// One step of the fold, in the order in which the recorder meets it.
#[derive(Debug, Clone, Copy)]
enum Step {
    /// A closed volume bar, handed over inside the dispatch of the trade that closed it.
    Bar { flow: f64, arrivals: u32 },
    /// The mid of the book once an update is committed.
    Commit(f64),
    /// A book reset: the committed mid reads nothing until the next commit.
    Reset,
}

fn bar(flow: f64, arrivals: u32) -> Step {
    Step::Bar { flow, arrivals }
}

fn commit(mid: f64) -> Step {
    Step::Commit(mid)
}

/// Feeds each step as the recorder does, and returns what each step gave back. A trade's dispatch
/// reads the mid of the last commit.
fn fold(feed: &mut KyleFeed, steps: &[Step]) -> Vec<Option<KyleEstimate>> {
    let mut committed = None;
    steps
        .iter()
        .map(|step| match *step {
            Step::Bar { flow, arrivals } => feed.on_bar(flow, arrivals, committed),
            Step::Commit(mid) => {
                committed = Some(mid);
                None
            }
            Step::Reset => {
                committed = None;
                None
            }
        })
        .collect()
}

fn newest(estimates: &[Option<KyleEstimate>]) -> Option<KyleEstimate> {
    estimates.iter().rev().find_map(|estimate| *estimate)
}

/// A trade bigger than one target closes several bars at once, and the volume clock books the
/// arrival to the FIRST of them — the ones it pours through carry a full target of notional and
/// zero arrivals (`volume_bars::on_volume_fires_once_per_closed_bar_oldest_first` pins that at the
/// producer). Every one of those bars also reaches the strategy inside a single trade dispatch, so
/// they all read the same mid.
///
/// The feed must therefore fold them into the run the arrival opened. Scoring them as observations
/// in their own right credits the trade's whole price move to its first slice and then pairs the
/// remaining slices with a mid that has not moved, which drags the slope toward zero and past it:
/// the sequence below fits +0.375 read as arrivals and -0.214 read as bars.
#[test]
fn a_bar_no_trade_arrived_in_joins_the_run_it_poured_out_of() {
    let mut feed = feed();
    // Four trades, each followed by the commit of its own book update. The second and the third
    // sweep through a second bar.
    let steps = [
        commit(100.0),
        bar(1.0, 1),
        commit(101.0),
        bar(2.0, 1),
        bar(6.0, 0),
        commit(105.0),
        bar(4.0, 1),
        bar(12.0, 0),
        commit(106.0),
        bar(1.0, 1),
    ];

    let KyleEstimate {
        observations,
        lambda,
        intercept,
        ..
    } = newest(&fold(&mut feed, &steps)).expect("two observations reach min_observations");
    assert_eq!(
        observations, 2,
        "one observation per trade, not one per closed bar"
    );
    // Two points, (8, 1.0) and (16, 4.0): the slope is exact, not a least-squares approximation.
    assert!(
        (lambda - 0.375).abs() < 1e-12,
        "lambda {lambda}, expected the slope through the two aggregated runs"
    );
    assert!(
        (intercept - -2.0).abs() < 1e-12,
        "intercept {intercept}, expected the line through the two aggregated runs"
    );
}

/// A book reset and a teardown each break the chain of mids. An observation that paired a mid
/// from before the break with one from after it would read the jump across the gap as the impact
/// of one run, with no error.
#[test]
fn no_observation_spans_a_missing_mid_or_a_clear() {
    // Each trade moves the mid by its own 0.5, and the flow alternates +4 and -4. So each
    // observation inside the stream reads a move of 0.5, and the line through them is exact:
    // slope 0 and intercept 0.5. A pair across the reset would read the jump of 48.5 to the
    // resynced book and move both.
    let mut gapped = feed();
    let mut steps = vec![commit(100.0)];
    for (index, flow) in [4.0, -4.0, 4.0, -4.0].into_iter().enumerate() {
        steps.push(bar(flow, 1));
        if index < 3 {
            steps.push(commit(100.5 + 0.5 * index as f64));
        }
    }
    steps.push(Step::Reset);
    let reset_at = steps.len();
    for (index, flow) in [4.0, -4.0, 4.0, -4.0, 4.0].into_iter().enumerate() {
        steps.push(bar(flow, 1));
        if index < 4 {
            steps.push(commit(150.0 + 0.5 * index as f64));
        }
    }
    let estimates = fold(&mut gapped, &steps);
    let before_reset = newest(&estimates[..reset_at]).expect("two observations before the reset");
    let last = newest(&estimates).expect("observations after the reset");
    assert_eq!(before_reset.observations, 2);
    assert!(
        last.observations > before_reset.observations,
        "no observation entered after the reset: the feed never anchored again"
    );
    assert_eq!(last.lambda, 0.0, "a pair spans the reset: {last:?}");
    assert_eq!(last.intercept, 0.5, "a pair spans the reset: {last:?}");

    // A cleared feed folds what follows exactly as a new one: no window, no waiting run and no
    // anchor survive. The prefix leaves all three behind.
    let prefix = [
        commit(100.0),
        bar(3.0, 1),
        commit(101.0),
        bar(-2.0, 1),
        commit(100.5),
        bar(5.0, 1),
        commit(102.0),
        bar(1.0, 1),
    ];
    let after = [
        commit(200.0),
        bar(2.0, 1),
        commit(200.5),
        bar(-6.0, 1),
        commit(199.0),
        bar(4.0, 1),
        commit(201.0),
        bar(-1.0, 1),
        commit(200.0),
        bar(1.0, 1),
    ];
    let mut cleared = feed();
    fold(&mut cleared, &prefix);
    cleared.reset();
    let from_cleared = fold(&mut cleared, &after);
    let from_new = fold(&mut feed(), &after);
    assert!(
        from_new.iter().any(Option::is_some),
        "the sequence after the clear must reach an estimate, or the equality proves nothing"
    );
    assert_eq!(
        from_cleared, from_new,
        "state of the prefix survived the clear"
    );
}
