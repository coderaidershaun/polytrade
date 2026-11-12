//! The park/resume seam on the STRATEGY side.
//!
//! `HotEngine::resume` wipes the engine's trackers and EwmaVol residents because "a Hawkes or EGARCH
//! fit spanning a multi-minute hole is poison the features would carry silently" — but Hawkes and
//! EGARCH live in the strategy, beside the Kyle estimator, the intensity warm-start, the resilience
//! window and the markout fill queues. Naming those one at a time would
//! date the moment a column is added, so the invariant is equivalence instead: a tape replayed after
//! a park/resume must emit exactly what a cold engine emits from that same tape. Anything at all a
//! strategy folds across the gap breaks it, and the break is silent — the features look fine.
//!
//! The position is not among them, and is the one column deliberately held out of the comparison:
//! the ENGINE owns it, and a park preserves it on purpose — parking sells no coin, so the resumed
//! engine enters the suffix holding what the prefix opened while a cold one starts flat. That
//! difference is the policy working, not a leak, and it cannot be rebased away either, because the
//! carried position is marked to a mid that moves. So `inventory_quote` is split out of the row
//! comparison, its emission COUNT is still held to the cold engine's, and its value policy is pinned
//! directly by `tests/fitness/position_ledger.rs`. Nothing else is exempt: the position also gates
//! the strategy's quoting through the risk budget, so a carry big enough to withdraw a side would
//! still surface here as a difference in the quote columns.
//!
//! The prefix opens that position through real venue fills, because there is no other way to open
//! one: a strategy can no longer assert a fill into existence. The recorder implements none of the
//! fill callbacks, so those messages move the ENGINE's ledger and touch nothing the strategy owns —
//! which is precisely the split this test exists to hold apart.

use polysim::config::{IntensitySpec, KlineInterval, RecordedTables, TableKind};
use polysim::hot::strategy::StrategyConfig;
use polysim::ids::{Price, Side};
use polysim::msg::inbound::InboundMessage;
use polysim::msg::persist::{FeatureId, FeatureRow, PersistRecord};
use polysim::registry::InstrumentRow;
use polysim::time::DurationUs;

use crate::engine_support::{
    FillPen, LinkedEngine, LinkedSetup, ONE, book_reset, delta_chunk, engine_view,
    engine_with_link, idle_at, instrument_row, kline, pop, recorder_spec, run_control, running_at,
    snapshot_pair, spin, tracker_spec_all, trade,
};
use crate::micro_strategy::features::FEATURE_NAMES;
use crate::micro_strategy::{MicroRecorder, MicroRecorderParams};

/// One message every half second over a six-phase cycle, so the spin the recorder emits on lands
/// every third second — and the engine view below has to agree, or every window it sizes from the
/// cadence would be wrong about its own horizon.
const STEP_US: i64 = 500_000;
const SPIN_INTERVAL: DurationUs = DurationUs::from_secs(3);

/// The grid the row is stamped with; prints walk out from the ask in whole ticks of it.
const TICK: i64 = ONE / 100;

/// Long enough to carry every estimator past its floor: 400 prints past the 100-arrival Hawkes
/// floor, and 400 closed candles past the 300-close EGARCH floor.
const PREFIX_STEPS: i64 = 2_400;

/// Walks the EGARCH close floor from BELOW it to PAST it — 310 candles against the fit's 300. Under
/// the floor, a recorder that carried its close history across the gap fits hundreds of candles
/// earlier than a cold one; over it, the fit actually runs, which is the only way anything the fit
/// itself carries across the gap (its Nelder-Mead warm start) can reach a column.
const SUFFIX_STEPS: i64 = 6 * 310;

/// Ten minutes parked — the span the resume reset exists for. Nothing arrives during it.
const HOLE_US: i64 = 600_000_000;

const PARK_TS_US: i64 = PREFIX_STEPS * STEP_US;
const RESUME_TS_US: i64 = PARK_TS_US + HOLE_US;

/// The prefix buys a clip every this many steps, so the park has a real position to preserve. Only
/// the prefix fills: the suffix must be the SAME tape for both engines, and the whole comparison is
/// about what the resumed one carried into it.
const FILL_EVERY_STEPS: i64 = 600;

/// One hundredth of a base unit per fill — four fills mark to roughly four quote units against the
/// row's million-unit exposure ceiling, so the carried position is unmistakably non-zero and nowhere
/// near the risk budget. A carry that DID breach would withdraw a side and show up as a difference
/// in the quote columns, which this test reads as an estimator leak.
const FILL_QTY: i64 = ONE / 100;

/// FITNESS: a park/resume round trip must leave the strategy's own estimators as empty as a cold
/// boot. Failure is silent research-data corruption — post-resume feature rows that look ordinary
/// and are folded over a hole the market spent somewhere else.
#[test]
fn resuming_emits_what_a_cold_engine_emits() {
    let instruments = [recorded_row()];
    let mut resumed = recorder_engine(&instruments);

    let mut prefix_counts = vec![0usize; FEATURE_NAMES.len()];
    let mut scratch = Vec::new();
    let mut pen = FillPen::new(0);
    seed_book(&mut resumed, 0, &mut scratch);
    for index in 0..PREFIX_STEPS {
        let when = index * STEP_US;
        dispatch(&mut resumed, &message(index, when), &mut scratch);
        // A position can only come from the venue now, so the park has something to preserve only
        // if this tape buys. The recorder implements no fill callback, so these move the engine's
        // ledger and nothing the strategy owns — which is exactly the split the test rests on.
        if index % FILL_EVERY_STEPS == 0 {
            for message in pen.fill(Side::Buy, 101 * ONE, FILL_QTY, when + 1) {
                dispatch(&mut resumed, &message, &mut scratch);
            }
        }
        for row in scratch.drain(..) {
            prefix_counts[usize::from(row.feature.0)] += 1;
        }
    }
    for column in ["egarch_vol_lt", "hawkes_mu_ask_per_sec", "inventory_quote"] {
        assert!(
            prefix_counts[column_index(column)] > 0,
            "the pre-park tape never warmed {column}, so this test would pass vacuously"
        );
    }

    // The resume must allocate nothing either, and the strategy's half of it is the heavy one:
    // `InstrumentState`
    // rebuilds every estimator it owns in place. The engine's half already runs under the counting
    // allocator in `zero_alloc`; measured here so a `Vec::new()` or a `Box` slipped into one of
    // those resets fails a test rather than a live park.
    let before = crate::alloc_count();
    for marker in [
        run_control(idle_at(1), PARK_TS_US),
        run_control(running_at(2), RESUME_TS_US),
    ] {
        resumed.engine.dispatch(pop(0, 0), &marker);
    }
    let after = crate::alloc_count();
    assert_eq!(
        after, before,
        "the park/resume pair allocated — the hot thread never reaches the allocator in steady \
         state, so a strategy reset clears its state in place instead of rebuilding it"
    );
    while resumed.persist.pop().is_ok() {}

    let mut after_resume = replay_suffix(&mut resumed);
    let mut cold = recorder_engine(&instruments);
    let mut from_cold = replay_suffix(&mut cold);
    rebase_lifetime_counters(&mut after_resume);
    rebase_lifetime_counters(&mut from_cold);

    assert!(
        from_cold.len() > 500,
        "the cold replay emitted only {} rows — nothing meaningful is being compared",
        from_cold.len()
    );
    assert!(
        counts(&from_cold)[column_index("egarch_omega")] > 0,
        "the suffix tape never reached the EGARCH close floor, so the fit never ran and the \
         warm-start simplex it caches went unexercised"
    );
    assert_eq!(
        counts(&after_resume),
        counts(&from_cold),
        "columns emitted a different number of times after a resume than from cold — {:?} \
         carried pre-pause state across the gap",
        differing_columns(&after_resume, &from_cold)
    );
    let (resumed_inventory, resumed_estimators) = split_inventory(&after_resume);
    let (cold_inventory, cold_estimators) = split_inventory(&from_cold);
    // The tripwire below accuses the ENGINE, so its premise has to be established here rather than
    // assumed: a prefix that happened to end flat would carry nothing across the park, the two
    // series would match honestly, and the failure would name a flatten that never came back.
    assert!(
        resumed_inventory
            .first()
            .is_some_and(|row| row.value != 0.0),
        "the pre-park tape left no position for the park to preserve, so the comparison below \
         would be vacuous — fix the recorder tape, not the engine"
    );
    assert_ne!(
        resumed_inventory, cold_inventory,
        "the park preserved a position, so the resumed engine's inventory must differ from a cold \
         engine's — an identical series means something flattened the ledger on resume again"
    );
    assert_eq!(
        resumed_estimators,
        cold_estimators,
        "post-resume feature rows differ from the cold ones — {:?} folded pre-pause samples \
         across the gap",
        differing_columns(&resumed_estimators, &cold_estimators)
    );
}

/// `(inventory rows, everything else)`. The engine's money is not a strategy estimator, and a park
/// preserves it deliberately, so it is the one column whose VALUE may legitimately differ between
/// the two runs — see this module's header.
fn split_inventory(rows: &[FeatureRow]) -> (Vec<FeatureRow>, Vec<FeatureRow>) {
    let inventory = FeatureId(column_index("inventory_quote") as u16);
    rows.iter()
        .copied()
        .partition(|row| row.feature == inventory)
}

/// The resync every adapter performs when it reconnects, then the tape. Both engines take exactly
/// this, so the only difference between them is the ten-minute hole one of them lived through.
fn replay_suffix(linked: &mut LinkedEngine) -> Vec<FeatureRow> {
    let mut rows = Vec::new();
    seed_book(linked, RESUME_TS_US + 1, &mut rows);
    rows.clear();
    for index in 0..SUFFIX_STEPS {
        dispatch(
            linked,
            &message(index, RESUME_TS_US + 3 + index * STEP_US),
            &mut rows,
        );
    }
    rows
}

/// A book reset and a fresh snapshot: what a reconnecting adapter sends, and what puts both engines'
/// books in the same state whatever they held before.
fn seed_book(linked: &mut LinkedEngine, when: i64, rows: &mut Vec<FeatureRow>) {
    let (bids, asks) = snapshot_pair(
        0,
        &[(100 * ONE, 2 * ONE), (99 * ONE, ONE)],
        &[(101 * ONE, ONE), (102 * ONE, 2 * ONE)],
        when + 1,
    );
    for message in [
        InboundMessage::BookReset(book_reset(0, when)),
        InboundMessage::Book(bids),
        InboundMessage::Book(asks),
    ] {
        dispatch(linked, &message, rows);
    }
}

/// One phase of the six-message cycle a binance feed shows the recorder: a print walking out from
/// the ask, depth churn either side of it, a closed candle, and the spin that emits.
fn message(index: i64, when: i64) -> InboundMessage {
    match index % 6 {
        0 => InboundMessage::Trade(trade(
            0,
            101 * ONE + (index % 4) * TICK,
            1_000_000,
            Side::Buy,
            when,
        )),
        1 => InboundMessage::Book(delta_chunk(
            0,
            Side::Buy,
            &[(100 * ONE, (1 + index % 5) * ONE)],
            when,
        )),
        2 => InboundMessage::Book(delta_chunk(
            0,
            Side::Buy,
            &[(99 * ONE, (1 + index % 5) * ONE)],
            when,
        )),
        3 => InboundMessage::Book(delta_chunk(
            0,
            Side::Sell,
            &[(102 * ONE, (1 + index % 5) * ONE)],
            when,
        )),
        4 => InboundMessage::Kline(kline(
            0,
            KlineInterval::OneMinute,
            (100 * ONE, 101 * ONE, 99 * ONE, (100 + index % 5) * ONE),
            true,
            when,
        )),
        _ => InboundMessage::SpinTick(spin(index as u64, when)),
    }
}

fn dispatch(linked: &mut LinkedEngine, message: &InboundMessage, rows: &mut Vec<FeatureRow>) {
    linked.engine.dispatch(pop(0, 0), message);
    while let Ok(record) = linked.persist.pop() {
        if let PersistRecord::Feature(row) = record {
            rows.push(row);
        }
    }
}

fn recorder_engine(instruments: &[InstrumentRow]) -> LinkedEngine {
    let spec = recorder_spec::<MicroRecorderParams>(vec![TableKind::Features]);
    engine_with_link(LinkedSetup {
        instruments,
        strategy: Box::new(MicroRecorder::from_spec(&spec, engine_view(SPIN_INTERVAL))),
        tables: RecordedTables::new(&[TableKind::Features]),
        // Zero, so the post-resume comparison is against a cold engine that is live from its first
        // message too. Re-arming a real span is `link_control`'s subject, not this one's.
        warmup: DurationUs::ZERO,
    })
}

/// The row the recorder needs to light every path: a tick grid for the Guéant snap and Kyle, and a
/// reach histogram for the (A, k) fit.
fn recorded_row() -> InstrumentRow {
    let mut row = instrument_row(0, tracker_spec_all(100), 128);
    row.tick_size = Some(Price(TICK));
    row.tracker.intensity = Some(IntensitySpec {
        max_depth_ticks: 16,
        half_life_secs: 600.0,
        min_events: 5.0,
    });
    row
}

/// The pseudo-fill tallies are LIFETIME counters by design: they sit beside the tracker's other
/// lifetime diagnostics, and `MarkoutTracker::clear` deliberately spares them through a market
/// rotation — the strongest reset the engine has — because a monotone count is not a sample folded
/// across a gap, and a researcher reads it by differencing consecutive rows. A resume therefore
/// carries an offset legitimately. What must not differ is how the count MOVES, so rebase both
/// columns on their first post-resume row and hold every later increment to the cold engine's.
fn rebase_lifetime_counters(rows: &mut [FeatureRow]) {
    for name in ["markout_bid_fills", "markout_ask_fills"] {
        let feature = FeatureId(column_index(name) as u16);
        let base = rows
            .iter()
            .find(|row| row.feature == feature)
            .map_or(0.0, |row| row.value);
        for row in rows.iter_mut().filter(|row| row.feature == feature) {
            row.value -= base;
        }
    }
}

fn counts(rows: &[FeatureRow]) -> Vec<usize> {
    let mut counts = vec![0usize; FEATURE_NAMES.len()];
    for row in rows {
        counts[usize::from(row.feature.0)] += 1;
    }
    counts
}

/// Names rather than indices, so a failure says `egarch_vol_lt` instead of `3`.
fn differing_columns(resumed: &[FeatureRow], cold: &[FeatureRow]) -> Vec<&'static str> {
    let cold_counts = counts(cold);
    let mut is_differing: Vec<bool> = counts(resumed)
        .iter()
        .zip(&cold_counts)
        .map(|(left, right)| left != right)
        .collect();
    for (left, right) in resumed.iter().zip(cold) {
        if left != right {
            is_differing[usize::from(left.feature.0)] = true;
            is_differing[usize::from(right.feature.0)] = true;
        }
    }
    FEATURE_NAMES
        .iter()
        .zip(is_differing)
        .filter(|(_, differs)| *differs)
        .map(|(name, _)| *name)
        .collect()
}

fn column_index(name: &str) -> usize {
    FEATURE_NAMES
        .iter()
        .position(|column| *column == name)
        .unwrap_or_else(|| panic!("{name} is not a recorder column"))
}
