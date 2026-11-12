//! DOM grouping fitness: a ladder row may cover N consecutive ticks, and the projection stays exact
//! integer work on the venue's own tick grid however wide the bucket. The invariants a wider row must
//! never break: a bucket's quantity is the exact sum of its members (a grouped ladder that loses or
//! double-counts depth misprices the book silently), the two sides never pool their levels even when
//! they share a bucket, bucket edges stay grid-aligned so a row keeps covering the same ticks as the
//! book moves, and grouping 1 is bit-for-bit the plain ungrouped ladder. The bps unit is pinned as a
//! SIZING input — it resolves to a whole tick count once per frame and never becomes a second price
//! grid. The label half pins the venue-price convention the DOM separator renders a mid at,
//! including the half-tick mid that must widen its decimals rather than round. The model half pins
//! the state the controls write: the workstation opens on a twentieth of a basis point, which is
//! deliberately NOT the projection's identity default, and each unit remembers its own grouping
//! across a flip.

use std::collections::HashMap;

use polysim::ids::{FIXED_SCALE, Price, Qty};
use polysim::msg::ui::{DomQuote, UiBookSnapshot, UiBookState};
use polysim::time::DurationUs;
use polysim::workstation::dom_view::{
    DomGrouping, DomOverlay, DomUnit, DomView, DomViewInput, FeedStatus, MAX_ROWS_PER_SIDE,
    QuotePlacement, ask_anchor, bid_anchor, bucket_low_edge, build_dom_view,
};
use polysim::workstation::format::{
    MISSING, price_decimals, write_opt_venue_mid, write_venue_mid, write_venue_price,
};
use polysim::workstation::model::UiModel;
use proptest::prelude::*;

use crate::workstation_support::{BookFixture, book_snapshot, overlay};

/// A one-mantissa tick, so a price mantissa reads directly as its tick index and the bucket math is
/// legible in the assertions.
const TICK: Price = Price(1);

/// BTCUSDT's tick, the venue that motivated grouping: 0.01 at the 1e-8 fixed-point scale.
const CENT_TICK: Price = Price(FIXED_SCALE / 100);

fn book(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> UiBookSnapshot {
    book_snapshot(BookFixture {
        instrument: 0,
        seq: 0,
        event_ts: 10,
        state: UiBookState::Valid,
        bids,
        asks,
    })
}

/// Levels walking away from `best` by `gaps`, each with its own quantity so a bucket that drops or
/// duplicates a member cannot sum to the right total by accident. `direction` is +1 for asks, -1 for
/// bids, matching the sorted-outward order the feed delivers.
fn ladder(best: i64, gaps: &[i64], direction: i64) -> Vec<(i64, i64)> {
    let mut levels = vec![(best, 11)];
    let mut tick = best;
    for (index, gap) in gaps.iter().enumerate() {
        tick += direction * gap;
        levels.push((tick, 12 + index as i64));
    }
    levels
}

fn view_of(snapshot: &UiBookSnapshot, grouping: DomGrouping, rows: usize) -> DomView {
    build_dom_view(DomViewInput {
        snapshot: Some(snapshot),
        overlay: DomOverlay::default(),
        tick: TICK,
        grouping,
        rows_per_side: rows,
        feed: FeedStatus::default(),
    })
}

/// The quantity every level of one side contributes to the bucket `edge` owns, or `None` when no
/// level falls in it — the expectation the projection must reproduce exactly.
fn bucket_qty(levels: &[(i64, i64)], edge: i64, ticks_per_bucket: i64) -> Option<Qty> {
    levels
        .iter()
        .filter(|(tick, _)| bucket_low_edge(*tick, ticks_per_bucket) == edge)
        .fold(None, |sum: Option<Qty>, (_, qty)| {
            Some(Qty(sum.map_or(0, |seen| seen.0) + qty))
        })
}

proptest! {
    /// FITNESS: a bucket carries the exact sum of its own side's levels and nothing else. A grouped
    /// row that drops a member under-reports depth, one that double-counts invents it, and one that
    /// pools the far side's levels shows a book that does not exist — all silent, all indistinguishable
    /// from a real market on screen.
    #[test]
    fn buckets_sum_their_own_sides_levels_exactly(
        best_bid in 10_000i64..20_000,
        spread in 0i64..8,
        bid_gaps in prop::collection::vec(1i64..40, 0..24),
        ask_gaps in prop::collection::vec(1i64..40, 0..24),
        ticks_per_bucket in 1i64..=100,
        rows in 1usize..=MAX_ROWS_PER_SIDE,
    ) {
        let bid_levels = ladder(best_bid, &bid_gaps, -1);
        let ask_levels = ladder(best_bid + spread, &ask_gaps, 1);
        let snapshot = book(&bid_levels, &ask_levels);
        let view = view_of(&snapshot, DomGrouping::Ticks { per_bucket: ticks_per_bucket }, rows);

        prop_assert_eq!(view.ticks_per_bucket, ticks_per_bucket);
        prop_assert_eq!(view.ask_rows().len(), rows);

        for row in view.ask_rows() {
            prop_assert_eq!(
                row.public_qty,
                bucket_qty(&ask_levels, row.tick_index, ticks_per_bucket),
                "ask bucket at {} must hold its own members and only them", row.tick_index
            );
        }
        for row in view.bid_rows() {
            prop_assert_eq!(
                row.public_qty,
                bucket_qty(&bid_levels, row.tick_index, ticks_per_bucket),
                "bid bucket at {} must hold its own members and only them", row.tick_index
            );
        }

        // Conservation across the window: every level whose bucket is visible is counted once.
        let visible_ask: Vec<i64> = view.ask_rows().iter().map(|row| row.tick_index).collect();
        let visible_bid: Vec<i64> = view.bid_rows().iter().map(|row| row.tick_index).collect();
        let shown_ask: i64 = view.ask_rows().iter().filter_map(|row| row.public_qty).map(|q| q.0).sum();
        let shown_bid: i64 = view.bid_rows().iter().filter_map(|row| row.public_qty).map(|q| q.0).sum();
        let in_window = |levels: &[(i64, i64)], edges: &[i64]| -> i64 {
            levels
                .iter()
                .filter(|(tick, _)| edges.contains(&bucket_low_edge(*tick, ticks_per_bucket)))
                .map(|(_, qty)| qty)
                .sum()
        };
        prop_assert_eq!(shown_ask, in_window(&ask_levels, &visible_ask));
        prop_assert_eq!(shown_bid, in_window(&bid_levels, &visible_bid));
    }
}

proptest! {
    /// FITNESS: rows step by exactly the bucket width from a grid-aligned anchor. Alignment to the
    /// absolute tick grid — not to the mid — is what keeps a row covering the same ticks as the book
    /// moves; an anchor that drifted off the grid would re-partition depth on every commit.
    #[test]
    fn rows_step_by_the_bucket_width_from_a_grid_aligned_anchor(
        best_bid in 10_000i64..20_000,
        spread in 1i64..8,
        ticks_per_bucket in 1i64..=100,
        rows in 1usize..=MAX_ROWS_PER_SIDE,
    ) {
        let snapshot = book(&[(best_bid, 5)], &[(best_bid + spread, 7)]);
        let view = view_of(&snapshot, DomGrouping::Ticks { per_bucket: ticks_per_bucket }, rows);
        let mid = view.mid_half_ticks.expect("a two-sided on-grid book has a mid");

        let ask_edge = view.ask_rows()[0].tick_index;
        let bid_edge = view.bid_rows()[0].tick_index;
        prop_assert_eq!(ask_edge.rem_euclid(ticks_per_bucket), 0);
        prop_assert_eq!(bid_edge.rem_euclid(ticks_per_bucket), 0);
        prop_assert_eq!(ask_edge, bucket_low_edge(ask_anchor(mid), ticks_per_bucket));
        prop_assert_eq!(bid_edge, bucket_low_edge(bid_anchor(mid), ticks_per_bucket));

        for (offset, row) in view.ask_rows().iter().enumerate() {
            prop_assert_eq!(row.tick_index, ask_edge + offset as i64 * ticks_per_bucket);
        }
        for (offset, row) in view.bid_rows().iter().enumerate() {
            prop_assert_eq!(row.tick_index, bid_edge - offset as i64 * ticks_per_bucket);
        }
    }
}

proptest! {
    /// FITNESS: grouping 1 is the ungrouped ladder. The default must stay one row per tick, with
    /// every book level on its own tick row, every row without a level empty, and nothing outside
    /// the window shown. Otherwise every existing DOM guarantee would silently change meaning as soon
    /// as a wider row is chosen.
    #[test]
    fn grouping_one_is_the_ungrouped_ladder(
        best_bid in 1_000i64..2_000,
        spread in 1i64..8,
        bid_gaps in prop::collection::vec(1i64..6, 0..8),
        ask_gaps in prop::collection::vec(1i64..6, 0..8),
    ) {
        prop_assert_eq!(DomGrouping::default(), DomGrouping::Ticks { per_bucket: 1 });

        let bid_levels = ladder(best_bid, &bid_gaps, -1);
        let ask_levels = ladder(best_bid + spread, &ask_gaps, 1);
        let snapshot = book(&bid_levels, &ask_levels);
        let defaulted = view_of(&snapshot, DomGrouping::default(), MAX_ROWS_PER_SIDE);

        // The default is one row per tick, anchored where the ungrouped formula says.
        let mid = defaulted.mid_half_ticks.expect("a two-sided on-grid book has a mid");
        prop_assert_eq!(defaulted.ticks_per_bucket, 1);
        let ask_by_tick: HashMap<i64, i64> = ask_levels.iter().copied().collect();
        let bid_by_tick: HashMap<i64, i64> = bid_levels.iter().copied().collect();
        for (offset, row) in defaulted.ask_rows().iter().enumerate() {
            prop_assert_eq!(row.tick_index, ask_anchor(mid) + offset as i64);
            prop_assert_eq!(row.public_qty, ask_by_tick.get(&row.tick_index).map(|&q| Qty(q)));
        }
        for (offset, row) in defaulted.bid_rows().iter().enumerate() {
            prop_assert_eq!(row.tick_index, bid_anchor(mid) - offset as i64);
            prop_assert_eq!(row.public_qty, bid_by_tick.get(&row.tick_index).map(|&q| Qty(q)));
        }
    }
}

#[test]
fn bps_resolves_to_a_whole_tick_count_against_the_mid() {
    // BTCUSDT at 118000.00 on a 0.01 tick sits at tick index 11_800_000, so mid_half_ticks is
    // 23_600_000 — the only input the bps division sees, whatever the tick's own size.
    let mid = 23_600_000;
    assert_eq!(
        DomGrouping::Bps {
            numerator: 1,
            denominator: 1
        }
        .ticks_per_bucket(mid),
        1180,
        "1 bps of 118000 is 11.80, which is 1180 ticks of 0.01"
    );
    assert_eq!(
        DomGrouping::Bps {
            numerator: 1,
            denominator: 4
        }
        .ticks_per_bucket(mid),
        295
    );
    assert_eq!(
        DomGrouping::Bps {
            numerator: 1,
            denominator: 10
        }
        .ticks_per_bucket(mid),
        118
    );
    assert_eq!(
        DomGrouping::Bps {
            numerator: 1,
            denominator: 20
        }
        .ticks_per_bucket(mid),
        59
    );
    assert_eq!(
        DomGrouping::Bps {
            numerator: 1,
            denominator: 100
        }
        .ticks_per_bucket(mid),
        11,
        "a hundredth of a basis point is 11.8 ticks here, and the width floors rather than rounds"
    );

    // Width never collapses below one tick: a fraction finer than the grid groups by one rather than
    // dividing by zero in `bucket_low_edge`.
    assert_eq!(
        DomGrouping::Bps {
            numerator: 1,
            denominator: 10_000
        }
        .ticks_per_bucket(mid),
        1
    );
    assert_eq!(
        DomGrouping::Ticks { per_bucket: 0 }.ticks_per_bucket(mid),
        1
    );
    assert_eq!(
        DomGrouping::Ticks { per_bucket: -5 }.ticks_per_bucket(mid),
        1
    );

    // The derived width reaches the view, and its rows land on the venue grid rather than on a
    // second grid of their own: 1180-tick buckets put an edge exactly on 118000.00.
    let snapshot = book(&[(11_799_999, 5)], &[(11_800_001, 7)]);
    let view = view_of(
        &snapshot,
        DomGrouping::Bps {
            numerator: 1,
            denominator: 1,
        },
        3,
    );
    assert_eq!(view.mid_half_ticks, Some(mid));
    assert_eq!(view.ticks_per_bucket, 1180);
    assert_eq!(view.ask_rows()[0].tick_index, 11_800_000);
    assert_eq!(view.ask_rows()[1].tick_index, 11_801_180);
}

/// A locked book's shared tick `k` is owned by whichever side's anchor bucket contains it: the ask
/// anchor is `k + 1` and the bid anchor `k - 1`, so the ask loses it when `k + 1` crosses a boundary
/// (`k ≡ n-1 mod n`) and the bid loses it when `k - 1` falls back over one (`k ≡ 0 mod n`). At `n = 1`
/// both conditions hold at once, which is exactly why the ungrouped ladder shows a locked price only
/// in the separator.
#[test]
fn bucket_ownership_at_boundaries_follows_the_grid_not_mid_parity() {
    // k = 100 ≡ 0 (mod 10): the bid anchor 99 floors to bucket 90, so only the ask keeps it.
    let at_a_boundary = book(&[(100, 4)], &[(100, 5), (103, 7)]);
    let view = view_of(&at_a_boundary, DomGrouping::Ticks { per_bucket: 10 }, 3);
    assert_eq!(view.mid_half_ticks, Some(200));
    assert_eq!(view.ask_rows()[0].tick_index, 100);
    assert_eq!(
        view.ask_rows()[0].public_qty,
        Some(Qty(12)),
        "both asks in the 100..109 bucket aggregate; the locked bid never joins them"
    );
    assert_eq!(view.bid_rows()[0].tick_index, 90);
    assert!(
        view.bid_rows().iter().all(|row| row.public_qty.is_none()),
        "the locked bid's bucket is in the ask window only, so its depth is legitimately unshown"
    );

    // k = 109 ≡ 9 (mod 10), the mirror: the ask anchor 110 opens the next bucket, so only the bid
    // keeps it.
    let below_a_boundary = book(&[(109, 4), (102, 3)], &[(109, 5)]);
    let view = view_of(&below_a_boundary, DomGrouping::Ticks { per_bucket: 10 }, 3);
    assert_eq!(view.mid_half_ticks, Some(218));
    assert_eq!(view.ask_rows()[0].tick_index, 110);
    assert!(view.ask_rows().iter().all(|row| row.public_qty.is_none()));
    assert_eq!(view.bid_rows()[0].tick_index, 100);
    assert_eq!(
        view.bid_rows()[0].public_qty,
        Some(Qty(7)),
        "109 and 102 share the bid's row 0 bucket"
    );

    // k = 105, interior to its bucket: both anchors (106 and 104) floor to 100, so BOTH sides show
    // the locked tick, each carrying its own quantity — a duplicate label, never a merged row.
    let inside_a_bucket = book(&[(105, 4)], &[(105, 5)]);
    let view = view_of(&inside_a_bucket, DomGrouping::Ticks { per_bucket: 10 }, 3);
    assert_eq!(view.mid_half_ticks, Some(210));
    assert_eq!(view.ask_rows()[0].tick_index, 100);
    assert_eq!(view.bid_rows()[0].tick_index, 100);
    assert_eq!(view.ask_rows()[0].public_qty, Some(Qty(5)));
    assert_eq!(view.bid_rows()[0].public_qty, Some(Qty(4)));

    // Whether the two sides' row 0 name the same bucket follows from where the anchors fall against
    // the bucket boundary — never from the mid's parity. Anchoring to the absolute tick grid is what
    // produces this, and a "fix" that made the row-0 edges track the mid would break every other
    // guarantee here.
    // Odd mid, best bid 100 and best ask 101: the anchors are 101 and 100, both in bucket 100, so the
    // bucket is shared while each side still aggregates only its own levels.
    let shared_at_an_odd_mid = book(
        &[(100, 4), (98, 3), (95, 2)],
        &[(101, 5), (104, 6), (109, 7)],
    );
    let view = view_of(
        &shared_at_an_odd_mid,
        DomGrouping::Ticks { per_bucket: 10 },
        3,
    );
    assert_eq!(view.mid_half_ticks, Some(201));
    assert_eq!(view.ask_rows()[0].tick_index, 100);
    assert_eq!(view.bid_rows()[0].tick_index, 100);
    assert_eq!(
        view.ask_rows()[0].public_qty,
        Some(Qty(18)),
        "the shared bucket's ask row holds 101, 104 and 109 only"
    );
    assert_eq!(
        view.bid_rows()[0].public_qty,
        Some(Qty(4)),
        "the shared bucket's bid row holds the bid at 100 only"
    );
    assert_eq!(view.bid_rows()[1].tick_index, 90);
    assert_eq!(view.bid_rows()[1].public_qty, Some(Qty(5)), "98 and 95");

    // Even mid, best bid 104 and best ask 106: the anchors are 106 and 104, two apart yet both in
    // bucket 100, so the bucket is shared.
    let shared_at_an_even_mid = book(&[(104, 4)], &[(106, 5)]);
    let view = view_of(
        &shared_at_an_even_mid,
        DomGrouping::Ticks { per_bucket: 10 },
        3,
    );
    assert_eq!(view.mid_half_ticks, Some(210));
    assert_eq!(view.ask_rows()[0].tick_index, 100);
    assert_eq!(view.bid_rows()[0].tick_index, 100);

    // Odd mid, best bid 109 and best ask 110: the anchors are 110 and 109 with a bucket boundary
    // between them, so the bucket is not shared.
    let split_at_an_odd_mid = book(&[(109, 4)], &[(110, 5)]);
    let view = view_of(
        &split_at_an_odd_mid,
        DomGrouping::Ticks { per_bucket: 10 },
        3,
    );
    assert_eq!(view.mid_half_ticks, Some(219));
    assert_eq!(view.ask_rows()[0].tick_index, 110);
    assert_eq!(view.bid_rows()[0].tick_index, 100);
}

#[test]
fn a_quote_lands_in_its_bucket_and_off_screen_stays_in_half_ticks() {
    let snapshot = book(&[(100, 4)], &[(102, 5)]);
    let inside = DomQuote::top(Some((Price(87), Qty(9))), Some((Price(115), Qty(9))));
    // With a mid of 202 half-ticks and 10-tick buckets, the ask rows are 100, 110 and 120 and the
    // bid rows are 100, 90 and 80.
    let view = build_dom_view(DomViewInput {
        snapshot: Some(&snapshot),
        overlay: overlay(inside),
        tick: TICK,
        grouping: DomGrouping::Ticks { per_bucket: 10 },
        rows_per_side: 3,
        feed: FeedStatus::default(),
    });
    assert_eq!(view.ask_placement, QuotePlacement::Visible);
    assert_eq!(view.bid_placement, QuotePlacement::Visible);
    let quoted: Vec<i64> = view
        .ask_rows()
        .iter()
        .chain(view.bid_rows())
        .filter(|row| row.is_quoted)
        .map(|row| row.tick_index)
        .collect();
    assert_eq!(
        quoted,
        vec![110, 80],
        "each quote flags exactly the row whose bucket owns its tick"
    );
    assert_eq!(view.ask_rows()[1].strategy_qty, Some(Qty(9)));
    assert_eq!(view.bid_rows()[2].strategy_qty, Some(Qty(9)));

    // Off-screen deltas are distances from mid, so grouping cannot rescale them.
    let far = DomQuote::top(Some((Price(10), Qty(9))), Some((Price(200), Qty(9))));
    for per_bucket in [1, 10] {
        let view = build_dom_view(DomViewInput {
            snapshot: Some(&snapshot),
            overlay: overlay(far),
            tick: TICK,
            grouping: DomGrouping::Ticks { per_bucket },
            rows_per_side: 3,
            feed: FeedStatus::default(),
        });
        assert_eq!(
            view.ask_placement,
            QuotePlacement::OffScreenAbove {
                delta_half_ticks: 198
            },
            "|2*200 - 202| half-ticks at every grouping"
        );
        assert_eq!(
            view.bid_placement,
            QuotePlacement::OffScreenBelow {
                delta_half_ticks: 182
            }
        );
    }

    // An off-grid quote has no tick index, so grouping gives it no home either.
    let on_grid = book(&[(1_000, 4)], &[(1_020, 5)]);
    let off_grid = DomQuote::top(Some((Price(995), Qty(9))), Some((Price(1_015), Qty(9))));
    let view = build_dom_view(DomViewInput {
        snapshot: Some(&on_grid),
        overlay: overlay(off_grid),
        tick: Price(10),
        grouping: DomGrouping::Ticks { per_bucket: 5 },
        rows_per_side: 3,
        feed: FeedStatus::default(),
    });
    assert_eq!(view.ask_placement, QuotePlacement::None);
    assert_eq!(view.bid_placement, QuotePlacement::None);
    assert!(
        view.ask_rows()
            .iter()
            .chain(view.bid_rows())
            .all(|row| !row.is_quoted),
        "an off-grid price is never rounded into a bucket"
    );
}

/// The workstation opens on a row a twentieth of a basis point wide, and the tick slot the operator
/// has never visited starts ungrouped.
///
/// This is deliberately NOT `DomGrouping::default()`. That default is the projection's IDENTITY —
/// one bucket per tick, the venue's own grid — and the projection pins read it that way. The
/// opening state is a product choice the model names itself, and the inequality below is what fails
/// loudly if a later tidy-up folds the two into one. Each unit also keeps its own remembered
/// grouping, so flipping from ticks to bps and back restores the value the operator chose in that unit rather
/// than resetting it — the whole reason the model holds two slots plus an active unit instead of a
/// single `DomGrouping`.
#[test]
fn dom_grouping_model_state() {
    let mut model = UiModel::with_capacity(1, DurationUs::from_micros(100_000));
    assert_eq!(model.dom_unit(), DomUnit::Bps);
    assert_eq!(
        model.dom_grouping(),
        DomGrouping::Bps {
            numerator: 1,
            denominator: 20
        }
    );
    assert_ne!(
        model.dom_grouping(),
        DomGrouping::default(),
        "the identity grouping is not the shipped opening state, and must not become it silently"
    );

    model.set_dom_unit(DomUnit::Ticks);
    assert_eq!(
        model.dom_grouping(),
        DomGrouping::Ticks { per_bucket: 1 },
        "the unvisited tick slot starts on the ungrouped ladder"
    );

    model.set_dom_grouping(DomGrouping::Bps {
        numerator: 1,
        denominator: 4,
    });
    assert_eq!(
        model.dom_unit(),
        DomUnit::Bps,
        "choosing a grouping makes its unit the active one"
    );
    assert_eq!(
        model.dom_grouping(),
        DomGrouping::Bps {
            numerator: 1,
            denominator: 4
        }
    );

    model.set_dom_unit(DomUnit::Ticks);
    assert_eq!(
        model.dom_grouping(),
        DomGrouping::Ticks { per_bucket: 1 },
        "switching units alone leaves the other slot untouched"
    );
    model.set_dom_grouping(DomGrouping::Ticks { per_bucket: 10 });

    model.set_dom_unit(DomUnit::Bps);
    assert_eq!(
        model.dom_grouping(),
        DomGrouping::Bps {
            numerator: 1,
            denominator: 4
        },
        "the bps slot kept the quarter-bp choice rather than falling back to its default"
    );
    model.set_dom_unit(DomUnit::Ticks);
    assert_eq!(model.dom_grouping(), DomGrouping::Ticks { per_bucket: 10 });
}

#[test]
fn venue_labels_render_the_price_exactly_at_its_own_precision() {
    let mut label = String::new();
    let whole_tick = Price(FIXED_SCALE);

    write_venue_mid(&mut label, 131_982, whole_tick);
    assert_eq!(label, "65991", "a 1.0 tick renders the tick count itself");
    write_venue_mid(&mut label, 131_983, whole_tick);
    assert_eq!(label, "65991.5", "an odd mid_half is half a tick");

    assert_eq!(price_decimals(CENT_TICK), 2);
    write_venue_mid(&mut label, 23_600_000, CENT_TICK);
    assert_eq!(label, "118000.00");
    write_venue_mid(&mut label, 23_600_001, CENT_TICK);
    assert_eq!(
        label, "118000.005",
        "a half-tick mid widens past the tick's precision rather than rounding"
    );

    write_venue_price(
        &mut label,
        Price(118_000 * FIXED_SCALE),
        price_decimals(CENT_TICK),
    );
    assert_eq!(
        label, "118000.00",
        "a row label carries the tick's decimals"
    );

    assert_eq!(
        price_decimals(Price(1)),
        8,
        "a one-mantissa tick is the finest the fixed-point scale expresses"
    );
    write_venue_price(&mut label, Price(123_456_789), price_decimals(Price(1)));
    assert_eq!(label, "1.23456789");

    write_opt_venue_mid(&mut label, None, CENT_TICK);
    assert_eq!(
        label, MISSING,
        "no trustworthy mid is em-dash, never a zero"
    );
}
