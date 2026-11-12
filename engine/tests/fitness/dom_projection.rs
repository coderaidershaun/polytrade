//! DOM projection fitness: the pure map from a book snapshot + simulated quote to a
//! fixed-center tick ladder. Anchors are exact integer math on tick indices at both mid parities;
//! empty ticks keep their rows as `None` (never a fabricated zero); off-grid prices are invalid, not
//! rounded; the quote highlights exactly one row per side or reports a directional off-screen delta.
//! The model half asserts latest-per-instrument retention, sequence-gap counting, and event-time
//! quote liveness; the formatters render the tick-unit convention, a mid of 65991.5 ticks reading
//! as `65991.5`. The fit half pins the one geometry rule that can fail silently: asking for more
//! rows than the panel holds costs rows, never legibility.

use polysim::ids::{ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty};
use polysim::msg::ui::{DomQuote, UiBookSnapshot, UiBookState, UiEvent};
use polysim::time::{DurationUs, TsUs};
use polysim::workstation::dom_view::{
    DEFAULT_ROWS_PER_SIDE, DomGrouping, DomOverlay, DomRow, DomStatus, DomViewInput, FeedStatus,
    MAX_ROWS_PER_SIDE, MIN_ROWS_PER_SIDE, QuotePlacement, ask_anchor, bid_anchor, build_dom_view,
    fit_rows, mid_half_ticks, price_for_row, tick_index,
};
use polysim::workstation::exec_model::{OrderCell, OrderStatus};
use polysim::workstation::format::{MISSING, write_mid, write_opt_qty, write_qty};
use polysim::workstation::model::UiModel;
use proptest::prelude::*;

use crate::workstation_support::{BookFixture, NamedCase, book_snapshot, overlay};

/// A one-mantissa tick, so a price mantissa reads directly as its tick index and the ladder math is
/// legible in the assertions.
const TICK: Price = Price(1);

fn book(
    instrument: u16,
    seq: u64,
    event_ts: i64,
    bids: &[(i64, i64)],
    asks: &[(i64, i64)],
) -> UiBookSnapshot {
    book_snapshot(BookFixture {
        instrument,
        seq,
        event_ts,
        state: UiBookState::Valid,
        bids,
        asks,
    })
}

/// Anchors are exact integer math at both mid parities: an even `mid_half` skips the shared tick (the
/// separator's own), an odd one skips no tick and every level between the best price and the mid is
/// `None`, never a fabricated zero. A locked book (best bid equal to best ask) shows the shared price only
/// in the separator. The tick-unit formatters render the same tick-count convention, a half-tick
/// mid keeping its half, and a negative sub-unit qty keeps its sign even though the integer part
/// rounds to zero.
#[test]
fn anchor_math_holds_at_both_mid_parities_and_a_locked_book() {
    // Best bid 100 and best ask 102 give 202 half-ticks, which is even, so the mid is the whole tick
    // 101 and it is the separator's own tick.
    let mid = mid_half_ticks(Price(100), Price(102), TICK).expect("on-grid");
    assert_eq!(mid, 202);
    assert_eq!(
        ask_anchor(mid),
        102,
        "nearest ask sits one tick above the integer mid"
    );
    assert_eq!(bid_anchor(mid), 100, "nearest bid sits one tick below it");

    let snapshot = book(0, 0, 10, &[(100, 4), (99, 2)], &[(102, 5), (103, 7)]);
    let view = build_dom_view(DomViewInput {
        snapshot: Some(&snapshot),
        overlay: DomOverlay::default(),
        tick: TICK,
        grouping: DomGrouping::default(),
        rows_per_side: 3,
        feed: FeedStatus::default(),
    });
    assert_eq!(view.mid_half_ticks, Some(202));
    assert_eq!(view.status, DomStatus::Live);

    let asks = view.ask_rows();
    assert_eq!(asks[0].tick_index, 102);
    assert_eq!(asks[0].public_qty, Some(Qty(5)));
    assert_eq!(asks[1].tick_index, 103);
    assert_eq!(asks[1].public_qty, Some(Qty(7)));
    assert_eq!(
        asks[2].public_qty, None,
        "the far ask tick is empty, not zero"
    );

    let bids = view.bid_rows();
    assert_eq!(bids[0].tick_index, 100);
    assert_eq!(bids[0].public_qty, Some(Qty(4)));
    assert_eq!(bids[1].tick_index, 99);
    assert_eq!(bids[1].public_qty, Some(Qty(2)));
    assert_eq!(bids[2].public_qty, None);

    // Wide spread: best bid 100 and best ask 105 give 205 half-ticks, which is odd, so the mid is
    // 102.5 and ticks 101 to 104 have no quantity.
    let odd_snapshot = book(0, 0, 10, &[(100, 4)], &[(105, 5)]);
    let odd_view = build_dom_view(DomViewInput {
        snapshot: Some(&odd_snapshot),
        overlay: DomOverlay::default(),
        tick: TICK,
        grouping: DomGrouping::default(),
        rows_per_side: 3,
        feed: FeedStatus::default(),
    });
    assert_eq!(odd_view.mid_half_ticks, Some(205));

    let odd_asks = odd_view.ask_rows();
    assert_eq!(odd_asks[0].tick_index, 103);
    assert_eq!(
        odd_asks[0].public_qty, None,
        "an empty tick nearer mid is None, not 0"
    );
    assert_eq!(odd_asks[1].public_qty, None);
    assert_eq!(odd_asks[2].tick_index, 105);
    assert_eq!(odd_asks[2].public_qty, Some(Qty(5)));

    let odd_bids = odd_view.bid_rows();
    assert_eq!(odd_bids[0].tick_index, 102);
    assert_eq!(odd_bids[0].public_qty, None);
    assert_eq!(odd_bids[2].tick_index, 100);
    assert_eq!(odd_bids[2].public_qty, Some(Qty(4)));

    // Best bid and best ask are both 100, which gives 200 half-ticks, so the mid is 100. The first
    // ask row is 101 and the first bid row is 99, so both skip tick 100 entirely.
    let locked_snapshot = book(0, 0, 10, &[(100, 4)], &[(100, 5)]);
    let locked_view = build_dom_view(DomViewInput {
        snapshot: Some(&locked_snapshot),
        overlay: DomOverlay::default(),
        tick: TICK,
        grouping: DomGrouping::default(),
        rows_per_side: 3,
        feed: FeedStatus::default(),
    });
    assert_eq!(locked_view.mid_half_ticks, Some(200));
    assert!(
        locked_view
            .ask_rows()
            .iter()
            .all(|r| r.public_qty.is_none()),
        "the locked ask sits at the mid tick, shown only in the separator"
    );
    assert!(
        locked_view
            .bid_rows()
            .iter()
            .all(|r| r.public_qty.is_none())
    );
    assert_eq!(locked_view.ask_rows()[0].tick_index, 101);
    assert_eq!(locked_view.bid_rows()[0].tick_index, 99);

    let mut label = String::new();
    write_mid(
        &mut label,
        locked_view
            .mid_half_ticks
            .expect("a locked book still has a mid"),
    );
    assert_eq!(label, "100", "an even mid_half renders a whole tick count");
    write_mid(&mut label, 131_983);
    assert_eq!(label, "65991.5", "an odd mid_half renders the half-tick");

    write_qty(&mut label, Qty(-FIXED_SCALE / 2), FIXED_SCALE, 3);
    assert_eq!(
        label, "-0.500",
        "a negative magnitude below one unit keeps its sign even though the integer part is zero"
    );
    write_opt_qty(&mut label, None, FIXED_SCALE, 3);
    assert_eq!(label, MISSING, "absent is em-dash");
    write_opt_qty(&mut label, Some(Qty(0)), FIXED_SCALE, 3);
    assert_eq!(
        label, "0.000",
        "a real zero is distinguishable from missing"
    );
}

#[test]
fn every_desired_ladder_level_is_projected_and_wide_buckets_accumulate() {
    let snapshot = book(0, 0, 10, &[(100, 4)], &[(102, 5)]);
    let mut quote = DomQuote::default();
    quote.bids[0] = Some((Price(100), Qty(3)));
    quote.bids[1] = Some((Price(99), Qty(4)));
    quote.asks[0] = Some((Price(102), Qty(5)));
    quote.asks[1] = Some((Price(103), Qty(6)));

    let view = build_dom_view(DomViewInput {
        snapshot: Some(&snapshot),
        overlay: overlay(quote),
        tick: TICK,
        grouping: DomGrouping::default(),
        rows_per_side: 3,
        feed: FeedStatus::default(),
    });
    assert_eq!(view.bid_placement, QuotePlacement::Visible);
    assert_eq!(view.ask_placement, QuotePlacement::Visible);
    assert_eq!(view.bid_rows()[0].strategy_qty, Some(Qty(3)));
    assert_eq!(view.bid_rows()[1].strategy_qty, Some(Qty(4)));
    assert_eq!(view.ask_rows()[0].strategy_qty, Some(Qty(5)));
    assert_eq!(view.ask_rows()[1].strategy_qty, Some(Qty(6)));
    assert_eq!(
        view.ask_rows()[2].strategy_qty,
        None,
        "no row past the quoted ladder carries a strategy qty"
    );
    let quoted: Vec<i64> = view
        .ask_rows()
        .iter()
        .chain(view.bid_rows())
        .filter(|row| row.is_quoted)
        .map(|row| row.tick_index)
        .collect();
    assert_eq!(
        quoted,
        vec![102, 103, 100, 99],
        "each quoted tick flags its own row and no other"
    );

    let grouped = build_dom_view(DomViewInput {
        snapshot: Some(&snapshot),
        overlay: overlay(quote),
        tick: TICK,
        grouping: DomGrouping::Ticks { per_bucket: 10 },
        rows_per_side: 3,
        feed: FeedStatus::default(),
    });
    assert_eq!(grouped.ask_rows()[0].strategy_qty, Some(Qty(11)));
}

/// A book that cannot yield a trustworthy mid — off-grid, one-sided, or simply absent — projects no
/// mid and no ladder rows, a display state rather than a fabricated number; the pure geometry
/// functions underneath guard the same invalid input directly. Status takes precedence
/// Disconnected > Stale > AwaitingBook, and a stale book still projects its last ladder rather than
/// blanking it.
#[test]
fn adverse_book_and_feed_conditions_yield_no_mid_or_the_right_status() {
    assert_eq!(
        tick_index(Price(105), Price(10)),
        None,
        "105 is off a 10 grid"
    );
    assert_eq!(tick_index(Price(100), Price(10)), Some(10));
    assert_eq!(
        tick_index(Price(5), Price(0)),
        None,
        "a non-positive tick has no grid"
    );
    assert_eq!(price_for_row(Price(10), 5), Some(Price(50)));
    assert_eq!(
        price_for_row(Price(i64::MAX), 2),
        None,
        "overflow yields None, not a wrap"
    );

    // An off-grid best price yields no trustworthy mid and no ladder rows.
    let off_grid_snapshot = book(0, 0, 10, &[(100, 4)], &[(105, 5)]);
    let off_grid_view = build_dom_view(DomViewInput {
        snapshot: Some(&off_grid_snapshot),
        overlay: DomOverlay::default(),
        tick: Price(10),
        grouping: DomGrouping::default(),
        rows_per_side: 3,
        feed: FeedStatus::default(),
    });
    assert_eq!(off_grid_view.mid_half_ticks, None);
    assert!(off_grid_view.ask_rows().is_empty());

    // A one-sided book is fresh, just un-mid-able.
    let one_sided_snapshot = book(0, 0, 10, &[(100, 4)], &[]);
    let one_sided_view = build_dom_view(DomViewInput {
        snapshot: Some(&one_sided_snapshot),
        overlay: DomOverlay::default(),
        tick: TICK,
        grouping: DomGrouping::default(),
        rows_per_side: 3,
        feed: FeedStatus::default(),
    });
    assert_eq!(one_sided_view.mid_half_ticks, None);
    assert!(one_sided_view.ask_rows().is_empty());
    assert_eq!(one_sided_view.status, DomStatus::Live);

    let valid = book(0, 0, 10, &[(100, 4)], &[(102, 5)]);
    let mut awaiting = valid;
    awaiting.state = UiBookState::AwaitingSnapshot;

    let disconnected = build_dom_view(DomViewInput {
        snapshot: Some(&valid),
        overlay: DomOverlay::default(),
        tick: TICK,
        grouping: DomGrouping::default(),
        rows_per_side: 3,
        feed: FeedStatus::Disconnected,
    });
    assert_eq!(disconnected.status, DomStatus::Disconnected);

    let stale = build_dom_view(DomViewInput {
        snapshot: Some(&valid),
        overlay: DomOverlay::default(),
        tick: TICK,
        grouping: DomGrouping::default(),
        rows_per_side: 3,
        feed: FeedStatus::Stale,
    });
    assert_eq!(stale.status, DomStatus::Stale);
    assert_eq!(
        stale.mid_half_ticks,
        Some(202),
        "a stale book still projects its last ladder"
    );

    let awaiting_view = build_dom_view(DomViewInput {
        snapshot: Some(&awaiting),
        overlay: DomOverlay::default(),
        tick: TICK,
        grouping: DomGrouping::default(),
        rows_per_side: 3,
        feed: FeedStatus::default(),
    });
    assert_eq!(awaiting_view.status, DomStatus::AwaitingBook);
    assert_eq!(awaiting_view.ask_rows().len(), 0);

    let no_book = build_dom_view(DomViewInput {
        snapshot: None,
        overlay: DomOverlay::default(),
        tick: TICK,
        grouping: DomGrouping::default(),
        rows_per_side: 3,
        feed: FeedStatus::default(),
    });
    assert_eq!(no_book.status, DomStatus::AwaitingBook);
}

proptest! {
    /// FITNESS: a ladder never paints a row under the legibility floor. Too many levels must cost
    /// ROWS — a fit that shrank the row instead would overlap price against quantity, and a DOM
    /// whose numbers overlap is worse than one showing fewer of them. The clamp must also bite only
    /// when it must: a fit that quietly returned a safe small count would be legible and useless.
    #[test]
    fn a_ladder_never_paints_a_row_under_the_legibility_floor(
        side_height in 0.0f32..2_000.0,
        requested in 0usize..200,
        floor_height in 1.0f32..30.0,
    ) {
        let fit = fit_rows(side_height, requested, floor_height);

        prop_assert!(fit.rows <= requested, "never more rows than asked for: {fit:?}");
        prop_assert!(fit.rows <= MAX_ROWS_PER_SIDE, "never past the array bound: {fit:?}");
        prop_assert!(fit.row_height.is_finite(), "no row height is a division by zero: {fit:?}");

        if fit.rows == 0 {
            prop_assert!(fit.row_height >= 0.0);
            return Ok(());
        }
        // The slack is f32 division rounding, not a design allowance.
        prop_assert!(
            fit.row_height >= floor_height - floor_height * 1e-5,
            "{} rows of {} in {side_height} breaches the {floor_height} floor",
            fit.rows, fit.row_height
        );
        prop_assert!(
            fit.rows == requested.min(MAX_ROWS_PER_SIDE)
                || (fit.rows + 1) as f32 * floor_height > side_height,
            "the clamp bit early: {fit:?} of {requested} asked in {side_height} at floor {floor_height}"
        );
    }
}

/// The level control writes through the model, so the model — not the control — owns the range. A
/// value from anywhere else (a restored preference, a fixture, a future keyboard shortcut) is
/// clamped on the way in rather than reaching the fixed arrays.
#[test]
fn the_model_holds_a_level_count_inside_the_controls_range() {
    // A range whose default sits outside it is a build failure, not a test failure.
    const { assert!(MIN_ROWS_PER_SIDE <= DEFAULT_ROWS_PER_SIDE) };
    const { assert!(DEFAULT_ROWS_PER_SIDE <= MAX_ROWS_PER_SIDE) };

    let mut model = UiModel::with_capacity(1, DurationUs::from_micros(100_000));
    assert_eq!(model.dom_levels(), DEFAULT_ROWS_PER_SIDE);
    assert_eq!(model.dom_levels(), 12);

    model.set_dom_levels(20);
    assert_eq!(model.dom_levels(), 20, "a value inside the range is kept");
    model.set_dom_levels(3);
    assert_eq!(model.dom_levels(), MIN_ROWS_PER_SIDE);
    model.set_dom_levels(999);
    assert_eq!(model.dom_levels(), MAX_ROWS_PER_SIDE);
}

#[test]
fn model_judges_quote_liveness_in_event_time() {
    // A 100 ms spin makes the liveness threshold 2.5 spins, which is 250 ms.
    let mut model = UiModel::with_capacity(1, DurationUs::from_micros(100_000));
    let inst = InstrumentId(0);
    assert!(
        !model.is_quote_live(inst),
        "no quote and no book is not live"
    );
    model.apply_book(book(0, 0, 10, &[(100, 1)], &[(102, 1)]));
    assert!(
        !model.is_quote_live(inst),
        "a book without a quote is not live"
    );

    model.apply_event(UiEvent::Quote {
        instrument: inst,
        seq: 0,
        event_ts_us: TsUs::from_micros(1_000_000),
        quote: DomQuote::top(Some((Price(100), Qty(1))), None),
    });

    model.apply_book(book(0, 1, 1_200_000, &[(100, 1)], &[(102, 1)]));
    assert!(
        model.is_quote_live(inst),
        "book 200 ms past the quote is within threshold"
    );

    model.apply_book(book(0, 2, 1_400_000, &[(100, 1)], &[(102, 1)]));
    assert!(
        !model.is_quote_live(inst),
        "book 400 ms past the quote is stale"
    );
}

/// A two-sided book at the tick grid the cases below use.
fn valid_snapshot(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> UiBookSnapshot {
    book(0, 0, 10, bids, asks)
}

/// One of this engine's working orders for the cases below.
fn working(id: u64, tick: i64, qty: i64, status: OrderStatus) -> OrderCell {
    OrderCell {
        client_id: ClientOrderId(id),
        quote_level: None,
        status,
        price: Price(tick * TICK.0),
        qty: Qty(qty),
        filled: Qty(0),
        at: TsUs::from_micros(0),
    }
}

fn row_at(rows: &[DomRow], tick_index: i64) -> DomRow {
    *rows
        .iter()
        .find(|row| row.tick_index == tick_index)
        .unwrap_or_else(|| panic!("tick {tick_index} is inside the window"))
}

/// A REAL working order and the engine's DESIRED quote occupy separate fields, painted from what is
/// REMAINING rather than what was ordered; two orders sharing a bucket report the LEAST certain of
/// their statuses; and an order beyond the window reports its own off-screen placement independently
/// of the desired quote's. Each is a distinct way a ladder cell could show exposure that is not real,
/// or hide exposure that is.
#[test]
fn order_overlay_rules() {
    fn a_real_order_and_a_desired_quote_are_separate_row_fields(name: &str) {
        let snapshot = valid_snapshot(&[(100, 5)], &[(102, 5)]);
        let bid_orders = [working(1, 99, 7, OrderStatus::Confirmed)];
        let view = build_dom_view(DomViewInput {
            snapshot: Some(&snapshot),
            overlay: DomOverlay {
                desired: Some(DomQuote::top(Some((Price(98), Qty(3))), None)),
                bid_orders: &bid_orders,
                ask_orders: &[],
            },
            tick: TICK,
            grouping: DomGrouping::default(),
            rows_per_side: 6,
            feed: FeedStatus::default(),
        });

        let order_row = row_at(view.bid_rows(), 99);
        assert_eq!(order_row.order_qty, Some(Qty(7)), "{name}: order qty");
        assert_eq!(
            order_row.order_status,
            Some(OrderStatus::Confirmed),
            "{name}: order status"
        );
        assert_eq!(
            order_row.strategy_qty, None,
            "{name}: the desired quote is on its own row, not merged onto the order's"
        );

        let desired_row = row_at(view.bid_rows(), 98);
        assert_eq!(
            desired_row.strategy_qty,
            Some(Qty(3)),
            "{name}: desired qty"
        );
        assert_eq!(
            desired_row.order_qty, None,
            "{name}: an intention must never paint into the field that means real exposure"
        );
    }

    fn a_partially_filled_order_shows_only_what_remains(name: &str) {
        let snapshot = valid_snapshot(&[(100, 5)], &[(102, 5)]);
        let mut order = working(1, 99, 10, OrderStatus::Confirmed);
        order.filled = Qty(4);
        let orders = [order];
        let view = build_dom_view(DomViewInput {
            snapshot: Some(&snapshot),
            overlay: DomOverlay {
                desired: None,
                bid_orders: &orders,
                ask_orders: &[],
            },
            tick: TICK,
            grouping: DomGrouping::default(),
            rows_per_side: 6,
            feed: FeedStatus::default(),
        });
        assert_eq!(
            row_at(view.bid_rows(), 99).order_qty,
            Some(Qty(6)),
            "{name}"
        );
    }

    fn a_shared_bucket_reports_the_least_certain_status(name: &str) {
        let snapshot = valid_snapshot(&[(100, 5)], &[(102, 5)]);
        let orders = [
            working(1, 98, 4, OrderStatus::Confirmed),
            working(2, 99, 6, OrderStatus::InFlight),
        ];
        let view = build_dom_view(DomViewInput {
            snapshot: Some(&snapshot),
            overlay: DomOverlay {
                desired: None,
                bid_orders: &orders,
                ask_orders: &[],
            },
            tick: TICK,
            // Five ticks a row, so both orders land in one bucket.
            grouping: DomGrouping::Ticks { per_bucket: 5 },
            rows_per_side: 6,
            feed: FeedStatus::default(),
        });
        let bucket = view
            .bid_rows()
            .iter()
            .find(|row| row.order_qty.is_some())
            .unwrap_or_else(|| panic!("{name}: both orders fall inside the window"));
        assert_eq!(
            bucket.order_qty,
            Some(Qty(10)),
            "{name}: the bucket sums its members"
        );
        assert_eq!(
            bucket.order_status,
            Some(OrderStatus::InFlight),
            "{name}: least certain status"
        );
    }

    fn an_off_window_order_reports_its_own_placement(name: &str) {
        let snapshot = valid_snapshot(&[(100, 5)], &[(102, 5)]);
        let orders = [working(1, 40, 6, OrderStatus::Lost)];
        let view = build_dom_view(DomViewInput {
            snapshot: Some(&snapshot),
            overlay: DomOverlay {
                desired: None,
                bid_orders: &orders,
                ask_orders: &[],
            },
            tick: TICK,
            grouping: DomGrouping::default(),
            rows_per_side: 6,
            feed: FeedStatus::default(),
        });
        assert!(
            matches!(
                view.bid_order_placement,
                QuotePlacement::OffScreenBelow { .. }
            ),
            "{name}: an order below the window reports below, got {:?}",
            view.bid_order_placement
        );
        assert_eq!(
            view.bid_placement,
            QuotePlacement::None,
            "{name}: the desired quote's placement is its own and stays absent"
        );
    }

    let cases: &[NamedCase] = &[
        (
            "a real order and a desired quote are separate row fields",
            a_real_order_and_a_desired_quote_are_separate_row_fields,
        ),
        (
            "a partially filled order shows only what remains",
            a_partially_filled_order_shows_only_what_remains,
        ),
        (
            "a shared bucket reports the least certain status",
            a_shared_bucket_reports_the_least_certain_status,
        ),
        (
            "an off-window order reports its own placement",
            an_off_window_order_reports_its_own_placement,
        ),
    ];
    for (name, case) in cases {
        case(name);
    }
}

/// The stale badge's age is a distance between two ENGINE stamps. The workstation is a separate
/// process, reachable over UDP at any address, so its own clock is not comparable with the engine's:
/// a few seconds of skew the wrong way would make a frozen ladder read as a live one, which is the
/// single reading this indicator exists to prevent. The reference is the freshest stamp seen on
/// EITHER lane, so an engine whose other instruments keep reporting still ages the frozen one.
#[test]
fn a_books_age_is_measured_against_the_engines_own_newest_stamp() {
    let mut model = UiModel::with_capacity(2, DurationUs::from_micros(100_000));
    assert_eq!(
        model.book_lag(InstrumentId(0)),
        None,
        "an instrument with no book has no age, not an age of zero"
    );

    model.apply_book(book(0, 0, 1_000_000, &[(100, 1)], &[(102, 1)]));
    model.apply_book(book(1, 0, 1_000_000, &[(100, 1)], &[(102, 1)]));
    assert_eq!(model.book_lag(InstrumentId(0)), Some(DurationUs::ZERO));

    // Instrument 0's book freezes while instrument 1 keeps arriving: the engine is plainly alive,
    // and the frozen ladder must say how far behind it is.
    model.apply_book(book(1, 1, 6_000_000, &[(100, 1)], &[(102, 1)]));
    assert_eq!(
        model.book_lag(InstrumentId(0)),
        Some(DurationUs::from_micros(5_000_000)),
    );
    assert_eq!(model.book_lag(InstrumentId(1)), Some(DurationUs::ZERO));

    // The event lane carries the same clock, so it advances the reference too — an engine emitting
    // quotes with no book updates at all still ages the ladder.
    model.apply_event(UiEvent::Quote {
        instrument: InstrumentId(1),
        seq: 0,
        event_ts_us: TsUs::from_micros(9_000_000),
        quote: DomQuote::default(),
    });
    assert_eq!(
        model.book_lag(InstrumentId(0)),
        Some(DurationUs::from_micros(8_000_000)),
    );

    // Reference stamps are absolute engine time, not an offset from this run's start: the same fold
    // a decade of wall clock later reports the same lag.
    let mut decade_later = UiModel::with_capacity(2, DurationUs::from_micros(100_000));
    decade_later.apply_book(book(0, 0, 1_315_000_000_000_000, &[(100, 1)], &[(102, 1)]));
    decade_later.apply_book(book(1, 0, 1_315_000_005_000_000, &[(100, 1)], &[(102, 1)]));
    assert_eq!(
        decade_later.book_lag(InstrumentId(0)),
        Some(DurationUs::from_micros(5_000_000)),
    );
}

#[test]
fn rows_per_side_is_capped_at_the_arrays_bound() {
    assert_eq!(
        MAX_ROWS_PER_SIDE, 30,
        "the level control's top end is the array bound; the two move together or not at all"
    );
    let snapshot = book(0, 0, 10, &[(100, 4)], &[(102, 5)]);
    let view = build_dom_view(DomViewInput {
        snapshot: Some(&snapshot),
        overlay: DomOverlay::default(),
        tick: TICK,
        grouping: DomGrouping::default(),
        rows_per_side: 200,
        feed: FeedStatus::default(),
    });
    assert_eq!(view.ask_rows().len(), MAX_ROWS_PER_SIDE);
    assert_eq!(view.bid_rows().len(), MAX_ROWS_PER_SIDE);
}
