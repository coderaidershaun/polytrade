//! Pins how the one declaration that a strategy makes for a market each spin becomes orders. A
//! quote that crosses, a quote left behind while the market gets flat, or a quote that outlives its
//! declaration is an order that nobody asked for, and nothing downstream can tell it apart.

use std::cell::Cell;
use std::sync::{Arc, Mutex};

use engine::hot::dispatch::HotEngine;
use engine::hot::exec::{
    BookTop, ClientIdLayout, CloseReason, DesiredQuote, ExecLimits, ExecSettings, FeeModel,
    FundsView, OrderState, PlaceIntent, QuoteLevel, ReconcileInput, ReconcileOutcome, RejectReason,
    RestingOrder, TickGrid, level_of_slot, reconcile_side,
};
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::msg::exec::{
    ExecCommand, ExecEvent, ExecKind, ExecLaneItem, OrderStyle, VenueOrderStatus,
};
use engine::msg::ids::{AssetId, ClientOrderId, FIXED_SCALE, Notional, Price, Qty, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::time::{DurationUs, LocalTs};
use engine::msg::ui::UiEvent;
use engine::runtime::config::{RecordedTables, TrackerSpec};
use engine::runtime::registry::InstrumentRow;
use proptest::prelude::{any, prop, prop_assert, prop_assert_eq, prop_oneof};
use proptest::strategy::{Just, Strategy as Generator};
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rtrb::Consumer;

use crate::engine_support::{
    ALL_TABLES, FillPen, ONE, account_chunk, exec_event, instrument_row, rich, snapshot_pair,
    stream_ready, top, ts,
};
use crate::exec_support::{
    ASK, BID, CEILING, INSTRUMENT, QuotingEngine, QuotingSetup, built_quoting_engine,
    drain_commands, exec_settings, make_ready, open_orders_snapshot, reseat_book, spin_at,
};

/// The grid of the named cases: a tick of a hundredth of a unit and a lot of 1e-5.
const TICK: i64 = ONE / 100;
const STEP: i64 = ONE / 100_000;

fn grid() -> TickGrid {
    TickGrid {
        tick: TICK,
        step: STEP,
        min_qty: Qty::from_mantissa(STEP),
        min_notional: Notional::from_mantissa(0),
        max_amends: 10,
        max_price: None,
    }
}

fn limits() -> ExecLimits {
    ExecLimits {
        requote_threshold_ticks: 1,
        // Wide enough that the band is never what refuses a named case.
        max_quote_distance_centi_bps: 100 * 100 * 100,
        max_book_age: DurationUs::from_secs(2),
        max_order_notional_quote: Notional::from_mantissa(i64::MAX / 2),
    }
}

fn want(price: i64, qty: i64) -> Option<DesiredQuote> {
    Some(DesiredQuote {
        price: Price::from_mantissa(price),
        qty: Qty::from_mantissa(qty),
    })
}

fn live(price: i64, qty: i64) -> Option<RestingOrder> {
    Some(RestingOrder {
        price: Price::from_mantissa(price),
        qty: Qty::from_mantissa(qty),
        filled: Qty::from_mantissa(0),
        amends_used: 0,
    })
}

/// One decision against a book one tick wide at 100, fresh and funded.
fn input(
    side: Side,
    desired: Option<DesiredQuote>,
    resting: Option<RestingOrder>,
) -> ReconcileInput {
    ReconcileInput {
        side,
        desired,
        resting,
        grid: grid(),
        top: top(100 * ONE, 100 * ONE + TICK),
        limits: limits(),
        funds: rich(),
    }
}

/// A price that rests passively on `side` against the book of [`input`], so that a case reaches
/// the rule under test before the cross guard refuses it.
fn passive(side: Side) -> i64 {
    match side {
        Side::Buy => 100 * ONE - TICK,
        Side::Sell => 100 * ONE + 2 * TICK,
    }
}

/// The order the venue holds once `outcome` is applied. A refusal changes nothing at the venue,
/// which is why the resting order stays.
fn apply(resting: Option<RestingOrder>, outcome: ReconcileOutcome) -> Option<RestingOrder> {
    match outcome {
        ReconcileOutcome::Nothing | ReconcileOutcome::Reject(_) => resting,
        ReconcileOutcome::Cancel => None,
        ReconcileOutcome::Place(intent) => Some(RestingOrder {
            price: intent.price,
            qty: intent.qty,
            filled: Qty::from_mantissa(0),
            amends_used: 0,
        }),
        ReconcileOutcome::AmendQty(qty) => resting.map(|order| RestingOrder {
            qty,
            amends_used: order.amends_used + 1,
            ..order
        }),
    }
}

fn emits_command(outcome: ReconcileOutcome) -> bool {
    !matches!(
        outcome,
        ReconcileOutcome::Nothing | ReconcileOutcome::Reject(_)
    )
}

/// FITNESS: an emitted quote never crosses the far touch, and the reconciler settles in at most
/// two commands. A quote snapped toward the touch, a size that grows, or a decision that keeps
/// sending commands against an unchanged declaration is a legal order that every later layer
/// agrees with: it pays the taker fee, spends the order rate budget, and gives up queue priority.
///
/// Over generated grids, books (valid or not, fresh or stale, a side missing), declarations,
/// resting orders, funds, and limits: a placed price never crosses and snaps away from the touch,
/// a size only shrinks, a price inside the requote band sends nothing, a shrink past the amend
/// budget cancels first, a crossing quote is refused with the resting order left alone, and a book
/// that cannot be used withdraws. The named cases, the two recorded seeds, and the ladder of an
/// engine hold their values, and a quote holds a price and a size only.
#[test]
fn emitted_quote_never_crosses_and_settles_in_two_commands() {
    a_quote_is_a_price_and_a_size();
    crossing_and_unusable_books();
    recorded_seeds();
    ladder_withdraws_before_it_places();
    let mut runner = TestRunner::new(Config {
        cases: 2_048,
        failure_persistence: None,
        ..Config::default()
    });
    let reached = Cell::new([0u32; 5]);
    let outcome = runner.run(&(decisions(), snaps()), |(input, (mantissa, increment))| {
        let mut counts = reached.get();
        counts[outcome_kind(reconcile_side(input))] += 1;
        reached.set(counts);
        decide_once(input)?;
        settle(input)?;
        snap(mantissa, increment)
    });
    if let Err(failure) = outcome {
        panic!("a quote decision broke a rule of the reconciler: {failure}");
    }
    let counts = reached.get();
    assert!(
        counts.iter().all(|count| *count > 0),
        "the generated decisions must reach no command, a place, a cancel, an amend, and a \
         refusal, and reached {counts:?}"
    );
}

fn outcome_kind(outcome: ReconcileOutcome) -> usize {
    match outcome {
        ReconcileOutcome::Nothing => 0,
        ReconcileOutcome::Place(_) => 1,
        ReconcileOutcome::Cancel => 2,
        ReconcileOutcome::AmendQty(_) => 3,
        ReconcileOutcome::Reject(_) => 4,
    }
}

/// A strategy cannot ask a quote to take liquidity: a quote has no field that could say so. This
/// destructure names each field, so a new one, such as an order style, fails the build here.
fn a_quote_is_a_price_and_a_size() {
    let DesiredQuote { price, qty } = want(passive(Side::Buy), ONE).expect("declared");
    assert_eq!(
        (price, qty),
        (
            Price::from_mantissa(passive(Side::Buy)),
            Qty::from_mantissa(ONE)
        )
    );
}

fn crossing_and_unusable_books() {
    let resting = live(100 * ONE - TICK, ONE);
    for (side, price, words) in [
        (Side::Buy, 100 * ONE + TICK, "a bid AT the best ask"),
        (
            Side::Buy,
            100 * ONE + 5 * TICK,
            "a bid THROUGH the best ask",
        ),
        (Side::Sell, 100 * ONE, "an ask AT the best bid"),
    ] {
        assert_eq!(
            reconcile_side(input(side, want(price, ONE), resting)),
            ReconcileOutcome::Reject(RejectReason::WouldCross),
            "{words} must be refused, and the resting order left alone"
        );
    }

    let desired = want(100 * ONE - TICK, ONE);
    let one_sided = BookTop {
        best_ask: None,
        ..top(100 * ONE, 100 * ONE + TICK)
    };
    let stale = BookTop {
        now_ts_us: LocalTs::from_micros(1_000 + DurationUs::from_secs(3).micros()),
        ..top(100 * ONE, 100 * ONE + TICK)
    };
    let invalid = BookTop {
        is_valid: false,
        ..top(100 * ONE, 100 * ONE + TICK)
    };
    for book in [one_sided, stale, invalid] {
        assert_eq!(
            reconcile_side(ReconcileInput {
                top: book,
                ..input(Side::Buy, desired, resting)
            }),
            ReconcileOutcome::Cancel,
            "a book that cannot be priced must withdraw the quote: {book:?}"
        );
    }
    assert_eq!(
        reconcile_side(ReconcileInput {
            top: invalid,
            ..input(Side::Buy, desired, None)
        }),
        ReconcileOutcome::Nothing,
        "with nothing resting there is nothing to withdraw"
    );
}

/// The two seeds that proptest once recorded for the reconciler, each as the case it shrank to.
fn recorded_seeds() {
    let resting = live(passive(Side::Sell), 10 * STEP);
    assert_eq!(
        reconcile_side(input(
            Side::Sell,
            want(passive(Side::Sell), 5 * STEP),
            resting
        )),
        ReconcileOutcome::AmendQty(Qty::from_mantissa(5 * STEP)),
        "an ask with no amend spent shrinks by an amend"
    );

    let unchanged = input(
        Side::Sell,
        want(passive(Side::Sell), STEP),
        live(passive(Side::Sell), STEP),
    );
    settle(unchanged).expect("an ask that rests as declared settles");
    assert_eq!(reconcile_side(unchanged), ReconcileOutcome::Nothing);
}

/// The first decision over one input: what a placement, an amend, a requote, or a refusal may be.
fn decide_once(input: ReconcileInput) -> Result<(), TestCaseError> {
    let ReconcileInput {
        side,
        desired,
        resting,
        grid,
        top,
        limits,
        ..
    } = input;
    let first = reconcile_side(input);
    let is_quotable = top.is_quotable(limits.max_book_age);
    if !is_quotable {
        let withdrawal = resting.map_or(ReconcileOutcome::Nothing, |_| ReconcileOutcome::Cancel);
        prop_assert_eq!(
            first,
            withdrawal,
            "a book that cannot be used must withdraw"
        );
    }
    if let (Some(wanted), true) = (desired, is_quotable) {
        let price = side.snap_passive(wanted.price, grid.tick);
        let would_cross = match side {
            Side::Buy => top.best_ask.is_some_and(|ask| price >= ask),
            Side::Sell => top.best_bid.is_some_and(|bid| price <= bid),
        };
        if would_cross {
            prop_assert!(
                matches!(first, ReconcileOutcome::Reject(_)),
                "a crossing quote must be refused with the resting order left alone, got {first:?}"
            );
        }
        if let Some(order) = resting {
            in_band_and_amends(
                price,
                wanted.qty.floor_to_step(grid.step),
                order,
                input,
                first,
            )?;
        }
    }
    match first {
        ReconcileOutcome::Place(intent) => {
            prop_assert!(
                resting.is_none(),
                "placed beside a resting order: {resting:?}"
            );
            let wanted = desired.expect("a placement comes from a declaration");
            placement(side, wanted, top, grid, intent)
        }
        ReconcileOutcome::AmendQty(qty) => {
            let order = resting.expect("an amend needs a resting order");
            prop_assert!(
                qty < order.qty && qty > order.filled,
                "an amend to {qty:?} of {order:?}"
            );
            prop_assert!(
                order.amends_used < grid.max_amends,
                "an amend past the budget"
            );
            Ok(())
        }
        _ => Ok(()),
    }
}

fn in_band_and_amends(
    price: Price,
    qty: Qty,
    order: RestingOrder,
    input: ReconcileInput,
    first: ReconcileOutcome,
) -> Result<(), TestCaseError> {
    let drift = price.mantissa().abs_diff(order.price.mantissa()) / input.grid.tick.unsigned_abs();
    let is_in_band = drift <= u64::from(input.limits.requote_threshold_ticks);
    if is_in_band && qty == order.qty && qty > order.filled {
        prop_assert!(
            !emits_command(first),
            "requoted for a {drift}-tick move inside the band: {first:?}"
        );
    }
    let is_shrink = qty > order.filled && qty < order.qty;
    if is_in_band && is_shrink && order.amends_used >= input.grid.max_amends {
        prop_assert!(
            matches!(
                first,
                ReconcileOutcome::Cancel | ReconcileOutcome::Reject(_)
            ),
            "a shrink past the amend budget must cancel first, got {first:?}"
        );
    }
    Ok(())
}

fn placement(
    side: Side,
    wanted: DesiredQuote,
    top: BookTop,
    grid: TickGrid,
    intent: PlaceIntent,
) -> Result<(), TestCaseError> {
    let (price, asked) = (intent.price.mantissa(), wanted.price.mantissa());
    match side {
        Side::Buy => {
            prop_assert!(
                top.best_ask.is_some_and(|ask| intent.price < ask),
                "a bid crossed"
            );
            prop_assert!(
                price <= asked && asked - price < grid.tick,
                "a bid snapped UP"
            );
        }
        Side::Sell => {
            prop_assert!(
                top.best_bid.is_some_and(|bid| intent.price > bid),
                "an ask crossed"
            );
            prop_assert!(
                price >= asked && price - asked < grid.tick,
                "an ask snapped DOWN"
            );
        }
    }
    prop_assert_eq!(price.rem_euclid(grid.tick), 0, "a price off the tick grid");
    prop_assert_eq!(
        intent.qty.mantissa().rem_euclid(grid.step),
        0,
        "a size off the step"
    );
    prop_assert!(
        intent.qty <= wanted.qty,
        "the size grew past the declaration"
    );
    prop_assert!(intent.qty >= grid.min_qty, "a size under the venue minimum");
    prop_assert_eq!(
        intent.style,
        OrderStyle::PostOnly,
        "a quote that does not rest"
    );
    Ok(())
}

/// Applies each command to the resting order until no command comes. Two commands, because an
/// order that has filled past what is still wanted takes a cancel and then a place.
fn settle(input: ReconcileInput) -> Result<(), TestCaseError> {
    let mut resting = input.resting;
    let mut issued = Vec::new();
    loop {
        let outcome = reconcile_side(ReconcileInput { resting, ..input });
        if !emits_command(outcome) {
            let again = reconcile_side(ReconcileInput { resting, ..input });
            prop_assert_eq!(again, outcome, "the settled answer is not stable");
            return Ok(());
        }
        issued.push(outcome);
        prop_assert!(issued.len() <= 2, "still sending after {issued:?}");
        resting = apply(resting, outcome);
    }
}

/// A snap on each side, at each price and grid: never toward the touch, on the grid, and never
/// further than one increment.
fn snap(mantissa: i64, increment: i64) -> Result<(), TestCaseError> {
    let raw = Price::from_mantissa(mantissa);
    let bid = Side::Buy.snap_passive(raw, increment).mantissa();
    let ask = Side::Sell.snap_passive(raw, increment).mantissa();
    prop_assert!(
        bid <= mantissa && mantissa - bid < increment,
        "a buy snapped UP to {bid}"
    );
    prop_assert!(
        ask >= mantissa && ask - mantissa < increment,
        "a sell snapped DOWN to {ask}"
    );
    prop_assert_eq!(bid.rem_euclid(increment), 0, "a buy left the grid");
    prop_assert_eq!(ask.rem_euclid(increment), 0, "a sell left the grid");
    Ok(())
}

fn snaps() -> impl Generator<Value = (i64, i64)> {
    (-1_000_000_000_000i64..1_000_000_000_000, 1i64..1_000_000)
}

/// The grid of the named cases, a finer one, and a tick of 7 that no price of the book sits on.
fn grids() -> impl Generator<Value = TickGrid> {
    (
        prop::sample::select(vec![TICK, TICK / 10, 7]),
        prop::sample::select(vec![STEP, ONE / 100]),
        1i64..=3,
        prop::sample::select(vec![0, ONE]),
        0u8..=10,
    )
        .prop_map(
            |(tick, step, min_steps, min_notional, max_amends)| TickGrid {
                tick,
                step,
                min_qty: Qty::from_mantissa(min_steps * step),
                min_notional: Notional::from_mantissa(min_notional),
                max_amends,
                max_price: None,
            },
        )
}

/// A best bid below its best ask near 100, each side missing at times, valid or not, and fresh
/// or past the age of [`limits`].
fn tops() -> impl Generator<Value = BookTop> {
    let age = prop_oneof![4 => 0i64..=2_000_000, 1 => 2_000_001i64..=4_000_000];
    (
        -ONE..=ONE,
        1i64..=ONE / 10,
        prop::bool::weighted(0.95),
        prop::bool::weighted(0.95),
        prop::bool::weighted(0.95),
        age,
    )
        .prop_map(|(offset, spread, has_bid, has_ask, is_valid, age)| {
            let (bid, ask) = (100 * ONE + offset, 100 * ONE + offset + spread);
            BookTop {
                best_bid: has_bid.then_some(Price::from_mantissa(bid)),
                best_ask: has_ask.then_some(Price::from_mantissa(ask)),
                mid: Price::from_mantissa(if has_bid && has_ask { bid + spread / 2 } else { 0 }),
                is_valid,
                last_commit_ts_us: ts(1_000),
                now_ts_us: ts(1_000 + age),
            }
        })
}

#[derive(Debug, Clone, Copy)]
enum RestingPrice {
    Anywhere(i64),
    TicksFromDeclared(i64),
}

/// One decision of one side: a declaration or none, a resting order on the grid or none, funds,
/// and limits.
fn decisions() -> impl Generator<Value = ReconcileInput> {
    let declaration = proptest::option::weighted(0.85, (-2 * ONE..=2 * ONE, 1i64..=5 * ONE));
    // Most resting orders sit within a few ticks of the declared price, with little filled and few
    // amends used, where a requote, an amend, and the amend budget are decided.
    let resting_price = prop_oneof![
        1 => (-2 * ONE..=2 * ONE).prop_map(RestingPrice::Anywhere),
        3 => (-2i64..=2).prop_map(RestingPrice::TicksFromDeclared),
    ];
    let filled_permille = prop_oneof![Just(0i64), 0i64..1_000];
    let amends_used = prop_oneof![3 => 0u8..=2, 1 => 0u8..=12];
    let resting = proptest::option::weighted(
        0.6,
        (resting_price, 1i64..=5 * ONE, filled_permille, amends_used),
    );
    let funds = prop_oneof![
        Just(rich()),
        (0i64..=10 * ONE).prop_map(|spendable| FundsView {
            spendable,
            floor: 0
        }),
    ];
    let limits = (
        0u32..=3,
        prop::sample::select(vec![100 * 100 * 100, 2 * 100 * 100]),
        prop::sample::select(vec![i64::MAX / 2, 400 * ONE]),
    )
        .prop_map(|(threshold, band, notional)| ExecLimits {
            requote_threshold_ticks: threshold,
            max_quote_distance_centi_bps: band,
            max_order_notional_quote: Notional::from_mantissa(notional),
            ..limits()
        });
    (
        grids(),
        tops(),
        any::<bool>(),
        declaration,
        resting,
        funds,
        limits,
    )
        .prop_map(|(grid, top, is_buy, declaration, resting, funds, limits)| {
            let side = if is_buy { Side::Buy } else { Side::Sell };
            let declared_offset = declaration.map_or(0, |(offset, _)| offset);
            ReconcileInput {
                side,
                desired: declaration.and_then(|(offset, qty)| want(100 * ONE + offset, qty)),
                resting: resting.map(|(price, qty, filled_permille, amends_used)| {
                    let offset = match price {
                        RestingPrice::Anywhere(offset) => offset,
                        RestingPrice::TicksFromDeclared(ticks) => {
                            declared_offset + ticks * grid.tick
                        }
                    };
                    let qty = Qty::from_mantissa(qty)
                        .floor_to_step(grid.step)
                        .max(grid.min_qty);
                    let filled = Qty::from_mantissa(qty.mantissa() * filled_permille / 1_000);
                    RestingOrder {
                        price: side
                            .snap_passive(Price::from_mantissa(100 * ONE + offset), grid.tick),
                        qty,
                        filled: filled.floor_to_step(grid.step),
                        amends_used,
                    }
                }),
                grid,
                top,
                limits,
                funds,
            }
        })
}

/// A two-level bid ladder. Level zero moves at spin 3, and level one moves onto the same price
/// at spin 6.
struct KeyedLadder;

const LEVEL_ONE_PRICE: i64 = BID - 2 * ONE;
const MOVED_LEVEL_ZERO_PRICE: i64 = BID - 4 * ONE;

impl Strategy for KeyedLadder {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        let level_zero = if tick.seq >= 3 { MOVED_LEVEL_ZERO_PRICE } else { BID };
        let level_one = if tick.seq >= 6 { MOVED_LEVEL_ZERO_PRICE } else { LEVEL_ONE_PRICE };
        for (level, price) in [(QuoteLevel::ZERO, level_zero), (level(1), level_one)] {
            ctx.quote(INSTRUMENT, Side::Buy, level, want(price, ONE / 10));
        }
    }
}

fn level(index: u8) -> QuoteLevel {
    QuoteLevel::new(index).expect("a level of the ladder")
}

/// One request in flight for each side, and a withdrawal before a placement: a level whose price
/// moves is cancelled first, nothing goes out while that cancel waits, its replacement goes out
/// only after the venue confirms it, and a level that lands on the price of a lower level is
/// withdrawn.
fn ladder_withdraws_before_it_places() {
    let mut row = instrument_row(0, TrackerSpec::default(), 64);
    row.tick_size = Some(Price::from_mantissa(ONE));
    row.lot_size = Some(Qty::from_mantissa(1));
    let mut settings = exec_settings();
    settings.max_orders_per_side = 2;
    let QuotingEngine {
        mut engine,
        mut commands,
        ..
    } = quoting_engine(row, Box::new(KeyedLadder), settings);
    make_ready(&mut engine, 0);
    for message in reseat_book(BID, ASK, 1) {
        engine.dispatch(&message);
    }

    spin_at(&mut engine, 1, 10);
    let first = only(&mut commands);
    assert!(matches!(first, ExecCommand::Place { .. }));
    assert_eq!(level_of(first), QuoteLevel::ZERO);
    answer(
        &mut engine,
        (client_id_of(first), Side::Buy),
        ExecKind::AckPlaced,
        11,
    );

    spin_at(&mut engine, 2, 20);
    let second = only(&mut commands);
    assert!(matches!(second, ExecCommand::Place { .. }));
    assert_eq!(level_of(second), level(1));
    answer(
        &mut engine,
        (client_id_of(second), Side::Buy),
        ExecKind::AckPlaced,
        21,
    );

    spin_at(&mut engine, 3, 30);
    let cancel = only(&mut commands);
    assert!(matches!(cancel, ExecCommand::Cancel { .. }));
    assert_eq!(level_of(cancel), QuoteLevel::ZERO);

    spin_at(&mut engine, 4, 40);
    assert!(
        drain_commands(&mut commands).is_empty(),
        "a replacement was sent while the cancel was still unconfirmed"
    );

    answer(
        &mut engine,
        (client_id_of(cancel), Side::Buy),
        ExecKind::AckCanceled,
        41,
    );
    spin_at(&mut engine, 5, 50);
    let replacement = only(&mut commands);
    assert!(matches!(
        replacement,
        ExecCommand::Place { price, .. } if price == Price::from_mantissa(MOVED_LEVEL_ZERO_PRICE)
    ));
    assert_eq!(level_of(replacement), QuoteLevel::ZERO);
    answer(
        &mut engine,
        (client_id_of(replacement), Side::Buy),
        ExecKind::AckPlaced,
        51,
    );

    spin_at(&mut engine, 6, 60);
    let duplicate = only(&mut commands);
    assert!(matches!(duplicate, ExecCommand::Cancel { .. }));
    assert_eq!(level_of(duplicate), level(1));
}

fn quoting_engine(
    row: InstrumentRow,
    strategy: Box<dyn Strategy>,
    settings: ExecSettings,
) -> QuotingEngine {
    built_quoting_engine(QuotingSetup {
        row,
        strategy,
        restored: &[],
        settings,
        tables: RecordedTables::new(&ALL_TABLES),
        run_nonce: 0,
    })
}

fn only(commands: &mut Consumer<ExecLaneItem>) -> ExecCommand {
    let commands = drain_commands(commands);
    assert_eq!(commands.len(), 1, "expected one command: {commands:?}");
    commands[0]
}

fn client_id_of(command: ExecCommand) -> ClientOrderId {
    match command {
        ExecCommand::Place { client_id, .. } | ExecCommand::Cancel { client_id, .. } => client_id,
        other => panic!("command has no order identity: {other:?}"),
    }
}

fn level_of(command: ExecCommand) -> QuoteLevel {
    level_of_slot(ClientIdLayout::slot_of(client_id_of(command)))
}

/// The venue's answer to a placement or a cancel of one order.
fn answer(engine: &mut HotEngine, order: (ClientOrderId, Side), kind: ExecKind, when: i64) {
    let (client_id, side) = order;
    let status = match kind {
        ExecKind::AckPlaced => VenueOrderStatus::New,
        ExecKind::AckCanceled => VenueOrderStatus::Canceled,
        _ => panic!("no answer of kind {kind:?}"),
    };
    engine.dispatch(&InboundMessage::new(
        ts(when),
        MessageBody::Exec(ExecEvent {
            kind,
            status: Some(status),
            ..exec_event(INSTRUMENT, client_id, side, 0, when)
        }),
    ));
}

/// One trade report of an order of `order_qty`: the fill at its price, and a status that says
/// whether the order is filled whole or in part.
fn trade(
    client_id: ClientOrderId,
    side: Side,
    order_qty: Qty,
    fill: (Price, Qty),
    status: VenueOrderStatus,
    when: i64,
) -> InboundMessage {
    let (price, qty) = fill;
    InboundMessage::new(
        ts(when),
        MessageBody::Exec(ExecEvent {
            kind: ExecKind::ReportTrade,
            status: Some(status),
            qty: order_qty,
            last_price: price,
            last_qty: qty,
            cumulative_qty: qty,
            cumulative_quote: price.notional(qty),
            ..exec_event(INSTRUMENT, client_id, side, price.mantissa(), when)
        }),
    )
}

const GET_FLAT_SPIN: u64 = 9;
const HALF: Qty = Qty::from_mantissa(ONE / 2);

/// The order of the calls in a spin that gets flat.
#[derive(Debug, Clone, Copy)]
enum CallOrder {
    /// The bid is quoted, then the market gets flat.
    QuoteThenFlatten,
    /// The market gets flat, then the bid and the offer are quoted.
    FlattenThenQuote,
}

/// Holds a bid and an offer of half a unit. From spin 9 it also asks to get flat, in `order`. It
/// keeps the position that it reads at each spin.
struct Exiter {
    positions: Arc<Mutex<Vec<Qty>>>,
    order: CallOrder,
}

impl Exiter {
    fn quote_bid(ctx: &mut StrategyCtx<'_>) {
        ctx.quote(
            INSTRUMENT,
            Side::Buy,
            QuoteLevel::ZERO,
            want(BID, HALF.mantissa()),
        );
    }

    fn quote_ask(ctx: &mut StrategyCtx<'_>) {
        ctx.quote(
            INSTRUMENT,
            Side::Sell,
            QuoteLevel::ZERO,
            want(ASK, HALF.mantissa()),
        );
    }
}

impl Strategy for Exiter {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        let position = ctx.position_base(INSTRUMENT);
        self.positions.lock().expect("positions").push(position);
        if tick.seq < GET_FLAT_SPIN {
            Self::quote_bid(ctx);
            Self::quote_ask(ctx);
            return;
        }
        match self.order {
            CallOrder::QuoteThenFlatten => {
                Self::quote_bid(ctx);
                ctx.flatten(INSTRUMENT);
            }
            CallOrder::FlattenThenQuote => {
                ctx.flatten(INSTRUMENT);
                Self::quote_bid(ctx);
                Self::quote_ask(ctx);
            }
        }
    }
}

/// CONTRACT: a market that gets flat holds no quote. Its resting quotes are cancelled in the spin
/// that declares it, the marketable order goes out once the side is free, and a partial fill sends
/// what is left in the next spin. A quote left resting while the engine sells out of a position
/// can fill and open the position again. A quote declared before or after the call to get flat,
/// in the same spin, is dropped.
#[test]
fn get_flat_withdraws_each_quote_of_its_market() {
    for order in [CallOrder::QuoteThenFlatten, CallOrder::FlattenThenQuote] {
        gets_flat(order);
    }
    flatten_values_hold();
}

fn gets_flat(order: CallOrder) {
    let positions = Arc::new(Mutex::new(Vec::new()));
    let QuotingEngine {
        mut engine,
        mut commands,
        ..
    } = quoting_engine(
        instrument_row(0, TrackerSpec::default(), 64),
        Box::new(Exiter {
            positions: Arc::clone(&positions),
            order,
        }),
        exec_settings(),
    );
    make_ready(&mut engine, 0);
    for message in reseat_book(BID, ASK, 1) {
        engine.dispatch(&message);
    }
    let (bid, ask) = long_half_with_two_quotes(&mut engine, &mut commands);
    for seq in 3..GET_FLAT_SPIN {
        spin_at(&mut engine, seq, 10 * seq as i64);
        assert!(
            drain_commands(&mut commands).is_empty(),
            "spin {seq} sent a command"
        );
    }

    spin_at(&mut engine, GET_FLAT_SPIN, 90);
    let sent = drain_commands(&mut commands);
    let cancelled: Vec<ClientOrderId> = sent
        .iter()
        .filter_map(|command| match command {
            ExecCommand::Cancel { client_id, .. } => Some(*client_id),
            _ => None,
        })
        .collect();
    assert!(
        sent.len() == 2 && cancelled.contains(&bid) && cancelled.contains(&ask),
        "getting flat cancels the bid and the ask in its spin, and sends no marketable order \
         while the offer still rests ({order:?}): {sent:?}"
    );
    for order in [(bid, Side::Buy), (ask, Side::Sell)] {
        answer(&mut engine, order, ExecKind::AckCanceled, 95);
    }

    spin_at(&mut engine, GET_FLAT_SPIN + 1, 100);
    let take = only(&mut commands);
    let ExecCommand::Place {
        side: Side::Sell,
        qty,
        style: OrderStyle::Immediate,
        client_id,
        ..
    } = take
    else {
        panic!("getting flat from a long sends one marketable sell ({order:?}), got {take:?}");
    };
    assert_eq!(qty, HALF, "the whole position");
    engine.dispatch(&trade(
        client_id,
        Side::Sell,
        HALF,
        (Price::from_mantissa(BID), HALF),
        VenueOrderStatus::Filled,
        105,
    ));

    spin_at(&mut engine, GET_FLAT_SPIN + 2, 110);
    assert!(
        drain_commands(&mut commands).is_empty(),
        "a flat market that gets flat sends nothing, and quotes nothing ({order:?})"
    );
    let positions = positions.lock().expect("positions");
    let at_spin = |seq: u64| positions[usize::try_from(seq - 1).expect("a spin of the test")];
    assert_eq!(
        at_spin(GET_FLAT_SPIN),
        HALF,
        "the market is long half a unit when it gets flat"
    );
    assert_eq!(
        at_spin(GET_FLAT_SPIN + 2),
        Qty::from_mantissa(0),
        "{positions:?}"
    );
}

/// Spin 1 places a bid and an offer of half a unit. The bid fills whole, and spin 2 places it
/// again. Gives the client ids of the bid and the offer that rest.
fn long_half_with_two_quotes(
    engine: &mut HotEngine,
    commands: &mut Consumer<ExecLaneItem>,
) -> (ClientOrderId, ClientOrderId) {
    spin_at(engine, 1, 10);
    let placed = drain_commands(commands);
    assert_eq!(placed.len(), 2, "a bid and an offer: {placed:?}");
    let side_of = |command: &ExecCommand| match command {
        ExecCommand::Place { side, .. } => *side,
        other => panic!("expected a placement, got {other:?}"),
    };
    for command in &placed {
        answer(
            engine,
            (client_id_of(*command), side_of(command)),
            ExecKind::AckPlaced,
            11,
        );
    }
    let first_bid = placed.iter().find(|command| side_of(command) == Side::Buy);
    let ask = placed.iter().find(|command| side_of(command) == Side::Sell);
    let (Some(first_bid), Some(ask)) = (first_bid, ask) else {
        panic!("expected a bid and an offer: {placed:?}");
    };
    engine.dispatch(&trade(
        client_id_of(*first_bid),
        Side::Buy,
        HALF,
        (Price::from_mantissa(BID), HALF),
        VenueOrderStatus::Filled,
        15,
    ));

    spin_at(engine, 2, 20);
    let bid = only(commands);
    assert!(matches!(
        bid,
        ExecCommand::Place {
            side: Side::Buy,
            ..
        }
    ));
    answer(
        engine,
        (client_id_of(bid), Side::Buy),
        ExecKind::AckPlaced,
        21,
    );
    (client_id_of(bid), client_id_of(*ask))
}

/// A polymarket-shaped grid: prices are probabilities on a 0.01 tick, sizes are shares to two
/// decimals, and the venue refuses anything below five shares or above `1 − tick`.
const POLY_TICK: i64 = FIXED_SCALE / 100;
const SHARE_STEP: i64 = FIXED_SCALE / 100;
const MIN_SHARES: Qty = Qty::from_mantissa(5 * FIXED_SCALE);
const MAX_PRICE: Price = Price::from_mantissa(FIXED_SCALE - POLY_TICK);
const POLY_BID: i64 = 40 * POLY_TICK;
const POLY_ASK: i64 = 42 * POLY_TICK;
const SLACK_TICKS: u32 = 2;

/// Gets flat at each spin while `is_declaring`, and quotes nothing.
struct Flattener {
    is_declaring: bool,
}

impl Strategy for Flattener {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        if self.is_declaring {
            ctx.flatten(INSTRUMENT);
        }
    }
}

/// On a share market long ten shares: one take of all ten in a spin, nothing once the strategy
/// stops asking, and after four of ten trade, a take of the six that are left in the next spin.
fn flatten_values_hold() {
    let ten = Qty::from_mantissa(10 * FIXED_SCALE);
    let (mut engine, mut commands) = long_ten_shares(Flattener { is_declaring: true });
    spin_at(&mut engine, 1, 30);
    let takes = placements(&drain_commands(&mut commands));
    assert_eq!(takes.len(), 1, "one take a spin: {takes:?}");
    assert_eq!(
        takes[0].0,
        (Side::Sell, ten, OrderStyle::Immediate),
        "a long is closed by one marketable sell of all of it"
    );

    let (mut engine, mut commands) = long_ten_shares(Flattener {
        is_declaring: false,
    });
    spin_at(&mut engine, 1, 30);
    assert!(
        placements(&drain_commands(&mut commands)).is_empty(),
        "the same position with nothing declared sends nothing"
    );

    let (mut engine, mut commands) = long_ten_shares(Flattener { is_declaring: true });
    spin_at(&mut engine, 1, 30);
    let client_id = placements(&drain_commands(&mut commands))[0].1;
    let price = Price::from_mantissa(POLY_BID - i64::from(SLACK_TICKS) * POLY_TICK);
    let four = Qty::from_mantissa(4 * FIXED_SCALE);
    let partial = VenueOrderStatus::PartiallyFilled;
    engine.dispatch(&trade(
        client_id,
        Side::Sell,
        ten,
        (price, four),
        partial,
        40,
    ));
    engine.dispatch(&InboundMessage::new(
        ts(41),
        MessageBody::Exec(ExecEvent {
            kind: ExecKind::ReportExpired,
            status: Some(VenueOrderStatus::Expired),
            qty: ten,
            cumulative_qty: four,
            cumulative_quote: price.notional(four),
            ..exec_event(INSTRUMENT, client_id, Side::Sell, price.mantissa(), 41)
        }),
    ));
    drain_commands(&mut commands);
    spin_at(&mut engine, 2, 50);
    let rest = placements(&drain_commands(&mut commands));
    assert_eq!(rest.len(), 1, "the rest still wants closing: {rest:?}");
    assert_eq!(
        (rest[0].0).1,
        Qty::from_mantissa(6 * FIXED_SCALE),
        "six shares are left"
    );
}

/// Each placement, with its client id.
fn placements(commands: &[ExecCommand]) -> Vec<((Side, Qty, OrderStyle), ClientOrderId)> {
    commands
        .iter()
        .filter_map(|command| match *command {
            ExecCommand::Place {
                side,
                qty,
                style,
                client_id,
                ..
            } => Some(((side, qty, style), client_id)),
            _ => None,
        })
        .collect()
}

/// A share market with a binary-outcome taker fee, long ten shares bought at 0.41 through the
/// real inbound path, with a share balance far past what any case sells.
fn long_ten_shares(strategy: Flattener) -> (HotEngine, Consumer<ExecLaneItem>) {
    let row = InstrumentRow {
        tick_size: Some(Price::from_mantissa(POLY_TICK)),
        lot_size: Some(Qty::from_mantissa(SHARE_STEP)),
        min_qty: Some(MIN_SHARES),
        max_num_orders: Some(32),
        max_num_order_amends: Some(0),
        max_price: Some(MAX_PRICE),
        max_exposure_quote: Notional::from_mantissa(CEILING),
        ..instrument_row(0, TrackerSpec::default(), 64)
    };
    let settings = ExecSettings {
        flatten_slack_ticks: SLACK_TICKS,
        fee_model: FeeModel::BinaryOutcome,
        taker_fee_rate: 7 * FIXED_SCALE / 100,
        ..exec_settings()
    };
    let QuotingEngine {
        mut engine,
        mut commands,
        ..
    } = quoting_engine(row, Box::new(strategy), settings);
    engine.dispatch(&stream_ready(INSTRUMENT, 0));
    let shares = [
        (AssetId(0), 10_000 * FIXED_SCALE),
        (AssetId(1), 10_000 * FIXED_SCALE),
    ];
    engine.dispatch(&InboundMessage::new(
        ts(0),
        MessageBody::Account(account_chunk(&shares, 0)),
    ));
    open_orders_snapshot(&mut engine, 0);
    let (bids, asks) = snapshot_pair(
        0,
        &[(POLY_BID, 100 * FIXED_SCALE)],
        &[(POLY_ASK, 100 * FIXED_SCALE)],
        10,
    );
    engine.dispatch(&InboundMessage::new(ts(10), MessageBody::Book(bids)));
    engine.dispatch(&InboundMessage::new(ts(10), MessageBody::Book(asks)));
    let mut pen = FillPen::new(0);
    for message in pen.fill(Side::Buy, 41 * POLY_TICK, 10 * FIXED_SCALE, 20) {
        engine.dispatch(&message);
    }
    drain_commands(&mut commands);
    (engine, commands)
}

/// Bids one unit at 100 in spin 4 only.
struct BidsAtSpinFour;

impl Strategy for BidsAtSpinFour {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        if tick.seq == 4 {
            ctx.quote(
                INSTRUMENT,
                Side::Buy,
                QuoteLevel::ZERO,
                want(100 * ONE, ONE),
            );
        }
    }
}

/// CONTRACT: a market with no declaration for a spin stands aside, so each of its quotes is
/// cancelled, and a side never declared sends nothing. A strategy stuck in its own logic must not
/// leave the engine quoting for it.
#[test]
fn missing_declaration_is_a_cancel() {
    assert_eq!(
        reconcile_side(input(Side::Buy, None, live(100 * ONE - TICK, ONE))),
        ReconcileOutcome::Cancel
    );
    assert_eq!(
        reconcile_side(input(Side::Buy, None, None)),
        ReconcileOutcome::Nothing
    );

    let QuotingEngine {
        mut engine,
        mut commands,
        mut ui_events,
    } = quoting_engine(
        instrument_row(0, TrackerSpec::default(), 64),
        Box::new(BidsAtSpinFour),
        exec_settings(),
    );
    make_ready(&mut engine, 0);
    for message in reseat_book(BID, ASK, 1) {
        engine.dispatch(&message);
    }
    spin_at(&mut engine, 4, 40);
    let bid = only(&mut commands);
    assert!(matches!(
        bid,
        ExecCommand::Place { side: Side::Buy, price, qty, .. }
            if price == Price::from_mantissa(100 * ONE) && qty == Qty::from_mantissa(ONE)
    ));
    let client_id = client_id_of(bid);
    answer(&mut engine, (client_id, Side::Buy), ExecKind::AckPlaced, 41);
    drain_ui(&mut ui_events);

    spin_at(&mut engine, 5, 50);
    assert_eq!(
        drain_commands(&mut commands),
        vec![ExecCommand::Cancel {
            instrument: INSTRUMENT,
            client_id
        }],
        "one cancel of the bid, and nothing on the side that was never declared"
    );
    assert_eq!(
        working_bids(&drain_ui(&mut ui_events)),
        vec![(client_id, OrderState::CancelInFlight)]
    );

    answer(
        &mut engine,
        (client_id, Side::Buy),
        ExecKind::AckCanceled,
        51,
    );
    let closed = drain_ui(&mut ui_events).into_iter().any(|event| {
        matches!(
            event,
            UiEvent::OrderUpdate { client_id: id, state: OrderState::Closed(CloseReason::Canceled), .. }
                if id == client_id
        )
    });
    assert!(closed, "the cancel's answer closes the order");

    spin_at(&mut engine, 6, 60);
    assert!(
        drain_commands(&mut commands).is_empty(),
        "nothing is left to withdraw"
    );
}

fn drain_ui(events: &mut Consumer<UiEvent>) -> Vec<UiEvent> {
    let mut drained = Vec::new();
    while let Ok(event) = events.pop() {
        drained.push(event);
    }
    drained
}

/// Each working bid of the last order snapshot, with its state.
fn working_bids(events: &[UiEvent]) -> Vec<(ClientOrderId, OrderState)> {
    let snapshot = events.iter().rev().find_map(|event| match event {
        UiEvent::OrderSnapshot {
            side: Side::Buy,
            detail_len,
            orders,
            ..
        } => Some(orders[..usize::from(*detail_len)].to_vec()),
        _ => None,
    });
    snapshot
        .expect("each spin states the orders of each side")
        .iter()
        .map(|order| (order.client_id, order.state))
        .collect()
}
