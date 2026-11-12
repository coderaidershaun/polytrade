//! FITNESS: ctx.flatten is the only liquidity-taking path. All safeguards against overuse
//! are tested here. Flatten is a DECLARATION (level-triggered like quotes), not a command:
//! market orders outliving intent should not exist. The engine plans a new order each spin
//! based on current position. The flatten lane is separate from ctx.quote by design: a quote
//! declared Immediate is still refused by the reconciler, because a strategy that meant to rest
//! but crossed the spread has made a mistake. Both halves are pinned together to prevent
//! convergence on the easiest path.

use polysim::config::{RecordedTables, TrackerSpec};
use polysim::hot::dispatch::HotEngine;
use polysim::hot::exec::{
    BookTop, DesiredQuote, ExecLimits, ExecSettings, FeeModel, FlattenInput, FlattenOutcome,
    FundsView, QuoteLevel, RejectReason, TickGrid, plan_flatten,
};
use polysim::hot::strategy::{Strategy, StrategyCtx};
use polysim::ids::{AssetId, ClientOrderId, FIXED_SCALE, Price, Qty, Side};
use polysim::msg::exec::{ExecCommand, ExecKind, ExecLaneItem, OrderStyle, VenueOrderStatus};
use polysim::msg::inbound::{InboundMessage, SpinTick};
use polysim::registry::InstrumentRow;
use polysim::time::DurationUs;
use rtrb::Consumer;

use crate::engine_support::{
    ALL_TABLES, FillPen, account_chunk, exec_event, instrument_row, pop, rich, snapshot_pair,
    stream_ready, top,
};
use crate::exec_support::{
    CEILING, INSTRUMENT, QuotingSetup, built_quoting_engine, drain_commands, exec_settings,
    open_orders_snapshot, spin_at,
};

/// A polymarket-shaped grid: prices are probabilities on a 0.01 tick, sizes are shares to two
/// decimals, and the venue refuses anything below five shares or above `1 − tick`.
const TICK: i64 = FIXED_SCALE / 100;
const SHARE_STEP: i64 = FIXED_SCALE / 100;
const MIN_SHARES: Qty = Qty(5 * FIXED_SCALE);
const MAX_PRICE: Price = Price(FIXED_SCALE - TICK);
/// The crypto taker rate: 0.07.
const FEE_RATE: i64 = 7 * FIXED_SCALE / 100;

const BID: i64 = 40 * TICK;
const ASK: i64 = 42 * TICK;
const SLACK_TICKS: u32 = 2;

fn poly_grid() -> TickGrid {
    TickGrid {
        tick: TICK,
        step: SHARE_STEP,
        min_qty: MIN_SHARES,
        min_notional: 0,
        max_amends: 0,
        max_price: Some(MAX_PRICE),
    }
}

fn poly_row() -> InstrumentRow {
    InstrumentRow {
        tick_size: Some(Price(TICK)),
        lot_size: Some(Qty(SHARE_STEP)),
        min_qty: Some(MIN_SHARES),
        max_num_orders: Some(32),
        max_num_order_amends: Some(0),
        max_price: Some(MAX_PRICE),
        max_exposure_quote: CEILING,
        ..instrument_row(0, TrackerSpec::default(), 64)
    }
}

fn limits() -> ExecLimits {
    ExecLimits {
        requote_threshold_ticks: 1,
        max_quote_distance_centi_bps: 100_000_000,
        max_book_age: DurationUs::from_secs(3_600),
        max_order_notional_quote: 1_000_000 * FIXED_SCALE,
    }
}

fn holding(position_base: i64) -> FlattenInput {
    FlattenInput {
        position_base: Qty(position_base),
        grid: poly_grid(),
        top: top(BID, ASK),
        limits: limits(),
        funds: rich(),
        slack_ticks: SLACK_TICKS,
        fee_model: FeeModel::BinaryOutcome,
        taker_fee_rate: FEE_RATE,
    }
}

fn placed(outcome: FlattenOutcome) -> polysim::hot::exec::PlaceIntent {
    match outcome {
        FlattenOutcome::Place(intent) => intent,
        other => panic!("expected a placement, got {other:?}"),
    }
}

/// FITNESS: `plan_flatten` outcomes match a named table. The order that closes a position trades in
/// the direction that SHRINKS it, takes the far side of the book, and asks for exactly what is held
/// — the direction is the one mistake here that compiles, passes every structural check and doubles
/// the position it was asked to close. The marketable price is then clamped into the venue's own
/// bounds: Polymarket prices are probabilities bounded by `[tick, 1 − tick]`, and the ends of that
/// range are exactly where a position most needs closing, while a venue that publishes no ceiling
/// (Binance's shape, which always pairs with no binary-outcome fee) gets the slack in full rather
/// than clamped to parity. Last, a residue the venue is too coarse to trade is REFUSED — reached by
/// ordinary partial fills — never rounded up into an order the venue would reject or down into one
/// of nothing; the floor itself still trades, so the refusal case alone would not prove the boundary.
#[test]
fn plan_flatten_outcomes_match_named_cases() {
    enum Expect {
        Nothing,
        Refuse(RejectReason),
        Place { price: Price, qty: Qty },
    }
    struct Case {
        name: &'static str,
        input: FlattenInput,
        expect: Expect,
    }
    let sell_price = Price(BID - SLACK_TICKS as i64 * TICK);
    let buy_price = Price(ASK + SLACK_TICKS as i64 * TICK);
    let cases = [
        Case {
            name: "a_long_sells_into_the_bid_through_the_slack",
            input: holding(10 * FIXED_SCALE),
            expect: Expect::Place {
                price: sell_price,
                qty: Qty(10 * FIXED_SCALE),
            },
        },
        Case {
            name: "a_short_buys_through_the_ask_through_the_slack",
            input: holding(-10 * FIXED_SCALE),
            expect: Expect::Place {
                price: buy_price,
                qty: Qty(10 * FIXED_SCALE),
            },
        },
        Case {
            name: "flat_is_the_goal_not_a_refusal",
            input: holding(0),
            expect: Expect::Nothing,
        },
        Case {
            name: "buying_through_an_ask_already_at_the_top_stops_at_the_top",
            input: FlattenInput {
                top: top(MAX_PRICE.0 - TICK, MAX_PRICE.0),
                ..holding(-10 * FIXED_SCALE)
            },
            expect: Expect::Place {
                price: MAX_PRICE,
                qty: Qty(10 * FIXED_SCALE),
            },
        },
        Case {
            name: "selling_through_a_bid_already_at_the_bottom_stops_at_the_bottom",
            input: FlattenInput {
                top: top(TICK, 2 * TICK),
                ..holding(10 * FIXED_SCALE)
            },
            expect: Expect::Place {
                price: Price(TICK),
                qty: Qty(10 * FIXED_SCALE),
            },
        },
        Case {
            name: "with_no_price_ceiling_the_slack_applies_in_full",
            input: FlattenInput {
                grid: TickGrid {
                    max_price: None,
                    ..poly_grid()
                },
                top: top(MAX_PRICE.0 - TICK, MAX_PRICE.0),
                fee_model: FeeModel::None,
                ..holding(-10 * FIXED_SCALE)
            },
            expect: Expect::Place {
                price: Price(MAX_PRICE.0 + SLACK_TICKS as i64 * TICK),
                qty: Qty(10 * FIXED_SCALE),
            },
        },
        Case {
            name: "one_step_under_the_minimum_is_refused_rather_than_rounded",
            input: holding(MIN_SHARES.0 - SHARE_STEP),
            expect: Expect::Refuse(RejectReason::QtyBelowMin),
        },
        Case {
            name: "the_minimum_itself_still_trades",
            input: holding(MIN_SHARES.0),
            expect: Expect::Place {
                price: sell_price,
                qty: MIN_SHARES,
            },
        },
        Case {
            name: "no_money_refuses_as_underfunded_rather_than_below_the_minimum",
            input: FlattenInput {
                funds: FundsView {
                    spendable: 0,
                    floor: 0,
                },
                ..holding(-10 * FIXED_SCALE)
            },
            expect: Expect::Refuse(RejectReason::Underfunded),
        },
        Case {
            name: "a_book_with_no_far_side_gives_no_price_to_take",
            input: FlattenInput {
                top: BookTop {
                    best_bid: None,
                    ..top(BID, ASK)
                },
                ..holding(10 * FIXED_SCALE)
            },
            expect: Expect::Refuse(RejectReason::BookNotQuotable),
        },
    ];
    for case in cases {
        let outcome = plan_flatten(case.input);
        match case.expect {
            Expect::Nothing => assert_eq!(
                outcome,
                FlattenOutcome::Nothing,
                "case {}: got {outcome:?}",
                case.name
            ),
            Expect::Refuse(reason) => assert_eq!(
                outcome,
                FlattenOutcome::Refuse(reason),
                "case {}: got {outcome:?}",
                case.name
            ),
            Expect::Place { price, qty } => {
                let intent = placed(outcome);
                assert_eq!(intent.price, price, "case {}: price", case.name);
                assert_eq!(intent.qty, qty, "case {}: qty", case.name);
                assert_eq!(
                    intent.style,
                    OrderStyle::Immediate,
                    "case {}: style",
                    case.name
                );
            }
        }
    }
}

/// FITNESS: the taker fee is the venue's published formula, pinned against the venue's own
/// published numbers rather than against a restatement of the code.
///
/// `fee = shares × rate × p × (1 − p)`, symmetric about even money: a hundred
/// shares at 0.50 costs 1.75, and the same hundred at 0.10 or 0.90 costs 0.63.
#[test]
fn the_taker_fee_matches_the_venues_published_worked_examples() {
    let hundred = Qty(100 * FIXED_SCALE);
    let even_money = Price(FIXED_SCALE / 2);
    assert_eq!(
        FeeModel::BinaryOutcome.taker_fee_quote(even_money, hundred, FEE_RATE),
        175 * FIXED_SCALE / 100,
        "100 shares at 0.50 costs 1.75"
    );
    for price in [Price(FIXED_SCALE / 10), Price(9 * FIXED_SCALE / 10)] {
        assert_eq!(
            FeeModel::BinaryOutcome.taker_fee_quote(price, hundred, FEE_RATE),
            63 * FIXED_SCALE / 100,
            "and 0.63 at either 0.10 or 0.90 — the curve is symmetric about even money"
        );
    }
    assert_eq!(
        FeeModel::BinaryOutcome.taker_fee_quote(even_money, hundred, 0),
        0,
        "a rate of nothing charges nothing"
    );
    // The curve is only meaningful between the bounds. Beyond them it must answer zero rather than
    // go negative: a negative fee hands a buy MORE headroom the further out of range the price is.
    for outside in [Price(0), Price(FIXED_SCALE), Price(100_000 * FIXED_SCALE)] {
        assert_eq!(
            FeeModel::BinaryOutcome.taker_fee_quote(outside, hundred, FEE_RATE),
            0
        );
    }
}

/// FITNESS: a BUY reserves the fee on TOP of the notional, so a flatten cannot ask for more
/// than the account can pay for.
///
/// The fee is charged at match time and is not part of the signed order, so an account funded
/// to the last mantissa of the notional has enough for the trade and not enough for the trade
/// plus its fee. The pair is what makes this a real pin: the same budget with a zero rate
/// buys strictly more, so the number cannot be coming from any other cap.
#[test]
fn a_buy_reserves_the_taker_fee_alongside_the_notional() {
    let price = Price(ASK + SLACK_TICKS as i64 * TICK);
    let budget = price.notional(Qty(10 * FIXED_SCALE));
    let short_of_ten = FlattenInput {
        funds: FundsView {
            spendable: budget,
            floor: 0,
        },
        ..holding(-10 * FIXED_SCALE)
    };

    let with_fee = placed(plan_flatten(short_of_ten)).qty;
    assert!(
        with_fee < Qty(10 * FIXED_SCALE),
        "the account holds exactly the notional of ten shares, so ten shares plus their fee is more \
         than it has: asked for {with_fee:?}"
    );

    let free_of_charge = placed(plan_flatten(FlattenInput {
        taker_fee_rate: 0,
        ..short_of_ten
    }))
    .qty;
    assert_eq!(
        free_of_charge,
        Qty(10 * FIXED_SCALE),
        "and the identical budget covers all ten when nothing is charged on top"
    );
    assert!(
        with_fee < free_of_charge,
        "a headroom that made no difference to the size is a headroom that is not being reserved"
    );
}

/// Declares a flatten on every spin until told to stop, and quotes nothing. The two lanes are
/// separate and this strategy uses only one of them.
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

/// Declares a MARKET order through the quote lane, which is the thing the engine must refuse.
struct CrossingQuoter;

impl Strategy for CrossingQuoter {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        ctx.quote(
            INSTRUMENT,
            Side::Sell,
            QuoteLevel::ZERO,
            Some(DesiredQuote {
                price: Price(BID),
                qty: MIN_SHARES,
                style: OrderStyle::Immediate,
            }),
        );
    }
}

fn flattening_engine(strategy: Box<dyn Strategy>) -> (HotEngine, Consumer<ExecLaneItem>) {
    let built = built_quoting_engine(QuotingSetup {
        row: poly_row(),
        strategy,
        restored: &[],
        settings: ExecSettings {
            flatten_slack_ticks: SLACK_TICKS,
            fee_model: FeeModel::BinaryOutcome,
            taker_fee_rate: FEE_RATE,
            ..exec_settings()
        },
        tables: RecordedTables::new(&ALL_TABLES),
        run_nonce: 0,
    });
    (built.engine, built.commands)
}

/// Stream up, open orders known, and a share balance far past anything these cases sell.
///
/// The headroom is not decoration. A placement RESERVES what it will spend, and the reservation
/// is released only once the venue reports a balance stamped later than the reservation itself.
/// An account holding exactly the position is starved for its second flatten until the edge
/// restates balances. That gate is `exec_order`'s to pin; here it would silently stop re-fire.
fn make_ready_with_inventory(engine: &mut HotEngine, when: i64) {
    engine.dispatch(pop(0, 0), &stream_ready(INSTRUMENT, when));
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Account(account_chunk(
            &[
                (AssetId(0), 10_000 * FIXED_SCALE),
                (AssetId(1), 10_000 * FIXED_SCALE),
            ],
            when,
        )),
    );
    open_orders_snapshot(engine, when);
}

/// The book these engine cases trade against, seated with enough depth on both sides that nothing
/// is refused for want of a touch.
fn seat_book(engine: &mut HotEngine, when: i64) {
    let (bids, asks) = snapshot_pair(
        0,
        &[(BID, 100 * FIXED_SCALE)],
        &[(ASK, 100 * FIXED_SCALE)],
        when,
    );
    engine.dispatch(pop(0, 0), &InboundMessage::Book(bids));
    engine.dispatch(pop(0, 0), &InboundMessage::Book(asks));
}

/// An engine long ten shares bought at 0.41 through the real inbound path, with its commands drained.
fn long_ten_shares(strategy: Box<dyn Strategy>) -> (HotEngine, Consumer<ExecLaneItem>) {
    let (mut engine, mut commands) = flattening_engine(strategy);
    make_ready_with_inventory(&mut engine, 0);
    seat_book(&mut engine, 10);
    let mut pen = FillPen::new(0);
    for message in pen.fill(Side::Buy, 41 * TICK, 10 * FIXED_SCALE, 20) {
        engine.dispatch(pop(0, 0), &message);
    }
    drain_commands(&mut commands);
    (engine, commands)
}

fn flatten_places(commands: &[ExecCommand]) -> Vec<(Side, Qty, OrderStyle, ClientOrderId)> {
    commands
        .iter()
        .filter_map(|command| match command {
            ExecCommand::Place {
                side,
                qty,
                style,
                client_id,
                ..
            } => Some((*side, *qty, *style, *client_id)),
            _ => None,
        })
        .collect()
}

/// FITNESS: a declared flatten against a real position sends exactly ONE marketable order,
/// and the declaration expiring stops it.
///
/// One order because the whole-side single-flight rule is what keeps `max_orders_per_side`
/// true; a market lane that ignored it would put two unanswered takes on the same side in
/// one spin. Expiring because the level-triggered contract is the only thing standing between
/// a strategy that stopped asking and an engine that keeps selling.
#[test]
fn a_declared_flatten_sends_one_marketable_order_and_expires_with_the_declaration() {
    let (mut engine, mut commands) = long_ten_shares(Box::new(Flattener { is_declaring: true }));

    spin_at(&mut engine, 1, 30);
    let sent = flatten_places(&drain_commands(&mut commands));
    assert_eq!(
        sent.len(),
        1,
        "one take per spin, whatever is declared: {sent:?}"
    );
    let (side, qty, style, client_id) = sent[0];
    assert_eq!(side, Side::Sell, "a long is closed by selling");
    assert_eq!(qty, Qty(10 * FIXED_SCALE), "all of it");
    assert_eq!(style, OrderStyle::Immediate);

    // The order dies without filling; the position is untouched and still wants closing.
    kill_unfilled(&mut engine, client_id, 40);

    let (mut engine, mut commands) = long_ten_shares(Box::new(Flattener {
        is_declaring: false,
    }));
    spin_at(&mut engine, 1, 30);
    assert!(
        flatten_places(&drain_commands(&mut commands)).is_empty(),
        "the same position with nothing declared must send nothing — a flatten that survived its \
         declaration is a market order nobody asked for"
    );
}

/// FITNESS: a flatten that fills PARTIALLY re-fires next spin, sized by what is left.
///
/// This is the whole reason the declaration is level-triggered rather than a command. There
/// is no retry, no outstanding-order bookkeeping and no remainder tracked anywhere: the next
/// spin reads the ledger and plans against it. The same code path that opens the position
/// closes the rest of it. A design that remembered the order would need bookkeeping for
/// every way one can end.
#[test]
fn a_partial_fill_re_fires_next_spin_sized_by_what_is_left() {
    let (mut engine, mut commands) = long_ten_shares(Box::new(Flattener { is_declaring: true }));

    spin_at(&mut engine, 1, 30);
    let first = flatten_places(&drain_commands(&mut commands));
    let (_, qty, _, client_id) = first[0];
    assert_eq!(qty, Qty(10 * FIXED_SCALE));

    // Four of the ten shares trade and the remainder is killed, which is what a fill-and-kill does.
    let price = Price(BID - SLACK_TICKS as i64 * TICK);
    let filled = Qty(4 * FIXED_SCALE);
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Exec(polysim::msg::exec::ExecEvent {
            kind: ExecKind::ReportTrade,
            status: Some(VenueOrderStatus::PartiallyFilled),
            qty: Qty(10 * FIXED_SCALE),
            last_price: price,
            last_qty: filled,
            cumulative_qty: filled,
            cumulative_quote: price.notional(filled),
            ..exec_event(INSTRUMENT, client_id, Side::Sell, price.0, 40)
        }),
    );
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Exec(polysim::msg::exec::ExecEvent {
            kind: ExecKind::ReportExpired,
            status: Some(VenueOrderStatus::Expired),
            qty: Qty(10 * FIXED_SCALE),
            cumulative_qty: filled,
            cumulative_quote: price.notional(filled),
            ..exec_event(INSTRUMENT, client_id, Side::Sell, price.0, 41)
        }),
    );
    drain_commands(&mut commands);

    spin_at(&mut engine, 2, 50);
    let second = flatten_places(&drain_commands(&mut commands));
    assert_eq!(second.len(), 1, "the rest still wants closing: {second:?}");
    assert_eq!(
        second[0].1,
        Qty(6 * FIXED_SCALE),
        "six shares left, so six shares asked for — not the original ten and not a remainder \
         tracked somewhere the ledger cannot correct"
    );
}

/// FITNESS: the two lanes stay separate. A market order declared through `ctx.quote` is
/// refused, and the SAME engine flattens through its own lane. The refusal is a property
/// of the quote lane, not of the engine having no way to take liquidity at all.
///
/// Without the second half, `exec_reconcile`'s style refusal is satisfiable by an engine
/// that cannot send a marketable order under any circumstances, which is what changed.
#[test]
fn a_market_order_declared_as_a_quote_is_still_refused_by_name() {
    let (mut engine, mut commands) = long_ten_shares(Box::new(CrossingQuoter));

    spin_at(&mut engine, 1, 30);
    assert!(
        flatten_places(&drain_commands(&mut commands)).is_empty(),
        "an Immediate declared through the quote lane must be refused, never downgraded and never \
         honoured — the strategy asked to REST an order"
    );

    let (mut engine, mut commands) = long_ten_shares(Box::new(Flattener { is_declaring: true }));
    spin_at(&mut engine, 1, 30);
    assert_eq!(
        flatten_places(&drain_commands(&mut commands)).len(),
        1,
        "and the flatten lane on the identical fixture does send one"
    );
}

/// Ends an unfilled marketable order, which is what a venue does with a take that found no match.
fn kill_unfilled(engine: &mut HotEngine, client_id: ClientOrderId, when: i64) {
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Exec(polysim::msg::exec::ExecEvent {
            kind: ExecKind::ReportExpired,
            status: Some(VenueOrderStatus::Expired),
            ..exec_event(INSTRUMENT, client_id, Side::Sell, BID, when)
        }),
    );
}
