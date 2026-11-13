//! Flatten is the one path that takes liquidity. These tests hold how the planner prices and sizes
//! the order that closes a position, with the taker fee of the venue on top.

use engine::hot::exec::{
    BookTop, ExecLimits, FeeModel, FlattenInput, FlattenOutcome, FundsView, RejectReason, TickGrid,
    plan_flatten,
};
use engine::msg::exec::OrderStyle;
use engine::msg::ids::{FIXED_SCALE, Notional, Price, Qty};
use engine::msg::time::DurationUs;

use crate::engine_support::{rich, top};

/// A polymarket-shaped grid: prices are probabilities on a 0.01 tick, sizes are shares to two
/// decimals, and the venue refuses anything below five shares or above `1 − tick`.
const TICK: i64 = FIXED_SCALE / 100;
const SHARE_STEP: i64 = FIXED_SCALE / 100;
const MIN_SHARES: Qty = Qty::from_mantissa(5 * FIXED_SCALE);
const MAX_PRICE: Price = Price::from_mantissa(FIXED_SCALE - TICK);
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
        min_notional: Notional::from_mantissa(0),
        max_amends: 0,
        max_price: Some(MAX_PRICE),
    }
}

fn limits() -> ExecLimits {
    ExecLimits {
        requote_threshold_ticks: 1,
        max_quote_distance_centi_bps: 100_000_000,
        max_book_age: DurationUs::from_secs(3_600),
        max_order_notional_quote: Notional::from_mantissa(1_000_000 * FIXED_SCALE),
    }
}

fn holding(position_base: i64) -> FlattenInput {
    FlattenInput {
        position_base: Qty::from_mantissa(position_base),
        grid: poly_grid(),
        top: top(BID, ASK),
        limits: limits(),
        funds: rich(),
        slack_ticks: SLACK_TICKS,
        fee_model: FeeModel::BinaryOutcome,
        taker_fee_rate: FEE_RATE,
    }
}

fn placed(outcome: FlattenOutcome) -> engine::hot::exec::PlaceIntent {
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
    let sell_price = Price::from_mantissa(BID - SLACK_TICKS as i64 * TICK);
    let buy_price = Price::from_mantissa(ASK + SLACK_TICKS as i64 * TICK);
    let cases = [
        Case {
            name: "a_long_sells_into_the_bid_through_the_slack",
            input: holding(10 * FIXED_SCALE),
            expect: Expect::Place {
                price: sell_price,
                qty: Qty::from_mantissa(10 * FIXED_SCALE),
            },
        },
        Case {
            name: "a_short_buys_through_the_ask_through_the_slack",
            input: holding(-10 * FIXED_SCALE),
            expect: Expect::Place {
                price: buy_price,
                qty: Qty::from_mantissa(10 * FIXED_SCALE),
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
                top: top(MAX_PRICE.mantissa() - TICK, MAX_PRICE.mantissa()),
                ..holding(-10 * FIXED_SCALE)
            },
            expect: Expect::Place {
                price: MAX_PRICE,
                qty: Qty::from_mantissa(10 * FIXED_SCALE),
            },
        },
        Case {
            name: "selling_through_a_bid_already_at_the_bottom_stops_at_the_bottom",
            input: FlattenInput {
                top: top(TICK, 2 * TICK),
                ..holding(10 * FIXED_SCALE)
            },
            expect: Expect::Place {
                price: Price::from_mantissa(TICK),
                qty: Qty::from_mantissa(10 * FIXED_SCALE),
            },
        },
        Case {
            name: "with_no_price_ceiling_the_slack_applies_in_full",
            input: FlattenInput {
                grid: TickGrid {
                    max_price: None,
                    ..poly_grid()
                },
                top: top(MAX_PRICE.mantissa() - TICK, MAX_PRICE.mantissa()),
                fee_model: FeeModel::None,
                ..holding(-10 * FIXED_SCALE)
            },
            expect: Expect::Place {
                price: Price::from_mantissa(MAX_PRICE.mantissa() + SLACK_TICKS as i64 * TICK),
                qty: Qty::from_mantissa(10 * FIXED_SCALE),
            },
        },
        Case {
            name: "one_step_under_the_minimum_is_refused_rather_than_rounded",
            input: holding(MIN_SHARES.mantissa() - SHARE_STEP),
            expect: Expect::Refuse(RejectReason::QtyBelowMin),
        },
        Case {
            name: "the_minimum_itself_still_trades",
            input: holding(MIN_SHARES.mantissa()),
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
    let hundred = Qty::from_mantissa(100 * FIXED_SCALE);
    let even_money = Price::from_mantissa(FIXED_SCALE / 2);
    assert_eq!(
        FeeModel::BinaryOutcome.taker_fee_quote(even_money, hundred, FEE_RATE),
        Notional::from_mantissa(175 * FIXED_SCALE / 100),
        "100 shares at 0.50 costs 1.75"
    );
    for price in [
        Price::from_mantissa(FIXED_SCALE / 10),
        Price::from_mantissa(9 * FIXED_SCALE / 10),
    ] {
        assert_eq!(
            FeeModel::BinaryOutcome.taker_fee_quote(price, hundred, FEE_RATE),
            Notional::from_mantissa(63 * FIXED_SCALE / 100),
            "and 0.63 at either 0.10 or 0.90 — the curve is symmetric about even money"
        );
    }
    assert_eq!(
        FeeModel::BinaryOutcome.taker_fee_quote(even_money, hundred, 0),
        Notional::from_mantissa(0),
        "a rate of nothing charges nothing"
    );
    // The curve is only meaningful between the bounds. Beyond them it must answer zero rather than
    // go negative: a negative fee hands a buy MORE headroom the further out of range the price is.
    for outside in [
        Price::from_mantissa(0),
        Price::from_mantissa(FIXED_SCALE),
        Price::from_mantissa(100_000 * FIXED_SCALE),
    ] {
        assert_eq!(
            FeeModel::BinaryOutcome.taker_fee_quote(outside, hundred, FEE_RATE),
            Notional::from_mantissa(0)
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
    let price = Price::from_mantissa(ASK + SLACK_TICKS as i64 * TICK);
    let budget = price.notional(Qty::from_mantissa(10 * FIXED_SCALE));
    let short_of_ten = FlattenInput {
        funds: FundsView {
            spendable: budget.mantissa(),
            floor: 0,
        },
        ..holding(-10 * FIXED_SCALE)
    };

    let with_fee = placed(plan_flatten(short_of_ten)).qty;
    assert!(
        with_fee < Qty::from_mantissa(10 * FIXED_SCALE),
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
        Qty::from_mantissa(10 * FIXED_SCALE),
        "and the identical budget covers all ten when nothing is charged on top"
    );
    assert!(
        with_fee < free_of_charge,
        "a headroom that made no difference to the size is a headroom that is not being reserved"
    );
}
