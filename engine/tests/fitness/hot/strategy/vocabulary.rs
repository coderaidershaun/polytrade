//! The words a strategy is written in keep the engine's rules: exact money never comes from a
//! float, a missing input is a named reason and never a panic, the engine resets the state of each
//! market, a knob out of its bounds refuses the start, and an assigned signal is recorded.

use std::sync::{Arc, Mutex};

use engine::hot::strategy::prelude::*;
use engine::hot::strategy::{FeatureKind, ParamUnit};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::persist::{FeatureId, ResearchRecord};
use engine::runtime::Exit;
use engine::runtime::config::{Config, ConfigError, RecordedTables};
use engine::runtime::registry::InstrumentRow;
use proptest::{prop_assert, prop_assert_eq, proptest};

use crate::engine_support::{
    LinkedSetup, ONE, engine_with_link, engine_without_warmup, idle_at, instrument_row,
    partial_snapshot_chunk, persist_ring, rotation, run_control, running_at, snapshot_pair, spin,
    strategy_log_ring, trade, ts,
};
use crate::runtime::config_guards::config_with_params;

const TICK: i64 = ONE / 100;

fn row() -> InstrumentRow {
    let mut row = instrument_row(0, Default::default(), 16);
    row.tick_size = Some(Price::from_mantissa(TICK));
    row.lot_size = Some(Qty::from_mantissa(ONE / 1_000));
    row.min_qty = Some(Qty::from_mantissa(ONE / 1_000));
    row
}

fn book(bids: &[(i64, i64)], asks: &[(i64, i64)], when: i64) -> [InboundMessage; 2] {
    let (bid_chunk, ask_chunk) = snapshot_pair(0, bids, asks, when);
    [
        InboundMessage::new(ts(when), MessageBody::Book(bid_chunk)),
        InboundMessage::new(ts(when), MessageBody::Book(ask_chunk)),
    ]
}

/// The market and position of market 0 that each spin shows.
type Seen = Arc<Mutex<Vec<(Market, Position)>>>;

struct Observer {
    seen: Seen,
}

impl Strategy for Observer {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let market = InstrumentId(0);
        let view = (ctx.market(market), ctx.position(market));
        self.seen.lock().expect("one writer").push(view);
    }
}

/// The market and position at each spin of `tape`, with no warmup.
fn observe(tape: &[InboundMessage]) -> Vec<(Market, Position)> {
    let seen = Seen::default();
    let observer = Observer {
        seen: Arc::clone(&seen),
    };
    let (wiring, _rings) = persist_ring(64);
    let (log_sink, _log) = strategy_log_ring(64);
    let mut engine = engine_without_warmup(&[row()], Box::new(observer), wiring, log_sink);
    for message in tape {
        engine.dispatch(message);
    }
    seen.lock().expect("one writer").clone()
}

fn live_market() -> Market {
    let mut tape = book(&[(100 * ONE, ONE)], &[(101 * ONE, ONE)], 1_000).to_vec();
    tape.push(spin(0, 2_000));
    observe(&tape)[0].0
}

/// CONTRACT: no conversion and no operator turns a fair value into an exact price, quantity or
/// money, and the three verbs of the market round it onto the grid in the direction they name.
/// A fair value off any grid is not ready.
#[test]
fn fair_becomes_price_only_by_rounding_verb() {
    assert_lacks!(Price: From<Fair>);
    assert_lacks!(Qty: From<Fair>);
    assert_lacks!(Notional: From<Fair>);
    assert_lacks!(Price: std::ops::Add<Bps>);
    assert_lacks!(Price: std::ops::Sub<Bps>);
    assert_lacks!(Price: std::ops::Add<Ticks>);
    assert_lacks!(Fair: std::ops::Add<Fair>);

    let market = live_market();
    let fair = Fair::new(100.007);
    let (down, up) = (
        Price::from_mantissa(100 * ONE),
        Price::from_mantissa(100 * ONE + TICK),
    );
    assert_eq!(market.price_at_or_below(fair), Ok(down));
    assert_eq!(market.price_at_or_above(Fair::new(100.003)), Ok(up));
    assert_eq!(market.nearest_price(fair), Ok(up));
    assert_eq!(market.nearest_price(Fair::new(100.003)), Ok(down));

    // An exact price of the grid is a fair value with no error, so each verb gives it back: 0.29
    // and 0.07 are the prices whose float quotient by 0.01 falls just below and just above a whole
    // number of ticks.
    let on_grid = [29 * TICK, 7 * TICK]
        .into_iter()
        .chain((1..=10_000).map(|ticks| ticks * TICK))
        .chain([64_680 * ONE + TICK, 99_999_999 * TICK]);
    for mantissa in on_grid {
        let price = Price::from_mantissa(mantissa);
        let fair = Fair::from(price);
        assert_eq!(
            market.price_at_or_below(fair),
            Ok(price),
            "{price:?} at or below"
        );
        assert_eq!(
            market.price_at_or_above(fair),
            Ok(price),
            "{price:?} at or above"
        );
        assert_eq!(market.nearest_price(fair), Ok(price), "{price:?} nearest");
    }

    let below = Fair::new(100.0) - Bps::new(10.0);
    let above = Fair::new(100.0) + Bps::new(10.0);
    assert!((below.value() - 99.9).abs() < 1e-9, "{below:?}");
    assert!((above.value() - 100.1).abs() < 1e-9, "{above:?}");
    assert_eq!(Bps::new(2.0) + Bps::new(3.0), Bps::new(5.0));
    assert_eq!(Bps::new(2.0) * 1.5, Bps::new(3.0));
    assert_eq!(
        Fair::from(Price::from_mantissa(100 * ONE)),
        Fair::new(100.0)
    );

    for off in [f64::NAN, f64::INFINITY, -1.0, 0.0] {
        assert_eq!(
            market.price_at_or_below(Fair::new(off)),
            Err(NotReady::OffGrid),
            "{off}"
        );
    }
}

/// CONTRACT: an exact type has no conversion from a float, and an exact knob reads the digits that
/// the config wrote: a value with more places than the mantissa holds is refused, never rounded.
#[test]
fn float_never_becomes_exact_type() {
    assert_lacks!(Price: From<f64>);
    assert_lacks!(Qty: From<f64>);
    assert_lacks!(Notional: From<f64>);
    assert_lacks!(Price: TryFrom<f64>);
    assert_lacks!(Qty: TryFrom<f64>);
    assert_lacks!(Notional: TryFrom<f64>);
    assert_lacks!(Ticks: From<f64>);
    assert_lacks!(Price: std::ops::Mul<f64>);
    assert_lacks!(Qty: std::ops::Mul<f64>);
    assert_lacks!(Notional: std::ops::Mul<f64>);

    assert_eq!(
        Price::from_mantissa(2 * ONE) * Qty::from_mantissa(3 * ONE),
        Notional::from_mantissa(6 * ONE)
    );
    assert_eq!(
        Qty::from_mantissa(2) + Qty::from_mantissa(3) - Qty::from_mantissa(1),
        Qty::from_mantissa(4)
    );

    let read = |text: &str| {
        Config::<SizeParams>::from_yaml(&config_with_params(&format!("    order_size: {text}\n")))
            .map(|config| config.strategy.params.order_size)
    };
    assert_eq!(
        read("0.30000001").ok(),
        Some(Qty::from_mantissa(30_000_001))
    );
    assert_eq!(read("0.1").ok(), Some(Qty::from_mantissa(10_000_000)));
    for refused in ["0.123456789", "1e3", ".nan", "-1"] {
        assert!(read(refused).is_err(), "{refused} must be refused");
    }
}

params! {
    struct SizeParams {
        order_size: Qty = 1,
    }
}

/// CONTRACT: each input that a market lacks gives its own reason: a book that is not whole, a book
/// with an empty side, a market that warms up. A market with a window closes within the span that
/// reaches its close, and one with no window never closes. No case panics.
#[test]
fn invalid_book_gives_named_reason() {
    let mut tape = vec![spin(0, 1_000)];
    tape.push(InboundMessage::new(
        ts(2_000),
        MessageBody::Book(partial_snapshot_chunk(
            0,
            Side::Buy,
            &[(100 * ONE, ONE)],
            2_000,
        )),
    ));
    tape.push(spin(1, 3_000));
    tape.extend(book(&[(100 * ONE, ONE)], &[], 4_000));
    tape.push(spin(2, 5_000));
    tape.extend(book(&[], &[(101 * ONE, ONE)], 6_000));
    tape.push(spin(3, 7_000));
    tape.extend(book(&[(100 * ONE, ONE)], &[(101 * ONE, ONE)], 8_000));
    tape.push(spin(4, 9_000));
    let touches: Vec<Ready<Touch>> = observe(&tape)
        .iter()
        .map(|(market, _)| market.touch())
        .collect();
    assert_eq!(
        touches,
        vec![
            Err(NotReady::BookInvalid),
            Err(NotReady::BookInvalid),
            Err(NotReady::NoAsk),
            Err(NotReady::NoBid),
            Ok(Touch {
                bid: Price::from_mantissa(100 * ONE),
                ask: Price::from_mantissa(101 * ONE),
            }),
        ]
    );
    assert!(!live_market().closes_within(DurationUs::from_secs(3_600)));

    let mut windowed = vec![rotation(0, 0, 10_000_000, 0)];
    windowed.extend(book(&[(100 * ONE, ONE)], &[(101 * ONE, ONE)], 1_000));
    windowed.push(spin(0, 8_000_000));
    let (market, _) = observe(&windowed)[0];
    assert!(market.closes_within(DurationUs::from_secs(3)));
    assert!(!market.closes_within(DurationUs::from_secs(1)));

    let seen = Seen::default();
    let observer = Observer {
        seen: Arc::clone(&seen),
    };
    let mut warming = engine_with_link(LinkedSetup {
        instruments: &[row()],
        strategy: Box::new(observer),
        tables: RecordedTables::new(&[]),
        warmup: DurationUs::from_secs(60),
    });
    for message in book(&[(100 * ONE, ONE)], &[(101 * ONE, ONE)], 1_000) {
        warming.engine.dispatch(&message);
    }
    warming.engine.dispatch(&spin(0, 2_000));
    let warm = seen.lock().expect("one writer").clone();
    assert!(
        warm.iter()
            .all(|(market, _)| market.touch() == Err(NotReady::Warming)),
        "a warming market gives no touch: {warm:?}"
    );
}

/// CONTRACT: before the market has a mark, the exposure of a position is not ready, so it never
/// reads as flat; once the book gives a mark, a flat position has an exposure of zero and nothing
/// to sell.
#[test]
fn exposure_with_no_mark_is_not_ready() {
    let mut tape = vec![spin(0, 1_000)];
    tape.extend(book(&[(100 * ONE, ONE)], &[(101 * ONE, ONE)], 2_000));
    tape.push(spin(1, 3_000));
    let views = observe(&tape);
    let (before, after) = (views[0].1, views[1].1);
    assert_eq!(before.exposure(), Err(NotReady::NoMark));
    assert_eq!(after.exposure(), Ok(Notional::from_mantissa(0)));
    assert!(after.is_flat() && !after.is_long() && !after.is_short());
    assert_eq!(after.size(), Qty::from_mantissa(0));
    assert_eq!(after.sellable(), Err(NotReady::BelowMinimum));
}

/// Counts the trades of its market, and writes down the count that each callback reads.
#[derive(Default)]
struct Trades {
    count: u32,
}

struct Counter {
    markets: PerMarket<Trades>,
    read: Arc<Mutex<Vec<(&'static str, u32)>>>,
}

impl Counter {
    fn write_down(&self, callback: &'static str, market: &Market) {
        let count = self.markets.of(market).count;
        self.read
            .lock()
            .expect("one writer")
            .push((callback, count));
    }
}

impl Strategy for Counter {
    fn market_states(&mut self) -> Option<&mut dyn MarketStates> {
        Some(&mut self.markets)
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        let market = ctx.market(event.instrument);
        self.write_down("trade", &market);
        self.markets.of_mut(&market).count += 1;
    }

    fn on_resume(&mut self, ctx: &mut StrategyCtx<'_>) {
        self.write_down("resume", &ctx.market(InstrumentId(0)));
    }

    fn on_market_rotation(&mut self, ctx: &mut StrategyCtx<'_>, rotation: &MarketRotation) {
        self.write_down("rotation", &ctx.market(rotation.instrument));
    }
}

/// FITNESS (A1): the state that a strategy keeps for a market is the default when the next
/// callback reads it after each resume and each rotation of that market, and the strategy holds
/// no reset code. A count carried over a park would describe trades the run never saw.
#[test]
fn engine_resets_market_state_before_next_callback() {
    let read = Arc::new(Mutex::new(Vec::new()));
    let counter = Counter {
        markets: PerMarket::new(),
        read: Arc::clone(&read),
    };
    let mut linked = engine_with_link(LinkedSetup {
        instruments: &[row()],
        strategy: Box::new(counter),
        tables: RecordedTables::new(&[]),
        warmup: DurationUs::ZERO,
    });
    let print = |when| {
        InboundMessage::new(
            ts(when),
            MessageBody::Trade(trade(0, 100 * ONE, ONE, Side::Buy, when)),
        )
    };
    let tape = [
        print(1_000),
        print(2_000),
        print(3_000),
        run_control(idle_at(1), 4_000),
        run_control(running_at(2), 5_000),
        print(6_000),
        rotation(0, 7_000, 300_000_000, 7_000),
        print(8_000),
    ];
    for message in &tape {
        linked.engine.dispatch(message);
    }
    assert_eq!(
        *read.lock().expect("one writer"),
        vec![
            ("trade", 0),
            ("trade", 1),
            ("trade", 2),
            ("resume", 0),
            ("trade", 0),
            ("rotation", 0),
            ("trade", 0),
        ]
    );
}

params! {
    struct NotionalParams {
        order_notional: Notional = 10 within 1 ..= 1000,
    }
}

fn notional_refusal(text: &str) -> ConfigError {
    let document = config_with_params(&format!("    order_notional: {text}\n"));
    match Config::<NotionalParams>::from_yaml(&document) {
        Ok(config) => panic!(
            "order_notional {text} must refuse the start, read {:?}",
            config.strategy.params
        ),
        Err(error) => error,
    }
}

/// The named cases of A2: zero, which the bounds refuse, and each value that is no number.
fn named_refusals() {
    let zero = notional_refusal("0");
    assert!(
        matches!(
            &zero,
            ConfigError::Param(ParamError::OutOfBounds {
                field: "order_notional",
                seen: ParamValue::Exact(0),
                low: ParamValue::Exact(low),
                high: ParamValue::Exact(high),
            }) if *low == ONE && *high == 1_000 * ONE
        ),
        "{zero}"
    );
    assert_eq!(
        zero.to_string(),
        "strategy.params.order_notional is 0, expected 1 to 1000"
    );
    assert_eq!(Exit::refused(zero.failure_class()).code(), 1);
    for word in [".nan", ".inf", "-5", "ten"] {
        notional_refusal(word);
    }
    assert_eq!(
        NotionalParams::default().order_notional,
        Notional::from_mantissa(10 * ONE)
    );
    assert_eq!(
        Notional::EXPECTED,
        "a decimal of quote money with at most 8 places"
    );
}

proptest! {
    /// FITNESS (A2): a knob outside the bounds that `params!` declares refuses the start, names
    /// the knob, the value and the bounds, and exits with the code of a refused start; a value that
    /// is no number refuses too. None panics, and each value inside the bounds is read exactly.
    #[test]
    fn param_out_of_bounds_refuses_start(mantissa in 0i64..=10_000 * ONE) {
        named_refusals();
        let text = format!("{}.{:08}", mantissa / ONE, mantissa % ONE);
        let document = config_with_params(&format!("    order_notional: {text}\n"));
        let read = Config::<NotionalParams>::from_yaml(&document);
        let inside = (ONE..=1_000 * ONE).contains(&mantissa);
        match read {
            Ok(config) => {
                prop_assert!(inside, "{text} read although out of bounds");
                prop_assert_eq!(
                    config.strategy.params.order_notional,
                    Notional::from_mantissa(mantissa)
                );
            }
            Err(error) => {
                prop_assert!(!inside, "{text} refused although inside: {error}");
                let is_bound_refusal =
                    matches!(error, ConfigError::Param(ParamError::OutOfBounds { .. }));
                prop_assert!(is_bound_refusal);
            }
        }
    }
}

signals! {
    const SIGNALS;
    market_only struct Micro {
        mid: Fair,
        spread: Bps,
    }
    execution_aware struct Model {
        bid_price: Price,
    }
}

struct Signaller {
    unassigned: Arc<Mutex<Vec<Ready<Bps>>>>,
}

impl Strategy for Signaller {
    fn features(&self) -> &'static [&'static str] {
        SIGNALS.names
    }

    fn feature_kinds(&self) -> &'static [FeatureKind] {
        SIGNALS.kinds
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let market = ctx.market(InstrumentId(0));
        let mut see = Micro::default();
        self.unassigned.lock().expect("one writer").push(see.spread);
        see.mid = Ok(Fair::new(100.5));
        ctx.record(&market, &see);
        let model = Model {
            bid_price: market.price_at_or_below(Fair::new(100.004)),
        };
        ctx.record(&market, &model);
    }
}

/// CONTRACT: assigning a signal and recording its struct writes one row in the column of its name,
/// with its value; a signal that was never assigned is not ready, names itself, and writes no row.
/// The list of the signals gives the engine the names and the kinds in one order.
#[test]
fn assigned_signal_is_recorded() {
    assert_eq!(SIGNALS.names, ["mid", "spread", "bid_price"]);
    assert_eq!(
        SIGNALS.kinds,
        [
            FeatureKind::MarketOnly,
            FeatureKind::MarketOnly,
            FeatureKind::ExecutionAware
        ]
    );
    let unassigned = Arc::new(Mutex::new(Vec::new()));
    let signaller = Signaller {
        unassigned: Arc::clone(&unassigned),
    };
    let (wiring, mut rings) = persist_ring(64);
    let (log_sink, _log) = strategy_log_ring(64);
    let mut engine = engine_without_warmup(&[row()], Box::new(signaller), wiring, log_sink);
    let mut tape = book(&[(100 * ONE, ONE)], &[(101 * ONE, ONE)], 1_000).to_vec();
    tape.push(spin(0, 2_000));
    for message in &tape {
        engine.dispatch(message);
    }
    let rows: Vec<(FeatureId, f64)> = rings
        .research()
        .into_iter()
        .filter_map(|record| match record {
            ResearchRecord::Feature(row) => Some((row.feature, row.value)),
            _ => None,
        })
        .collect();
    assert_eq!(rows, vec![(FeatureId(0), 100.5), (FeatureId(2), 100.0)]);
    assert_eq!(
        *unassigned.lock().expect("one writer"),
        vec![Err(NotReady::Signal("spread"))]
    );
}
