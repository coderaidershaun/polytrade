//! Monitor projection fitness: the pure map from the ordered UI event lane to the center
//! panel's state. The quote summary reads unsigned tick distances from the book mid at both mid
//! parities and blanks each cell independently; the feature store folds latest-per-(instrument,
//! feature) in catalog order, separating a genuine value change from a re-emission and a stale feed
//! from a legitimate zero; the bounded channel histories evict oldest-first and iterate newest-first
//! with a monotonic appended total behind the unseen count; the System channel synthesizes rows from
//! book-state transitions, rotations, gap counts and lifecycle notes. All of it is event-time
//! only, so replay reproduces the state exactly. The formatters render the time, delta and
//! feature-value conventions the panel paints.

use proptest::prelude::*;

use polysim::config::ExecutionMode;
use polysim::hot::exec::{CloseReason, ExecHalt, HaltReason, OrderState, RejectOrigin};
use polysim::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side};
use polysim::msg::exec::RejectClass;
use polysim::msg::ui::{
    DomQuote, UI_ORDER_SNAPSHOT_CAPACITY, UiBookSnapshot, UiBookState, UiCatalog, UiEvent,
    UiInstrument, UiWorkingOrder,
};
use polysim::time::{DurationUs, TsUs};
use polysim::workstation::format::{MISSING, write_bps_delta, write_opt_bps_delta};
use polysim::workstation::model::UiModel;
use polysim::workstation::monitor::model::{FeatureCell, OrderEvent, SystemEvent};
use polysim::workstation::monitor::view::{
    Channel, FeatureRowView, MonitorUiState, account, feature_rows, quote_summary,
};

use crate::workstation_support::{BookFixture, NamedCase, book_snapshot};

/// One-mantissa tick, so a price reads directly as its tick index in the assertions.
const TICK: Price = Price(1);

/// 100 ms spin: the DOM liveness threshold is 2.5 spins = 250 ms, the changed window 2 spins =
/// 200 ms, the stale threshold 5 spins = 500 ms.
const SPIN: DurationUs = DurationUs::from_micros(100_000);

fn ui_model(instruments: usize, features: usize) -> UiModel {
    UiModel::with_monitor_capacity(instruments, features, SPIN)
}

fn valid_book(instrument: u16, seq: u64, event_ts: i64, bid: i64, ask: i64) -> UiBookSnapshot {
    book_snapshot(BookFixture {
        instrument,
        seq,
        event_ts,
        state: UiBookState::Valid,
        bids: &[(bid, 1)],
        asks: &[(ask, 1)],
    })
}

fn quote_event(
    instrument: u16,
    seq: u64,
    event_ts: i64,
    bid: Option<(i64, i64)>,
    ask: Option<(i64, i64)>,
) -> UiEvent {
    let leg = |value: Option<(i64, i64)>| value.map(|(p, q)| (Price(p), Qty(q)));
    UiEvent::Quote {
        instrument: InstrumentId(instrument),
        seq,
        event_ts_us: TsUs::from_micros(event_ts),
        quote: DomQuote::top(leg(bid), leg(ask)),
    }
}

fn feature_event(instrument: u16, seq: u64, event_ts: i64, feature: u16, value: f64) -> UiEvent {
    UiEvent::Feature {
        instrument: InstrumentId(instrument),
        seq,
        event_ts_us: TsUs::from_micros(event_ts),
        feature: polysim::msg::persist::FeatureId(feature),
        value,
    }
}

fn trade_event(instrument: u16, seq: u64, event_ts: i64, qty: i64) -> UiEvent {
    UiEvent::Trade {
        instrument: InstrumentId(instrument),
        seq,
        event_ts_us: TsUs::from_micros(event_ts),
        aggressor: Side::Buy,
        price: Price(100),
        qty: Qty(qty),
    }
}

fn feature(model: &UiModel, instrument: u16, feature: u16) -> Option<FeatureCell> {
    model.monitor().feature(
        InstrumentId(instrument),
        polysim::msg::persist::FeatureId(feature),
    )
}

fn rows(model: &UiModel, instrument: u16) -> Vec<FeatureRowView> {
    feature_rows(model, InstrumentId(instrument)).collect()
}

#[test]
fn quote_summary_behavior() {
    // The book's best bid and ask are 100 and 102, so the mid is tick 101 (202 half-ticks). The
    // strategy bids at tick 99, two ticks below the mid, and asks at tick 104, three ticks above it.
    let mut model = ui_model(1, 0);
    model.apply_event(quote_event(0, 0, 1_000, Some((99, 5)), Some((104, 5))));
    model.apply_book(valid_book(0, 0, 1_050, 100, 102));
    let summary = quote_summary(&model, InstrumentId(0), TICK);
    assert_eq!(
        summary.mid_half_ticks,
        Some(202),
        "even mid: mid_half_ticks"
    );
    assert_eq!(
        summary.bid_delta_half_ticks,
        Some(4),
        "even mid: bid at tick 99 is 4 half-ticks (2 ticks) from the mid, unsigned"
    );
    assert_eq!(summary.ask_delta_half_ticks, Some(6), "even mid: ask delta");

    // No book yet: every cell is blank.
    let mut blanking = ui_model(1, 0);
    blanking.apply_event(quote_event(0, 0, 1_000, Some((99, 5)), Some((104, 5))));
    let none = quote_summary(&blanking, InstrumentId(0), TICK);
    assert_eq!(none.mid_half_ticks, None, "no book: mid");
    assert_eq!(none.bid_delta_half_ticks, None, "no book: bid delta");
    assert_eq!(none.ask_delta_half_ticks, None, "no book: ask delta");

    // A stale quote (book 400 ms past it) keeps the mid but blanks both deltas.
    blanking.apply_book(valid_book(0, 0, 1_000 + 400_000, 100, 102));
    let stale = quote_summary(&blanking, InstrumentId(0), TICK);
    assert_eq!(
        stale.mid_half_ticks,
        Some(202),
        "stale quote: the book still yields a mid"
    );
    assert_eq!(
        stale.bid_delta_half_ticks, None,
        "stale quote: a stale quote blanks its deltas"
    );
    assert_eq!(stale.ask_delta_half_ticks, None, "stale quote: ask delta");

    // A live one-legged quote: only the present leg carries a delta.
    blanking.apply_event(quote_event(0, 1, 1_500_000, Some((99, 5)), None));
    blanking.apply_book(valid_book(0, 1, 1_550_000, 100, 102));
    let one_legged = quote_summary(&blanking, InstrumentId(0), TICK);
    assert_eq!(
        one_legged.bid_delta_half_ticks,
        Some(4),
        "one-legged quote: bid delta present"
    );
    assert_eq!(
        one_legged.ask_delta_half_ticks, None,
        "one-legged quote: no ask leg, no delta"
    );
}

#[test]
fn feature_fold_distinguishes_change_from_reemission() {
    let mut model = ui_model(1, 3);
    model.apply_event(feature_event(0, 0, 1_000, 0, 1.0));
    model.apply_event(feature_event(0, 1, 2_000, 0, 2.0));

    let cell = feature(&model, 0, 0).expect("value change: feature seen");
    assert_eq!(cell.value, 2.0, "value change: the later value wins");
    assert_eq!(
        cell.last_update_ts,
        TsUs::from_micros(2_000),
        "value change: update ts"
    );
    assert_eq!(
        cell.last_changed_ts,
        TsUs::from_micros(2_000),
        "value change: changed ts"
    );

    model.apply_event(feature_event(0, 2, 3_000, 0, 2.0));
    let cell = feature(&model, 0, 0).expect("reemission: feature seen");
    assert_eq!(
        cell.last_update_ts,
        TsUs::from_micros(3_000),
        "reemission: a re-emission still refreshes the update time"
    );
    assert_eq!(
        cell.last_changed_ts,
        TsUs::from_micros(2_000),
        "reemission: but the change time holds — the value did not differ"
    );

    // A bit-equal NaN re-emission is not a change either, and it must not corrupt the stored value.
    let mut nan_model = ui_model(1, 1);
    nan_model.apply_event(feature_event(0, 0, 1_000, 0, f64::NAN));
    nan_model.apply_event(feature_event(0, 1, 2_000, 0, f64::NAN));

    let cell = feature(&nan_model, 0, 0).expect("nan reemission: seen");
    assert!(
        cell.value.is_nan(),
        "nan reemission: the stored value stays NaN"
    );
    assert_eq!(
        cell.last_update_ts,
        TsUs::from_micros(2_000),
        "nan reemission: a re-emitted NaN still refreshes the update time"
    );
    assert_eq!(
        cell.last_changed_ts,
        TsUs::from_micros(1_000),
        "nan reemission: bit-equal NaN re-emission is not a change — the change time holds"
    );
}

#[test]
fn feature_stale_is_distinct_from_missing_and_from_a_real_zero() {
    let mut model = ui_model(1, 3);
    // Feature 2 last updates at 1_000_000; feature 0 (a legitimate zero) refreshes 6 spins later,
    // dragging the freshest feed time past feature 2's staleness threshold. Feature 1 never emits.
    model.apply_event(feature_event(0, 0, 1_000_000, 2, 4.0));
    model.apply_event(feature_event(0, 1, 1_600_000, 0, 0.0));

    let rows = rows(&model, 0);
    assert_eq!(
        rows[0].value,
        Some(0.0),
        "a real zero is a value, not missing"
    );
    assert!(!rows[0].stale, "the fresh zero is not stale");
    assert_eq!(
        rows[1].value, None,
        "an unseen feature is missing (—), not 0"
    );
    assert!(
        !rows[1].stale,
        "a missing feature is not stale, it is absent"
    );
    assert_eq!(rows[2].value, Some(4.0));
    assert!(
        rows[2].stale,
        "feature 2 has not updated in 6 spins — stale, though its value is a real 4.0"
    );
}

/// The Trades, Orders and System channels share one shape: bounded history, evicted oldest-first,
/// read newest-first, behind a monotonic appended total. The Orders channel additionally carries
/// refusals alongside transitions — both answer the same operator question, what happened to the
/// quote I asked for, and a refusal kept elsewhere is a refusal nobody sees.
#[test]
fn channel_projections_cases() {
    fn trade_tape_evicts_oldest_and_iterates_newest_first(name: &str) {
        let mut model = ui_model(1, 0);
        for i in 0..300u64 {
            model.apply_event(trade_event(0, i, 10_000 + i as i64, i as i64));
        }
        assert_eq!(
            model.monitor().trades_appended(InstrumentId(0)),
            300,
            "{name}: the appended total counts every print, evicted or not"
        );
        let tape: Vec<_> = model.monitor().trades(InstrumentId(0)).collect();
        assert_eq!(
            tape.len(),
            256,
            "{name}: the tape retains its bounded capacity"
        );
        assert_eq!(tape[0].qty, Qty(299), "{name}: newest print is first");
        assert_eq!(
            tape[255].qty,
            Qty(44),
            "{name}: the oldest 44 prints evicted; 44 is the oldest retained"
        );
    }

    fn orders_channel_carries_transitions_and_refusals(name: &str) {
        let mut model = ui_model(1, 0);
        model.apply_event(UiEvent::OrderUpdate {
            instrument: InstrumentId(0),
            seq: 0,
            event_ts_us: TsUs::from_micros(5),
            client_id: ClientOrderId(41),
            quote_level: None,
            side: Side::Buy,
            state: OrderState::Closed(CloseReason::Canceled),
            price: Price(100),
            qty: Qty(5),
            filled: Qty(0),
        });
        model.apply_event(UiEvent::Reject {
            instrument: InstrumentId(0),
            seq: 1,
            event_ts_us: TsUs::from_micros(10),
            side: Side::Sell,
            origin: RejectOrigin::Venue {
                class: RejectClass::Gone,
                code: -2010,
            },
        });

        assert_eq!(
            model.monitor().orders_appended(),
            2,
            "{name}: appended count"
        );
        let orders: Vec<_> = model.monitor().orders().collect();
        assert_eq!(orders.len(), 2, "{name}: both land in the one channel");
        assert_eq!(
            orders[0].event,
            OrderEvent::Refused {
                origin: RejectOrigin::Venue {
                    class: RejectClass::Gone,
                    code: -2010,
                }
            },
            "{name}: the refusal carries the venue's own code and class, not a re-derived one"
        );
        let OrderEvent::Transition { client_id, .. } = orders[1].event else {
            panic!(
                "{name}: an order update records a transition, got {:?}",
                orders[1]
            );
        };
        assert_eq!(client_id, ClientOrderId(41), "{name}: transition client id");
    }

    fn system_rows_synthesize_rotation_and_gap_notes(name: &str) {
        let mut model = ui_model(1, 0);
        // A rotation event.
        model.apply_event(UiEvent::Rotation {
            instrument: InstrumentId(0),
            seq: 0,
            event_ts_us: TsUs::from_micros(500),
        });
        // An event-lane gap: the sequence jumps from 0 to 2, dropping one.
        model.apply_event(trade_event(0, 2, 900, 1));
        // A book-lane gap: the book sequence jumps from 0 to 2, dropping one. Both books are valid,
        // so no reset or rebuild row appears.
        model.apply_book(valid_book(0, 0, 1_000, 100, 102));
        model.apply_book(valid_book(0, 2, 1_100, 100, 102));

        let events: Vec<SystemEvent> = model.monitor().system().map(|r| r.event.clone()).collect();
        assert!(
            events.contains(&SystemEvent::Rotation {
                instrument: InstrumentId(0)
            }),
            "{name}: the rotation is recorded"
        );
        assert!(
            events.contains(&SystemEvent::EventsLost { count: 1 }),
            "{name}: the one dropped event is noted, saw {events:?}"
        );
        assert!(
            events.contains(&SystemEvent::BooksLost { count: 1 }),
            "{name}: the one dropped snapshot is noted"
        );
    }

    let cases: &[NamedCase] = &[
        (
            "trade tape evicts oldest and iterates newest first",
            trade_tape_evicts_oldest_and_iterates_newest_first,
        ),
        (
            "the orders channel carries transitions and refusals",
            orders_channel_carries_transitions_and_refusals,
        ),
        (
            "system rows synthesize rotation and gap notes",
            system_rows_synthesize_rotation_and_gap_notes,
        ),
    ];
    for (name, case) in cases {
        case(name);
    }
}

/// BTC at 118000 on a 0.01 tick: `2 * 118000 / 0.01`. One tick is 0.00085 bp here, which is why the
/// delta cell cannot render at a fixed two places.
const BTC_MID_HALF_TICKS: i64 = 23_600_000;

#[test]
fn a_bps_delta_without_a_usable_ratio_is_missing() {
    let mut buffer = String::new();
    write_bps_delta(&mut buffer, 4, 0);
    assert_eq!(buffer, MISSING, "a non-positive mid forms no ratio");
    write_bps_delta(&mut buffer, 4, -1);
    assert_eq!(buffer, MISSING);
    write_bps_delta(&mut buffer, -4, BTC_MID_HALF_TICKS);
    assert_eq!(
        buffer, MISSING,
        "a distance is unsigned here; a signed bps convention would be an invention"
    );

    write_opt_bps_delta(&mut buffer, None, Some(BTC_MID_HALF_TICKS));
    assert_eq!(buffer, MISSING);
    write_opt_bps_delta(&mut buffer, Some(4), None);
    assert_eq!(buffer, MISSING);
    write_opt_bps_delta(&mut buffer, Some(4), Some(BTC_MID_HALF_TICKS));
    assert_eq!(buffer, "0.0017", "both present renders the value");
}

/// The delta column's width in characters at the minimum window size.
const DELTA_CELL_CHARS: usize = 9;

proptest! {
    /// FITNESS: the delta cell is nine characters wide at the minimum window size. A wider string
    /// does not truncate — it paints over the neighbouring cell, so the number the operator reads
    /// belongs to no column at all.
    #[test]
    fn a_bps_delta_never_exceeds_the_cell_width(
        delta in 0i64..=i64::MAX,
        mid in 1i64..=i64::MAX,
    ) {
        let mut buffer = String::new();
        write_bps_delta(&mut buffer, delta, mid);
        prop_assert!(
            buffer.chars().count() <= DELTA_CELL_CHARS,
            "{buffer} is wider than the cell"
        );
    }
}

proptest! {
    /// FITNESS: a real distance never renders as a run of zeros. `0.00` on a quote two ticks off the
    /// mid says the quote is AT the mid, which is the one thing the cell exists to disprove — and
    /// the tick sizes that provoke it are ordinary, not extreme.
    #[test]
    fn a_non_zero_bps_delta_never_renders_as_all_zeros(
        delta in 1i64..=i64::MAX,
        mid in 1i64..=i64::MAX,
    ) {
        let mut buffer = String::new();
        write_bps_delta(&mut buffer, delta, mid);
        prop_assert!(
            buffer.bytes().any(|byte| byte.is_ascii_digit() && byte != b'0'),
            "a distance of {delta} half-ticks rendered as {buffer}"
        );
    }
}

/// A one-instrument catalog whose two assets are DISTINCT ids, so a projection that read one where it
/// meant the other would show the same balance twice instead of quietly passing.
fn catalog_with_assets(base_asset: AssetId, quote_asset: AssetId) -> UiCatalog {
    UiCatalog {
        strategy_id: "fitness".into(),
        window_title: "fitness".into(),
        execution_mode: Some(ExecutionMode::Live),
        spin_interval_us: SPIN.micros() as u64,
        instruments: vec![UiInstrument {
            instrument_id: InstrumentId(0),
            display: "BTCUSDT".into(),
            base: "BTC".into(),
            quote: "USDT".into(),
            base_asset,
            quote_asset,
            tick_size: Some(Price(1)),
            lot_size: Some(Qty(1)),
            qty_scale: 100_000_000,
        }],
        feature_names: Vec::new(),
    }
}

fn balance_event(seq: u64, asset: AssetId, free: i64, locked: i64) -> UiEvent {
    UiEvent::Balance {
        asset,
        seq,
        event_ts_us: TsUs::from_micros(seq as i64),
        free,
        locked,
    }
}

fn order_event(seq: u64, id: u64, side: Side, state: OrderState) -> UiEvent {
    UiEvent::OrderUpdate {
        instrument: InstrumentId(0),
        seq,
        event_ts_us: TsUs::from_micros(seq as i64),
        client_id: ClientOrderId(id),
        quote_level: None,
        side,
        state,
        price: Price(100),
        qty: Qty(5),
        filled: Qty(0),
    }
}

fn working_order(id: u64, state: OrderState) -> UiWorkingOrder {
    UiWorkingOrder {
        client_id: ClientOrderId(id),
        quote_level: None,
        state,
        price: Price(100 + id as i64),
        qty: Qty(5),
        filled: Qty(0),
    }
}

fn order_snapshot(seq: u64, side: Side, total_working: u16, details: &[UiWorkingOrder]) -> UiEvent {
    let mut orders = [UiWorkingOrder::EMPTY; UI_ORDER_SNAPSHOT_CAPACITY];
    orders[..details.len()].copy_from_slice(details);
    UiEvent::OrderSnapshot {
        instrument: InstrumentId(0),
        seq,
        event_ts_us: TsUs::from_micros(seq as i64),
        side,
        detail_len: details.len() as u8,
        total_working,
        orders,
    }
}

/// A 0.01 tick, so the fixtures below read as venue prices on a 118000 book rather than as tick
/// counts, and the valuation arithmetic is exercised at a real scale.
const CENT_TICK: Price = Price(1_000_000);

/// Best 117999.99 / 118000.01, a mid of exactly 118000.00.
fn btc_book(seq: u64, event_ts: i64) -> UiBookSnapshot {
    valid_book(0, seq, event_ts, 11_799_999_000_000, 11_800_001_000_000)
}

#[test]
fn account_view_cases() {
    // The account band's asset rows resolve THROUGH the catalog's `base_asset`/`quote_asset`, never
    // by matching an asset name. A workstation attached over the link never sees a registry, so name
    // matching would be a second identity for the same thing — and the one number it would get wrong
    // is how much money the risk gate may spend.
    fn two_asset_balances_resolve_by_id(name: &str) {
        let mut model = ui_model(1, 0);
        model.set_catalog(catalog_with_assets(AssetId(3), AssetId(7)));
        model.apply_event(balance_event(0, AssetId(3), 4_210_000, 8_000));
        model.apply_event(balance_event(1, AssetId(7), 81_233_000_000, 946_000_000));
        // An asset the instrument does not name must not leak into either row.
        model.apply_event(balance_event(2, AssetId(9), 999, 999));

        let view = account(&model, InstrumentId(0), TICK);
        assert_eq!(view.base.label, "BTC", "{name}: base label");
        assert_eq!(view.quote.label, "USDT", "{name}: quote label");
        assert_eq!(
            view.base.balance.map(|b| (b.free, b.locked)),
            Some((4_210_000, 8_000)),
            "{name}: base balance"
        );
        assert_eq!(
            view.quote.balance.map(|b| (b.free, b.locked)),
            Some((81_233_000_000, 946_000_000)),
            "{name}: quote balance"
        );
        assert_eq!(
            view.quote.value,
            Some(82_179_000_000),
            "{name}: the quote row's value is its own total holding, free plus locked"
        );
        assert_eq!(
            view.base.value, None,
            "{name}: no book here, so there is no mid to value the base holding at"
        );
    }

    // The base holding is valued at the SAME mid the MID cell shows, so the band and the summary can
    // never disagree about what the position is worth.
    fn base_holding_values_at_the_book_mid(name: &str) {
        let mut model = ui_model(1, 0);
        model.set_catalog(catalog_with_assets(AssetId(3), AssetId(7)));
        // 0.04210000 free + 0.00008000 locked = 0.04218 BTC held.
        model.apply_event(balance_event(0, AssetId(3), 4_210_000, 8_000));
        model.apply_book(btc_book(0, 1_000));

        let view = account(&model, InstrumentId(0), CENT_TICK);
        assert_eq!(
            view.base.value,
            Some(497_724_000_000),
            "{name}: 0.04218 BTC at 118000 is 4977.24 USDT — free AND locked, because a coin held \
             against a resting order is still a coin you own"
        );
    }

    // A clamped money figure is a WRONG money figure, and the operator has no way to tell it from a
    // real one. An unrepresentable valuation takes the same road as an absent one.
    fn valuation_overflow_is_absent_not_clamped(name: &str) {
        let mut model = ui_model(1, 0);
        model.set_catalog(catalog_with_assets(AssetId(3), AssetId(7)));
        model.apply_event(balance_event(0, AssetId(3), i64::MAX / 2, i64::MAX / 2));
        model.apply_book(btc_book(0, 1_000));

        assert_eq!(
            account(&model, InstrumentId(0), CENT_TICK).base.value,
            None,
            "{name}"
        );
    }

    // A balance nobody has reported is ABSENT, never zero. "You hold nothing" and "nobody has said"
    // are different claims, and the first is a reason to stop trading while the second is a reason to
    // wait.
    fn unreported_balance_is_absent_not_zero(name: &str) {
        let mut model = ui_model(1, 0);
        model.set_catalog(catalog_with_assets(AssetId(3), AssetId(7)));
        model.apply_book(btc_book(0, 1_000));

        let view = account(&model, InstrumentId(0), CENT_TICK);
        assert_eq!(view.base.balance, None, "{name}: base balance");
        assert_eq!(view.quote.balance, None, "{name}: quote balance");
        assert_eq!(view.base.value, None, "{name}: base value");
        assert_eq!(view.quote.value, None, "{name}: quote value");
        assert_eq!(
            view.halt, None,
            "{name}: no execution frame means no claim about the gate"
        );
    }

    // Dust collapses onto one id at the edge, so its per-asset values cannot be told apart. The band
    // counts those balances rather than showing a number that would be the last dust asset's, wearing
    // every other one's name.
    fn untracked_asset_balances_are_counted_not_valued(name: &str) {
        let mut model = ui_model(1, 0);
        model.set_catalog(catalog_with_assets(AssetId(3), AssetId(7)));
        model.apply_event(balance_event(0, AssetId::UNKNOWN, 5, 0));
        model.apply_event(balance_event(1, AssetId::UNKNOWN, 11, 0));

        let view = account(&model, InstrumentId(0), TICK);
        assert_eq!(view.unknown_asset_balances, 2, "{name}: count");
        assert_eq!(
            model.exec().balance(AssetId::UNKNOWN),
            None,
            "{name}: an untracked balance holds no value, because the value it would hold is not \
             one asset's"
        );
    }

    // The halt latch travels verbatim, reason and all. An operator reading `HALTED` needs to know
    // which halt it was — a reject streak and a realised loss call for entirely different next moves.
    fn halt_latch_travels_with_its_reason(name: &str) {
        let mut model = ui_model(1, 0);
        model.set_catalog(catalog_with_assets(AssetId(3), AssetId(7)));
        model.apply_event(UiEvent::Execution {
            seq: 0,
            event_ts_us: TsUs::from_micros(10),
            halt: ExecHalt::Armed,
        });
        assert_eq!(
            account(&model, InstrumentId(0), TICK).halt,
            Some(ExecHalt::Armed),
            "{name}: armed"
        );

        let halted = ExecHalt::Halted {
            reason: HaltReason::RealisedLoss,
            halted_ts_us: TsUs::from_micros(20),
        };
        model.apply_event(UiEvent::Execution {
            seq: 1,
            event_ts_us: TsUs::from_micros(20),
            halt: halted,
        });
        assert_eq!(
            account(&model, InstrumentId(0), TICK).halt,
            Some(halted),
            "{name}: the reason travels with the latch"
        );
    }

    // ACKs and stream reports can describe the same client order more than once. Identity, not event
    // count, drives both the account band and the DOM: repeated reports replace one cell.
    fn duplicate_order_reports_do_not_inflate_the_open_count(name: &str) {
        let mut model = ui_model(1, 0);
        model.set_catalog(catalog_with_assets(AssetId(3), AssetId(7)));
        model.apply_event(order_event(0, 41, Side::Buy, OrderState::PendingNew));
        model.apply_event(order_event(1, 41, Side::Buy, OrderState::Live));
        model.apply_event(order_event(2, 41, Side::Buy, OrderState::Live));

        let view = account(&model, InstrumentId(0), TICK);
        assert_eq!(
            (view.bid.open, view.bid.in_flight, view.bid.lost),
            (1, 0, 0),
            "{name}: repeated reports of one order do not inflate the count"
        );
        assert_eq!(
            model.exec().working(InstrumentId(0), Side::Buy).len(),
            1,
            "{name}: the same client id must occupy exactly one projection cell"
        );
    }

    let cases: &[NamedCase] = &[
        (
            "two asset balances resolve by id, unrelated asset excluded",
            two_asset_balances_resolve_by_id,
        ),
        (
            "base holding values at the book mid",
            base_holding_values_at_the_book_mid,
        ),
        (
            "a valuation past i64 range is absent, not clamped",
            valuation_overflow_is_absent_not_clamped,
        ),
        (
            "an unreported balance is absent, not zero",
            unreported_balance_is_absent_not_zero,
        ),
        (
            "untracked asset balances are counted, not valued",
            untracked_asset_balances_are_counted_not_valued,
        ),
        (
            "the halt latch travels with its reason",
            halt_latch_travels_with_its_reason,
        ),
        (
            "duplicate order reports do not inflate the open count",
            duplicate_order_reports_do_not_inflate_the_open_count,
        ),
    ];
    for (name, case) in cases {
        case(name);
    }
}

/// "I hold 0.04218 BTC" and "that is worth nothing" are different claims. A holding whose mid is
/// absent must blank its VALUE while keeping its BALANCE, or the band reports a flat book as a
/// worthless position.
#[test]
fn a_missing_mid_blanks_the_base_value_but_not_the_balance() {
    let mut model = ui_model(1, 0);
    model.set_catalog(catalog_with_assets(AssetId(3), AssetId(7)));
    model.apply_event(balance_event(0, AssetId(3), 4_210_000, 8_000));

    let unpriced = account(&model, InstrumentId(0), CENT_TICK);
    assert_eq!(
        unpriced.base.balance.map(|balance| balance.free),
        Some(4_210_000),
        "the balance is known"
    );
    assert_eq!(
        unpriced.base.value, None,
        "its worth is not, and that is not 0"
    );

    // A one-sided book yields no mid either.
    model.apply_book(book_snapshot(BookFixture {
        instrument: 0,
        seq: 1,
        event_ts: 2_000,
        state: UiBookState::Valid,
        bids: &[(11_799_999_000_000, 1)],
        asks: &[],
    }));
    assert_eq!(
        account(&model, InstrumentId(0), CENT_TICK).base.value,
        None,
        "half a book is not a mid"
    );

    model.apply_book(btc_book(2, 3_000));
    assert_eq!(
        account(&model, InstrumentId(0), CENT_TICK).base.value,
        Some(497_724_000_000),
        "the value arrives with the mid, and the band's shape never moved"
    );
}

/// A gap in the shared event lane could be the terminal fill for any order already on screen. The
/// model that only logged the gap would retain the stale order as confirmed, so the next real order
/// would make the account band say `open 2` and the DOM would paint both as live. It must keep the
/// possible exposure visible while withdrawing the confirmation claim.
#[test]
fn an_event_gap_demotes_pre_gap_orders_before_counting_a_replacement() {
    let mut model = ui_model(1, 0);
    model.set_catalog(catalog_with_assets(AssetId(3), AssetId(7)));
    model.apply_event(order_event(0, 11, Side::Buy, OrderState::Live));

    // Sequence 1 is absent. It could have been order 11's terminal fill; sequence 2 is the
    // replacement the engine legitimately admitted after that fill.
    model.apply_event(order_event(2, 12, Side::Buy, OrderState::Live));

    let view = account(&model, InstrumentId(0), TICK);
    assert_eq!(
        (view.bid.open, view.bid.in_flight, view.bid.lost),
        (1, 0, 1),
        "only the post-gap update is confirmed; the stale cell is possible exposure, not `open`"
    );
    let orders = model.exec().working(InstrumentId(0), Side::Buy);
    assert_eq!(orders.len(), 2, "possible exposure stays visible");
    assert_eq!(
        orders
            .iter()
            .find(|order| order.client_id == ClientOrderId(11))
            .map(|order| order.status),
        Some(polysim::workstation::exec_model::OrderStatus::Lost)
    );
    assert_eq!(
        orders
            .iter()
            .find(|order| order.client_id == ClientOrderId(12))
            .map(|order| order.status),
        Some(polysim::workstation::exec_model::OrderStatus::Confirmed)
    );
}

#[test]
fn snapshot_healing_cases() {
    fn first_snapshot_heals_a_late_attach_at_any_embedded_sequence(name: &str) {
        let mut model = ui_model(1, 0);
        model.apply_event(order_snapshot(
            900,
            Side::Buy,
            1,
            &[working_order(41, OrderState::Live)],
        ));

        assert_eq!(model.event_gaps(), 0, "{name}: no gap");
        let working = model.exec().working(InstrumentId(0), Side::Buy);
        assert_eq!(working.len(), 1, "{name}: working len");
        assert_eq!(working[0].client_id, ClientOrderId(41), "{name}: client id");
    }

    fn lost_snapshot_cannot_clear_visible_working_exposure(name: &str) {
        let mut model = ui_model(1, 0);
        model.apply_event(order_snapshot(
            0,
            Side::Buy,
            1,
            &[working_order(41, OrderState::Live)],
        ));

        // Sequence 1 was the complete empty snapshot, but its whole datagram was lost.
        model.apply_event(balance_event(2, AssetId(3), 10, 0));
        let retained = model.exec().working(InstrumentId(0), Side::Buy);
        assert_eq!(retained.len(), 1, "{name}: retained len");
        assert_eq!(
            retained[0].client_id,
            ClientOrderId(41),
            "{name}: retained id"
        );

        model.apply_event(order_snapshot(3, Side::Buy, 0, &[]));
        assert!(
            model.exec().working(InstrumentId(0), Side::Buy).is_empty(),
            "{name}: healed empty"
        );
    }

    fn snapshot_reports_exact_working_overflow_past_detail_capacity(name: &str) {
        let mut model = ui_model(1, 0);
        let details: [UiWorkingOrder; UI_ORDER_SNAPSHOT_CAPACITY] =
            core::array::from_fn(|index| working_order(index as u64 + 1, OrderState::Live));
        model.apply_event(order_snapshot(0, Side::Buy, 12, &details));

        assert_eq!(
            model.exec().working(InstrumentId(0), Side::Buy).len(),
            UI_ORDER_SNAPSHOT_CAPACITY,
            "{name}: working len"
        );
        assert_eq!(
            model
                .exec()
                .side(InstrumentId(0), Side::Buy)
                .expect("snapshot created the side")
                .leaked(),
            4,
            "{name}: leaked"
        );
    }

    fn invalid_in_process_snapshot_cannot_erase_a_valid_side(name: &str) {
        let mut model = ui_model(1, 0);
        model.apply_event(order_snapshot(
            0,
            Side::Buy,
            1,
            &[working_order(41, OrderState::Live)],
        ));
        let duplicate = working_order(42, OrderState::Live);
        model.apply_event(order_snapshot(1, Side::Buy, 2, &[duplicate, duplicate]));

        let retained = model.exec().working(InstrumentId(0), Side::Buy);
        assert_eq!(retained.len(), 1, "{name}: retained len");
        assert_eq!(
            retained[0].client_id,
            ClientOrderId(41),
            "{name}: retained id"
        );
        assert_eq!(
            retained[0].status,
            polysim::workstation::exec_model::OrderStatus::Lost,
            "{name}: invalid authoritative input preserves possible exposure but cannot leave it \
             confirmed"
        );
        assert_eq!(
            account(&model, InstrumentId(0), TICK).bid.leaked,
            1,
            "{name}: the cut declared two working identities but only one old identity can be \
             retained"
        );
    }

    fn after_a_gap_only_the_side_with_a_complete_snapshot_heals(name: &str) {
        let mut model = ui_model(1, 0);
        model.apply_event(order_event(0, 61, Side::Buy, OrderState::Live));
        model.apply_event(order_event(1, 62, Side::Sell, OrderState::Live));

        // Sequence 2 was lost. Gap handling demotes both old sides before this complete BID cut
        // applies.
        model.apply_event(order_snapshot(
            3,
            Side::Buy,
            1,
            &[working_order(63, OrderState::Live)],
        ));
        let halfway = account(&model, InstrumentId(0), TICK);
        assert_eq!(
            (halfway.bid.open, halfway.bid.lost),
            (1, 0),
            "{name}: the current complete BID cut heals atomically"
        );
        assert_eq!(
            (halfway.ask.open, halfway.ask.lost),
            (0, 1),
            "{name}: the pre-gap ASK cannot remain confirmed until its own cut arrives"
        );

        model.apply_event(order_snapshot(4, Side::Sell, 0, &[]));
        let healed = account(&model, InstrumentId(0), TICK);
        assert_eq!(
            (healed.ask.open, healed.ask.in_flight, healed.ask.lost),
            (0, 0, 0),
            "{name}: healed"
        );
    }

    fn snapshots_and_transitions_follow_the_shared_lane_order(name: &str) {
        let mut model = ui_model(1, 0);
        model.apply_event(order_snapshot(
            0,
            Side::Buy,
            1,
            &[working_order(71, OrderState::Live)],
        ));
        model.apply_event(order_event(1, 71, Side::Buy, OrderState::CancelInFlight));
        let transitioned = account(&model, InstrumentId(0), TICK).bid;
        assert_eq!(
            (transitioned.open, transitioned.in_flight),
            (0, 1),
            "{name}: after transition"
        );

        // A later absolute cut wins over that transition.
        model.apply_event(order_snapshot(
            2,
            Side::Buy,
            1,
            &[working_order(71, OrderState::Live)],
        ));
        let snapped = account(&model, InstrumentId(0), TICK).bid;
        assert_eq!(
            (snapped.open, snapped.in_flight),
            (1, 0),
            "{name}: the later cut wins"
        );

        // A still-later terminal transition clears immediately; it need not wait for another spin.
        model.apply_event(order_event(
            3,
            71,
            Side::Buy,
            OrderState::Closed(CloseReason::Filled),
        ));
        assert_eq!(
            account(&model, InstrumentId(0), TICK).bid.open,
            0,
            "{name}: terminal transition clears immediately"
        );
        assert!(
            model.exec().working(InstrumentId(0), Side::Buy).is_empty(),
            "{name}: no longer working"
        );
    }

    let cases: &[NamedCase] = &[
        (
            "first snapshot heals a late attach at any embedded sequence",
            first_snapshot_heals_a_late_attach_at_any_embedded_sequence,
        ),
        (
            "a lost snapshot cannot clear visible working exposure",
            lost_snapshot_cannot_clear_visible_working_exposure,
        ),
        (
            "a snapshot reports exact working overflow past detail capacity",
            snapshot_reports_exact_working_overflow_past_detail_capacity,
        ),
        (
            "an invalid in-process snapshot cannot erase a valid side",
            invalid_in_process_snapshot_cannot_erase_a_valid_side,
        ),
        (
            "after a gap, only the side with a complete snapshot heals",
            after_a_gap_only_the_side_with_a_complete_snapshot_heals,
        ),
        (
            "snapshots and transitions follow the shared lane order",
            snapshots_and_transitions_follow_the_shared_lane_order,
        ),
    ];
    for (name, case) in cases {
        case(name);
    }
}

/// Every declared channel owns a scroll slot. The scroll state is a fixed array, and only two of the
/// three places that size it are compiler-checked when a channel is added — the third would index
/// past the end and panic on the UI thread the first time an operator clicked the new tab.
#[test]
fn every_channel_accepts_a_scroll_state() {
    let mut state = MonitorUiState::new();
    for (index, channel) in Channel::ALL.iter().enumerate() {
        state.set_scrolled_away(*channel, index as u64, index as f32);
    }
}
