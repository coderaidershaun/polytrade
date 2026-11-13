//! A position outlives the process that opened it: the file that keeps it, the start that refuses a
//! file it cannot trust, and the writer that stops quoting when it can no longer keep the file.

use engine::hot::dispatch::{ExecWiring, ExposureWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{DesiredQuote, ExecSettings, QuoteLevel};
use engine::hot::sink::{ExecSink, ExposureSink};
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::msg::exec::{ExecCommand, ExecEvent, ExecKind, ExecLaneItem};
use engine::msg::failure::{FailureClass, Halt};
use engine::msg::ids::{ClientOrderId, FIXED_SCALE, InstrumentId, Notional, Qty, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::time::Stamper;
use engine::runtime::StartError;
use engine::runtime::config::{ExecutionMode, TrackerSpec};
use engine::runtime::exposure::{
    ExposureError, ExposureSnapshot, ExposureState, InstrumentExposure, MAX_EXPOSURE_INSTRUMENTS,
    file_path,
};
use engine::runtime::registry::{InstrumentRow, Registry};
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    FillPen, ONE, book_reset, detached_setup, exec_event, exposure_ring, instrument_row,
    persist_ring, snapshot_pair, ts,
};
use crate::exec_support::{drain_commands, exec_settings, make_ready, spin_at};
use crate::parquet_readback::TempDir;
use crate::runtime::exposure_state::{
    BINANCE_SOURCE, POLY_SOURCE, drain_writer, identity, load_live, registry_for, row, snapshot,
    spawn_live_writer, write_from, write_through_the_writer,
};

const INSTRUMENT: InstrumentId = InstrumentId(0);

/// One whole base unit bought at 60,000: the shape a restart while holding inventory has.
const RESTORED_POSITION: i64 = ONE;
const RESTORED_COST: i64 = -60_000 * ONE;

/// Small enough that the restored cost basis dwarfs it by four orders of magnitude, so a gate that
/// read raw PnL would spend it before the first order.
const MAX_SESSION_LOSS: i64 = 5 * ONE;

/// FITNESS: each instrument that holds a position when a run ends holds the same position, cash and
/// cost basis when the next run starts. A position that a restart loses is traded against as if it
/// were flat, and no record shows that it was ever held: the file that would prove it is the file
/// that lost it.
#[test]
fn each_instrument_with_a_position_survives_restart() {
    a_missing_file_is_a_cold_start();
    a_written_position_reloads_as_its_cost_basis();
    a_run_that_folds_nothing_leaves_its_inherited_file_as_it_was();
    the_sections_for_a_person_are_never_read_back();
    assets_sum_over_each_instrument_that_shares_them();
    a_fill_reaches_the_file_that_the_next_start_reads();
    the_last_snapshot_leaves_the_sink_when_its_owner_ends();
    a_restored_position_with_no_mark_is_not_flat();
}

fn a_missing_file_is_a_cold_start() {
    let root = TempDir::new("exposure-cold");
    let registry = registry_for(BINANCE_SOURCE);
    let state = load_live(root.path(), &registry).expect("a first run has no past");
    assert!(state.is_empty());
    assert!(state.assets().is_empty());
    assert!(
        !file_path(root.path(), &identity(), Some(ExecutionMode::Live)).exists(),
        "reading a position must not create one"
    );
}

fn a_written_position_reloads_as_its_cost_basis() {
    let root = TempDir::new("exposure-round-trip");
    let registry = registry_for(BINANCE_SOURCE);
    // The basis is not minus the cash: this engine banked 3 USDT before it bought the position that
    // it holds. A writer or a load that derived one from the other would pass with any pair where
    // they happen to mirror.
    let rows = [row(0, 100_000, -11_800_000_000, 11_500_000_000)];
    write_through_the_writer(root.path(), &registry, &[snapshot(1, &rows)]);

    let state = load_live(root.path(), &registry).expect("the file this build wrote loads");
    assert_eq!(state.instruments(), rows);
    assert_eq!(
        asset_amounts(&registry, &state),
        [("BTC", 100_000), ("USDT", -11_800_000_000)],
        "the position and what it cost, each against its own asset"
    );
}

/// A writer that started out believing the disk empty would put that belief over a real position at
/// its first write, and the run that did it would look healthy.
fn a_run_that_folds_nothing_leaves_its_inherited_file_as_it_was() {
    let root = TempDir::new("exposure-no-erase");
    let registry = registry_for(BINANCE_SOURCE);
    let rows = [row(0, 250_000, -29_500_000_000, 30_100_000_000)];
    write_through_the_writer(root.path(), &registry, &[snapshot(1, &rows)]);
    let path = file_path(root.path(), &identity(), Some(ExecutionMode::Live));
    let after_first_run = std::fs::read_to_string(&path).expect("the first run wrote a file");

    let restored = load_live(root.path(), &registry).expect("loads");
    write_from(root.path(), &registry, &restored, &[]);

    assert_eq!(
        std::fs::read_to_string(&path).expect("the file is still there"),
        after_first_run,
        "a run that folded nothing leaves the position it inherited as it found it"
    );
    assert_eq!(
        load_live(root.path(), &registry)
            .expect("loads")
            .instruments(),
        rows
    );
}

/// The asset totals and the last exposure are written for a person. Both are wrong here on purpose:
/// if either were read back, the run would restore a position nobody held.
fn the_sections_for_a_person_are_never_read_back() {
    let root = TempDir::new("exposure-informational");
    let registry = registry_for(BINANCE_SOURCE);
    let path = file_path(root.path(), &identity(), Some(ExecutionMode::Live));
    std::fs::create_dir_all(root.path()).expect("create the exposure directory");
    let rows = r#"{ "venue": "binance", "symbol": "btcusdt", "position_base": 100000, "cash_quote": -11800000000, "basis_quote": 11800000000 }"#;
    let body = document(CURRENT_VERSION, "te-recorder", FIXED_SCALE, rows).replace(
        r#""instruments""#,
        r#""last_exposure_quote": -123456789, "assets": [{ "asset": "BTC", "amount": 999999999 }, { "asset": "DOGE", "amount": 42 }], "instruments""#,
    );
    std::fs::write(&path, body).expect("write the handmade document");

    let state = load_live(root.path(), &registry).expect("loads");
    assert_eq!(
        state.instruments(),
        [row(0, 100_000, -11_800_000_000, 11_800_000_000)]
    );
    let amounts = asset_amounts(&registry, &state);
    assert_eq!(
        amounts,
        [("BTC", 100_000), ("USDT", -11_800_000_000)],
        "the asset section is summed from the rows again, never parsed, and DOGE reaches nothing"
    );
}

/// Four markets settle against one collateral, so the collateral sums to one number over the four,
/// while the four share balances stay apart. Summing those together would report one leg's
/// inventory as another's, and the sell side spends against that number.
fn assets_sum_over_each_instrument_that_shares_them() {
    let root = TempDir::new("exposure-aggregate");
    let registry = registry_for(POLY_SOURCE);
    let rows = [
        row(0, 10, -100, 100),
        row(1, 20, -200, 200),
        row(2, 30, -300, 300),
        row(3, -5, 50, -50),
    ];
    write_through_the_writer(root.path(), &registry, &[snapshot(1, &rows)]);

    let state = load_live(root.path(), &registry).expect("loads");
    assert_eq!(state.instruments(), rows);
    assert_eq!(
        asset_amounts(&registry, &state),
        [
            ("btc-updown-5m-a-up", 10),
            ("USD", -550),
            ("btc-updown-5m-a-down", 20),
            ("btc-updown-5m-b-up", 30),
            ("btc-updown-5m-b-down", -5),
        ],
        "one collateral summed over four markets, four share balances left apart"
    );
}

/// A fill folded by the engine reaches the file that the next start reads, through the ledger, the
/// snapshot, the lane, the writer thread and the drain. The registry is the real one, because the
/// file names each instrument by its venue and its symbol.
fn a_fill_reaches_the_file_that_the_next_start_reads() {
    let root = TempDir::new("exposure-write-through");
    let registry = registry_for(BINANCE_SOURCE);
    let (writer, sink) = spawn_live_writer(root.path(), &registry, &ExposureState::default());
    let (mut engine, _commands) = engine_with_exposure(
        registry.instruments(),
        ExposureWiring {
            restored: &[],
            sink,
        },
    );

    make_ready(&mut engine, 0);
    let mut pen = FillPen::new(0);
    for message in pen.fill(Side::Buy, 60_000 * ONE, ONE / 100, 10) {
        engine.dispatch(&message);
    }
    // Half of it back out at a profit, so the position on disk has a cost that is not minus the
    // cash beside it.
    for message in pen.fill(Side::Sell, 62_000 * ONE, ONE / 200, 20) {
        engine.dispatch(&message);
    }
    // The hot thread ending: the engine drops its sink, which flushes what it still holds.
    drop(engine);
    drain_writer(writer).expect("the final write succeeds");

    let state = load_live(root.path(), &registry).expect("the run's own file loads");
    assert_eq!(
        state.instruments(),
        [InstrumentExposure {
            instrument: INSTRUMENT,
            position_base: Qty::from_mantissa(ONE / 200),
            cash_quote: -290 * ONE,
            basis_quote: 300 * ONE,
        }],
        "0.005 base still held cost 300; the cash is that less the 10 banked selling the other half"
    );
}

/// The snapshot that a sink still holds when its owner ends is the run's final position. The ring
/// is one deep and full, so the last push has nowhere to go until the writer's next poll.
fn the_last_snapshot_leaves_the_sink_when_its_owner_ends() {
    let (producer, mut consumer) = RingBuffer::<ExposureSnapshot>::new(1);
    let mut sink = ExposureSink::new(producer);
    sink.push(final_position(1, ONE));
    sink.push(final_position(2, 2 * ONE));
    assert!(
        sink.is_pending(),
        "a one-deep ring cannot hold the second snapshot, so the sink still carries it"
    );

    // The writer's poll, which comes only after the hot side has let go of the sink.
    let reader = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(20));
        let mut seen = Vec::new();
        for _ in 0..2_000 {
            match consumer.pop() {
                Ok(snapshot) => seen.push(snapshot),
                Err(_) if seen.len() < 2 => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(_) => break,
            }
            if seen.len() == 2 {
                break;
            }
        }
        seen
    });
    drop(sink);

    let seen = reader.join().expect("the reader thread finished");
    assert_eq!(
        seen.len(),
        2,
        "both snapshots reach the writer: the one that fitted, and the one the destructor flushed"
    );
    assert_eq!(
        seen[1].active(),
        [InstrumentExposure {
            instrument: INSTRUMENT,
            position_base: Qty::from_mantissa(2 * ONE),
            cash_quote: 0,
            basis_quote: 0,
        }],
        "and the second is the newest state, which the next start restores"
    );
}

/// A position with no mark is not flat. Anything that read "no valuation" as "no position" would
/// size the next order against inventory it does not know it holds.
fn a_restored_position_with_no_mark_is_not_flat() {
    let readings = readings_of_a_restored_long_before_any_book();
    assert_eq!(
        readings,
        Readings {
            position_base: Qty::from_mantissa(RESTORED_POSITION),
            has_mark: false,
            exposure_quote: 0,
            pnl_quote: RESTORED_COST,
        },
        "the restored position is there from the first message, with no valuation, so its \
         exposure is zero and its raw PnL reads as the whole cost basis lost"
    );
}

/// FITNESS: a start refuses each exposure file that it cannot trust, by the reason it cannot, and
/// leaves the file as it was. A file of another engine, of another version, or of another fixed
/// scale holds numbers that mean something else, and a row with a position that no instrument of
/// this config names would be erased by the next write with no record that it was held. A flat row
/// of such an instrument costs nothing to drop.
#[test]
fn untrusted_exposure_file_refuses_start() {
    let registry = registry_for(BINANCE_SOURCE);
    let cases: [(&str, String, Expect); 9] = [
        (
            "malformed",
            "{ not json".to_owned(),
            Expect::Refuses(|error| matches!(error, ExposureError::Malformed { .. })),
        ),
        (
            "another engine",
            document(CURRENT_VERSION, "te-other", FIXED_SCALE, "").replace("recorder", "other"),
            Expect::Refuses(|error| matches!(error, ExposureError::WrongIdentity { .. })),
        ),
        (
            "an unknown version",
            document(99, "te-recorder", FIXED_SCALE, ""),
            Expect::Refuses(|error| {
                matches!(error, ExposureError::UnknownVersion { found: 99, .. })
            }),
        ),
        (
            "another scale",
            document(CURRENT_VERSION, "te-recorder", 1_000, ""),
            Expect::Refuses(|error| matches!(error, ExposureError::ScaleMismatch { .. })),
        ),
        // Rows with no cost basis: what the position cost cannot be read from its cash, so the
        // file is refused, and by its version, so the message names what changed.
        (
            "version 1, with no basis",
            document(
                1,
                "te-recorder",
                FIXED_SCALE,
                r#"{"symbol":"btcusdt","position_base":100000,"cash_quote":-11800000000}"#,
            ),
            Expect::Refuses(|error| matches!(error, ExposureError::UnknownVersion { found: 1, .. })),
        ),
        (
            "a flat row of an instrument this config does not name",
            document_naming("binance", "ethusdt", 0, 0),
            Expect::LoadsEmpty,
        ),
        (
            "a row with a position of an instrument this config does not name",
            document_naming("binance", "ethusdt", 100_000, -300_000_000),
            Expect::Refuses(|error| {
                matches!(
                    error,
                    ExposureError::UnknownInstrument { venue, symbol, .. }
                        if venue.as_ref() == "binance" && symbol.as_ref() == "ethusdt"
                )
            }),
        ),
        (
            "a row with a position of this config's symbol at another venue",
            document_naming("polymarket", "btcusdt", 100_000, -300_000_000),
            Expect::Refuses(|error| {
                matches!(
                    error,
                    ExposureError::UnknownInstrument { venue, symbol, .. }
                        if venue.as_ref() == "polymarket" && symbol.as_ref() == "btcusdt"
                )
            }),
        ),
        // A file of version 2 names each row by its symbol alone, and a symbol names an instrument
        // only within its venue.
        (
            "version 2",
            r#"{"version":2,"strategy_id":"recorder","te_id":"te-recorder","written_ts_us":1,"seq":1,"fixed_scale":100000000,"instruments":[{"symbol":"btcusdt","position_base":100000,"cash_quote":-11800000000,"basis_quote":11800000000}]}"#.to_owned(),
            Expect::Refuses(|error| {
                matches!(error, ExposureError::UnknownVersion { found: 2, expected: 3, .. })
            }),
        ),
    ];
    for (case, body, expect) in cases {
        let root = TempDir::new("exposure-untrusted");
        std::fs::create_dir_all(root.path()).expect("create the exposure directory");
        let path = file_path(root.path(), &identity(), Some(ExecutionMode::Live));
        std::fs::write(&path, &body).expect("write the case");
        let loaded = load_live(root.path(), &registry);
        assert_eq!(
            std::fs::read_to_string(&path).expect("the file is still there"),
            body,
            "{case}: a load leaves the file as it was"
        );
        match expect {
            Expect::LoadsEmpty => assert!(
                loaded
                    .unwrap_or_else(|error| panic!("{case}: a flat row costs nothing: {error}"))
                    .is_empty(),
                "{case}: expected an empty state"
            ),
            Expect::Refuses(is_expected) => {
                let error = loaded.expect_err(case);
                assert!(is_expected(&error), "{case} gave {error:?}");
                assert_eq!(
                    StartError::from(error).failure_class(),
                    FailureClass::Halt(Halt::Start),
                    "{case}: a file the start cannot trust refuses the start"
                );
            }
        }
    }
}

/// The format version that this build writes.
const CURRENT_VERSION: u32 = 3;

/// An exposure document of this engine's identity, whose `instruments` list holds `rows`.
fn document(version: u32, te_id: &str, fixed_scale: i64, rows: &str) -> String {
    format!(
        r#"{{"version":{version},"strategy_id":"recorder","te_id":"{te_id}","written_ts_us":1,"seq":1,"fixed_scale":{fixed_scale},"instruments":[{rows}]}}"#
    )
}

fn document_naming(venue: &str, symbol: &str, position_base: i64, cash_quote: i64) -> String {
    document(
        CURRENT_VERSION,
        "te-recorder",
        FIXED_SCALE,
        &format!(
            r#"{{"venue":"{venue}","symbol":"{symbol}","position_base":{position_base},"cash_quote":{cash_quote},"basis_quote":0}}"#
        ),
    )
}

/// Whether a case loads, and reads empty, or refuses the start for its own reason, so a case cannot
/// pass because the start refused for another reason.
enum Expect {
    LoadsEmpty,
    Refuses(fn(&ExposureError) -> bool),
}

fn asset_amounts<'a>(registry: &'a Registry, state: &ExposureState) -> Vec<(&'a str, i64)> {
    state
        .assets()
        .iter()
        .map(|entry| {
            (
                registry.assets().name(entry.asset).expect("a named asset"),
                entry.amount,
            )
        })
        .collect()
}

/// A run that inherits `restored` and writes its own changes nowhere.
fn restored_engine(
    instruments: &[InstrumentRow],
    restored: &[InstrumentExposure],
) -> (HotEngine, Consumer<ExecLaneItem>) {
    let (sink, _snapshots) = exposure_ring(64);
    engine_with_exposure(instruments, ExposureWiring { restored, sink })
}

fn engine_with_exposure(
    instruments: &[InstrumentRow],
    exposure: ExposureWiring<'_>,
) -> (HotEngine, Consumer<ExecLaneItem>) {
    let (persistence, _persist) = persist_ring(1_024);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(256);
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        exec: Some(ExecWiring {
            sink: ExecSink::new(commands_producer, Stamper::Off),
            // A wide band and a long book age: a quote refused by the price band or a stale book
            // would read the same as one that the loss gate pulled.
            settings: ExecSettings {
                max_session_loss_quote: Notional::from_mantissa(MAX_SESSION_LOSS),
                ..exec_settings()
            },
            run_nonce: 0,
        }),
        exposure,
        ..detached_setup(instruments, Box::new(TwoSidedQuoter))
    });
    (engine, commands)
}

/// Declares a resting quote on both sides at the touch, at each spin, and records what the ledger
/// says through the strategy's own view of it.
struct TwoSidedQuoter;

impl Strategy for TwoSidedQuoter {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        record_readings(ctx);
        let book = ctx.book(INSTRUMENT);
        let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else {
            return;
        };
        let qty = Qty::from_mantissa(ONE / 100);
        ctx.quote(
            INSTRUMENT,
            Side::Buy,
            QuoteLevel::ZERO,
            Some(DesiredQuote {
                price: bid.price,
                qty,
            }),
        );
        ctx.quote(
            INSTRUMENT,
            Side::Sell,
            QuoteLevel::ZERO,
            Some(DesiredQuote {
                price: ask.price,
                qty,
            }),
        );
    }
}

/// Restates the whole book, so the mark moves with no crossed top in between.
fn reseat_book(engine: &mut HotEngine, bid: i64, ask: i64, when: i64) {
    let (bids, asks) = snapshot_pair(0, &[(bid, ONE)], &[(ask, ONE)], when);
    engine.dispatch(&book_reset(0, when));
    engine.dispatch(&InboundMessage::new(ts(when), MessageBody::Book(bids)));
    engine.dispatch(&InboundMessage::new(ts(when), MessageBody::Book(asks)));
}

fn placed(commands: &[ExecCommand], wanted: Side) -> Option<ClientOrderId> {
    commands.iter().find_map(|command| match command {
        ExecCommand::Place {
            client_id, side, ..
        } if *side == wanted => Some(*client_id),
        _ => None,
    })
}

fn is_cancelled(commands: &[ExecCommand], wanted: ClientOrderId) -> bool {
    commands.iter().any(
        |command| matches!(command, ExecCommand::Cancel { client_id, .. } if *client_id == wanted),
    )
}

fn ack_placed(engine: &mut HotEngine, client_id: ClientOrderId, side: Side, price: i64, when: i64) {
    engine.dispatch(&InboundMessage::new(
        ts(when),
        MessageBody::Exec(ExecEvent {
            kind: ExecKind::AckPlaced,
            qty: Qty::from_mantissa(ONE / 100),
            ..exec_event(INSTRUMENT, client_id, side, price, when)
        }),
    ));
}

fn ack_canceled(engine: &mut HotEngine, client_id: ClientOrderId, side: Side, when: i64) {
    engine.dispatch(&InboundMessage::new(
        ts(when),
        MessageBody::Exec(ExecEvent {
            kind: ExecKind::AckCanceled,
            ..exec_event(INSTRUMENT, client_id, side, 0, when)
        }),
    ));
}

fn restored_long() -> [InstrumentExposure; 1] {
    [InstrumentExposure {
        instrument: INSTRUMENT,
        position_base: Qty::from_mantissa(RESTORED_POSITION),
        cash_quote: RESTORED_COST,
        basis_quote: -RESTORED_COST,
    }]
}

/// What the ledger of an engine that restored [`restored_long`] says at its first spin, before any
/// book has given a mark.
fn readings_of_a_restored_long_before_any_book() -> Readings {
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let (mut engine, _commands) = restored_engine(&instruments, &restored_long());
    spin_at(&mut engine, 1, 10);
    last_readings()
}

/// FITNESS: an engine that starts with inventory quotes, and the session loss limit still bites once
/// it does. The restored cost basis is 60,000 against a loss budget of 5, so a gate that read raw
/// PnL, or took its baseline before the first mark, would withdraw both sides at the start and
/// never place an order; a baseline taken at construction would swallow the same 60,000 as headroom,
/// and no session loss could reach the limit.
#[test]
fn a_restored_position_boots_quoting_and_the_session_limit_still_bites() {
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let (mut engine, mut commands) = restored_engine(&instruments, &restored_long());
    make_ready(&mut engine, 0);
    // A spin before any book, which a real start reaches long before the first snapshot lands.
    spin_at(&mut engine, 1, 5);
    drain_commands(&mut commands);

    // The market has moved against the restored position: marked at 59,000 against a 60,000 basis,
    // raw PnL is -1,000, two hundred times the loss budget, and all of it last session's.
    reseat_book(&mut engine, 58_990 * ONE, 59_010 * ONE, 10);
    spin_at(&mut engine, 2, 20);

    let boot = drain_commands(&mut commands);
    assert!(
        placed(&boot, Side::Buy).is_some(),
        "the side that would add to the restored position still quotes: {boot:?}"
    );
    assert!(
        placed(&boot, Side::Sell).is_some(),
        "and so does the side that would reduce it: {boot:?}"
    );

    let buy = placed(&boot, Side::Buy).expect("the buy side quoted at the first mark");
    let sell = placed(&boot, Side::Sell).expect("the sell side quoted at the first mark");
    ack_placed(&mut engine, buy, Side::Buy, 58_990 * ONE, 30);
    ack_placed(&mut engine, sell, Side::Sell, 59_010 * ONE, 30);

    // A move of 1,000 against one whole base unit, all of it since the baseline.
    reseat_book(&mut engine, 57_990 * ONE, 58_010 * ONE, 40);
    spin_at(&mut engine, 3, 50);

    let second = drain_commands(&mut commands);
    assert!(
        is_cancelled(&second, buy),
        "a session loss past the budget withdraws the side that would add: {second:?}"
    );
    assert!(
        is_cancelled(&second, sell),
        "the reducing quote also moved price, so serial requoting cancels it first: {second:?}"
    );

    ack_canceled(&mut engine, buy, Side::Buy, 60);
    ack_canceled(&mut engine, sell, Side::Sell, 60);
    spin_at(&mut engine, 4, 70);
    let after_confirmation = drain_commands(&mut commands);
    assert!(
        placed(&after_confirmation, Side::Buy).is_none(),
        "the adding side was placed again after the session limit withdrew it: \
         {after_confirmation:?}"
    );
    assert!(
        placed(&after_confirmation, Side::Sell).is_some(),
        "the reducing side was not placed again after its cancel ended: {after_confirmation:?}"
    );
}

/// FITNESS: a restored position with no mark does not read as flat.
#[test]
fn a_restored_position_without_a_mark_is_not_flat() {
    let readings = readings_of_a_restored_long_before_any_book();
    assert_eq!(
        readings.position_base,
        Qty::from_mantissa(RESTORED_POSITION),
        "the restored position is there from the first message"
    );
    assert!(
        !readings.has_mark,
        "and carries no valuation: no book has produced one yet"
    );
    assert_eq!(
        readings.exposure_quote, 0,
        "so exposure is zero: a position with no honest mark must not invent one"
    );
    assert_eq!(
        readings.pnl_quote, RESTORED_COST,
        "and raw PnL reads as the whole cost basis lost, which is why no loss limit may compare \
         against it"
    );
}

/// FITNESS: a writer that dies never reports a clean drain, so the run cannot exit 0 while the file
/// on disk holds a position that the next start restores and trades against.
#[test]
fn a_writer_that_died_cannot_report_a_clean_drain() {
    let root = TempDir::new("exposure-writer-panic");
    let registry = registry_for(BINANCE_SOURCE);
    let (writer, mut sink) = spawn_live_writer(root.path(), &registry, &ExposureState::default());

    // More rows than the fixed array holds is corrupt state, and the writer must die on it rather
    // than write it. The writer thread's own panic text on stderr is part of a green run.
    let mut corrupt = final_position(1, ONE);
    corrupt.len = MAX_EXPOSURE_INSTRUMENTS as u8 + 1;
    sink.push(corrupt);
    drop(sink);

    let outcome = drain_writer(writer);
    assert!(
        matches!(outcome, Err(ExposureError::WriterPanicked { .. })),
        "a panicked writer surfaces as WriterPanicked, so the exit is not graceful, got {outcome:?}"
    );
}

fn final_position(seq: u64, position_base: i64) -> ExposureSnapshot {
    let mut snapshot = ExposureSnapshot::EMPTY;
    snapshot.instruments[0] = InstrumentExposure {
        instrument: INSTRUMENT,
        position_base: Qty::from_mantissa(position_base),
        cash_quote: 0,
        basis_quote: 0,
    };
    snapshot.len = 1;
    snapshot.seq = seq;
    snapshot
}

/// What the ledger says, read through the strategy, the only view a test has of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Readings {
    position_base: Qty,
    has_mark: bool,
    exposure_quote: i64,
    pnl_quote: i64,
}

thread_local! {
    static READINGS: std::cell::Cell<Option<Readings>> = const { std::cell::Cell::new(None) };
}

fn record_readings(ctx: &StrategyCtx<'_>) {
    READINGS.with(|cell| {
        cell.set(Some(Readings {
            position_base: ctx.position_base(INSTRUMENT),
            has_mark: ctx.has_mark(INSTRUMENT),
            exposure_quote: ctx.exposure_quote(INSTRUMENT).mantissa(),
            pnl_quote: ctx.pnl_quote(INSTRUMENT).mantissa(),
        }));
    });
}

fn last_readings() -> Readings {
    READINGS
        .with(std::cell::Cell::get)
        .expect("the spin ran, so the strategy read the ledger")
}
