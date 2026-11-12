//! FITNESS: the keyed ladder's two safety rules at the real HOT boundary — whole-side single
//! flight, and cancel-confirm-place replacement — and the reservation-release policy that rides the
//! same boundary, in both directions, on a venue that locks an order's funds and on one that
//! does not.

use polysim::config::TrackerSpec;
use std::sync::{Arc, Mutex};

use polysim::adapters::polymarket::exec::codec::{
    AccountStamps, SettlementWatermark, account_snapshot,
};
use polysim::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use polysim::hot::exec::{ClientIdLayout, DesiredQuote, ExecSettings, QuoteLevel, level_of_slot};
use polysim::hot::strategy::{Strategy, StrategyCtx};
use polysim::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side};
use polysim::msg::exec::{
    AccountChunk, AccountChunkKind, AssetBalance, ExecCommand, ExecEvent, ExecKind, ExecLaneItem,
    OrderStyle, VenueOrderStatus,
};
use polysim::msg::inbound::{InboundMessage, SpinTick};
use polysim::sink::ExecSink;
use polysim::time::TsUs;
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{ONE, account_chunk, detached_setup, exec_event, instrument_row, pop};
use crate::exec_support::{
    ASK, BID, drain_commands, exec_settings, make_ready, reseat_book, spin_at,
};

const INSTRUMENT: InstrumentId = InstrumentId(0);
const LEVEL_ONE_PRICE: i64 = BID - 2 * ONE;
const MOVED_LEVEL_ZERO_PRICE: i64 = BID - 4 * ONE;

struct KeyedLadder;

impl Strategy for KeyedLadder {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        let level_zero_price = if tick.seq >= 3 { MOVED_LEVEL_ZERO_PRICE } else { BID };
        let level_one_price = if tick.seq >= 6 { MOVED_LEVEL_ZERO_PRICE } else { LEVEL_ONE_PRICE };
        for (level, price) in [
            (QuoteLevel::ZERO, level_zero_price),
            (QuoteLevel::new(1).expect("level one"), level_one_price),
        ] {
            ctx.quote(
                INSTRUMENT,
                Side::Buy,
                level,
                Some(DesiredQuote {
                    price: Price(price),
                    qty: Qty(ONE / 10),
                    style: OrderStyle::PostOnly,
                }),
            );
        }
    }
}

fn ladder_engine() -> (HotEngine, Consumer<ExecLaneItem>) {
    engine_with_strategy(Box::new(KeyedLadder), 2)
}

fn engine_with_strategy(
    strategy: Box<dyn Strategy>,
    max_orders_per_side: usize,
) -> (HotEngine, Consumer<ExecLaneItem>) {
    let mut settings = exec_settings();
    settings.max_orders_per_side = max_orders_per_side;
    settings.limits.requote_threshold_ticks = 1;
    engine_with_settings(strategy, settings)
}

fn engine_with_settings(
    strategy: Box<dyn Strategy>,
    settings: ExecSettings,
) -> (HotEngine, Consumer<ExecLaneItem>) {
    let mut row = instrument_row(0, TrackerSpec::default(), 64);
    row.tick_size = Some(Price(ONE));
    row.lot_size = Some(Qty(1));
    row.max_exposure_quote = 1_000_000 * ONE;
    let instruments = [row];
    let (producer, commands) = RingBuffer::new(256);
    let engine = HotEngine::new(HotEngineSetup {
        exec: Some(ExecWiring {
            sink: ExecSink::new(producer),
            settings,
            run_nonce: 0,
        }),
        ..detached_setup(&instruments, strategy)
    });
    (engine, commands)
}

struct ReservationQuoter {
    observed: Arc<Mutex<Vec<i64>>>,
}

impl Strategy for ReservationQuoter {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        self.observed
            .lock()
            .expect("reservation probe")
            .push(ctx.balance(AssetId(1)).reserved);
        ctx.quote(
            INSTRUMENT,
            Side::Buy,
            QuoteLevel::ZERO,
            Some(DesiredQuote {
                price: Price(BID),
                qty: Qty(ONE / 10),
                style: OrderStyle::PostOnly,
            }),
        );
    }
}

fn acknowledge(
    engine: &mut HotEngine,
    command: ExecCommand,
    kind: ExecKind,
    when: i64,
) -> ClientOrderId {
    let (client_id, side) = match command {
        ExecCommand::Place {
            client_id, side, ..
        } => (client_id, side),
        ExecCommand::Cancel { client_id, .. } => (client_id, Side::Buy),
        other => panic!("fixture cannot acknowledge {other:?}"),
    };
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Exec(ExecEvent {
            kind,
            status: Some(match kind {
                ExecKind::AckPlaced => VenueOrderStatus::New,
                ExecKind::AckCanceled => VenueOrderStatus::Canceled,
                _ => panic!("unsupported acknowledgement {kind:?}"),
            }),
            ..exec_event(INSTRUMENT, client_id, side, 0, when)
        }),
    );
    client_id
}

fn place_and_ack(engine: &mut HotEngine, commands: &mut Consumer<ExecLaneItem>) -> ExecCommand {
    make_ready(engine, 0);
    for message in reseat_book(BID, ASK, 1) {
        engine.dispatch(pop(0, 0), &message);
    }
    spin_at(engine, 1, 10);
    let place = only(commands);
    acknowledge(engine, place, ExecKind::AckPlaced, 11);
    place
}

fn restate_free_balance(engine: &mut HotEngine, venue_update_ts_ms: u64, when: i64) {
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Account(AccountChunk {
            kind: AccountChunkKind::Update,
            venue_update_ts_ms,
            ..account_chunk(&[(AssetId(1), 1_000_000 * ONE)], when)
        }),
    );
}

fn only(commands: &mut Consumer<ExecLaneItem>) -> ExecCommand {
    let commands = drain_commands(commands);
    assert_eq!(
        commands.len(),
        1,
        "expected one side mutation: {commands:?}"
    );
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

#[test]
fn ladder_serialises_and_requotes_only_after_terminal_confirmation() {
    let (mut engine, mut commands) = ladder_engine();
    let first = place_and_ack(&mut engine, &mut commands);
    assert!(matches!(first, ExecCommand::Place { .. }));
    assert_eq!(level_of(first), QuoteLevel::ZERO);

    spin_at(&mut engine, 2, 20);
    let second = only(&mut commands);
    assert!(matches!(second, ExecCommand::Place { .. }));
    assert_eq!(level_of(second), QuoteLevel::new(1).expect("level one"));
    acknowledge(&mut engine, second, ExecKind::AckPlaced, 21);

    spin_at(&mut engine, 3, 30);
    let cancel = only(&mut commands);
    assert!(matches!(cancel, ExecCommand::Cancel { .. }));
    assert_eq!(level_of(cancel), QuoteLevel::ZERO);

    spin_at(&mut engine, 4, 40);
    assert!(
        drain_commands(&mut commands).is_empty(),
        "a replacement was sent while the cancel was still unconfirmed"
    );

    acknowledge(&mut engine, cancel, ExecKind::AckCanceled, 41);
    spin_at(&mut engine, 5, 50);
    let replacement = only(&mut commands);
    assert!(matches!(
        replacement,
        ExecCommand::Place {
            price: Price(MOVED_LEVEL_ZERO_PRICE),
            ..
        }
    ));
    assert_eq!(level_of(replacement), QuoteLevel::ZERO);
    acknowledge(&mut engine, replacement, ExecKind::AckPlaced, 51);

    // Both desired levels now snap to the same price. The lower key wins and the existing higher
    // keyed order is actively withdrawn.
    spin_at(&mut engine, 6, 60);
    let duplicate_cancel = only(&mut commands);
    assert!(matches!(duplicate_cancel, ExecCommand::Cancel { .. }));
    assert_eq!(
        level_of(duplicate_cancel),
        QuoteLevel::new(1).expect("level one")
    );
}

#[test]
fn a_newer_account_snapshot_retries_a_held_reservation_release() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let (mut engine, mut commands) = engine_with_strategy(
        Box::new(ReservationQuoter {
            observed: Arc::clone(&observed),
        }),
        1,
    );
    place_and_ack(&mut engine, &mut commands);

    // The ack beat a newer absolute balance, so its release is deliberately held.
    spin_at(&mut engine, 2, 20);
    assert!(
        observed.lock().expect("reservation probe")[1] > 0,
        "the ack released against a stale account balance"
    );

    restate_free_balance(&mut engine, 2, 21);
    spin_at(&mut engine, 3, 30);
    assert_eq!(
        observed.lock().expect("reservation probe")[2],
        0,
        "the newer account snapshot did not retry the held release"
    );
}

/// A quoter over a venue that does NOT lock an order's funds on placement (Polymarket). The release
/// policy differs from the gated-watermark one the tests above pin, and the tests below exercise
/// both directions of that difference against it.
fn non_locking_reservation_engine(
    observed: Arc<Mutex<Vec<i64>>>,
) -> (HotEngine, Consumer<ExecLaneItem>) {
    let mut settings = exec_settings();
    settings.max_orders_per_side = 1;
    settings.limits.requote_threshold_ticks = 1;
    settings.holds_reservations_until_settled = false;
    engine_with_settings(Box::new(ReservationQuoter { observed }), settings)
}

/// The mirror of `a_newer_account_snapshot_retries_a_held_reservation_release`. On a venue that
/// never locked an open order's funds, a resting order moved no balance, so a later restatement — a
/// fill on some OTHER order — must NOT release the resting order's reservation. Releasing it would
/// overstate spendable while the order still rests and admit orders against money already committed;
/// on the shipped ladder that surfaces as an oversell the day a second slot exists.
#[test]
fn a_non_locking_resting_reservation_survives_a_newer_snapshot() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let (mut engine, mut commands) = non_locking_reservation_engine(Arc::clone(&observed));
    place_and_ack(&mut engine, &mut commands);

    spin_at(&mut engine, 2, 20);
    let reserved_while_resting = observed.lock().expect("reservation probe")[1];
    assert!(
        reserved_while_resting > 0,
        "a placed order reserves its funds"
    );

    // A fill elsewhere restates balances at a later stamp — the exact event that frees a held release
    // on Binance. Here the resting order is untouched.
    restate_free_balance(&mut engine, 2, 21);
    spin_at(&mut engine, 3, 30);
    assert_eq!(
        observed.lock().expect("reservation probe")[2],
        reserved_while_resting,
        "a restatement released a resting order's reservation on a venue that never locked it",
    );
}

/// On a non-locking venue a cancelled quote that never filled must free its reservation the SAME
/// spin, with no restatement to wait for — the venue moved no money, so the release is ungated.
/// Under the gated (Binance) release the reservation would be held forever, because the fill-driven
/// restatement that would clear it never comes: the exit reserves, its offer is cancelled, and every
/// subsequent flatten then reads Underfunded and the position rides to resolution.
#[test]
fn a_non_locking_zero_fill_cancel_frees_its_reservation_ungated() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let (mut engine, mut commands) = non_locking_reservation_engine(Arc::clone(&observed));
    let client_id = client_id_of(place_and_ack(&mut engine, &mut commands));

    spin_at(&mut engine, 2, 20);
    assert!(
        observed.lock().expect("reservation probe")[1] > 0,
        "a placed order reserves its funds"
    );

    // The venue cancels the resting quote with nothing filled, and no account restatement follows —
    // an open order never moved a balance to restate.
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Exec(ExecEvent {
            kind: ExecKind::AckCanceled,
            status: Some(VenueOrderStatus::Canceled),
            ..exec_event(INSTRUMENT, client_id, Side::Buy, 0, 25)
        }),
    );
    spin_at(&mut engine, 3, 30);
    assert_eq!(
        observed.lock().expect("reservation probe")[2],
        0,
        "a zero-fill cancel left its reservation held with no restatement to ever release it",
    );
}

/// A real wall-clock reading, because that is what the edge stamps a balance answer with and the
/// engine's own tick counter is far too small to tell a millisecond stamp from zero.
const RECEIVED_AT: TsUs = TsUs::from_micros(1_786_046_895_000_000);

/// The venue's own stamp on the trade that carried the fill below, seconds resolution as this venue
/// reports it, and later than the readiness chunk the reservation was taken against.
const SETTLED_AT: TsUs = TsUs::from_micros(1_786_046_890_000_000);

/// One balance sweep as the Polymarket edge builds it, at whatever it has watched settle so far.
fn balance_sweep(settled_through: SettlementWatermark) -> Vec<AccountChunk> {
    account_snapshot(
        &[AssetBalance {
            asset: AssetId(1),
            free: 1_000_000 * ONE,
            locked: 0,
        }],
        AccountChunkKind::Update,
        AccountStamps {
            settled_through,
            received_ts_us: RECEIVED_AT,
        },
    )
}

fn feed(engine: &mut HotEngine, chunks: Vec<AccountChunk>) {
    for chunk in chunks {
        engine.dispatch(pop(0, 0), &InboundMessage::Account(chunk));
    }
}

/// A filled order's reservation, released against the chunk the POLYMARKET edge actually builds.
/// That venue publishes no account clock and answers a balance read taken right after a fill with
/// the PRE-fill number, so such a read is not evidence the money moved; only the fill's own trade
/// reaching settlement is. Freeing the reservation early makes the collateral spendable twice — the
/// strategy funds a fresh order against money the venue is still holding for the one that filled.
#[test]
fn a_filled_reservation_waits_for_the_venues_own_settlement() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let (mut engine, mut commands) = non_locking_reservation_engine(Arc::clone(&observed));
    let client_id = client_id_of(place_and_ack(&mut engine, &mut commands));

    spin_at(&mut engine, 2, 20);
    let one_order = observed.lock().expect("reservation probe")[1];
    assert!(one_order > 0, "a placed order reserves its funds");

    let size = Qty(ONE / 10);
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Exec(ExecEvent {
            kind: ExecKind::ReportTrade,
            status: Some(VenueOrderStatus::Filled),
            qty: size,
            last_price: Price(BID),
            last_qty: size,
            cumulative_qty: size,
            cumulative_quote: Price(BID).notional(size),
            ..exec_event(INSTRUMENT, client_id, Side::Buy, BID, 25)
        }),
    );
    // The edge re-reads balances the moment it reports the fill. Nothing has settled yet, so the
    // chunk it builds carries no evidence of moved money and must free nothing.
    feed(&mut engine, balance_sweep(SettlementWatermark::NONE));

    spin_at(&mut engine, 3, 30);
    assert_eq!(
        observed.lock().expect("reservation probe")[2],
        one_order,
        "a balance read taken before the fill settled freed the reservation that fill took",
    );
    let replacement = only(&mut commands);
    assert!(
        matches!(replacement, ExecCommand::Place { .. }),
        "the closed slot re-quotes, and its reservation is what must remain after the release",
    );

    // The fill's own trade reaches the chain, and the balance read that follows THAT is the one
    // carrying money the venue has actually moved.
    let mut settled = SettlementWatermark::NONE;
    assert!(settled.advance_to(SETTLED_AT));
    feed(&mut engine, balance_sweep(settled));

    spin_at(&mut engine, 4, 40);
    assert_eq!(
        observed.lock().expect("reservation probe")[3],
        one_order,
        "settlement landed and the filled order's reservation is still held on top of the live one",
    );
}
