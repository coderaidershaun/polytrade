//! Workstation link fitness: what the workstation does with the datagrams an engine sends it,
//! driven only through the session's public seam (bytes and time in; deliveries, status and
//! subscribe bytes out). Each invariant is a silent-corruption risk rather than a crash. The catalog
//! is announced as loose per-item frames, so an assembly that completed early or adopted a stale
//! epoch would label every DOM row and feature value with the wrong name. The feed rides UDP, so
//! loss must read as a counted gap and never as a book that travelled backwards. The control epoch
//! decides whether this workstation can stop an engine at all, or seizes one another controller
//! holds. And a frame from the wrong engine, the wrong run or the wrong peer must never reach a
//! model that is showing another one.

use std::collections::HashMap;
use std::net::SocketAddr;

use polysim::config::{ExecutionMode, StrategyId};
use polysim::ids::{AssetId, InstrumentId, Price, Qty, Side};
use polysim::link::{
    CatalogFeature, CatalogInstrument, Envelope, FrameGuard, GateVerdict, LINK_MAX_DATAGRAM,
    LINK_MAX_GATE_KEYS, Lifecycle, LinkBody, LinkDatagram, LinkHash, LinkIdentity, RunPhase,
    RunState, SequenceGate, Subscribe, TopicId, WireName,
};
use polysim::msg::persist::FeatureId;
use polysim::msg::ui::{UiBookSnapshot, UiBookState, UiCatalog, UiEvent};
use polysim::time::{DurationUs, TsUs};
use polysim::workstation::link_model::{ConnectionState, ControlVerdict};
use polysim::workstation::link_session::{Delivery, LinkSession, LinkSessionError, LinkTarget};
use polysim::workstation::model::UiModel;
use proptest::prelude::*;

use crate::workstation_support::BookFixture;

const STRATEGY: &str = "strat-fitness";
const CATALOG_EPOCH: TsUs = TsUs::from_micros(1_700_000_000_000_000);
const SPIN_INTERVAL: DurationUs = DurationUs::from_micros(1_000_000);
const SPIN_INTERVAL_US: u64 = SPIN_INTERVAL.micros() as u64;

/// The caller's clock reading for every test that does not drive time itself. One instant for a
/// whole feed means no gate slot ever ages out mid-property.
const NOW: TsUs = CATALOG_EPOCH;
const FIRST_BOOT_US: i64 = 1_000;
const SECOND_BOOT_US: i64 = 2_000;
const THIRD_BOOT_US: i64 = 3_000;
const SENDER_NONCE: u64 = 7;
const SUBSCRIBE_INTERVAL: DurationUs = DurationUs::from_secs(2);
const SILENCE_LIMIT: DurationUs = DurationUs::from_secs(5);

fn peers() -> Vec<SocketAddr> {
    ["127.0.0.1:9310", "127.0.0.1:9311"]
        .map(|raw| raw.parse().expect("the fixture peer address is a literal"))
        .to_vec()
}

fn target(peers: Vec<SocketAddr>) -> LinkTarget {
    LinkTarget {
        strategy: StrategyId::new(STRATEGY).expect("the fixture strategy id is a literal"),
        peers,
        token_hash: LinkHash::of_name(""),
        sender_nonce: SENDER_NONCE,
    }
}

fn session_at(now: TsUs) -> LinkSession {
    LinkSession::new(target(peers()), now).expect("the fixture names two peers")
}

fn session() -> LinkSession {
    session_at(NOW)
}

fn guard() -> FrameGuard {
    FrameGuard {
        token_hash: LinkHash::of_name(""),
        strategy_hash: LinkHash::of_name(STRATEGY),
        schema_hash: LinkHash::of_fields(&[]),
    }
}

fn identity(boot_ts_us: i64) -> LinkIdentity {
    LinkIdentity {
        token_hash: LinkHash::of_name(""),
        strategy_hash: LinkHash::of_name(STRATEGY),
        sender_te_hash: LinkHash::of_name("te-fitness"),
        boot_ts_us: TsUs::from_micros(boot_ts_us),
    }
}

/// The sending side of a link: one run of one trading engine, numbering each topic's frames the
/// way the real link actor does.
struct Engine {
    identity: LinkIdentity,
    sent: HashMap<TopicId, u64>,
}

impl Engine {
    fn booted_at(boot_ts_us: i64) -> Self {
        Self::with_identity(identity(boot_ts_us))
    }

    fn with_identity(identity: LinkIdentity) -> Self {
        Self {
            identity,
            sent: HashMap::new(),
        }
    }

    fn send(&mut self, body: LinkBody) -> Vec<u8> {
        let sent = self.sent.entry(topic_of(&body)).or_insert(0);
        *sent += 1;
        let seq = *sent;
        self.frame(seq, body)
    }

    fn frame(&self, seq: u64, body: LinkBody) -> Vec<u8> {
        let mut buffer = [0u8; LINK_MAX_DATAGRAM];
        let written = LinkDatagram {
            envelope: Envelope::new(self.identity, topic_of(&body), seq),
            body,
        }
        .encode(&mut buffer);
        buffer[..written].to_vec()
    }
}

fn topic_of(body: &LinkBody) -> TopicId {
    match body {
        LinkBody::Book(_) => TopicId::BOOKS,
        LinkBody::Event(_) => TopicId::EVENTS,
        LinkBody::CatalogInstrument(_) => TopicId::CATALOG_INSTRUMENTS,
        LinkBody::CatalogFeature(_) => TopicId::CATALOG_FEATURES,
        LinkBody::Lifecycle(_) => TopicId::LIFECYCLE,
        other => panic!("an engine never sends {other:?} to a workstation"),
    }
}

fn instrument_frame(index: u16, total: u16, epoch: TsUs) -> CatalogInstrument {
    CatalogInstrument {
        catalog_ts_us: epoch,
        total_count: total,
        instrument: InstrumentId(index),
        display: WireName::new(&format!("INSTRUMENT-{index}")),
        tick_size: Some(Price(10 * i64::from(index) + 1)),
        lot_size: Some(Qty(100 * i64::from(index) + 1)),
        qty_scale: 100_000_000,
        base_asset: AssetId(2 * index),
        quote_asset: AssetId(2 * index + 1),
        base: WireName::new(&format!("BASE{index}")),
        quote: WireName::new(&format!("QUOTE{index}")),
    }
}

fn feature_frame(index: u16, total: u16, epoch: TsUs) -> CatalogFeature {
    CatalogFeature {
        catalog_ts_us: epoch,
        total_count: total,
        feature: FeatureId(index),
        name: WireName::new(&format!("feature_{index}")),
    }
}

fn lifecycle(acknowledged_epoch: u64, run_state: RunState) -> Lifecycle {
    Lifecycle {
        phase: RunPhase::Ready,
        run_state,
        execution_mode: Some(ExecutionMode::Live),
        acknowledged_epoch,
        spin_interval_us: SPIN_INTERVAL,
        feature_count: 0,
    }
}

/// The heartbeat an engine declaring `feature_count` columns sends. It is the heartbeat and not the
/// catalog frames that carries the total, so a subscriber knows how many features to wait for
/// before the first, or the zeroth, of them arrives.
fn heartbeat(feature_count: u16) -> LinkBody {
    LinkBody::Lifecycle(Lifecycle {
        feature_count,
        ..lifecycle(0, RunState::Running)
    })
}

/// The frames of a one-instrument, one-feature catalog stamped `epoch`, heartbeat first.
fn catalog_frames(engine: &mut Engine, epoch: TsUs) -> Vec<Vec<u8>> {
    [
        heartbeat(1),
        LinkBody::CatalogInstrument(instrument_frame(0, 1, epoch)),
        LinkBody::CatalogFeature(feature_frame(0, 1, epoch)),
    ]
    .into_iter()
    .map(|body| engine.send(body))
    .collect()
}

fn catalog_of(delivery: Option<Delivery>) -> Option<UiCatalog> {
    match delivery {
        Some(Delivery::Catalog(catalog)) => Some(catalog),
        _ => None,
    }
}

/// Which catalog frame an announcement carries. Instruments and features share a `catalog_ts_us`
/// but declare their own totals, so an assembly must satisfy both before it can be complete.
#[derive(Debug, Clone, Copy)]
enum Announcement {
    Instrument(u16),
    Feature(u16),
}

fn announcements(instruments: u16, features: u16) -> Vec<Announcement> {
    (0..instruments)
        .map(Announcement::Instrument)
        .chain((0..features).map(Announcement::Feature))
        .collect()
}

fn announce(
    engine: &mut Engine,
    session: &mut LinkSession,
    item: Announcement,
    totals: (u16, u16),
    epoch: TsUs,
) -> Option<UiCatalog> {
    let body = match item {
        Announcement::Instrument(index) => {
            LinkBody::CatalogInstrument(instrument_frame(index, totals.0, epoch))
        }
        Announcement::Feature(index) => {
            LinkBody::CatalogFeature(feature_frame(index, totals.1, epoch))
        }
    };
    catalog_of(session.on_datagram(&engine.send(body), NOW))
}

proptest! {
    /// FITNESS: the catalog is delivered exactly when every declared item has arrived, never one
    /// frame earlier and never twice, whatever order the datagrams land in and however many are
    /// repeated. A catalog delivered early would hand the monitor a short `feature_names` list and
    /// every feature row after the hole would be labelled with its neighbour's name.
    #[test]
    fn catalog_completes_only_once_every_declared_item_has_arrived(
        instruments in 1u16..6,
        features in 1u16..8,
        order in prop::collection::vec(0usize..64, 0..64),
        duplicates in prop::collection::vec(0usize..64, 0..24),
    ) {
        let items = announcements(instruments, features);
        let totals = (instruments, features);
        let mut engine = Engine::booted_at(FIRST_BOOT_US);
        let mut session = session();
        session.on_datagram(&engine.send(heartbeat(features)), NOW);

        let mut seen = vec![false; items.len()];
        let mut catalogs = Vec::new();
        for pick in order.iter().chain(duplicates.iter()) {
            let index = pick % items.len();
            let was_complete = seen.iter().all(|has_arrived| *has_arrived);
            seen[index] = true;
            let is_complete = seen.iter().all(|has_arrived| *has_arrived);
            let delivered = announce(&mut engine, &mut session, items[index], totals, CATALOG_EPOCH);
            prop_assert_eq!(
                delivered.is_some(),
                is_complete && !was_complete,
                "the catalog must arrive on the frame that completes the set of distinct items, and on no other"
            );
            catalogs.extend(delivered);
        }

        // Then everything, so the run always reaches a complete catalog and it matches the source.
        for item in &items {
            catalogs.extend(announce(&mut engine, &mut session, *item, totals, CATALOG_EPOCH));
        }
        prop_assert_eq!(catalogs.len(), 1, "one catalog epoch is delivered exactly once");
        let catalog = &catalogs[0];
        prop_assert_eq!(catalog.instruments.len(), usize::from(instruments));
        prop_assert_eq!(catalog.feature_names.len(), usize::from(features));
        prop_assert_eq!(catalog.spin_interval_us, SPIN_INTERVAL_US);
        for (position, row) in catalog.instruments.iter().enumerate() {
            prop_assert_eq!(row.instrument_id, InstrumentId(position as u16));
            prop_assert_eq!(row.display.as_ref(), format!("INSTRUMENT-{position}"));
            prop_assert_eq!(row.tick_size, Some(Price(10 * position as i64 + 1)));
        }
        for (index, name) in catalog.feature_names.iter().enumerate() {
            prop_assert_eq!(name.as_ref(), format!("feature_{index}"));
        }
    }
}

/// Epoch discipline cuts both ways: a NEWER epoch discards whatever the old one gathered (a
/// restarted engine announcing fewer features must not inherit the old run's count), and an OLDER,
/// delayed datagram must not mix into the epoch that has already superseded it. The assembly also
/// validates a frame's claimed total two different ways, and an untrusted sender can get either
/// wrong without the workstation panicking: a dense feature list with a hole in it (an id past the
/// frame's OWN declared total) must never complete, and a sender whose per-frame total disagrees
/// with the heartbeat's yields ids that do not tile the assembled list. A peer's arithmetic must
/// never panic us, since anyone who can reach the port can forge a frame. A trading engine that
/// declares no feature columns (the polymarket publisher's own shape) announces no feature frame at
/// all, so a total learned from those frames alone would never arrive; the count must ride the
/// once-a-second heartbeat instead.
#[test]
fn catalog_assembly_epoch_and_total_validation() {
    let mut engine = Engine::booted_at(FIRST_BOOT_US);
    let mut session = session();
    let next = CATALOG_EPOCH + DurationUs::RESOLUTION;
    session.on_datagram(&engine.send(heartbeat(2)), NOW);
    announce(
        &mut engine,
        &mut session,
        Announcement::Instrument(0),
        (1, 2),
        CATALOG_EPOCH,
    );
    announce(
        &mut engine,
        &mut session,
        Announcement::Feature(0),
        (1, 2),
        CATALOG_EPOCH,
    );

    // The restarted engine announces ONE feature where the old run had two.
    session.on_datagram(&engine.send(heartbeat(1)), NOW);
    let held_session = session.status(NOW).session;
    let opening = announce(
        &mut engine,
        &mut session,
        Announcement::Instrument(0),
        (1, 1),
        next,
    );
    assert!(
        opening.is_none(),
        "the previous epoch's feature must not count towards the new one"
    );
    let catalog = announce(
        &mut engine,
        &mut session,
        Announcement::Feature(0),
        (1, 1),
        next,
    )
    .expect("the new epoch is complete");
    assert_eq!(catalog.feature_names.len(), 1);
    assert_eq!(catalog.feature_names[0].as_ref(), "feature_0");
    assert_eq!(
        session.status(NOW).session,
        held_session + 1,
        "a newer catalog epoch inside one run is a new session, so the model starts over"
    );

    // A datagram from the previous epoch, delayed on the wire past the gate's reach.
    let stale = announce(
        &mut engine,
        &mut session,
        Announcement::Feature(0),
        (1, 4),
        CATALOG_EPOCH,
    );
    assert!(stale.is_none(), "an older epoch must not be delivered");
    assert_eq!(
        session.status(NOW).session,
        held_session + 1,
        "an older epoch must not reset the model the newer one is filling"
    );

    totals_validation_cases();
}

fn totals_validation_cases() {
    struct Case {
        name: &'static str,
        instruments: &'static [(u16, u16)],
        features: &'static [(u16, u16)],
        heartbeat_features: u16,
        expect: Option<(usize, usize)>,
    }
    let cases = [
        Case {
            name: "feature id past its own frame's declared total is refused",
            instruments: &[(0, 1)],
            features: &[(0, 2), (7, 2)],
            heartbeat_features: 2,
            expect: None,
        },
        Case {
            name: "an engine with no feature columns completes from the heartbeat total",
            instruments: &[(0, 1)],
            features: &[],
            heartbeat_features: 0,
            expect: Some((1, 0)),
        },
        Case {
            name: "a sender whose per-frame total disagrees with the heartbeat never panics",
            instruments: &[(0, 1)],
            features: &[(0, 10), (7, 10)],
            heartbeat_features: 2,
            expect: None,
        },
    ];

    for case in cases {
        let mut engine = Engine::booted_at(FIRST_BOOT_US);
        let mut session = session();
        let mut catalogs = Vec::new();
        session.on_datagram(&engine.send(heartbeat(case.heartbeat_features)), NOW);
        for &(index, total) in case.instruments {
            let item = Announcement::Instrument(index);
            catalogs.extend(announce(
                &mut engine,
                &mut session,
                item,
                (total, 0),
                CATALOG_EPOCH,
            ));
        }
        for &(index, total) in case.features {
            let item = Announcement::Feature(index);
            catalogs.extend(announce(
                &mut engine,
                &mut session,
                item,
                (0, total),
                CATALOG_EPOCH,
            ));
        }
        match case.expect {
            None => assert!(
                catalogs.is_empty(),
                "case {}: expected no catalog",
                case.name
            ),
            Some((instruments, features)) => {
                assert_eq!(
                    catalogs.len(),
                    1,
                    "case {}: expected one catalog",
                    case.name
                );
                assert_eq!(
                    catalogs[0].instruments.len(),
                    instruments,
                    "case {}: instrument count",
                    case.name
                );
                assert_eq!(
                    catalogs[0].feature_names.len(),
                    features,
                    "case {}: feature count",
                    case.name
                );
            }
        }
    }
}

/// A complete set of frames is not enough: the heartbeat carries the feature total, so a catalog
/// whose items all arrived first still waits for it.
#[test]
fn a_complete_catalog_waits_for_the_heartbeat_that_declares_its_total() {
    let mut engine = Engine::booted_at(FIRST_BOOT_US);
    let mut session = session();
    let mut early = Vec::new();
    early.extend(announce(
        &mut engine,
        &mut session,
        Announcement::Instrument(0),
        (1, 1),
        CATALOG_EPOCH,
    ));
    early.extend(announce(
        &mut engine,
        &mut session,
        Announcement::Feature(0),
        (1, 1),
        CATALOG_EPOCH,
    ));
    assert!(
        early.is_empty(),
        "no heartbeat has said how many features to expect"
    );

    let catalog = catalog_of(session.on_datagram(&engine.send(heartbeat(1)), NOW))
        .expect("the heartbeat completes the catalog");
    assert_eq!(&*catalog.strategy_id, STRATEGY);
    assert_eq!(catalog.execution_mode, Some(ExecutionMode::Live));
    assert_eq!(catalog.spin_interval_us, SPIN_INTERVAL_US);
}

/// The engine repeats its catalog every few seconds and heartbeats every second. Neither may deliver
/// the same catalog twice, or the model would be rebuilt under the operator once per repeat. A
/// genuinely new catalog epoch is delivered exactly once more.
#[test]
fn the_catalog_is_delivered_once_per_catalog_epoch() {
    let mut engine = Engine::booted_at(FIRST_BOOT_US);
    let mut session = session();
    session.on_datagram(&engine.send(heartbeat(1)), NOW);
    let items = announcements(1, 1);
    let totals = (1, 1);

    let mut first_epoch = 0;
    for _ in 0..3 {
        for item in &items {
            first_epoch += usize::from(
                announce(&mut engine, &mut session, *item, totals, CATALOG_EPOCH).is_some(),
            );
        }
        let beat = session.on_datagram(&engine.send(heartbeat(1)), NOW);
        first_epoch += usize::from(catalog_of(beat).is_some());
    }
    assert_eq!(
        first_epoch, 1,
        "three announcements of one epoch are one catalog"
    );

    let next = CATALOG_EPOCH + DurationUs::RESOLUTION;
    let mut second_epoch = 0;
    for _ in 0..3 {
        for item in &items {
            second_epoch +=
                usize::from(announce(&mut engine, &mut session, *item, totals, next).is_some());
        }
    }
    assert_eq!(
        second_epoch, 1,
        "a new epoch is one more catalog, and no more"
    );
}

fn book_snapshot(seq: u64, best_bid: i64) -> UiBookSnapshot {
    crate::workstation_support::book_snapshot(BookFixture {
        instrument: 0,
        seq,
        event_ts: seq as i64 * 1_000,
        state: UiBookState::Valid,
        bids: &[(best_bid, 1_000)],
        asks: &[],
    })
}

fn trade_event(seq: u64) -> UiEvent {
    UiEvent::Trade {
        instrument: InstrumentId(0),
        seq,
        event_ts_us: TsUs::from_micros(seq as i64 * 1_000),
        aggressor: Side::Buy,
        price: Price(100 + seq as i64),
        qty: Qty(7),
    }
}

proptest! {
    /// FITNESS: a lossy, duplicating, reordering network must reach the workstation's model as
    /// counted gaps and a monotonic latest, never as a book that travelled backwards. This is the
    /// whole justification for carrying the feed on UDP with no reliability layer, so it is pinned
    /// against the REAL decode, sequence gate and model fold, not a stand-in for them.
    #[test]
    fn a_lossy_feed_counts_its_gaps_and_never_travels_backwards(
        count in 2usize..40,
        keep in prop::collection::vec(any::<bool>(), 40),
        replay_last in any::<bool>(),
        deliver_stale in any::<bool>(),
    ) {
        let engine = Engine::booted_at(FIRST_BOOT_US);
        let mut session = session();
        let mut model = UiModel::with_capacity(1, SPIN_INTERVAL);

        let mut accepted_books: Vec<u64> = Vec::new();
        let mut accepted_events: Vec<u64> = Vec::new();
        for (index, is_delivered) in keep.iter().enumerate().take(count) {
            let seq = index as u64 + 1;
            if !is_delivered {
                continue;
            }
            for (body, accepted) in [
                (LinkBody::Book(book_snapshot(seq, 500 + seq as i64)), &mut accepted_books),
                (LinkBody::Event(trade_event(seq)), &mut accepted_events),
            ] {
                let Some(delivery) = session.on_datagram(&engine.frame(seq, body), NOW) else {
                    continue;
                };
                accepted.push(seq);
                match delivery {
                    Delivery::Book(snapshot) => model.apply_book(snapshot),
                    Delivery::Event(event) => model.apply_event(event),
                    Delivery::Catalog(_) => panic!("a feed frame produced a catalog"),
                }
            }
        }
        prop_assume!(!accepted_books.is_empty());

        // A duplicate and a rewind, the two things UDP hands a consumer that an in-process ring
        // cannot: both must die at the gate rather than reach the fold.
        let expected_latest = *accepted_books.last().expect("checked non-empty");
        if replay_last {
            let replay = engine.frame(expected_latest, LinkBody::Book(book_snapshot(expected_latest, 1)));
            prop_assert!(session.on_datagram(&replay, NOW).is_none());
        }
        if deliver_stale {
            let rewind = engine.frame(1, LinkBody::Book(book_snapshot(1, 2)));
            prop_assert!(session.on_datagram(&rewind, NOW).is_none());
        }

        let latest = model
            .book(InstrumentId(0))
            .expect("at least one snapshot was accepted");
        prop_assert_eq!(latest.seq, expected_latest);
        prop_assert_eq!(latest.bids[0].price, Price(500 + expected_latest as i64));

        let span = |accepted: &[u64]| match accepted.split_first() {
            Some((first, rest)) => accepted.last().unwrap_or(first) - first - rest.len() as u64,
            None => 0,
        };
        prop_assert_eq!(model.book_gaps(), span(&accepted_books));
        prop_assert_eq!(model.event_gaps(), span(&accepted_events));
    }
}

fn decode_subscribe(bytes: &[u8]) -> (Envelope, Subscribe) {
    let datagram = LinkDatagram::decode(bytes, &guard())
        .expect("a subscribe this session encoded must decode");
    match datagram.body {
        LinkBody::Subscribe(subscribe) => (datagram.envelope, subscribe),
        other => panic!("expected a subscribe, got {other:?}"),
    }
}

fn next_subscribe(session: &mut LinkSession, now: TsUs) -> (Envelope, Subscribe) {
    decode_subscribe(session.subscribe_due(now).expect("a subscribe is due"))
}

/// The whole trust model of the control epoch in one place: 0 is the reserved "no opinion" that lets
/// a workstation watch an engine without taking control away from whoever holds it; asserting yields
/// an epoch strictly above 0, releasing drops the current assertion back to "no opinion" without
/// rewinding the underlying counter, and a later boot stamp (a restarted workstation) must outrank
/// this process's own previous run rather than reuse its epochs. Against an engine's acknowledged
/// epoch, an older one reads as still in flight, a match reads as applied, and a higher one reads as
/// lost to another controller.
#[test]
fn controller_epoch_rises_and_its_verdict_tracks_the_engines_acknowledgement() {
    let mut session = session();

    let (_, watching) = next_subscribe(&mut session, NOW);
    assert_eq!(
        watching.desired_epoch, 0,
        "epoch 0 is the reserved 'no opinion': it is what lets a workstation watch an engine \
         without taking control of it away from whoever holds it"
    );
    let status = session.status(NOW);
    assert_eq!(status.asserted_state, None);
    assert_eq!(status.control, ControlVerdict::NoOpinion);

    session.assert_run_state(RunState::Idle);
    assert_eq!(
        session.status(NOW).control,
        ControlVerdict::Pending,
        "an assertion no heartbeat has acknowledged yet is still in flight"
    );
    let (_, opening) = next_subscribe(&mut session, NOW);
    assert_eq!(opening.desired_state, RunState::Idle);
    assert!(
        opening.desired_epoch > 0,
        "the engine accepts on a strict >, so 0 could never win"
    );

    assert!(session.attach_to_next_peer());
    let status = session.status(NOW);
    assert_eq!(
        status.asserted_state, None,
        "moving to another engine releases the assertion"
    );
    assert_eq!(status.control, ControlVerdict::NoOpinion);
    let (_, released) = next_subscribe(&mut session, NOW);
    assert_eq!(released.desired_epoch, 0);

    session.assert_run_state(RunState::Running);
    let (_, held) = next_subscribe(&mut session, NOW);
    assert!(
        held.desired_epoch > opening.desired_epoch,
        "epochs are per-controller and monotonic: attaching to another engine must not rewind them"
    );

    // A later boot stamp is what makes a restarted workstation outrank its own previous run.
    let mut restarted = session_at(NOW + DurationUs::from_secs(60));
    restarted.assert_run_state(RunState::Idle);
    let (_, outranking) = next_subscribe(&mut restarted, NOW + DurationUs::from_secs(60));
    assert!(outranking.desired_epoch > held.desired_epoch);

    let mut engine = Engine::booted_at(FIRST_BOOT_US);
    let mut session = session_at(NOW);
    session.assert_run_state(RunState::Idle);
    let (_, asserted) = next_subscribe(&mut session, NOW);
    let mine = asserted.desired_epoch;
    let mut verdict_after = |acknowledged_epoch: u64, run_state: RunState| {
        let frame = engine.send(LinkBody::Lifecycle(lifecycle(
            acknowledged_epoch,
            run_state,
        )));
        session.on_datagram(&frame, NOW);
        session.status(NOW).control
    };

    assert_eq!(
        verdict_after(mine - 1, RunState::Running),
        ControlVerdict::Pending,
        "an older acknowledged epoch means our frame has not landed yet"
    );
    assert_eq!(verdict_after(mine, RunState::Idle), ControlVerdict::Applied);
    assert_eq!(
        verdict_after(mine + 5, RunState::Running),
        ControlVerdict::Lost {
            holder_epoch: mine + 5
        },
        "a higher epoch holds the engine, and re-asserting ours would never take it back"
    );
}

/// A subscribe is the only thing the workstation ever sends, and it must be sent at the right
/// moments: at once on attaching, every two seconds after that so the engine's five-second table
/// survives a lost refresh, and immediately when the operator asserts a state, which is what makes
/// START and STOP answer in one round trip instead of up to two seconds later.
#[test]
fn a_subscribe_is_due_at_start_every_two_seconds_and_at_once_after_an_assert() {
    let mut session = session();
    let just_short = DurationUs::from_micros(SUBSCRIBE_INTERVAL.micros() - 1);

    let (envelope, subscribe) = next_subscribe(&mut session, NOW);
    assert_eq!(envelope.topic, TopicId::SUBSCRIBE);
    assert_eq!(envelope.seq, 1);
    assert_eq!(
        envelope.boot_ts_us, NOW,
        "the first clock reading is the boot stamp"
    );
    assert_eq!(
        subscribe.topics.topics(),
        [
            TopicId::BOOKS,
            TopicId::EVENTS,
            TopicId::CATALOG_INSTRUMENTS,
            TopicId::CATALOG_FEATURES,
            TopicId::LIFECYCLE
        ],
        "the workstation names the topics it reads: 'all' would have the engine unicast strategy \
         payload frames at a process that can only count them as rejects"
    );
    assert!(
        session.subscribe_due(NOW).is_none(),
        "one subscribe per interval"
    );
    assert!(session.subscribe_due(NOW + just_short).is_none());

    let refresh = NOW + SUBSCRIBE_INTERVAL;
    let (envelope, _) = next_subscribe(&mut session, refresh);
    assert_eq!(
        envelope.seq, 2,
        "each subscribe numbers itself above the last"
    );

    let asserted_at = refresh + DurationUs::from_millis(500);
    assert!(session.subscribe_due(asserted_at).is_none());
    session.assert_run_state(RunState::Idle);
    let (_, subscribe) = next_subscribe(&mut session, asserted_at);
    assert_eq!(subscribe.desired_state, RunState::Idle);
    assert!(
        subscribe.desired_epoch > 0,
        "the raised epoch rides the very next subscribe"
    );

    assert!(session.subscribe_due(asserted_at + just_short).is_none());
    assert!(
        session
            .subscribe_due(asserted_at + SUBSCRIBE_INTERVAL)
            .is_some()
    );
}

/// Datagrams reach this port from anyone who can send to it. A wrong token or strategy is another
/// deployment's engine, and anything else is noise: none of it may reach the model, and none of it
/// may flip the connection chip to LIVE, which would tell the operator an engine is answering that
/// is not.
#[test]
fn a_frame_that_is_not_from_this_engine_delivers_nothing_and_never_goes_live() {
    let wrong_token = LinkIdentity {
        token_hash: LinkHash::of_name("not-the-token"),
        ..identity(FIRST_BOOT_US)
    };
    let wrong_strategy = LinkIdentity {
        strategy_hash: LinkHash::of_name("strat-another"),
        ..identity(FIRST_BOOT_US)
    };

    for (name, identity) in [("token", wrong_token), ("strategy", wrong_strategy)] {
        let mut engine = Engine::with_identity(identity);
        let mut session = session();
        for body in [
            LinkBody::Book(book_snapshot(1, 500)),
            LinkBody::Event(trade_event(1)),
            heartbeat(0),
        ] {
            let delivery = session.on_datagram(&engine.send(body), NOW);
            assert!(
                delivery.is_none(),
                "a frame with the wrong {name} was delivered"
            );
        }
        let status = session.status(NOW);
        assert_eq!(
            status.connection,
            ConnectionState::Connecting,
            "wrong {name} went live"
        );
        assert_eq!(status.phase, None, "wrong {name} reported a phase");
    }

    let engine = Engine::booted_at(FIRST_BOOT_US);
    let mut flipped_magic = engine.frame(1, LinkBody::Book(book_snapshot(1, 500)));
    flipped_magic[0] ^= 0xFF;
    let garbage = [
        ("truncated", vec![0u8; 8]),
        ("zeroes", vec![0u8; LINK_MAX_DATAGRAM]),
        ("flipped magic", flipped_magic),
    ];
    let mut session = session();
    for (name, bytes) in garbage {
        assert!(
            session.on_datagram(&bytes, NOW).is_none(),
            "{name} was delivered"
        );
    }
    assert_eq!(session.status(NOW).connection, ConnectionState::Connecting);
}

/// The caller's side of the contract: after EVERY datagram it compares the session number, starts
/// its model over when it moved, and only then applies the delivery. Written out here because the
/// ordering is the point: a restarted engine's first frame must land in the fresh model, not be
/// folded into the old run's or dropped with it.
struct Replica {
    session: LinkSession,
    model: UiModel,
    held_session: u64,
    resets: u32,
}

impl Replica {
    fn attach() -> Self {
        let session = session();
        let held_session = session.status(NOW).session;
        Self {
            session,
            model: fresh_model(),
            held_session,
            resets: 0,
        }
    }

    fn deliver(&mut self, bytes: &[u8]) -> Option<Delivery> {
        let delivery = self.session.on_datagram(bytes, NOW);
        let current = self.session.status(NOW).session;
        if current != self.held_session {
            self.held_session = current;
            self.model = fresh_model();
            self.resets += 1;
        }
        match &delivery {
            Some(Delivery::Book(snapshot)) => self.model.apply_book(*snapshot),
            Some(Delivery::Event(event)) => self.model.apply_event(*event),
            Some(Delivery::Catalog(_)) | None => {}
        }
        delivery
    }
}

fn fresh_model() -> UiModel {
    UiModel::with_capacity(1, SPIN_INTERVAL)
}

/// A restarted engine restarts its sequence numbers at 1, so its frames look like a rewind of the
/// old run. The boot stamp is what tells them apart. The session must move on BEFORE it hands back
/// the new run's first frame, so the model the caller resets is the one that frame lands in.
#[test]
fn a_new_boot_raises_the_session_and_its_first_frame_belongs_to_the_new_run() {
    let mut replica = Replica::attach();
    let mut old_run = Engine::booted_at(FIRST_BOOT_US);
    let announced = catalog_frames(&mut old_run, CATALOG_EPOCH);
    let old_catalogs: Vec<UiCatalog> = announced
        .iter()
        .filter_map(|frame| catalog_of(replica.deliver(frame)))
        .collect();
    assert_eq!(old_catalogs.len(), 1, "the old run's catalog is delivered");
    for seq in 1..=3 {
        let book = LinkBody::Book(book_snapshot(seq, 500 + seq as i64));
        assert!(replica.deliver(&old_run.send(book)).is_some());
    }
    assert_eq!(
        replica
            .model
            .book(InstrumentId(0))
            .expect("old run's book")
            .seq,
        3
    );
    let before = replica.held_session;

    let mut new_run = Engine::booted_at(SECOND_BOOT_US);
    let first = replica.deliver(&new_run.send(LinkBody::Book(book_snapshot(1, 900))));

    assert!(
        matches!(first, Some(Delivery::Book(_))),
        "the restarted run's first frame must be delivered, not lost to the reset"
    );
    assert_eq!(replica.resets, 1);
    assert_eq!(
        replica.held_session,
        before + 1,
        "a new boot raises the session"
    );
    assert_eq!(
        replica.session.status(NOW).phase,
        None,
        "the old run's heartbeat does not outlive it"
    );
    let book = replica
        .model
        .book(InstrumentId(0))
        .expect("the new run's book");
    assert_eq!(
        book.seq, 1,
        "the model holds the new run, not the old one's seq 3"
    );
    assert_eq!(book.bids[0].price, Price(900));
    assert_eq!(replica.model.book_gaps(), 0);

    // The gate is per topic, so a straggler on a topic the new run has not used yet gets past it;
    // only the run check refuses it.
    let straggler = old_run.send(LinkBody::Book(book_snapshot(4, 777)));
    assert!(
        replica.deliver(&straggler).is_none(),
        "a book from the previous boot is refused"
    );
    let late_trade = old_run.send(LinkBody::Event(trade_event(1)));
    assert!(
        replica.deliver(&late_trade).is_none(),
        "a trade from the previous boot is refused"
    );
    assert_eq!(
        replica.held_session,
        before + 1,
        "a straggler must not reset anything"
    );
    assert_eq!(replica.model.book(InstrumentId(0)).expect("book").seq, 1);

    let catalogs: Vec<UiCatalog> = catalog_frames(&mut new_run, CATALOG_EPOCH)
        .iter()
        .filter_map(|frame| catalog_of(replica.deliver(frame)))
        .collect();
    assert_eq!(
        catalogs.len(),
        1,
        "the new run announces the same catalog epoch and still gets it delivered, once"
    );

    // An engine restarted on a host whose clock sits behind stamps its catalog with an OLDER epoch,
    // and may declare a different set of features. The previous run's assembly must neither shut
    // it out nor leak its own items into what is delivered.
    let mut third_run = Engine::booted_at(THIRD_BOOT_US);
    let earlier = CATALOG_EPOCH - DurationUs::from_secs(1);
    let catalogs: Vec<UiCatalog> = [
        heartbeat(2),
        LinkBody::CatalogInstrument(instrument_frame(0, 1, earlier)),
        LinkBody::CatalogFeature(feature_frame(0, 2, earlier)),
        LinkBody::CatalogFeature(feature_frame(1, 2, earlier)),
    ]
    .into_iter()
    .filter_map(|body| catalog_of(replica.deliver(&third_run.send(body))))
    .collect();
    assert_eq!(
        catalogs.len(),
        1,
        "a restarted run's catalog is accepted whatever its epoch"
    );
    assert_eq!(
        catalogs[0].feature_names.len(),
        2,
        "the catalog is the third run's own"
    );
}

/// Moving to another engine starts everything about the first one over: its sequence gate (the
/// second engine numbers from 1 as well), its catalog (which must be delivered again for the new
/// peer, not suppressed as already seen) and the assertion (a START asserted against one engine
/// must never silently follow the operator to another).
#[test]
fn moving_to_the_next_peer_starts_the_gate_the_catalog_and_the_controller_over() {
    let mut session = session();
    let first_peer = session.status(NOW).peer;
    let settle = |engine: &mut Engine, session: &mut LinkSession| {
        let catalogs = catalog_frames(engine, CATALOG_EPOCH)
            .iter()
            .filter_map(|frame| catalog_of(session.on_datagram(frame, NOW)))
            .count();
        let book = session.on_datagram(&engine.send(LinkBody::Book(book_snapshot(1, 500))), NOW);
        (catalogs, book.is_some())
    };
    assert_eq!(
        settle(&mut Engine::booted_at(FIRST_BOOT_US), &mut session),
        (1, true)
    );
    session.assert_run_state(RunState::Idle);
    let before = session.status(NOW);
    assert_eq!(before.control, ControlVerdict::Pending);
    let (first_subscribe, _) = next_subscribe(&mut session, NOW);
    assert_eq!(first_subscribe.seq, 1);

    assert!(session.attach_to_next_peer());

    assert_eq!(session.active_peer(), 1);
    let after = session.status(NOW);
    assert_ne!(after.peer, first_peer);
    assert_eq!((after.peer_index, after.peer_count), (1, 2));
    assert_eq!(after.session, before.session + 1);
    assert_eq!(
        after.connection,
        ConnectionState::Connecting,
        "nothing has been heard from the new peer"
    );
    assert_eq!(
        after.phase, None,
        "the previous peer's heartbeat is not the new peer's"
    );
    assert_eq!(after.asserted_state, None);
    assert_eq!(after.control, ControlVerdict::NoOpinion);

    let (envelope, subscribe) = next_subscribe(&mut session, NOW);
    assert_eq!(
        envelope.seq,
        first_subscribe.seq + 1,
        "the subscribe count keeps rising across a move, because an engine remembers the highest it heard"
    );
    assert_eq!(subscribe.desired_epoch, 0, "the assertion was released");

    // The same positions the first engine used: stale to an old gate, fresh to a reset one.
    assert_eq!(
        settle(&mut Engine::booted_at(FIRST_BOOT_US), &mut session),
        (1, true),
        "the new peer's frames are admitted and its catalog is delivered again"
    );

    // Another engine entirely, one that booted BEFORE the first. A run check left over from the
    // previous peer would refuse every one of its frames as stale, for good.
    assert!(session.attach_to_next_peer());
    let older_engine = LinkIdentity {
        sender_te_hash: LinkHash::of_name("te-other"),
        ..identity(FIRST_BOOT_US - 500)
    };
    assert_eq!(
        settle(&mut Engine::with_identity(older_engine), &mut session),
        (1, true),
        "an engine that booted earlier than the last peer's is still a live engine"
    );
}

/// An engine remembers the highest subscribe number it has heard from a sender for as long as that
/// sender keeps its boot stamp, and the workstation keeps both across a move between engines. A
/// subscribe numbered from the start on returning would be refused as stale, silently, and the page
/// would sit on CONNECTING until the count overtook the old one.
#[test]
fn returning_to_an_engine_already_attached_is_heard_at_once() {
    let mut session = session();
    let mut first_engine = SequenceGate::new();
    let mut second_engine = SequenceGate::new();
    let mut now = NOW;
    let mut subscribe_to = |session: &mut LinkSession, engine: &mut SequenceGate| {
        let sent_at = now;
        now = now + SUBSCRIBE_INTERVAL;
        let (envelope, _) = next_subscribe(session, sent_at);
        engine.admit(&envelope, sent_at)
    };

    for _ in 0..3 {
        assert_eq!(
            subscribe_to(&mut session, &mut first_engine),
            GateVerdict::Accepted
        );
    }
    assert!(session.attach_to_next_peer());
    for _ in 0..2 {
        assert_eq!(
            subscribe_to(&mut session, &mut second_engine),
            GateVerdict::Accepted
        );
    }
    assert!(session.attach_to_next_peer());
    assert_eq!(session.active_peer(), 0);

    assert_eq!(
        subscribe_to(&mut session, &mut first_engine),
        GateVerdict::Accepted,
        "the first engine still holds the old count for this sender"
    );
}

#[test]
fn with_a_single_peer_there_is_nowhere_to_move_and_nothing_resets() {
    let mut session = LinkSession::new(target(peers()[..1].to_vec()), NOW).expect("one peer");
    session.assert_run_state(RunState::Idle);
    let before = session.status(NOW);

    assert!(!session.attach_to_next_peer());

    assert_eq!(session.status(NOW), before);
    assert_eq!(session.active_peer(), 0);
}

#[test]
fn a_target_that_names_no_peer_is_refused() {
    let outcome = LinkSession::new(target(Vec::new()), NOW);
    assert!(matches!(outcome, Err(LinkSessionError::NoPeers)));
}

/// UDP silence looks the same as an engine with nothing to say, so the connection chip must age
/// from LIVE to STALE on the caller's clock alone, at exactly the engine's subscription lifetime,
/// and recover on the next frame.
#[test]
fn connection_reads_connecting_then_live_then_stale_and_live_again() {
    let mut engine = Engine::booted_at(FIRST_BOOT_US);
    let mut session = session();
    assert_eq!(session.status(NOW).connection, ConnectionState::Connecting);

    session.on_datagram(&engine.send(heartbeat(0)), NOW);
    assert_eq!(session.status(NOW).connection, ConnectionState::Live);
    let just_inside = NOW + SILENCE_LIMIT - DurationUs::RESOLUTION;
    assert_eq!(
        session.status(just_inside).connection,
        ConnectionState::Live
    );
    assert_eq!(
        session.status(NOW + SILENCE_LIMIT).connection,
        ConnectionState::Stale
    );

    let back = NOW + DurationUs::from_secs(6);
    session.on_datagram(&engine.send(heartbeat(0)), back);
    assert_eq!(session.status(back).connection, ConnectionState::Live);
}

#[test]
fn heartbeats_report_the_engines_phase_and_run_state() {
    let mut engine = Engine::booted_at(FIRST_BOOT_US);
    let mut session = session();
    let before = session.status(NOW);
    assert_eq!((before.phase, before.reported_state), (None, None));

    let draining = Lifecycle {
        phase: RunPhase::Draining,
        ..lifecycle(0, RunState::Idle)
    };
    session.on_datagram(&engine.send(LinkBody::Lifecycle(draining)), NOW);

    let status = session.status(NOW);
    assert_eq!(status.phase, Some(RunPhase::Draining));
    assert_eq!(status.reported_state, Some(RunState::Idle));
}

/// Two workstations that start in the same instant, or one that restarts, must not present to the
/// engine as one sender: the engine's gate keys on the sender hash, and a collision would read one
/// tab's subscribes as another's replays.
#[test]
fn workstations_that_start_together_present_as_different_senders() {
    let sender_of = |nonce: u64, now: TsUs| {
        let mut other = target(peers());
        other.sender_nonce = nonce;
        let mut session = LinkSession::new(other, now).expect("two peers");
        next_subscribe(&mut session, now).0.sender_te_hash
    };

    let first = sender_of(1, NOW);
    assert_eq!(
        first,
        sender_of(1, NOW),
        "the same start presents as the same sender"
    );
    assert_ne!(
        first,
        sender_of(2, NOW),
        "a different nonce is a different sender"
    );
    assert_ne!(
        first,
        sender_of(1, NOW + DurationUs::RESOLUTION),
        "a later boot is a different sender"
    );
}

/// The token is the only thing keeping another deployment's engine out of this workstation, so the
/// session must speak, and demand, the one its target names rather than a default.
#[test]
fn the_session_speaks_and_demands_the_token_the_target_names() {
    let token_hash = LinkHash::of_name("hunter2");
    let mut named = target(peers());
    named.token_hash = token_hash;
    let mut session = LinkSession::new(named, NOW).expect("two peers");

    let speaks_it = Engine::with_identity(LinkIdentity {
        token_hash,
        ..identity(FIRST_BOOT_US)
    });
    let bare = Engine::booted_at(FIRST_BOOT_US);
    let book = LinkBody::Book(book_snapshot(1, 500));
    assert!(
        session
            .on_datagram(&speaks_it.frame(1, book), NOW)
            .is_some()
    );
    let intruder = bare.frame(2, LinkBody::Book(book_snapshot(2, 501)));
    assert!(
        session.on_datagram(&intruder, NOW).is_none(),
        "an empty token is not this token"
    );

    let bytes = session.subscribe_due(NOW).expect("due at start");
    let engines_guard = FrameGuard {
        token_hash,
        ..guard()
    };
    LinkDatagram::decode(bytes, &engines_guard).expect("the subscribe carries the named token");
}

/// The sequence gate has one slot per sender and topic and ages them on the time the caller passes.
/// A workstation that has filled it still admits a newcomer once the oldest sender has been quiet
/// for the engine's subscription lifetime, and not before.
#[test]
fn the_sequence_gate_ages_its_slots_on_the_callers_clock() {
    let mut session = session();
    for sender in 0..LINK_MAX_GATE_KEYS {
        let mut engine = Engine::with_identity(LinkIdentity {
            sender_te_hash: LinkHash::of_name(&format!("te-{sender}")),
            ..identity(FIRST_BOOT_US)
        });
        let book = LinkBody::Book(book_snapshot(1, 500));
        assert!(session.on_datagram(&engine.send(book), NOW).is_some());
    }
    let mut newcomer = Engine::with_identity(LinkIdentity {
        sender_te_hash: LinkHash::of_name("te-newcomer"),
        ..identity(FIRST_BOOT_US)
    });
    let bytes = newcomer.send(LinkBody::Book(book_snapshot(1, 500)));

    assert!(
        session.on_datagram(&bytes, NOW).is_none(),
        "every slot is fresh, so none can be given up"
    );
    assert!(
        session.on_datagram(&bytes, NOW + SILENCE_LIMIT).is_some(),
        "the quiet slots have aged out on the caller's clock"
    );
}
