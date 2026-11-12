//! Link wire fitness: every frame kind survives an encode-then-decode round trip unchanged,
//! every envelope rejection reports a distinct cause, and the sequence gate drops duplicates
//! and reorders while letting a restarted peer back in. A fixed frame of every kind also
//! encodes to the exact bytes checked in as `link_frames.txt`, which no round trip can pin.
//! The failure this suite exists to prevent is silent: a trading engine and a separately built
//! UI misdecoding each other's bytes into a plausible-looking book ladder.

use std::net::SocketAddr;

use polysim::config::ExecutionMode;
use polysim::hot::exec::{
    CloseReason, ExecHalt, HaltReason, OrderState, QuoteLevel, ReadinessGap, RejectOrigin,
    RejectReason,
};
use polysim::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side};
use polysim::link::{
    CatalogFeature, CatalogInstrument, Envelope, FrameGuard, GateVerdict, LINK_MAGIC,
    LINK_MAX_DATAGRAM, LINK_MAX_FIELDS, LINK_MAX_GATE_KEYS, LINK_MAX_SUBSCRIBERS, LINK_MAX_TOPICS,
    LINK_SUBSCRIPTION_TTL, LINK_VERSION, Lifecycle, LinkBody, LinkDatagram, LinkDecodeError,
    LinkHash, LinkIdentity, LinkPayload, RefreshOutcome, RunPhase, RunState, SequenceGate,
    Subscribe, SubscriberTable, TopicId, TopicSet, WireName,
};
use polysim::msg::exec::{Liquidity, RejectClass};
use polysim::msg::inbound::Level;
use polysim::msg::persist::FeatureId;
use polysim::msg::ui::{
    DomQuote, UI_BOOK_LEVELS, UI_ORDER_SNAPSHOT_CAPACITY, UI_ORDER_SNAPSHOT_MAX_TOTAL,
    UiBookSnapshot, UiBookState, UiEvent, UiLatencyCell, UiLatencyRow, UiLatencySummary,
    UiWorkingOrder,
};
use polysim::time::{DurationUs, TsUs};
use proptest::prelude::*;

const TOKEN: LinkHash = LinkHash::of_name("fitness-run-token");
const STRATEGY: LinkHash = LinkHash::of_name("strat-micro-recorder");
const SCHEMA: LinkHash = LinkHash::of_fields(&["mid", "microprice", "imbalance"]);
const SENDER: LinkHash = LinkHash::of_name("te-binance-spot-btcusdt");

/// The topic field sits at bytes 6..8 of the envelope. Patching the wire is the only way to build a
/// datagram this build cannot construct through its own types — which is exactly what a peer on a
/// newer wire does.
const TOPIC_OFFSET: usize = 6;
/// `count` follows the 80-byte envelope, the 8-byte schema hash and the 8-byte event stamp.
const PAYLOAD_COUNT_OFFSET: usize = 96;
/// `bid_len` follows the 80-byte envelope, the 2-byte instrument, the 8-byte sequence, the 8-byte
/// event stamp and the 1-byte book state; `ask_len` is the two bytes after it.
const BOOK_BID_LEN_OFFSET: usize = 99;

fn guard() -> FrameGuard {
    FrameGuard {
        token_hash: TOKEN,
        strategy_hash: STRATEGY,
        schema_hash: SCHEMA,
    }
}

fn identity() -> LinkIdentity {
    LinkIdentity {
        token_hash: TOKEN,
        strategy_hash: STRATEGY,
        sender_te_hash: SENDER,
        boot_ts_us: TsUs::from_micros(1_700_000_000_000_000),
    }
}

fn envelope(topic: TopicId, seq: u64) -> Envelope {
    Envelope::new(identity(), topic, seq)
}

fn encode(datagram: &LinkDatagram) -> Vec<u8> {
    let mut buffer = [0; LINK_MAX_DATAGRAM];
    let len = datagram.encode(&mut buffer);
    buffer[..len].to_vec()
}

fn decode(bytes: &[u8]) -> Result<LinkDatagram, LinkDecodeError> {
    LinkDatagram::decode(bytes, &guard())
}

fn round_trip(datagram: &LinkDatagram) -> Result<LinkDatagram, LinkDecodeError> {
    decode(&encode(datagram))
}

fn book_datagram() -> LinkDatagram {
    LinkDatagram {
        envelope: envelope(TopicId::BOOKS, 1),
        body: LinkBody::Book(UiBookSnapshot {
            instrument: InstrumentId(0),
            seq: 7,
            event_ts_us: TsUs::from_micros(42),
            state: UiBookState::Valid,
            bid_len: 1,
            ask_len: 1,
            bids: [Level {
                price: Price(100),
                qty: Qty(5),
            }; UI_BOOK_LEVELS],
            asks: [Level {
                price: Price(101),
                qty: Qty(5),
            }; UI_BOOK_LEVELS],
        }),
    }
}

fn event_datagram(event: UiEvent) -> LinkDatagram {
    LinkDatagram {
        envelope: envelope(TopicId::EVENTS, 1),
        body: LinkBody::Event(event),
    }
}

fn order_snapshot(detail_len: u8, total_working: u16, details: &[UiWorkingOrder]) -> UiEvent {
    let mut orders = [UiWorkingOrder::EMPTY; UI_ORDER_SNAPSHOT_CAPACITY];
    orders[..details.len()].copy_from_slice(details);
    UiEvent::OrderSnapshot {
        instrument: InstrumentId(3),
        seq: 41,
        event_ts_us: TsUs::from_micros(77),
        side: Side::Buy,
        detail_len,
        total_working,
        orders,
    }
}

fn working_order(client_id: u64, state: OrderState) -> UiWorkingOrder {
    UiWorkingOrder {
        client_id: ClientOrderId(client_id),
        quote_level: QuoteLevel::new((client_id % 8) as u8),
        state,
        price: Price(100 + client_id as i64),
        qty: Qty(5),
        filled: Qty(1),
    }
}

fn payload_datagram(schema_hash: LinkHash) -> LinkDatagram {
    LinkDatagram {
        envelope: envelope(TopicId::FIRST_STRATEGY, 1),
        body: LinkBody::Payload(LinkPayload::new(
            schema_hash,
            TsUs::from_micros(99),
            &[1.5, -2.5],
        )),
    }
}

fn arb_envelope(topic: TopicId) -> impl Strategy<Value = Envelope> {
    (
        any::<u64>(),
        any::<i64>(),
        any::<u64>(),
        prop::collection::vec(any::<u8>(), 32),
    )
        .prop_map(move |(sender, boot, seq, mac)| {
            let mut envelope = Envelope::new(
                LinkIdentity {
                    token_hash: TOKEN,
                    strategy_hash: STRATEGY,
                    sender_te_hash: LinkHash(sender),
                    boot_ts_us: TsUs::from_micros(boot),
                },
                topic,
                seq,
            );
            envelope.mac = mac.try_into().expect("32 generated bytes");
            envelope
        })
}

fn arb_topic_set() -> impl Strategy<Value = TopicSet> {
    prop::collection::vec(any::<u16>().prop_map(TopicId), 0..=LINK_MAX_TOPICS)
        .prop_map(|topics| TopicSet::new(&topics).expect("generated within capacity"))
}

fn arb_run_state() -> impl Strategy<Value = RunState> {
    prop_oneof![Just(RunState::Running), Just(RunState::Idle)]
}

fn arb_levels() -> impl Strategy<Value = [Level; UI_BOOK_LEVELS]> {
    prop::collection::vec(
        (any::<i64>(), any::<i64>()).prop_map(|(price, qty)| Level {
            price: Price(price),
            qty: Qty(qty),
        }),
        UI_BOOK_LEVELS,
    )
    .prop_map(|levels| levels.try_into().expect("generated exactly one side"))
}

fn arb_optional_level() -> impl Strategy<Value = Option<(Price, Qty)>> {
    prop::option::of((any::<i64>(), any::<i64>()).prop_map(|(price, qty)| (Price(price), Qty(qty))))
}

fn arb_side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

/// The float domain the round-trip generator puts on the wire. `any::<f64>()` alone is
/// `POSITIVE|NEGATIVE|ZERO|SUBNORMAL|NORMAL` (proptest's `Arbitrary` impl, which deliberately omits
/// `INFINITE` and `QUIET_NAN`), so no non-finite value would ever ride an event body — while a quant
/// feature genuinely can be infinite, as Kyle's lambda has already shipped. The infinities are added
/// back here. NaN cannot join them: it is unequal to itself, so NO structural round-trip can express
/// it however perfect the codec; [`event_floats_preserve_exact_bits`] covers that half bitwise.
fn arb_wire_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        8 => any::<f64>(),
        1 => Just(f64::INFINITY),
        1 => Just(f64::NEG_INFINITY),
    ]
}

/// Every non-finite bit pattern: a sign, an all-ones exponent, and an arbitrary mantissa — zero for
/// an infinity, anything else for a NaN, quiet or signalling.
fn arb_non_finite_bits() -> impl Strategy<Value = u64> {
    (any::<bool>(), 0u64..(1 << 52)).prop_map(|(negative, mantissa)| {
        (u64::from(negative) << 63) | 0x7ff0_0000_0000_0000 | mantissa
    })
}

/// Half arbitrary patterns, half deliberately non-finite. A uniform `any::<u64>()` lands on an
/// all-ones exponent once in 2048 draws, so the case this generator exists to reach would be absent
/// from most runs — measured, not assumed: an encoder mutated to normalise non-finite values passed
/// the bit test while it drew from `any::<u64>()` alone.
fn arb_f64_bits() -> impl Strategy<Value = u64> {
    prop_oneof![any::<u64>(), arb_non_finite_bits()]
}

fn arb_name() -> impl Strategy<Value = WireName> {
    "[a-z_]{0,32}".prop_map(|name| WireName::new(&name))
}

fn arb_book() -> impl Strategy<Value = LinkBody> {
    (
        any::<u16>(),
        any::<u64>(),
        any::<i64>(),
        prop_oneof![
            Just(UiBookState::AwaitingSnapshot),
            Just(UiBookState::Valid)
        ],
        0..=UI_BOOK_LEVELS as u16,
        0..=UI_BOOK_LEVELS as u16,
        arb_levels(),
        arb_levels(),
    )
        .prop_map(
            |(instrument, seq, stamp, state, bid_len, ask_len, bids, asks)| {
                LinkBody::Book(UiBookSnapshot {
                    instrument: InstrumentId(instrument),
                    seq,
                    event_ts_us: TsUs::from_micros(stamp),
                    state,
                    bid_len,
                    ask_len,
                    bids,
                    asks,
                })
            },
        )
}

/// Every order state, including all five close reasons: the wire flattens `Closed(_)` into the same
/// byte as the open states to fit the tail, so a reason that failed to round-trip would silently
/// report a cancel as a fill.
fn arb_order_state() -> impl Strategy<Value = OrderState> {
    prop::sample::select(EVERY_ORDER_STATE.to_vec())
}

fn arb_quote_level() -> impl Strategy<Value = Option<QuoteLevel>> {
    prop_oneof![Just(None), (0_u8..8).prop_map(QuoteLevel::new),]
}

fn arb_liquidity() -> impl Strategy<Value = Option<Liquidity>> {
    prop_oneof![
        Just(None),
        Just(Some(Liquidity::Maker)),
        Just(Some(Liquidity::Taker)),
    ]
}

const REJECT_REASONS: [RejectReason; 19] = [
    RejectReason::QtyBelowMin,
    RejectReason::NotionalBelowMin,
    RejectReason::NotionalAboveMax,
    RejectReason::WouldCross,
    RejectReason::OutsideBand,
    RejectReason::Underfunded,
    RejectReason::StyleNotPermitted,
    RejectReason::NotReady(ReadinessGap::Stream),
    RejectReason::NotReady(ReadinessGap::Balances),
    RejectReason::NotReady(ReadinessGap::OpenOrders),
    RejectReason::Halted,
    RejectReason::SessionReducingOnly,
    RejectReason::ExposureCeiling,
    RejectReason::NoQuoteDeclared,
    RejectReason::BookNotQuotable,
    RejectReason::DuplicatePrice,
    RejectReason::OrderLimit,
    RejectReason::OutsideWindow,
    RejectReason::RateBudget,
];

/// The slot each refusal claims in [`REJECT_REASONS`], which is what stops that list rotting into a
/// subset of the enum. A new `RejectReason` lands here as a missing arm, and the arm it needs
/// indexes one past the array, so it will not compile until the array grows with it and the
/// round-trip below starts generating it.
///
/// The `const` blocks are load-bearing, not decoration: a bare `REJECT_REASONS[19]` compiles, since
/// the out-of-bounds lint does not reach a const of this shape. Only const evaluation refuses it.
fn listed_as(reason: RejectReason) -> RejectReason {
    match reason {
        RejectReason::QtyBelowMin => const { REJECT_REASONS[0] },
        RejectReason::NotionalBelowMin => const { REJECT_REASONS[1] },
        RejectReason::NotionalAboveMax => const { REJECT_REASONS[2] },
        RejectReason::WouldCross => const { REJECT_REASONS[3] },
        RejectReason::OutsideBand => const { REJECT_REASONS[4] },
        RejectReason::Underfunded => const { REJECT_REASONS[5] },
        RejectReason::StyleNotPermitted => const { REJECT_REASONS[6] },
        RejectReason::NotReady(ReadinessGap::Stream) => const { REJECT_REASONS[7] },
        RejectReason::NotReady(ReadinessGap::Balances) => const { REJECT_REASONS[8] },
        RejectReason::NotReady(ReadinessGap::OpenOrders) => const { REJECT_REASONS[9] },
        RejectReason::Halted => const { REJECT_REASONS[10] },
        RejectReason::SessionReducingOnly => const { REJECT_REASONS[11] },
        RejectReason::ExposureCeiling => const { REJECT_REASONS[12] },
        RejectReason::NoQuoteDeclared => const { REJECT_REASONS[13] },
        RejectReason::BookNotQuotable => const { REJECT_REASONS[14] },
        RejectReason::DuplicatePrice => const { REJECT_REASONS[15] },
        RejectReason::OrderLimit => const { REJECT_REASONS[16] },
        RejectReason::OutsideWindow => const { REJECT_REASONS[17] },
        RejectReason::RateBudget => const { REJECT_REASONS[18] },
    }
}

#[test]
fn every_reject_reason_claims_its_own_slot() {
    for (slot, reason) in REJECT_REASONS.into_iter().enumerate() {
        assert_eq!(
            listed_as(reason),
            reason,
            "reject reason at slot {slot} disagrees with the slot it claims, so at least one \
             refusal is generated twice and another not at all"
        );
    }
}

/// Both refusal origins. The local arm pads the venue arm's code field, so a decoder that read the
/// padding as a code would report a venue rejection that never happened.
fn arb_reject_origin() -> impl Strategy<Value = RejectOrigin> {
    prop_oneof![
        prop::sample::select(REJECT_REASONS.to_vec()).prop_map(RejectOrigin::Local),
        (
            prop_oneof![
                Just(RejectClass::StillLive),
                Just(RejectClass::Refused),
                Just(RejectClass::Gone),
                Just(RejectClass::Ambiguous),
                Just(RejectClass::Fatal),
            ],
            any::<i32>()
        )
            .prop_map(|(class, code)| RejectOrigin::Venue { class, code }),
    ]
}

fn arb_halt() -> impl Strategy<Value = ExecHalt> {
    prop_oneof![
        Just(ExecHalt::Armed),
        (
            prop_oneof![
                Just(HaltReason::RejectStreak),
                Just(HaltReason::RealisedLoss),
                Just(HaltReason::FatalReject),
                Just(HaltReason::SlotLeak),
                Just(HaltReason::FilterViolation),
                Just(HaltReason::CommandBankOverflow),
                Just(HaltReason::DuplicateResting),
            ],
            any::<i64>()
        )
            .prop_map(|(reason, at)| ExecHalt::Halted {
                reason,
                halted_ts_us: TsUs::from_micros(at)
            }),
    ]
}

/// Counts stay inside `u32` because that is what the tail carries: the encoder saturates above it,
/// and a generator that drew wider would be asserting a round-trip the wire never promised.
fn arb_latency_cell() -> impl Strategy<Value = UiLatencyCell> {
    (any::<u32>(), any::<i64>()).prop_map(|(count, sum_us)| UiLatencyCell {
        count: u64::from(count),
        sum_us,
    })
}

fn arb_latency_row() -> impl Strategy<Value = UiLatencyRow> {
    (
        arb_latency_cell(),
        arb_latency_cell(),
        arb_latency_cell(),
        arb_latency_cell(),
        arb_latency_cell(),
        arb_latency_cell(),
    )
        .prop_map(
            |(
                exchange_to_received,
                received_to_queued,
                queue_wait,
                processing,
                end_to_end,
                order_round_trip,
            )| UiLatencyRow {
                exchange_to_received,
                received_to_queued,
                queue_wait,
                processing,
                end_to_end,
                order_round_trip,
            },
        )
}

fn arb_latency() -> impl Strategy<Value = UiLatencySummary> {
    (
        arb_latency_row(),
        arb_latency_row(),
        arb_latency_row(),
        prop::option::of(arb_wire_f64()),
    )
        .prop_map(
            |(market_data, execution, hot_path, backlog_ema)| UiLatencySummary {
                market_data,
                execution,
                hot_path,
                backlog_ema,
            },
        )
}

fn arb_event() -> impl Strategy<Value = LinkBody> {
    let head = (any::<u16>(), any::<u64>(), any::<i64>());
    prop_oneof![
        (head, arb_optional_level(), arb_optional_level()).prop_map(
            |((instrument, seq, stamp), bid, ask)| UiEvent::Quote {
                instrument: InstrumentId(instrument),
                seq,
                event_ts_us: TsUs::from_micros(stamp),
                quote: DomQuote::top(bid, ask),
            }
        ),
        (head, arb_side(), any::<i64>(), any::<i64>()).prop_map(
            |((instrument, seq, stamp), aggressor, price, qty)| UiEvent::Trade {
                instrument: InstrumentId(instrument),
                seq,
                event_ts_us: TsUs::from_micros(stamp),
                aggressor,
                price: Price(price),
                qty: Qty(qty),
            }
        ),
        (
            head,
            any::<u64>(),
            arb_quote_level(),
            arb_side(),
            arb_order_state(),
            (any::<i64>(), any::<i64>(), any::<i64>())
        )
            .prop_map(
                |(
                    (instrument, seq, stamp),
                    client_id,
                    quote_level,
                    side,
                    state,
                    (price, qty, filled),
                )| {
                    UiEvent::OrderUpdate {
                        instrument: InstrumentId(instrument),
                        seq,
                        event_ts_us: TsUs::from_micros(stamp),
                        client_id: ClientOrderId(client_id),
                        quote_level,
                        side,
                        state,
                        price: Price(price),
                        qty: Qty(qty),
                        filled: Qty(filled),
                    }
                }
            ),
        (head, any::<u16>(), any::<i64>(), any::<i64>()).prop_map(
            |((_, seq, stamp), asset, free, locked)| UiEvent::Balance {
                asset: AssetId(asset),
                seq,
                event_ts_us: TsUs::from_micros(stamp),
                free,
                locked,
            }
        ),
        (head, arb_side(), arb_reject_origin()).prop_map(
            |((instrument, seq, stamp), side, origin)| UiEvent::Reject {
                instrument: InstrumentId(instrument),
                seq,
                event_ts_us: TsUs::from_micros(stamp),
                side,
                origin,
            }
        ),
        (any::<u64>(), any::<i64>(), arb_halt()).prop_map(|(seq, stamp, halt)| {
            UiEvent::Execution {
                seq,
                event_ts_us: TsUs::from_micros(stamp),
                halt,
            }
        }),
        (head, any::<u16>(), arb_wire_f64()).prop_map(
            |((instrument, seq, stamp), feature, value)| UiEvent::Feature {
                instrument: InstrumentId(instrument),
                seq,
                event_ts_us: TsUs::from_micros(stamp),
                feature: FeatureId(feature),
                value,
            }
        ),
        (
            head,
            arb_quote_level(),
            arb_side(),
            (any::<i64>(), any::<i64>(), any::<i64>()),
            any::<u16>(),
            arb_liquidity()
        )
            .prop_map(
                |(
                    (instrument, seq, stamp),
                    quote_level,
                    side,
                    (price, qty, commission),
                    commission_asset,
                    liquidity,
                )| UiEvent::Fill {
                    instrument: InstrumentId(instrument),
                    seq,
                    event_ts_us: TsUs::from_micros(stamp),
                    quote_level,
                    side,
                    price: Price(price),
                    qty: Qty(qty),
                    commission,
                    commission_asset: AssetId(commission_asset),
                    liquidity,
                }
            ),
        head.prop_map(|(instrument, seq, stamp)| UiEvent::Rotation {
            instrument: InstrumentId(instrument),
            seq,
            event_ts_us: TsUs::from_micros(stamp),
        }),
        (head, arb_wire_f64(), arb_wire_f64()).prop_map(
            |((instrument, seq, stamp), exposure_quote, pnl_quote)| UiEvent::Position {
                instrument: InstrumentId(instrument),
                seq,
                event_ts_us: TsUs::from_micros(stamp),
                exposure_quote,
                pnl_quote,
            }
        ),
        (any::<u64>(), any::<i64>(), arb_latency()).prop_map(|(seq, stamp, summary)| {
            UiEvent::Latency {
                seq,
                event_ts_us: TsUs::from_micros(stamp),
                summary,
            }
        }),
    ]
    .prop_map(LinkBody::Event)
}

fn arb_catalog_instrument() -> impl Strategy<Value = LinkBody> {
    (
        any::<i64>(),
        any::<u16>(),
        any::<u16>(),
        arb_name(),
        prop::option::of(any::<i64>()),
        prop::option::of(any::<i64>()),
        any::<i64>(),
        (any::<u16>(), any::<u16>(), arb_name(), arb_name()),
    )
        .prop_map(
            |(
                catalog_ts_us,
                total_count,
                instrument,
                display,
                tick,
                lot,
                qty_scale,
                (base_asset, quote_asset, base, quote),
            )| {
                LinkBody::CatalogInstrument(CatalogInstrument {
                    catalog_ts_us: TsUs::from_micros(catalog_ts_us),
                    total_count,
                    instrument: InstrumentId(instrument),
                    display,
                    tick_size: tick.map(Price),
                    lot_size: lot.map(Qty),
                    qty_scale,
                    base_asset: AssetId(base_asset),
                    quote_asset: AssetId(quote_asset),
                    base,
                    quote,
                })
            },
        )
}

fn arb_datagram() -> impl Strategy<Value = LinkDatagram> {
    prop_oneof![
        (
            arb_envelope(TopicId::SUBSCRIBE),
            arb_topic_set(),
            arb_run_state(),
            any::<u64>()
        )
            .prop_map(
                |(envelope, topics, desired_state, desired_epoch)| LinkDatagram {
                    envelope,
                    body: LinkBody::Subscribe(Subscribe {
                        topics,
                        desired_state,
                        desired_epoch,
                    }),
                }
            ),
        (arb_envelope(TopicId::BOOKS), arb_book())
            .prop_map(|(envelope, body)| LinkDatagram { envelope, body }),
        (arb_envelope(TopicId::EVENTS), arb_event())
            .prop_map(|(envelope, body)| LinkDatagram { envelope, body }),
        (
            arb_envelope(TopicId::CATALOG_INSTRUMENTS),
            arb_catalog_instrument()
        )
            .prop_map(|(envelope, body)| LinkDatagram { envelope, body }),
        (
            arb_envelope(TopicId::CATALOG_FEATURES),
            any::<i64>(),
            any::<u16>(),
            any::<u16>(),
            arb_name()
        )
            .prop_map(|(envelope, catalog_ts_us, total_count, feature, name)| {
                LinkDatagram {
                    envelope,
                    body: LinkBody::CatalogFeature(CatalogFeature {
                        catalog_ts_us: TsUs::from_micros(catalog_ts_us),
                        total_count,
                        feature: FeatureId(feature),
                        name,
                    }),
                }
            }),
        (
            arb_envelope(TopicId::LIFECYCLE),
            prop_oneof![
                Just(RunPhase::Starting),
                Just(RunPhase::Ready),
                Just(RunPhase::Draining),
                Just(RunPhase::Stopped)
            ],
            arb_run_state(),
            prop_oneof![
                Just(None),
                Just(Some(ExecutionMode::Off)),
                Just(Some(ExecutionMode::Sim)),
                Just(Some(ExecutionMode::Live))
            ],
            any::<u64>(),
            any::<i64>(),
            any::<u16>()
        )
            .prop_map(
                |(
                    envelope,
                    phase,
                    run_state,
                    execution_mode,
                    acknowledged_epoch,
                    spin_interval_us,
                    feature_count,
                )| {
                    LinkDatagram {
                        envelope,
                        body: LinkBody::Lifecycle(Lifecycle {
                            phase,
                            run_state,
                            execution_mode,
                            acknowledged_epoch,
                            spin_interval_us: DurationUs::from_micros(spin_interval_us),
                            feature_count,
                        }),
                    }
                }
            ),
        (
            arb_envelope(TopicId(LINK_MAX_TOPICS as u16)),
            any::<i64>(),
            prop::collection::vec(-1e12f64..1e12, 0..=LINK_MAX_FIELDS)
        )
            .prop_map(|(envelope, stamp, values)| LinkDatagram {
                envelope,
                body: LinkBody::Payload(LinkPayload::new(
                    SCHEMA,
                    TsUs::from_micros(stamp),
                    &values
                )),
            }),
    ]
}

proptest! {
    /// FITNESS: a decoded frame equals the frame that was encoded, for the frame kinds the
    /// generator draws, including the reserved MAC bytes. The order-snapshot event is not drawn
    /// here: the named cases below round-trip it and refuse its malformed forms. A drift here
    /// corrupts a UI's book, a peer's signal or a subscription silently — nothing crashes, the
    /// numbers are just wrong.
    #[test]
    fn round_trips_every_frame_kind(datagram in arb_datagram()) {
        let decoded = round_trip(&datagram).expect("a canonical frame decodes");
        prop_assert_eq!(decoded, datagram);
    }

    /// FITNESS: float slots ride the wire bit-exactly. Normalising a NaN or losing a signed zero
    /// would make a receiver's statistics disagree with the sender's for no visible reason.
    #[test]
    fn payload_preserves_exact_float_bits(
        bits in prop::collection::vec(any::<u64>(), 1..=LINK_MAX_FIELDS),
    ) {
        let values: Vec<f64> = bits.iter().copied().map(f64::from_bits).collect();
        let datagram = LinkDatagram {
            envelope: envelope(TopicId::FIRST_STRATEGY, 3),
            body: LinkBody::Payload(LinkPayload::new(SCHEMA, TsUs::from_micros(1), &values)),
        };
        let decoded = round_trip(&datagram).expect("a canonical payload decodes");
        let LinkBody::Payload(payload) = decoded.body else {
            panic!("payload topic decoded as {:?}", decoded.body);
        };
        let decoded_bits: Vec<u64> = payload.values().iter().copied().map(f64::to_bits).collect();
        prop_assert_eq!(decoded_bits, bits);
    }

    /// FITNESS: the same bit-exactness for the EVENT body's float slots, which carry a strategy's
    /// feature values and the engine's exposure/PnL. The round-trip above cannot reach here: it
    /// compares structurally, and a NaN is unequal to itself, so a codec that quietly canonicalised
    /// one would pass every case it could generate. A normalised non-finite is the dangerous
    /// outcome — an infinity that arrives as 0 reads as a real measurement instead of the blown-up
    /// estimate it was, and a NaN payload that arrives as a different NaN breaks nothing visibly.
    #[test]
    fn event_floats_preserve_exact_bits(
        feature_bits in arb_f64_bits(),
        exposure_bits in arb_f64_bits(),
        pnl_bits in arb_f64_bits(),
    ) {
        let decoded = round_trip(&event_datagram(UiEvent::Feature {
            instrument: InstrumentId(3),
            seq: 8,
            event_ts_us: TsUs::from_micros(11),
            feature: FeatureId(2),
            value: f64::from_bits(feature_bits),
        }))
        .expect("a canonical event decodes");
        let LinkBody::Event(UiEvent::Feature { value, .. }) = decoded.body else {
            panic!("the events topic decoded as {:?}", decoded.body);
        };
        prop_assert_eq!(value.to_bits(), feature_bits);

        let decoded = round_trip(&event_datagram(UiEvent::Position {
            instrument: InstrumentId(3),
            seq: 9,
            event_ts_us: TsUs::from_micros(12),
            exposure_quote: f64::from_bits(exposure_bits),
            pnl_quote: f64::from_bits(pnl_bits),
        }))
        .expect("a canonical event decodes");
        let LinkBody::Event(UiEvent::Position {
            exposure_quote,
            pnl_quote,
            ..
        }) = decoded.body
        else {
            panic!("the events topic decoded as {:?}", decoded.body);
        };
        prop_assert_eq!(exposure_quote.to_bits(), exposure_bits);
        prop_assert_eq!(pnl_quote.to_bits(), pnl_bits);
    }
}

/// Every distinct cause the decoder can refuse a frame for, corrupted one at a time. A peer on a
/// skewed wire version and a peer from someone else's run present the identical symptom — nothing
/// decodes — so each has to name its own cause or an operator is left guessing which of them is
/// happening. The book-length cases matter most: `bid_len`/`ask_len` are read straight off the wire
/// and the UI slices its fixed `[Level; UI_BOOK_LEVELS]` arrays by them (`workstation::dom_view`), so a
/// forged length is a remote abort of the UI for anyone who can reach the port — the link's producer
/// is UNTRUSTED by design rather than by assumption. The order-snapshot counts are the same hazard
/// one body further in, and are refused before any of it reaches projection.
#[test]
fn rejects_frames_with_distinct_causes() {
    const OTHER_RUN: LinkHash = LinkHash::of_name("someone-elses-run");
    const OTHER_STRATEGY: LinkHash = LinkHash::of_name("strat-something-else");
    fn snapshot_bytes(detail_len: u8, total_working: u16, details: &[UiWorkingOrder]) -> Vec<u8> {
        encode(&event_datagram(order_snapshot(
            detail_len,
            total_working,
            details,
        )))
    }

    let drifted = LinkHash::of_fields(&["microprice", "mid", "imbalance"]);
    let book_bytes = encode(&book_datagram());
    let unknown_topic = TopicId(TopicId::LIFECYCLE.0 + 1);
    let forged_field_count = LINK_MAX_FIELDS as u8 + 1;
    let forged_book_len = UI_BOOK_LEVELS as u16 + 1;
    let over_capacity = (UI_ORDER_SNAPSHOT_CAPACITY + 1) as u8;
    let terminal = OrderState::Closed(CloseReason::Filled);
    let live = working_order(7, OrderState::Live);
    let pair = [live, working_order(8, OrderState::Live)];

    let cases: [(&str, Vec<u8>, LinkDecodeError); 16] = [
        (
            "magic mismatch",
            {
                let mut datagram = book_datagram();
                datagram.envelope.magic = LINK_MAGIC ^ 1;
                encode(&datagram)
            },
            LinkDecodeError::MagicMismatch {
                found: LINK_MAGIC ^ 1,
            },
        ),
        (
            "version mismatch",
            {
                let mut datagram = book_datagram();
                datagram.envelope.version = LINK_VERSION + 1;
                encode(&datagram)
            },
            LinkDecodeError::VersionMismatch {
                found: LINK_VERSION + 1,
            },
        ),
        (
            "token mismatch",
            {
                let mut datagram = book_datagram();
                datagram.envelope.token_hash = OTHER_RUN;
                encode(&datagram)
            },
            LinkDecodeError::TokenMismatch {
                found: OTHER_RUN,
                expected: TOKEN,
            },
        ),
        (
            "strategy mismatch",
            {
                let mut datagram = book_datagram();
                datagram.envelope.strategy_hash = OTHER_STRATEGY;
                encode(&datagram)
            },
            LinkDecodeError::StrategyMismatch {
                found: OTHER_STRATEGY,
                expected: STRATEGY,
            },
        ),
        (
            "schema mismatch (drifted link_fields)",
            encode(&payload_datagram(drifted)),
            LinkDecodeError::SchemaMismatch {
                found: drifted,
                expected: SCHEMA,
            },
        ),
        (
            "truncated below the envelope",
            book_bytes[..8].to_vec(),
            LinkDecodeError::Truncated { found: 8 },
        ),
        (
            "length does not match its kind",
            book_bytes[..book_bytes.len() - 1].to_vec(),
            LinkDecodeError::LengthMismatch {
                topic: TopicId::BOOKS,
                expected: book_bytes.len(),
                found: book_bytes.len() - 1,
            },
        ),
        (
            "unknown engine topic",
            {
                let mut bytes = book_bytes.clone();
                bytes[TOPIC_OFFSET..TOPIC_OFFSET + 2]
                    .copy_from_slice(&unknown_topic.0.to_le_bytes());
                bytes
            },
            LinkDecodeError::UnknownTopic {
                topic: unknown_topic,
            },
        ),
        (
            "payload count beyond capacity",
            {
                let mut bytes = encode(&payload_datagram(SCHEMA));
                bytes[PAYLOAD_COUNT_OFFSET] = forged_field_count;
                bytes
            },
            LinkDecodeError::FieldCountExceeded {
                count: forged_field_count,
                capacity: LINK_MAX_FIELDS,
            },
        ),
        (
            "book bid_len beyond capacity",
            {
                let mut bytes = book_bytes.clone();
                bytes[BOOK_BID_LEN_OFFSET..BOOK_BID_LEN_OFFSET + 2]
                    .copy_from_slice(&forged_book_len.to_le_bytes());
                bytes
            },
            LinkDecodeError::BookLevelsExceeded {
                count: forged_book_len,
                capacity: UI_BOOK_LEVELS,
            },
        ),
        (
            "book ask_len beyond capacity",
            {
                let mut bytes = book_bytes.clone();
                bytes[BOOK_BID_LEN_OFFSET + 2..BOOK_BID_LEN_OFFSET + 4]
                    .copy_from_slice(&forged_book_len.to_le_bytes());
                bytes
            },
            LinkDecodeError::BookLevelsExceeded {
                count: forged_book_len,
                capacity: UI_BOOK_LEVELS,
            },
        ),
        (
            "a snapshot detailing more orders than it can carry",
            snapshot_bytes(over_capacity, 9, &pair),
            LinkDecodeError::OrderSnapshotCountsInvalid {
                detail_len: over_capacity,
                total_working: 9,
                detail_capacity: UI_ORDER_SNAPSHOT_CAPACITY,
                total_capacity: UI_ORDER_SNAPSHOT_MAX_TOTAL,
            },
        ),
        (
            "a snapshot detailing more orders than it says are working",
            snapshot_bytes(2, 1, &pair),
            LinkDecodeError::OrderSnapshotCountsInvalid {
                detail_len: 2,
                total_working: 1,
                detail_capacity: UI_ORDER_SNAPSHOT_CAPACITY,
                total_capacity: UI_ORDER_SNAPSHOT_MAX_TOTAL,
            },
        ),
        (
            "a snapshot claiming more working orders than the engine can hold",
            snapshot_bytes(0, UI_ORDER_SNAPSHOT_MAX_TOTAL + 1, &pair),
            LinkDecodeError::OrderSnapshotCountsInvalid {
                detail_len: 0,
                total_working: UI_ORDER_SNAPSHOT_MAX_TOTAL + 1,
                detail_capacity: UI_ORDER_SNAPSHOT_CAPACITY,
                total_capacity: UI_ORDER_SNAPSHOT_MAX_TOTAL,
            },
        ),
        (
            "a snapshot carrying a closed order",
            snapshot_bytes(1, 1, &[working_order(7, terminal)]),
            LinkDecodeError::OrderSnapshotTerminalState { state: terminal },
        ),
        (
            "a snapshot repeating a client id",
            snapshot_bytes(2, 2, &[live, live]),
            LinkDecodeError::OrderSnapshotDuplicate {
                client_id: live.client_id,
            },
        ),
    ];

    for (name, bytes, expected) in cases {
        assert_eq!(
            decode(&bytes),
            Err(expected),
            "case {name}: wrong rejection cause"
        );
    }

    assert!(
        round_trip(&payload_datagram(SCHEMA)).is_ok(),
        "the correct schema still round-trips"
    );
}

/// The link actor indexes its per-topic sequence array by raw topic id, unchecked, on the send
/// path. Every id the engine can stamp — the fixed engine topics, and one per topic the strategy
/// declares — must land inside the array that the same declaration sized, or a send panics inside
/// the actor instead of the config being refused at startup.
#[test]
fn every_topic_the_engine_can_stamp_indexes_inside_its_sequence_array() {
    for declared in [0, 1, 2, 8, 1024] {
        let slots = TopicId::space_len(declared);
        for engine in [
            TopicId::SUBSCRIBE,
            TopicId::BOOKS,
            TopicId::EVENTS,
            TopicId::CATALOG_INSTRUMENTS,
            TopicId::CATALOG_FEATURES,
            TopicId::LIFECYCLE,
        ] {
            assert!(
                usize::from(engine.0) < slots,
                "engine topic {engine:?} falls outside the {slots} slots sized for {declared} strategy topics"
            );
        }
        for index in 0..declared {
            assert!(
                usize::from(TopicId::strategy(index).0) < slots,
                "strategy topic {index} falls outside the {slots} slots sized for {declared} strategy topics"
            );
        }
    }
}

/// Every `OrderState`, listed by hand. It feeds both the generator and the deterministic pin below,
/// so it protects exactly the states named here. A variant added to `OrderState` is not picked up
/// until it is listed.
const EVERY_ORDER_STATE: [OrderState; 11] = [
    OrderState::Free,
    OrderState::PendingNew,
    OrderState::Live,
    OrderState::CancelInFlight,
    OrderState::AmendInFlight,
    OrderState::Unknown,
    OrderState::Closed(CloseReason::Filled),
    OrderState::Closed(CloseReason::Canceled),
    OrderState::Closed(CloseReason::Rejected),
    OrderState::Closed(CloseReason::Expired),
    OrderState::Closed(CloseReason::ReconciledGone),
];

/// Saturated, so a field written narrower than it is read cannot hide in a small value.
fn saturated_order_update(state: OrderState) -> UiEvent {
    UiEvent::OrderUpdate {
        instrument: InstrumentId(u16::MAX),
        seq: u64::MAX,
        event_ts_us: TsUs::from_micros(i64::MAX),
        client_id: ClientOrderId(u64::MAX),
        quote_level: Some(QuoteLevel::new(7).expect("valid quote level")),
        side: Side::Sell,
        state,
        price: Price(i64::MIN),
        qty: Qty(i64::MAX),
        filled: Qty(i64::MIN),
    }
}

fn full_quote_ladder() -> UiEvent {
    let mut quote = DomQuote::default();
    for level in 0..8 {
        quote.bids[level] = Some((Price(100 - level as i64), Qty(level as i64 + 1)));
        quote.asks[level] = Some((Price(101 + level as i64), Qty(level as i64 + 11)));
    }
    UiEvent::Quote {
        instrument: InstrumentId(3),
        seq: 9,
        event_ts_us: TsUs::from_micros(77),
        quote,
    }
}

/// FITNESS: every `OrderState`, and every body that fills its fixed capacity, survives the tail on
/// every run.
///
/// `OrderUpdate` is the widest event kind and its tail is EXACTLY the budget — `ORDER_UPDATE_LEN`
/// and `EVENT_TAIL_LEN` are both 36, so there is no slack to absorb a mistake. The room exists
/// because the wire flattens `Closed(_)` into the same byte as the open states, which turns five
/// close reasons into five wire discriminants in one eleven-state table — and a table of eleven
/// near-identical tags is where a copy-pasted tag hides.
///
/// [`round_trips_every_frame_kind`] generates these states too, so this is deliberately NOT a second
/// round-trip: it is the DETERMINISM the generator cannot give. One state sits behind four nested
/// choices there — datagram kind, then market-vs-exec, then event kind, then the state itself — so a
/// given state arrives in roughly one case in 900, and 256 cases reach it about a quarter of the
/// time. Measured, not assumed: against a mutant mapping `Closed(ReconciledGone)` to the `Expired`
/// tag, the proptest passed 6 clean runs out of 6 while this failed all 6.
///
/// A quarter of the time is a test that passes CI and loses the distinction between "we reconciled
/// it away" and "the venue took our order away for a reason we did not choose".
#[test]
fn every_event_body_survives_the_event_tail() {
    let complete_snapshot = order_snapshot(
        2,
        5,
        &[
            working_order(1, OrderState::Live),
            UiWorkingOrder {
                quote_level: None,
                ..working_order(2, OrderState::Unknown)
            },
        ],
    );
    let states = EVERY_ORDER_STATE.map(|state| {
        (
            format!("a saturated OrderUpdate in {state:?}"),
            saturated_order_update(state),
        )
    });
    let bodies = [
        (
            "the full sixteen-level quote ladder".to_string(),
            full_quote_ladder(),
        ),
        (
            "a complete snapshot whose working count overflows its details".to_string(),
            complete_snapshot,
        ),
    ];

    for (name, event) in states.into_iter().chain(bodies) {
        let decoded = round_trip(&event_datagram(event)).expect("a canonical event decodes");
        assert_eq!(
            decoded.body,
            LinkBody::Event(event),
            "case {name}: did not survive the event tail"
        );
    }
}

fn latency_cell(index: u64) -> UiLatencyCell {
    UiLatencyCell {
        count: index,
        sum_us: -(index as i64) * 1_000,
    }
}

fn latency_row(base: u64) -> UiLatencyRow {
    UiLatencyRow {
        exchange_to_received: latency_cell(base + 1),
        received_to_queued: latency_cell(base + 2),
        queue_wait: latency_cell(base + 3),
        processing: latency_cell(base + 4),
        end_to_end: latency_cell(base + 5),
        order_round_trip: latency_cell(base + 6),
    }
}

/// FITNESS: every self-timing cell keeps its own place in the tail, and an over-wide count saturates
/// rather than wrapping.
///
/// Eighteen structurally identical cells encode as one flat run of bytes, which is where a crossed
/// pair hides: a summary whose exec round-trip decoded into the market-data row still round-trips as
/// a whole. [`round_trips_every_frame_kind`] generates this kind too, but as one arm of eleven
/// inside one topic of seven — about one draw in seventy-seven, so a crossing would be caught on
/// most runs and missed on some. Distinct values in all eighteen cells make it every run.
#[test]
fn every_latency_cell_keeps_its_place_in_the_event_tail() {
    let event = UiEvent::Latency {
        seq: u64::MAX,
        event_ts_us: TsUs::from_micros(i64::MAX),
        summary: UiLatencySummary {
            market_data: latency_row(0),
            execution: latency_row(10),
            hot_path: latency_row(20),
            backlog_ema: Some(3.5),
        },
    };
    let decoded = round_trip(&event_datagram(event)).expect("a canonical event decodes");
    assert_eq!(decoded.body, LinkBody::Event(event));

    // A wrapped count would report a saturated ten-minute window as a nearly idle engine — the one
    // reading an operator would act on backwards.
    let overflowing = UiEvent::Latency {
        seq: 1,
        event_ts_us: TsUs::from_micros(1),
        summary: UiLatencySummary {
            market_data: UiLatencyRow {
                processing: UiLatencyCell {
                    count: u64::from(u32::MAX) + 5,
                    sum_us: 7,
                },
                ..UiLatencyRow::default()
            },
            ..UiLatencySummary::default()
        },
    };
    let LinkBody::Event(UiEvent::Latency { summary, .. }) =
        round_trip(&event_datagram(overflowing))
            .expect("a canonical event decodes")
            .body
    else {
        panic!("the events topic decoded as another body");
    };
    assert_eq!(
        summary.market_data.processing.count,
        u64::from(u32::MAX),
        "an over-wide count clamps to the tail's ceiling"
    );
    assert_eq!(
        summary.market_data.processing.sum_us, 7,
        "and the sum beside it is untouched"
    );
}

/// A reused send buffer must not leak the previous datagram into an event frame's unused tail:
/// identical events would encode to different bytes, and a future MAC would reject them.
#[test]
fn event_encoding_is_independent_of_buffer_reuse() {
    let rotation = LinkDatagram {
        envelope: envelope(TopicId::EVENTS, 2),
        body: LinkBody::Event(UiEvent::Rotation {
            instrument: InstrumentId(1),
            seq: 5,
            event_ts_us: TsUs::from_micros(11),
        }),
    };
    let quote = LinkDatagram {
        envelope: envelope(TopicId::EVENTS, 1),
        body: LinkBody::Event(UiEvent::Quote {
            instrument: InstrumentId(1),
            seq: 4,
            event_ts_us: TsUs::from_micros(10),
            quote: DomQuote::top(
                Some((Price(i64::MAX), Qty(i64::MAX))),
                Some((Price(i64::MIN), Qty(i64::MIN))),
            ),
        }),
    };
    let mut dirty = [0xff; LINK_MAX_DATAGRAM];
    quote.encode(&mut dirty);
    let reused = rotation.encode(&mut dirty);
    assert_eq!(&dirty[..reused], encode(&rotation).as_slice());
}

const FRAMES: &str = include_str!("link_frames.txt");

/// Where `POLYSIM_BLESS_LINK` writes, relative to the package root the suite runs from.
const FRAMES_PATH: &str = "tests/fitness/link_frames.txt";

fn event(seq: u64, event: UiEvent) -> LinkDatagram {
    let mut datagram = event_datagram(event);
    datagram.envelope.seq = seq;
    datagram
}

/// One fixed frame of every kind. Fields carry distinct non-zero values wherever the type allows,
/// so two fields that swap places or a field whose width changes moves bytes the fixture pins, and
/// each optional is present in one frame and absent in another.
fn golden_frames() -> Vec<(&'static str, LinkDatagram)> {
    let mut subscribe_envelope = envelope(TopicId::SUBSCRIBE, 0x0102_0304_0506_0708);
    subscribe_envelope.mac = std::array::from_fn(|index| 0xa0 + index as u8);
    let subscribe = LinkDatagram {
        envelope: subscribe_envelope,
        body: LinkBody::Subscribe(Subscribe {
            topics: TopicSet::new(&[TopicId::BOOKS, TopicId::LIFECYCLE, TopicId(0x1234)])
                .expect("three topics fit"),
            desired_state: RunState::Idle,
            desired_epoch: 0x1122_3344_5566_7788,
        }),
    };

    let book = LinkDatagram {
        envelope: envelope(TopicId::BOOKS, 0x0203_0405_0607_0809),
        body: LinkBody::Book(UiBookSnapshot {
            instrument: InstrumentId(0x0a0b),
            seq: 0x0c0d_0e0f_1011_1213,
            event_ts_us: TsUs::from_micros(1_700_000_123_456_789),
            state: UiBookState::AwaitingSnapshot,
            bid_len: 29,
            ask_len: 31,
            bids: std::array::from_fn(|level| Level {
                price: Price(11_800_000_000_000 - 1_000_000 * level as i64),
                qty: Qty(1_000 + 7 * level as i64),
            }),
            asks: std::array::from_fn(|level| Level {
                price: Price(-(2_000_000_000_000 + 3_000_000 * level as i64)),
                qty: Qty(-(500 + 11 * level as i64)),
            }),
        }),
    };

    let lifecycle = |seq, lifecycle| LinkDatagram {
        envelope: envelope(TopicId::LIFECYCLE, seq),
        body: LinkBody::Lifecycle(lifecycle),
    };
    let catalog_instrument = |seq, instrument| LinkDatagram {
        envelope: envelope(TopicId::CATALOG_INSTRUMENTS, seq),
        body: LinkBody::CatalogInstrument(instrument),
    };
    let stamped = CatalogInstrument {
        catalog_ts_us: TsUs::from_micros(1_700_000_000_000_001),
        total_count: 0x0203,
        instrument: InstrumentId(0x0405),
        display: WireName::new("BTCUSDT"),
        tick_size: Some(Price(1_000_000)),
        lot_size: Some(Qty(-7_000)),
        qty_scale: 100_000_000,
        base_asset: AssetId(0x0607),
        quote_asset: AssetId(0x0809),
        base: WireName::new("BTC"),
        quote: WireName::new("USDT"),
    };
    let unstamped = CatalogInstrument {
        catalog_ts_us: TsUs::from_micros(1_700_000_000_000_002),
        total_count: 0x0a0b,
        instrument: InstrumentId(0x0c0d),
        display: WireName::new("ETH-UP-15M"),
        tick_size: None,
        lot_size: None,
        qty_scale: 1_000_000,
        base_asset: AssetId(0x0e0f),
        quote_asset: AssetId(0x1011),
        base: WireName::new("UP"),
        quote: WireName::new("USDC"),
    };

    let mut quote = DomQuote::default();
    for level in [0, 1, 2, 5] {
        quote.bids[level] = Some((Price(-90_000 - level as i64), Qty(300 + level as i64)));
    }
    for level in [0, 3, 7] {
        quote.asks[level] = Some((Price(91_000 + level as i64), Qty(-400 - level as i64)));
    }

    vec![
        ("subscribe", subscribe),
        ("book", book),
        (
            "lifecycle",
            lifecycle(
                0x0304_0506_0708_090a,
                Lifecycle {
                    phase: RunPhase::Draining,
                    run_state: RunState::Running,
                    execution_mode: Some(ExecutionMode::Live),
                    acknowledged_epoch: 0x2233_4455_6677_8899,
                    spin_interval_us: DurationUs::from_micros(250_000),
                    feature_count: 0x0301,
                },
            ),
        ),
        (
            "lifecycle-no-mode",
            lifecycle(
                0x0405_0607_0809_0a0b,
                Lifecycle {
                    phase: RunPhase::Starting,
                    run_state: RunState::Idle,
                    execution_mode: None,
                    acknowledged_epoch: 0x3344_5566_7788_99aa,
                    spin_interval_us: DurationUs::from_micros(-1_000_001),
                    feature_count: 0x0402,
                },
            ),
        ),
        (
            "catalog-instrument",
            catalog_instrument(0x0506_0708_090a_0b0c, stamped),
        ),
        (
            "catalog-instrument-unstamped",
            catalog_instrument(0x0607_0809_0a0b_0c0d, unstamped),
        ),
        (
            "catalog-feature",
            LinkDatagram {
                envelope: envelope(TopicId::CATALOG_FEATURES, 0x0708_090a_0b0c_0d0e),
                body: LinkBody::CatalogFeature(CatalogFeature {
                    catalog_ts_us: TsUs::from_micros(1_700_000_000_000_003),
                    total_count: 0x1213,
                    feature: FeatureId(0x1415),
                    name: WireName::new("microprice_gap_bps"),
                }),
            },
        ),
        (
            "payload",
            LinkDatagram {
                envelope: envelope(TopicId(0x0011), 0x0809_0a0b_0c0d_0e0f),
                body: LinkBody::Payload(LinkPayload::new(
                    SCHEMA,
                    TsUs::from_micros(1_700_000_000_000_004),
                    &[
                        std::f64::consts::PI,
                        -std::f64::consts::E,
                        1.0e-300,
                        -0.0,
                        f64::MAX,
                        6.022_140_76e23,
                        f64::NEG_INFINITY,
                        f64::from_bits(0x7ff8_0000_dead_beef),
                    ],
                )),
            },
        ),
        (
            "event-quote",
            event(
                0x1001,
                UiEvent::Quote {
                    instrument: InstrumentId(0x0102),
                    seq: 0x1101,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_001),
                    quote,
                },
            ),
        ),
        (
            "event-trade",
            event(
                0x1002,
                UiEvent::Trade {
                    instrument: InstrumentId(0x0203),
                    seq: 0x1102,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_002),
                    aggressor: Side::Sell,
                    price: Price(-123_456_789),
                    qty: Qty(987_654_321),
                },
            ),
        ),
        (
            "event-order-update",
            event(
                0x1003,
                UiEvent::OrderUpdate {
                    instrument: InstrumentId(0x0304),
                    seq: 0x1103,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_003),
                    client_id: ClientOrderId(0x1a2b_3c4d_5e6f_7081),
                    quote_level: QuoteLevel::new(5),
                    side: Side::Buy,
                    state: OrderState::CancelInFlight,
                    price: Price(118_000_123_000_000),
                    qty: Qty(-5_000_001),
                    filled: Qty(2_000_003),
                },
            ),
        ),
        (
            "event-feature",
            event(
                0x1004,
                UiEvent::Feature {
                    instrument: InstrumentId(0x0405),
                    seq: 0x1104,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_004),
                    feature: FeatureId(0x0506),
                    value: -std::f64::consts::PI * 1.0e-7,
                },
            ),
        ),
        (
            "event-fill",
            event(
                0x1005,
                UiEvent::Fill {
                    instrument: InstrumentId(0x0607),
                    seq: 0x1105,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_005),
                    quote_level: None,
                    side: Side::Sell,
                    price: Price(66_123_450_000_000),
                    qty: Qty(1_250_000),
                    commission: -944_000,
                    commission_asset: AssetId(0x0708),
                    liquidity: Some(Liquidity::Taker),
                },
            ),
        ),
        (
            "event-rotation",
            event(
                0x1006,
                UiEvent::Rotation {
                    instrument: InstrumentId(0x0809),
                    seq: 0x1106,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_006),
                },
            ),
        ),
        (
            "event-position",
            event(
                0x1007,
                UiEvent::Position {
                    instrument: InstrumentId(0x0a0b),
                    seq: 0x1107,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_007),
                    exposure_quote: 12_345.678_9,
                    pnl_quote: -0.000_123_4,
                },
            ),
        ),
        (
            "event-balance",
            event(
                0x1008,
                UiEvent::Balance {
                    asset: AssetId(0x0c0d),
                    seq: 0x1108,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_008),
                    free: 5_000_000_000_000,
                    locked: -250_000_000,
                },
            ),
        ),
        (
            "event-reject-local",
            event(
                0x1009,
                UiEvent::Reject {
                    instrument: InstrumentId(0x0e0f),
                    seq: 0x1109,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_009),
                    side: Side::Buy,
                    origin: RejectOrigin::Local(RejectReason::NotReady(ReadinessGap::Balances)),
                },
            ),
        ),
        (
            "event-reject-venue",
            event(
                0x100a,
                UiEvent::Reject {
                    instrument: InstrumentId(0x1011),
                    seq: 0x110a,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_010),
                    side: Side::Sell,
                    origin: RejectOrigin::Venue {
                        class: RejectClass::Ambiguous,
                        code: -2_015,
                    },
                },
            ),
        ),
        (
            "event-execution-armed",
            event(
                0x100b,
                UiEvent::Execution {
                    seq: 0x110b,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_011),
                    halt: ExecHalt::Armed,
                },
            ),
        ),
        (
            "event-execution-halted",
            event(
                0x100c,
                UiEvent::Execution {
                    seq: 0x110c,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_012),
                    halt: ExecHalt::Halted {
                        reason: HaltReason::DuplicateResting,
                        halted_ts_us: TsUs::from_micros(1_700_000_000_000_999),
                    },
                },
            ),
        ),
        (
            "event-order-snapshot",
            event(
                0x100d,
                order_snapshot(
                    3,
                    261,
                    &[
                        working_order(0x11, OrderState::Live),
                        UiWorkingOrder {
                            quote_level: None,
                            ..working_order(0x22, OrderState::AmendInFlight)
                        },
                        working_order(0x33, OrderState::PendingNew),
                    ],
                ),
            ),
        ),
        (
            "event-latency",
            event(
                0x100e,
                UiEvent::Latency {
                    seq: 0x110e,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_014),
                    summary: UiLatencySummary {
                        market_data: latency_row(0),
                        execution: latency_row(10),
                        hot_path: latency_row(20),
                        backlog_ema: Some(0.312_5 + 1.0e-9),
                    },
                },
            ),
        ),
        (
            "event-latency-no-backlog",
            event(
                0x100f,
                UiEvent::Latency {
                    seq: 0x110f,
                    event_ts_us: TsUs::from_micros(1_700_000_000_001_015),
                    summary: UiLatencySummary {
                        market_data: latency_row(30),
                        execution: latency_row(40),
                        hot_path: latency_row(50),
                        backlog_ema: None,
                    },
                },
            ),
        ),
    ]
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn from_hex(kind: &str, hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .unwrap_or_else(|| panic!("{FRAMES_PATH}: frame {kind} holds a non-hex pair"))
        })
        .collect()
}

fn first_difference(encoded: &[u8], checked_in: &[u8]) -> Option<usize> {
    encoded
        .iter()
        .zip(checked_in)
        .position(|(left, right)| left != right)
        .or_else(|| {
            (encoded.len() != checked_in.len()).then(|| encoded.len().min(checked_in.len()))
        })
}

/// FITNESS: a fixed frame of every kind encodes to exactly the bytes checked in. Every other test
/// here is a round trip, and a round trip still passes when the writer and the reader drift
/// together, while a separately built peer on the old layout misreads every frame.
///
/// To accept an intended layout change, bump `LINK_VERSION`, run the suite once with
/// `POLYSIM_BLESS_LINK=1` to rewrite `link_frames.txt`, then read the diff.
#[test]
fn every_frame_kind_encodes_to_checked_in_bytes() {
    let encoded: Vec<(&str, Vec<u8>)> = golden_frames()
        .iter()
        .map(|(kind, datagram)| (*kind, encode(datagram)))
        .collect();
    if std::env::var_os("POLYSIM_BLESS_LINK").is_some() {
        let lines: String = encoded
            .iter()
            .map(|(kind, bytes)| format!("{kind} {}\n", to_hex(bytes)))
            .collect();
        std::fs::write(FRAMES_PATH, lines).expect("the fixture is writable");
        return;
    }

    let checked_in: Vec<(&str, Vec<u8>)> = FRAMES
        .lines()
        .map(|line| {
            let (kind, hex) = line
                .split_once(' ')
                .unwrap_or_else(|| panic!("{FRAMES_PATH}: line {line:?} is not `<kind> <hex>`"));
            (kind, from_hex(kind, hex))
        })
        .collect();
    let encoded_kinds: Vec<&str> = encoded.iter().map(|(kind, _)| *kind).collect();
    let checked_in_kinds: Vec<&str> = checked_in.iter().map(|(kind, _)| *kind).collect();
    assert_eq!(
        encoded_kinds, checked_in_kinds,
        "the frame kinds drifted from {FRAMES_PATH}; if intended, rerun with POLYSIM_BLESS_LINK=1"
    );

    let drifted: Vec<String> = encoded
        .iter()
        .zip(&checked_in)
        .filter_map(|((kind, bytes), (_, expected))| {
            first_difference(bytes, expected).map(|offset| {
                format!(
                    "{kind} first differs at byte {offset} ({} bytes encoded, {} checked in)",
                    bytes.len(),
                    expected.len()
                )
            })
        })
        .collect();
    assert!(
        drifted.is_empty(),
        "the link encoding drifted from {FRAMES_PATH}: {drifted:#?}; if intended, bump LINK_VERSION \
         and rerun with POLYSIM_BLESS_LINK=1"
    );
}

/// The gate ages its slots, so every fixture stamps from one instant unless it is deliberately
/// walking the clock past the TTL.
const GATE_START: TsUs = TsUs::from_micros(1_000_000);

fn admit_from(gate: &mut SequenceGate, sender: u64, seq: u64, now: TsUs) -> GateVerdict {
    let mut envelope = envelope(TopicId::FIRST_STRATEGY, seq);
    envelope.sender_te_hash = LinkHash(sender);
    gate.admit(&envelope, now)
}

fn admit_boot(gate: &mut SequenceGate, boot_ts_us: i64, topic: TopicId, seq: u64) -> bool {
    let mut envelope = envelope(topic, seq);
    envelope.boot_ts_us = TsUs::from_micros(boot_ts_us);
    gate.admit(&envelope, GATE_START).is_accepted()
}

/// FITNESS: a slot is keyed on `(sender, boot, topic)` and the consumed sequence is the record, so a
/// duplicate or reordered datagram must never reach the hot thread — a replayed tape would then not
/// reproduce the run.
///
/// Both halves of the key earn their place. Collapsing the TOPIC out of it needs no error to do
/// damage: two interleaved streams each look like a reorder of the other, the gate drops most of
/// both, and a topic simply goes quiet with every frame accounted for as stale. Dropping the BOOT
/// stamp costs the other direction — a restarted peer's `seq` returns to 0, so it would stay dark
/// until its seq climbed past the old high-water mark, hours on a slow topic.
#[test]
fn gate_sequences_each_stream_on_its_sender_boot_and_topic() {
    for (name, steps, stale, restarts, stale_boots) in [
        (
            "one peer's topics each carry their own sequence",
            &[
                (100, TopicId::FIRST_STRATEGY, 1, true),
                (100, TopicId::BOOKS, 1, true),
                (100, TopicId::EVENTS, 1, true),
                (100, TopicId::BOOKS, 1, false),
                (100, TopicId::FIRST_STRATEGY, 2, true),
            ][..],
            1,
            0,
            0,
        ),
        (
            "a duplicate and a reorder are both dropped, and the stream carries on",
            &[
                (100, TopicId::FIRST_STRATEGY, 1, true),
                (100, TopicId::FIRST_STRATEGY, 2, true),
                (100, TopicId::FIRST_STRATEGY, 3, true),
                (100, TopicId::FIRST_STRATEGY, 3, false),
                (100, TopicId::FIRST_STRATEGY, 2, false),
                (100, TopicId::FIRST_STRATEGY, 4, true),
            ][..],
            2,
            0,
            0,
        ),
        (
            "a restarted peer is let back in and the boot it replaced is not",
            &[
                (100, TopicId::FIRST_STRATEGY, 5_000, true),
                (200, TopicId::FIRST_STRATEGY, 0, true),
                (200, TopicId::FIRST_STRATEGY, 1, true),
                (100, TopicId::FIRST_STRATEGY, 9_999, false),
            ][..],
            0,
            1,
            1,
        ),
    ] {
        let mut gate = SequenceGate::new();
        for &(boot_ts_us, topic, seq, accepted) in steps {
            assert_eq!(
                admit_boot(&mut gate, boot_ts_us, topic, seq),
                accepted,
                "case {name}: seq {seq} on {topic:?} from boot {boot_ts_us}"
            );
        }
        let counts = gate.counts();
        assert_eq!(counts.stale, stale, "case {name}: stale count");
        assert_eq!(counts.restarts, restarts, "case {name}: restart count");
        assert_eq!(
            counts.stale_boots, stale_boots,
            "case {name}: stale boot count"
        );
    }
}

/// FITNESS: every workstation that attaches is a NEW `(sender, topic)` stream, so a gate that never
/// released a DEAD slot would refuse every peer after the capacity-th one for the rest of the run —
/// leaving a restart of the trading engine as the only recovery, which is the thing running the UI
/// in its own process exists to avoid. Eviction reclaims DEAD slots only: dropping a stream still
/// carrying traffic would trade a visible lockout for silent gaps in a feed nobody was told about,
/// so a live sender keeps its slot right up to the TTL boundary and loses it exactly at.
#[test]
fn gate_eviction_is_gated_on_the_subscription_ttl() {
    let newcomer = LINK_MAX_GATE_KEYS as u64;
    for (name, now, sender_still_live, expect_admitted) in [
        (
            "just inside the ttl, every slot still live",
            GATE_START + LINK_SUBSCRIPTION_TTL - DurationUs::from_micros(1),
            false,
            false,
        ),
        (
            "at the ttl, a quiet slot is reclaimed",
            GATE_START + LINK_SUBSCRIPTION_TTL,
            true,
            true,
        ),
    ] {
        let mut gate = SequenceGate::new();
        for sender in 0..LINK_MAX_GATE_KEYS as u64 {
            assert!(
                admit_from(&mut gate, sender, 1, GATE_START).is_accepted(),
                "case {name}: filling capacity"
            );
        }
        if sender_still_live {
            assert!(
                admit_from(&mut gate, 0, 2, now).is_accepted(),
                "case {name}: refreshing sender 0 keeps its slot live"
            );
        }
        assert_eq!(
            admit_from(&mut gate, newcomer, 1, now).is_accepted(),
            expect_admitted,
            "case {name}: newcomer admission"
        );
        assert_eq!(
            gate.counts().evicted,
            expect_admitted as u64,
            "case {name}: evicted count"
        );
        assert_eq!(
            gate.counts().untracked,
            !expect_admitted as u64,
            "case {name}: untracked count"
        );
        if sender_still_live {
            // Sender 0 kept its slot, so its next frame is gated on the seq that stream actually
            // reached rather than admitted as a fresh one.
            assert!(
                !admit_from(&mut gate, 0, 2, now).is_accepted(),
                "case {name}: the spared sender is gated on its real seq, not readmitted"
            );
            assert_eq!(gate.counts().stale, 1, "case {name}: stale count");
        }
    }
}

fn subscriber(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}")
        .parse()
        .expect("a literal loopback address")
}

/// Soft state means a dead subscriber needs no teardown — but it also means the feed stops silently,
/// so the expiry boundary is the thing the staleness reporting is built on. A renewal has to land on
/// the SAME slot with the newly asserted topic set, since a workstation narrowing its interest and a
/// workstation that died look alike from here.
#[test]
fn a_subscription_renews_in_place_and_expires_at_its_ttl() {
    let mut table = SubscriberTable::new();
    let start = TsUs::from_micros(1_000_000);
    assert_eq!(
        table.refresh(subscriber(9310), TopicSet::ALL, start),
        RefreshOutcome::Added
    );
    assert_eq!(
        table.refresh(
            subscriber(9310),
            TopicSet::new(&[TopicId::BOOKS]).expect("one topic"),
            start
        ),
        RefreshOutcome::Renewed
    );
    assert_eq!(table.len(), 1);
    assert_eq!(table.recipients(TopicId::BOOKS, start).count(), 1);
    assert_eq!(table.recipients(TopicId::EVENTS, start).count(), 0);

    let alive = start + LINK_SUBSCRIPTION_TTL - DurationUs::from_micros(1);
    assert_eq!(table.recipients(TopicId::BOOKS, alive).count(), 1);
    let expired = start + LINK_SUBSCRIPTION_TTL;
    assert_eq!(table.recipients(TopicId::BOOKS, expired).count(), 0);

    assert_eq!(
        table.refresh(subscriber(9310), TopicSet::ALL, expired),
        RefreshOutcome::Added
    );
    assert_eq!(table.len(), 1);
}

/// A capacity hit is a designed event: the table refuses the subscription and reports the refusal
/// for the actor to count, rather than growing without bound because a port is reachable.
#[test]
fn full_subscriber_table_rejects_and_counts() {
    let mut table = SubscriberTable::new();
    let now = TsUs::from_micros(1_000_000);
    for port in 0..LINK_MAX_SUBSCRIBERS as u16 {
        assert_eq!(
            table.refresh(subscriber(9310 + port), TopicSet::ALL, now),
            RefreshOutcome::Added
        );
    }
    let overflow = subscriber(9310 + LINK_MAX_SUBSCRIBERS as u16);
    assert_eq!(
        table.refresh(overflow, TopicSet::ALL, now),
        RefreshOutcome::Rejected
    );
    assert_eq!(table.len(), LINK_MAX_SUBSCRIBERS);

    let after_ttl = now + LINK_SUBSCRIPTION_TTL;
    assert_eq!(
        table.refresh(overflow, TopicSet::ALL, after_ttl),
        RefreshOutcome::Added
    );
    assert_eq!(table.len(), 1);
}
