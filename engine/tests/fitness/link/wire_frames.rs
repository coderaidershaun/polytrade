//! The frames that the wire tests draw and pin: a generator for every frame kind, one fixed frame
//! of each kind for the checked-in bytes, and the guard and helpers that encode and decode them.

use engine::hot::exec::{
    CloseReason, HaltReason, OrderState, QuoteLevel, ReadinessGap, RejectOrigin, RejectReason,
};
use engine::link::{
    CatalogFeature, CatalogInstrument, Envelope, FrameGuard, LINK_MAX_DATAGRAM, LINK_MAX_FIELDS,
    LINK_MAX_TOPICS, Lifecycle, LinkBody, LinkDatagram, LinkDecodeError, LinkHash, LinkIdentity,
    LinkPayload, RunPhase, Subscribe, TopicId, TopicSet, WireName,
};
use engine::msg::exec::{Liquidity, RejectClass};
use engine::msg::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side};
use engine::msg::inbound::Level;
use engine::msg::persist::FeatureId;
use engine::msg::run_state::RunState;
use engine::msg::time::{DurationUs, VenueTs};
use engine::msg::ui::{
    DomQuote, LATENCY_REPORT_CELLS, LatencyCell, LatencyReport, UI_BOOK_LEVELS,
    UI_ORDER_SNAPSHOT_CAPACITY, UiBookSnapshot, UiBookState, UiEvent, UiHalt, UiWorkingOrder,
};
use engine::runtime::config::ExecutionMode;
use proptest::prelude::*;

pub(super) const TOKEN: LinkHash = LinkHash::of_name("fitness-run-token");
pub(super) const STRATEGY: LinkHash = LinkHash::of_name("strat-micro-recorder");
pub(super) const SCHEMA: LinkHash = LinkHash::of_fields(&["mid", "microprice", "imbalance"]);
pub(super) const SENDER: LinkHash = LinkHash::of_name("te-binance-spot-btcusdt");

pub(super) fn guard() -> FrameGuard {
    FrameGuard {
        token_hash: TOKEN,
        strategy_hash: STRATEGY,
        schema_hash: SCHEMA,
    }
}

pub(super) fn identity() -> LinkIdentity {
    LinkIdentity {
        token_hash: TOKEN,
        strategy_hash: STRATEGY,
        sender_te_hash: SENDER,
        boot_ts_us: VenueTs::from_micros(1_700_000_000_000_000),
    }
}

pub(super) fn envelope(topic: TopicId, seq: u64) -> Envelope {
    Envelope::new(identity(), topic, seq)
}

pub(super) fn encode(datagram: &LinkDatagram) -> Vec<u8> {
    let mut buffer = [0; LINK_MAX_DATAGRAM];
    let len = datagram.encode(&mut buffer);
    buffer[..len].to_vec()
}

pub(super) fn decode(bytes: &[u8]) -> Result<LinkDatagram, LinkDecodeError> {
    LinkDatagram::decode(bytes, &guard())
}

pub(super) fn round_trip(datagram: &LinkDatagram) -> Result<LinkDatagram, LinkDecodeError> {
    decode(&encode(datagram))
}

pub(super) fn book_datagram() -> LinkDatagram {
    LinkDatagram {
        envelope: envelope(TopicId::BOOKS, 1),
        body: LinkBody::Book(UiBookSnapshot {
            instrument: InstrumentId(0),
            seq: 7,
            event_ts_us: VenueTs::from_micros(42),
            state: UiBookState::Valid,
            bid_len: 1,
            ask_len: 1,
            bids: [Level {
                price: Price::from_mantissa(100),
                qty: Qty::from_mantissa(5),
            }; UI_BOOK_LEVELS],
            asks: [Level {
                price: Price::from_mantissa(101),
                qty: Qty::from_mantissa(5),
            }; UI_BOOK_LEVELS],
        }),
    }
}

pub(super) fn event_datagram(event: UiEvent) -> LinkDatagram {
    LinkDatagram {
        envelope: envelope(TopicId::EVENTS, 1),
        body: LinkBody::Event(event),
    }
}

pub(super) fn order_snapshot(
    detail_len: u8,
    total_working: u16,
    details: &[UiWorkingOrder],
) -> UiEvent {
    let mut orders = [UiWorkingOrder::EMPTY; UI_ORDER_SNAPSHOT_CAPACITY];
    orders[..details.len()].copy_from_slice(details);
    UiEvent::OrderSnapshot {
        instrument: InstrumentId(3),
        seq: 41,
        event_ts_us: VenueTs::from_micros(77),
        side: Side::Buy,
        detail_len,
        total_working,
        orders,
    }
}

pub(super) fn working_order(client_id: u64, state: OrderState) -> UiWorkingOrder {
    UiWorkingOrder {
        client_id: ClientOrderId(client_id),
        quote_level: QuoteLevel::new((client_id % 8) as u8),
        state,
        price: Price::from_mantissa(100 + client_id as i64),
        qty: Qty::from_mantissa(5),
        filled: Qty::from_mantissa(1),
    }
}

pub(super) fn payload_datagram(schema_hash: LinkHash) -> LinkDatagram {
    LinkDatagram {
        envelope: envelope(TopicId::FIRST_STRATEGY, 1),
        body: LinkBody::Payload(LinkPayload::new(
            schema_hash,
            VenueTs::from_micros(99),
            &[1.5, -2.5],
        )),
    }
}

pub(super) fn arb_envelope(topic: TopicId) -> impl Strategy<Value = Envelope> {
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
                    boot_ts_us: VenueTs::from_micros(boot),
                },
                topic,
                seq,
            );
            envelope.mac = mac.try_into().expect("32 generated bytes");
            envelope
        })
}

pub(super) fn arb_topic_set() -> impl Strategy<Value = TopicSet> {
    prop::collection::vec(any::<u16>().prop_map(TopicId), 0..=LINK_MAX_TOPICS)
        .prop_map(|topics| TopicSet::new(&topics).expect("generated within capacity"))
}

pub(super) fn arb_run_state() -> impl Strategy<Value = RunState> {
    prop_oneof![Just(RunState::Running), Just(RunState::Idle)]
}

pub(super) fn arb_levels() -> impl Strategy<Value = [Level; UI_BOOK_LEVELS]> {
    prop::collection::vec(
        (any::<i64>(), any::<i64>()).prop_map(|(price, qty)| Level {
            price: Price::from_mantissa(price),
            qty: Qty::from_mantissa(qty),
        }),
        UI_BOOK_LEVELS,
    )
    .prop_map(|levels| levels.try_into().expect("generated exactly one side"))
}

pub(super) fn arb_optional_level() -> impl Strategy<Value = Option<(Price, Qty)>> {
    prop::option::of(
        (any::<i64>(), any::<i64>())
            .prop_map(|(price, qty)| (Price::from_mantissa(price), Qty::from_mantissa(qty))),
    )
}

pub(super) fn arb_side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

/// The float domain the round-trip generator puts on the wire. `any::<f64>()` alone is
/// `POSITIVE|NEGATIVE|ZERO|SUBNORMAL|NORMAL` (proptest's `Arbitrary` impl, which deliberately omits
/// `INFINITE` and `QUIET_NAN`), so no non-finite value would ever ride an event body — while a quant
/// feature genuinely can be infinite, as Kyle's lambda has already shipped. The infinities are added
/// back here. NaN cannot join them: it is unequal to itself, so NO structural round-trip can express
/// it however perfect the codec; [`event_floats_preserve_exact_bits`] covers that half bitwise.
pub(super) fn arb_wire_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        8 => any::<f64>(),
        1 => Just(f64::INFINITY),
        1 => Just(f64::NEG_INFINITY),
    ]
}

/// Every non-finite bit pattern: a sign, an all-ones exponent, and an arbitrary mantissa — zero for
/// an infinity, anything else for a NaN, quiet or signalling.
pub(super) fn arb_non_finite_bits() -> impl Strategy<Value = u64> {
    (any::<bool>(), 0u64..(1 << 52)).prop_map(|(negative, mantissa)| {
        (u64::from(negative) << 63) | 0x7ff0_0000_0000_0000 | mantissa
    })
}

/// Half arbitrary patterns, half deliberately non-finite. A uniform `any::<u64>()` lands on an
/// all-ones exponent once in 2048 draws, so the case this generator exists to reach would be absent
/// from most runs — measured, not assumed: an encoder mutated to normalise non-finite values passed
/// the bit test while it drew from `any::<u64>()` alone.
pub(super) fn arb_f64_bits() -> impl Strategy<Value = u64> {
    prop_oneof![any::<u64>(), arb_non_finite_bits()]
}

pub(super) fn arb_name() -> impl Strategy<Value = WireName> {
    "[a-z_]{0,32}".prop_map(|name| WireName::new(&name))
}

pub(super) fn arb_book() -> impl Strategy<Value = LinkBody> {
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
                    event_ts_us: VenueTs::from_micros(stamp),
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
pub(super) fn arb_order_state() -> impl Strategy<Value = OrderState> {
    prop::sample::select(EVERY_ORDER_STATE.to_vec())
}

pub(super) fn arb_quote_level() -> impl Strategy<Value = Option<QuoteLevel>> {
    prop_oneof![Just(None), (0_u8..8).prop_map(QuoteLevel::new),]
}

pub(super) fn arb_liquidity() -> impl Strategy<Value = Option<Liquidity>> {
    prop_oneof![
        Just(None),
        Just(Some(Liquidity::Maker)),
        Just(Some(Liquidity::Taker)),
    ]
}

pub(super) const REJECT_REASONS: [RejectReason; 20] = [
    RejectReason::QtyBelowMin,
    RejectReason::NotionalBelowMin,
    RejectReason::NotionalAboveMax,
    RejectReason::WouldCross,
    RejectReason::OutsideBand,
    RejectReason::Underfunded,
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
    RejectReason::Warming,
    RejectReason::IdsSpent,
];

/// Both refusal origins. The local arm pads the venue arm's code field, so a decoder that read the
/// padding as a code would report a venue rejection that never happened.
pub(super) fn arb_reject_origin() -> impl Strategy<Value = RejectOrigin> {
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

pub(super) fn arb_halt() -> impl Strategy<Value = UiHalt> {
    prop_oneof![
        Just(UiHalt::Armed),
        (
            prop_oneof![
                Just(HaltReason::RejectStreak),
                Just(HaltReason::RealisedLoss),
                Just(HaltReason::FatalReject),
                Just(HaltReason::SlotLeak),
                Just(HaltReason::FilterViolation),
                Just(HaltReason::CommandBankOverflow),
                Just(HaltReason::DuplicateResting),
                Just(HaltReason::SealNotAccepted),
                Just(HaltReason::OrphanFill),
                Just(HaltReason::PriorRunOverflow),
                Just(HaltReason::StatusProbesSpent),
                Just(HaltReason::StreamSilent),
                Just(HaltReason::WriterFailed),
                Just(HaltReason::PriorRunFillUnbooked),
                Just(HaltReason::UnprovenOrderResting),
            ],
            any::<i64>()
        )
            .prop_map(|(reason, at)| UiHalt::Halted {
                reason,
                halted_ts_us: VenueTs::from_micros(at)
            }),
    ]
}

pub(super) fn arb_report_cell() -> impl Strategy<Value = LatencyCell> {
    any::<[u64; 5]>().prop_map(|[count, p50_ns, p99_ns, p999_ns, max_ns]| LatencyCell {
        count,
        p50_ns,
        p99_ns,
        p999_ns,
        max_ns,
    })
}

pub(super) fn arb_latency_report() -> impl Strategy<Value = LinkBody> {
    (
        any::<i64>(),
        prop::array::uniform3(prop::option::of(any::<i64>())),
        prop::collection::vec(arb_report_cell(), LATENCY_REPORT_CELLS),
        prop::option::of(arb_wire_f64()),
    )
        .prop_map(|(built_ts_us, ages, cells, backlog_ema)| {
            LinkBody::Latency(LatencyReport {
                built_ts_us: VenueTs::from_micros(built_ts_us),
                ages: ages.map(|age| age.map(DurationUs::from_micros)),
                cells: cells
                    .try_into()
                    .expect("the generator draws one cell for each slot"),
                backlog_ema,
            })
        })
}

pub(super) fn arb_event() -> impl Strategy<Value = LinkBody> {
    let head = (any::<u16>(), any::<u64>(), any::<i64>());
    prop_oneof![
        (head, arb_optional_level(), arb_optional_level()).prop_map(
            |((instrument, seq, stamp), bid, ask)| UiEvent::Quote {
                instrument: InstrumentId(instrument),
                seq,
                event_ts_us: VenueTs::from_micros(stamp),
                quote: DomQuote::top(bid, ask),
            }
        ),
        (head, arb_side(), any::<i64>(), any::<i64>()).prop_map(
            |((instrument, seq, stamp), aggressor, price, qty)| UiEvent::Trade {
                instrument: InstrumentId(instrument),
                seq,
                event_ts_us: VenueTs::from_micros(stamp),
                aggressor,
                price: Price::from_mantissa(price),
                qty: Qty::from_mantissa(qty),
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
                        event_ts_us: VenueTs::from_micros(stamp),
                        client_id: ClientOrderId(client_id),
                        quote_level,
                        side,
                        state,
                        price: Price::from_mantissa(price),
                        qty: Qty::from_mantissa(qty),
                        filled: Qty::from_mantissa(filled),
                    }
                }
            ),
        (head, any::<u16>(), any::<i64>(), any::<i64>()).prop_map(
            |((_, seq, stamp), asset, free, locked)| UiEvent::Balance {
                asset: AssetId(asset),
                seq,
                event_ts_us: VenueTs::from_micros(stamp),
                free,
                locked,
            }
        ),
        (head, arb_side(), arb_reject_origin()).prop_map(
            |((instrument, seq, stamp), side, origin)| UiEvent::Reject {
                instrument: InstrumentId(instrument),
                seq,
                event_ts_us: VenueTs::from_micros(stamp),
                side,
                origin,
            }
        ),
        (any::<u64>(), any::<i64>(), arb_halt()).prop_map(|(seq, stamp, halt)| {
            UiEvent::Execution {
                seq,
                event_ts_us: VenueTs::from_micros(stamp),
                halt,
            }
        }),
        (head, any::<u16>(), arb_wire_f64()).prop_map(
            |((instrument, seq, stamp), feature, value)| UiEvent::Feature {
                instrument: InstrumentId(instrument),
                seq,
                event_ts_us: VenueTs::from_micros(stamp),
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
                    event_ts_us: VenueTs::from_micros(stamp),
                    quote_level,
                    side,
                    price: Price::from_mantissa(price),
                    qty: Qty::from_mantissa(qty),
                    commission,
                    commission_asset: AssetId(commission_asset),
                    liquidity,
                }
            ),
        head.prop_map(|(instrument, seq, stamp)| UiEvent::Rotation {
            instrument: InstrumentId(instrument),
            seq,
            event_ts_us: VenueTs::from_micros(stamp),
        }),
        (head, arb_wire_f64(), arb_wire_f64()).prop_map(
            |((instrument, seq, stamp), exposure_quote, pnl_quote)| UiEvent::Position {
                instrument: InstrumentId(instrument),
                seq,
                event_ts_us: VenueTs::from_micros(stamp),
                exposure_quote,
                pnl_quote,
            }
        ),
    ]
    .prop_map(LinkBody::Event)
}

pub(super) fn arb_catalog_instrument() -> impl Strategy<Value = LinkBody> {
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
                    catalog_ts_us: VenueTs::from_micros(catalog_ts_us),
                    total_count,
                    instrument: InstrumentId(instrument),
                    display,
                    tick_size: tick.map(Price::from_mantissa),
                    lot_size: lot.map(Qty::from_mantissa),
                    qty_scale,
                    base_asset: AssetId(base_asset),
                    quote_asset: AssetId(quote_asset),
                    base,
                    quote,
                })
            },
        )
}

/// A controller epoch. Epochs of 2^63 and above are refused on the wire, so none is drawn.
fn arb_epoch() -> impl Strategy<Value = u64> {
    0..1_u64 << 63
}

pub(super) fn arb_datagram() -> impl Strategy<Value = LinkDatagram> {
    prop_oneof![
        (
            arb_envelope(TopicId::SUBSCRIBE),
            arb_topic_set(),
            arb_run_state(),
            arb_epoch()
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
                        catalog_ts_us: VenueTs::from_micros(catalog_ts_us),
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
            arb_epoch(),
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
        (arb_envelope(TopicId::LATENCY), arb_latency_report())
            .prop_map(|(envelope, body)| LinkDatagram { envelope, body }),
        (
            arb_envelope(TopicId(LINK_MAX_TOPICS as u16)),
            any::<i64>(),
            prop::collection::vec(-1e12f64..1e12, 0..=LINK_MAX_FIELDS)
        )
            .prop_map(|(envelope, stamp, values)| LinkDatagram {
                envelope,
                body: LinkBody::Payload(LinkPayload::new(
                    SCHEMA,
                    VenueTs::from_micros(stamp),
                    &values
                )),
            }),
    ]
}

/// Every `OrderState`, listed by hand. It feeds both the generator and the deterministic pin below,
/// so it protects exactly the states named here. A variant added to `OrderState` is not picked up
/// until it is listed.
pub(super) const EVERY_ORDER_STATE: [OrderState; 11] = [
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

pub(super) const FRAMES: &str = include_str!("link_frames.txt");

/// Where `POLYSIM_BLESS_LINK` writes, relative to the package root the suite runs from.
pub(super) const FRAMES_PATH: &str = "tests/fitness/link/link_frames.txt";

pub(super) fn event(seq: u64, event: UiEvent) -> LinkDatagram {
    let mut datagram = event_datagram(event);
    datagram.envelope.seq = seq;
    datagram
}

/// One fixed frame of every kind. Fields carry distinct non-zero values wherever the type allows,
/// so two fields that swap places or a field whose width changes moves bytes the fixture pins, and
/// each optional is present in one frame and absent in another.
pub(super) fn golden_frames() -> Vec<(&'static str, LinkDatagram)> {
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
            event_ts_us: VenueTs::from_micros(1_700_000_123_456_789),
            state: UiBookState::AwaitingSnapshot,
            bid_len: 29,
            ask_len: 31,
            bids: std::array::from_fn(|level| Level {
                price: Price::from_mantissa(11_800_000_000_000 - 1_000_000 * level as i64),
                qty: Qty::from_mantissa(1_000 + 7 * level as i64),
            }),
            asks: std::array::from_fn(|level| Level {
                price: Price::from_mantissa(-(2_000_000_000_000 + 3_000_000 * level as i64)),
                qty: Qty::from_mantissa(-(500 + 11 * level as i64)),
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
        catalog_ts_us: VenueTs::from_micros(1_700_000_000_000_001),
        total_count: 0x0203,
        instrument: InstrumentId(0x0405),
        display: WireName::new("BTCUSDT"),
        tick_size: Some(Price::from_mantissa(1_000_000)),
        lot_size: Some(Qty::from_mantissa(-7_000)),
        qty_scale: 100_000_000,
        base_asset: AssetId(0x0607),
        quote_asset: AssetId(0x0809),
        base: WireName::new("BTC"),
        quote: WireName::new("USDT"),
    };
    let unstamped = CatalogInstrument {
        catalog_ts_us: VenueTs::from_micros(1_700_000_000_000_002),
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
        quote.bids[level] = Some((
            Price::from_mantissa(-90_000 - level as i64),
            Qty::from_mantissa(300 + level as i64),
        ));
    }
    for level in [0, 3, 7] {
        quote.asks[level] = Some((
            Price::from_mantissa(91_000 + level as i64),
            Qty::from_mantissa(-400 - level as i64),
        ));
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
                    catalog_ts_us: VenueTs::from_micros(1_700_000_000_000_003),
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
                    VenueTs::from_micros(1_700_000_000_000_004),
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
                    event_ts_us: VenueTs::from_micros(1_700_000_000_001_001),
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
                    event_ts_us: VenueTs::from_micros(1_700_000_000_001_002),
                    aggressor: Side::Sell,
                    price: Price::from_mantissa(-123_456_789),
                    qty: Qty::from_mantissa(987_654_321),
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
                    event_ts_us: VenueTs::from_micros(1_700_000_000_001_003),
                    client_id: ClientOrderId(0x1a2b_3c4d_5e6f_7081),
                    quote_level: QuoteLevel::new(5),
                    side: Side::Buy,
                    state: OrderState::CancelInFlight,
                    price: Price::from_mantissa(118_000_123_000_000),
                    qty: Qty::from_mantissa(-5_000_001),
                    filled: Qty::from_mantissa(2_000_003),
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
                    event_ts_us: VenueTs::from_micros(1_700_000_000_001_004),
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
                    event_ts_us: VenueTs::from_micros(1_700_000_000_001_005),
                    quote_level: None,
                    side: Side::Sell,
                    price: Price::from_mantissa(66_123_450_000_000),
                    qty: Qty::from_mantissa(1_250_000),
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
                    event_ts_us: VenueTs::from_micros(1_700_000_000_001_006),
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
                    event_ts_us: VenueTs::from_micros(1_700_000_000_001_007),
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
                    event_ts_us: VenueTs::from_micros(1_700_000_000_001_008),
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
                    event_ts_us: VenueTs::from_micros(1_700_000_000_001_009),
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
                    event_ts_us: VenueTs::from_micros(1_700_000_000_001_010),
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
                    event_ts_us: VenueTs::from_micros(1_700_000_000_001_011),
                    halt: UiHalt::Armed,
                },
            ),
        ),
        (
            "event-execution-halted",
            event(
                0x100c,
                UiEvent::Execution {
                    seq: 0x110c,
                    event_ts_us: VenueTs::from_micros(1_700_000_000_001_012),
                    halt: UiHalt::Halted {
                        reason: HaltReason::DuplicateResting,
                        halted_ts_us: VenueTs::from_micros(1_700_000_000_000_999),
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
            "latency-report",
            LinkDatagram {
                envelope: envelope(TopicId::LATENCY, 0x1010),
                body: LinkBody::Latency(LatencyReport {
                    built_ts_us: VenueTs::from_micros(1_700_000_000_001_016),
                    ages: [
                        Some(DurationUs::from_micros(0x0102_0304)),
                        None,
                        Some(DurationUs::from_micros(-0x0506)),
                    ],
                    cells: std::array::from_fn(|index| {
                        let base = 0x0100_0000_0000 * (index as u64 + 1);
                        LatencyCell {
                            count: base + 1,
                            p50_ns: base + 2,
                            p99_ns: base + 3,
                            p999_ns: base + 4,
                            max_ns: base + 5,
                        }
                    }),
                    backlog_ema: Some(0.312_5 + 1.0e-9),
                }),
            },
        ),
        (
            "latency-report-empty",
            LinkDatagram {
                envelope: envelope(TopicId::LATENCY, 0x1011),
                body: LinkBody::Latency(LatencyReport {
                    built_ts_us: VenueTs::from_micros(1_700_000_000_001_017),
                    ages: [None; 3],
                    cells: [LatencyCell::EMPTY; LATENCY_REPORT_CELLS],
                    backlog_ema: None,
                }),
            },
        ),
    ]
}

pub(super) fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(super) fn from_hex(kind: &str, hex: &str) -> Vec<u8> {
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

pub(super) fn first_difference(encoded: &[u8], checked_in: &[u8]) -> Option<usize> {
    encoded
        .iter()
        .zip(checked_in)
        .position(|(left, right)| left != right)
        .or_else(|| {
            (encoded.len() != checked_in.len()).then(|| encoded.len().min(checked_in.len()))
        })
}
