//! What a drawn sequence is made of: one draw for each kind of message the fold takes, the
//! generator of drawn sequences, and a fixed sequence that is sure to place an order and fold a fill.

use engine::msg::exec::{ExecKind, VenueOrderStatus};
use engine::msg::ids::Side;
use engine::msg::inbound::{InboundMessage, MessageBody};
use proptest::prelude::{Just, any, prop, prop_oneof};
use proptest::strategy::Strategy as Generator;

/// Long enough that a drawn sequence passes the warmup and places, short enough to fold often.
const SEQUENCE_LEN: usize = 200;

/// One name for each kind of message, in the order of [`kind_of`].
pub(crate) const KINDS: [&str; 12] = [
    "trade",
    "book",
    "book reset",
    "market rotation",
    "kline",
    "spin",
    "link",
    "run control",
    "exec",
    "account",
    "writer failed",
    "audit bank full",
];

/// No arm catches the rest, so a new kind of message stops this file compiling here. Its name then
/// joins [`KINDS`], and the test fails until the generator draws it.
pub(crate) fn kind_of(message: &InboundMessage) -> usize {
    match &message.body {
        MessageBody::Trade(_) => 0,
        MessageBody::Book(_) => 1,
        MessageBody::BookReset(_) => 2,
        MessageBody::MarketRotation(_) => 3,
        MessageBody::Kline(_) => 4,
        MessageBody::SpinTick(_) => 5,
        MessageBody::Link(_) => 6,
        MessageBody::RunControl(_) => 7,
        MessageBody::Exec(_) => 8,
        MessageBody::Account(_) => 9,
        MessageBody::WriterFailed(_) => 10,
        MessageBody::AuditBankFull(_) => 11,
    }
}

/// The values of one drawn message. Each variant makes one kind of message.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Draw {
    Trade {
        market: u16,
        offset: i64,
        lots: i64,
        side: Side,
    },
    Book {
        market: u16,
        chunk: Chunk,
        side: Side,
        offset: i64,
        lots: i64,
    },
    BookReset {
        market: u16,
    },
    Rotation {
        market: u16,
    },
    Kline {
        market: u16,
        offset: i64,
        is_closed: bool,
        is_tracked: bool,
    },
    Spin,
    Link {
        value: i32,
    },
    RunControl {
        is_running: bool,
        is_new_epoch: bool,
    },
    Exec {
        market: u16,
        side: Side,
        answer: Answer,
    },
    Account {
        base_units: i64,
        quote_units: i64,
    },
    WriterFailed {
        consecutive_failures: u32,
    },
    AuditBankFull {
        banked: u32,
    },
}

/// A book chunk: a delta, or the first or the last chunk of a snapshot.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Chunk {
    Delta,
    SnapshotBids,
    SnapshotAsks,
}

/// What the venue says about the orders of one side of one market.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Answer {
    StreamReady,
    SnapshotEnd,
    /// A fill of an order that an earlier run left resting.
    PriorFill {
        offset: i64,
        lots: i64,
    },
    /// An answer about one of the first slots of a side, which this run may have placed.
    Slot {
        slot: usize,
        generation: u16,
        kind: ExecKind,
        status: VenueOrderStatus,
    },
}

/// One drawn message and the gap of message time before it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Step {
    pub(crate) draw: Draw,
    pub(crate) gap_us: i64,
}

fn side() -> impl Generator<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

fn answer() -> impl Generator<Value = Answer> {
    let kind = prop_oneof![
        Just((ExecKind::AckPlaced, VenueOrderStatus::New)),
        Just((ExecKind::AckCanceled, VenueOrderStatus::Canceled)),
        Just((ExecKind::ReportTrade, VenueOrderStatus::Filled)),
        Just((ExecKind::ReportCanceled, VenueOrderStatus::Canceled)),
    ];
    prop_oneof![
        Just(Answer::StreamReady),
        Just(Answer::SnapshotEnd),
        (0..5_i64, 1..4_i64).prop_map(|(offset, lots)| Answer::PriorFill { offset, lots }),
        (0..4_usize, 1..4_u16, kind).prop_map(|(slot, generation, (kind, status))| {
            Answer::Slot {
                slot,
                generation,
                kind,
                status,
            }
        }),
    ]
}

fn draw() -> impl Generator<Value = Draw> {
    let market = 0..2_u16;
    let chunk = prop_oneof![
        2 => Just(Chunk::Delta),
        1 => Just(Chunk::SnapshotBids),
        1 => Just(Chunk::SnapshotAsks),
    ];
    prop_oneof![
        3 => (market.clone(), 0..5_i64, 1..4_i64, side())
            .prop_map(|(market, offset, lots, side)| Draw::Trade { market, offset, lots, side }),
        4 => (market.clone(), chunk, side(), 0..5_i64, 0..4_i64).prop_map(
            |(market, chunk, side, offset, lots)| Draw::Book { market, chunk, side, offset, lots }
        ),
        1 => market.clone().prop_map(|market| Draw::BookReset { market }),
        1 => market.clone().prop_map(|market| Draw::Rotation { market }),
        2 => (market.clone(), 0..5_i64, any::<bool>(), any::<bool>()).prop_map(
            |(market, offset, is_closed, is_tracked)| Draw::Kline {
                market,
                offset,
                is_closed,
                is_tracked
            }
        ),
        4 => Just(Draw::Spin),
        1 => any::<i32>().prop_map(|value| Draw::Link { value }),
        1 => (any::<bool>(), any::<bool>()).prop_map(|(is_running, is_new_epoch)| {
            Draw::RunControl { is_running, is_new_epoch }
        }),
        4 => (market.clone(), side(), answer())
            .prop_map(|(market, side, answer)| Draw::Exec { market, side, answer }),
        1 => (0..20_i64, 0..2_000_000_i64).prop_map(|(base_units, quote_units)| Draw::Account {
            base_units,
            quote_units
        }),
        1 => (1..5_u32).prop_map(|consecutive_failures| Draw::WriterFailed {
            consecutive_failures
        }),
        1 => (1..65_536_u32).prop_map(|banked| Draw::AuditBankFull { banked }),
    ]
}

pub(crate) fn steps() -> impl Generator<Value = Vec<Step>> {
    let step = (draw(), 0..20_000_i64).prop_map(|(draw, gap_us)| Step { draw, gap_us });
    prop::collection::vec(step, SEQUENCE_LEN)
}

/// A sequence that is sure to place and to fill: the account stream is up, the open orders have
/// landed, the balances are known, both sides of market 0 are booked, a spin past `warmup_us` lets
/// the probe quote its touch, and then an order that an earlier run left resting is seated and
/// filled.
pub(crate) fn placing_and_filling_steps(warmup_us: i64) -> Vec<Step> {
    let exec = |answer| Draw::Exec {
        market: 0,
        side: Side::Buy,
        answer,
    };
    let book = |chunk, side| Draw::Book {
        market: 0,
        chunk,
        side,
        offset: 0,
        lots: 1,
    };
    let prior_fill = exec(Answer::PriorFill { offset: 0, lots: 1 });
    [
        (exec(Answer::StreamReady), 0),
        (exec(Answer::SnapshotEnd), 0),
        (
            Draw::Account {
                base_units: 10,
                quote_units: 1_000_000,
            },
            0,
        ),
        (book(Chunk::SnapshotBids, Side::Buy), 0),
        (book(Chunk::SnapshotAsks, Side::Sell), 0),
        (Draw::Spin, warmup_us + 100_000),
        (prior_fill, 1_000),
        (prior_fill, 1_000),
    ]
    .map(|(draw, gap_us)| Step { draw, gap_us })
    .to_vec()
}
