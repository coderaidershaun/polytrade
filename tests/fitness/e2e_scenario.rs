//! The fixed replay scenario the e2e persistence test drives: two epochs an hour apart, each opening
//! with a market rotation and exercising every message category. Split from the harness so the input
//! definitions read on their own. Determinism makes this one sequence the whole basis of
//! the read-back assertions.

use polysim::config::KlineInterval;
use polysim::ids::{InstrumentId, Side};
use polysim::msg::inbound::InboundMessage;
use polysim::msg::persist::RotationRow;
use polysim::time::TsUs;

use crate::engine_support::{
    ONE, book_reset, delta_chunk, kline, rotation, snapshot_pair, spin, trade,
};

pub const HOUR_US: i64 = 3_600_000_000;
/// Two epochs straddling an hour boundary (buckets 1 and 2) force every table to rotate into two
/// files.
pub const EPOCH_A_BASE: i64 = HOUR_US + 1_000_000;
pub const EPOCH_B_BASE: i64 = 2 * HOUR_US + 1_000_000;

/// The rotation each epoch opens with. Each drives BOTH pathways — a `MarketRotation` through the
/// hot rings (resetting the slot's derived state) and a `RotationRow` down the lineage side-channel
/// — sharing one instrument, window, and receipt stamp, exactly as the adapter emits both.
pub struct RotationFixture {
    pub instrument: u16,
    pub received_ts_us: i64,
    pub window_open_ts_us: i64,
    pub window_close_ts_us: i64,
    pub token_up: &'static str,
    pub token_down: &'static str,
    pub condition_id: &'static str,
}

impl RotationFixture {
    fn message(&self) -> InboundMessage {
        InboundMessage::MarketRotation(rotation(
            self.instrument,
            self.window_open_ts_us,
            self.window_close_ts_us,
            self.received_ts_us,
        ))
    }

    pub fn row(&self) -> RotationRow {
        RotationRow {
            instrument: InstrumentId(self.instrument),
            window_open_ts_us: TsUs::from_micros(self.window_open_ts_us),
            window_close_ts_us: TsUs::from_micros(self.window_close_ts_us),
            token_id_up: self.token_up.into(),
            token_id_down: self.token_down.into(),
            condition_id: self.condition_id.into(),
            received_ts_us: TsUs::from_micros(self.received_ts_us),
        }
    }
}

/// One rotation per epoch, in the two hour buckets — so the rotations table rotates across the
/// boundary like every other table. The window opens ~60s after the subscribe-time receipt stamp.
pub const ROTATIONS: [RotationFixture; 2] = [
    RotationFixture {
        instrument: 0,
        received_ts_us: EPOCH_A_BASE,
        window_open_ts_us: EPOCH_A_BASE + 60_000_000,
        window_close_ts_us: EPOCH_A_BASE + 360_000_000,
        token_up: "tok-a-up",
        token_down: "tok-a-down",
        condition_id: "0xcond-a",
    },
    RotationFixture {
        instrument: 0,
        received_ts_us: EPOCH_B_BASE,
        window_open_ts_us: EPOCH_B_BASE + 60_000_000,
        window_close_ts_us: EPOCH_B_BASE + 360_000_000,
        token_up: "tok-b-up",
        token_down: "tok-b-down",
        condition_id: "0xcond-b",
    },
];

/// Two epochs an hour apart, each exercising every message category. Each opens with a rotation
/// (new window → derived-state wipe) and ends with the reset, so the next opens with a fresh snapshot.
pub fn message_sequence() -> Vec<InboundMessage> {
    let mut sequence = vec![ROTATIONS[0].message()];
    sequence.extend(epoch(EPOCH_A_BASE, 1));
    sequence.push(ROTATIONS[1].message());
    sequence.extend(epoch(EPOCH_B_BASE, 2));
    sequence
}

fn epoch(base: i64, spin_seq: u64) -> Vec<InboundMessage> {
    let (bids, asks) = snapshot_pair(
        0,
        &[(100 * ONE, 2 * ONE), (99 * ONE, ONE)],
        &[(101 * ONE, ONE), (102 * ONE, 2 * ONE)],
        base,
    );
    vec![
        InboundMessage::Book(bids),
        InboundMessage::Book(asks),
        InboundMessage::Book(delta_chunk(
            0,
            Side::Buy,
            &[(100 * ONE, 5 * ONE)],
            base + 10,
        )),
        InboundMessage::Book(delta_chunk(
            0,
            Side::Sell,
            &[(101 * ONE, 3 * ONE)],
            base + 20,
        )),
        InboundMessage::Trade(trade(0, 100 * ONE, 1_000_000, Side::Buy, base + 30)),
        InboundMessage::Trade(trade(0, 101 * ONE, 2_000_000, Side::Sell, base + 40)),
        InboundMessage::Kline(kline(
            0,
            KlineInterval::OneMinute,
            (100 * ONE, 103 * ONE, 98 * ONE, 101 * ONE),
            false,
            base + 50,
        )),
        InboundMessage::Kline(kline(
            0,
            KlineInterval::OneMinute,
            (100 * ONE, 103 * ONE, 98 * ONE, 101 * ONE),
            true,
            base + 60,
        )),
        InboundMessage::SpinTick(spin(spin_seq, base + 70)),
        InboundMessage::BookReset(book_reset(0, base + 80)),
    ]
}
