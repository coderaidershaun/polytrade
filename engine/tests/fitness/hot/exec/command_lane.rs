//! The command lane to the edge: what a command carries when the lane makes it wait in the bank.
//! The simulator orders its answers by these stamps, so a stamp that moved would reorder a run.

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{DesiredQuote, QuoteLevel};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::msg::exec::{ExecCommand, ExecLaneItem, OrderStyle, StampedExecCommand};
use engine::msg::ids::{ClientOrderId, InstrumentId, Price, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::time::Stamper;
use engine::venues::exec::open_orders_snapshot_end;
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{ONE, detached_setup, snapshot_pair, stream_ready, ts};
use crate::exec_support::{
    ASK, BID, CEILING, INSTRUMENT, QUOTE_QTY, exec_settings, make_ready, reseat_book,
    row_with_ceiling, spin_at,
};

/// The second instrument quotes beside the first, so one spin banks three placements: both sides of
/// the first instrument and the bid of the second. The engine places one order a side a spin.
const SECOND: InstrumentId = InstrumentId(1);

struct ThreePlacements;

impl Strategy for ThreePlacements {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let quotes = [
            (INSTRUMENT, Side::Buy, BID),
            (INSTRUMENT, Side::Sell, ASK),
            (SECOND, Side::Buy, BID),
        ];
        for (instrument, side, price) in quotes {
            ctx.quote(
                instrument,
                side,
                QuoteLevel::ZERO,
                Some(DesiredQuote {
                    price: Price::from_mantissa(price),
                    qty: QUOTE_QTY,
                    style: OrderStyle::PostOnly,
                }),
            );
        }
    }
}

fn read(lane: &mut Consumer<ExecLaneItem>) -> Vec<ExecLaneItem> {
    std::iter::from_fn(|| lane.pop().ok()).collect()
}

fn command(item: &ExecLaneItem) -> StampedExecCommand {
    match item {
        ExecLaneItem::Command(stamped) => *stamped,
        ExecLaneItem::Watermark(at) => panic!("a command was due here, not the watermark {at:?}"),
    }
}

/// CONTRACT: a command that the full lane leaves in the bank goes out at a later spin with the
/// stamp and the tick of its banking, behind the commands banked before it, and the watermark
/// follows only the full drain. A held command stamped at its drain would look younger than it is,
/// and the simulator would answer it after commands that the fold made later.
#[test]
fn held_command_keeps_bank_stamp() {
    let (clock, mock) = quanta::Clock::mock();
    let (producer, mut lane) = RingBuffer::<ExecLaneItem>::new(2);
    let mut second = row_with_ceiling(CEILING);
    second.instrument_id = SECOND;
    second.venue_symbol = "ETHUSDT".into();
    let instruments = [row_with_ceiling(CEILING), second];
    let mut engine = HotEngine::new(HotEngineSetup {
        exec: Some(ExecWiring {
            sink: ExecSink::new(producer, Stamper::Counter(clock)),
            settings: exec_settings(),
            run_nonce: 0,
        }),
        ..detached_setup(&instruments, Box::new(ThreePlacements))
    });
    make_ready(&mut engine, 500_000);
    engine.dispatch(&stream_ready(SECOND, 500_000));
    engine.dispatch(&InboundMessage::new(
        ts(500_000),
        MessageBody::Exec(open_orders_snapshot_end(SECOND, ts(500_000))),
    ));
    for message in reseat_book(BID, ASK, 500_000) {
        engine.dispatch(&message);
    }
    let (bids, asks) = snapshot_pair(1, &[(BID, ONE)], &[(ASK, ONE)], 500_000);
    engine.dispatch(&InboundMessage::new(ts(500_000), MessageBody::Book(bids)));
    engine.dispatch(&InboundMessage::new(ts(500_000), MessageBody::Book(asks)));

    spin_at(&mut engine, 5, 1_000_000);
    let slot_five = read(&mut lane);
    assert_eq!(
        slot_five.len(),
        2,
        "the lane takes two items: {slot_five:?}"
    );
    let (first, second) = (command(&slot_five[0]), command(&slot_five[1]));
    assert_eq!(first.issued_ts_us, ts(1_000_000));
    assert_eq!(second.issued_ts_us, ts(1_000_000));

    mock.increment(1_000_u64);
    spin_at(&mut engine, 6, 1_001_000);
    let slot_six = read(&mut lane);
    assert_eq!(
        slot_six.len(),
        2,
        "the held command and the watermark: {slot_six:?}"
    );
    let third = command(&slot_six[0]);
    assert_eq!(
        third.issued_ts_us,
        ts(1_000_000),
        "the held command keeps the stamp of its banking"
    );
    assert_eq!(
        third.issue_tick, first.issue_tick,
        "and the latency tick of its banking"
    );
    assert!(
        matches!(slot_six[1], ExecLaneItem::Watermark(at) if at == ts(1_001_000)),
        "the watermark follows the full drain: {:?}",
        slot_six[1]
    );

    let ids: Vec<ClientOrderId> = [first, second, third]
        .iter()
        .map(|stamped| match stamped.command {
            ExecCommand::Place { client_id, .. } => client_id,
            other => panic!("only placements were banked, got {other:?}"),
        })
        .collect();
    let mut in_order = ids.clone();
    in_order.sort_unstable_by_key(|id| id.0);
    in_order.dedup();
    assert_eq!(
        ids, in_order,
        "C1, C2, C3 in the order of their banking, each sent once"
    );
    assert!(read(&mut lane).is_empty(), "no command is sent two times");
}
