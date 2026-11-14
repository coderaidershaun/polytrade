//! One engine trades on two venues, and the state of each venue stays its own: its balances, its
//! rate budget, its readiness, and its orders through a stream reset. One run of two venues gives
//! each venue what a run of that venue alone gives it.

use std::sync::{Arc, Mutex};

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{
    Balance, ClientIdLayout, DesiredQuote, ExecSettings, OrderBudget, OrderBudgetWindow,
    OrderState, QuoteLevel, side_base,
};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::msg::exec::{
    AccountChunk, AccountChunkKind, AssetBalance, ExecCommand, ExecEvent, ExecKind, ExecLaneItem,
    VenueOrderStatus,
};
use engine::msg::ids::{
    AssetId, ClientOrderId, InstrumentId, Notional, Price, Qty, Side, TradeId, VenueId,
};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::persist::{OrderRow, OrderTransition, PersistRecord};
use engine::msg::time::{DurationUs, Stamper};
use engine::runtime::config::{RecordedTables, TableKind, TrackerSpec};
use engine::runtime::registry::InstrumentRow;
use engine::venues::exec::open_orders_snapshot_end;
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    ONE, book_reset, detached_setup, exec_event, instrument_row, persist_ring_for, snapshot_pair,
    spin, ts, venue_ts,
};
use crate::exec_support::{ASK, BID, QUOTE_QTY, drain_commands, exec_settings};

const SECOND: i64 = 1_000_000;

/// One instrument for each venue: instrument `v` at venue `v`, with base asset `2v` and quote asset
/// USDT `2v + 1`, the ids that interning by venue and name gives venues that each name USDT.
fn rows(venues: usize) -> Vec<InstrumentRow> {
    (0..venues as u16)
        .map(|index| InstrumentRow {
            venue: VenueId(index as u8),
            base_asset: AssetId(2 * index),
            quote_asset: AssetId(2 * index + 1),
            ..instrument_row(index, TrackerSpec::default(), 64)
        })
        .collect()
}

/// The ledger row of an instrument: position, exposure, and PnL.
type Ledger = (Qty, Notional, Notional);

/// What the strategy read of each venue's instrument at each spin: the quote balance, the state of
/// the bid, and the ledger row.
#[derive(Debug, Clone, PartialEq)]
struct Seen {
    second: i64,
    quote_balance: Vec<Balance>,
    bid: Vec<Option<OrderState>>,
    ledger: Vec<Ledger>,
}

/// Declares one bid at the touch on each instrument each spin, and keeps what it reads.
struct BidOnEach {
    rows: Vec<InstrumentRow>,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Strategy for BidOnEach {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let instruments: Vec<InstrumentId> =
            self.rows.iter().map(|row| row.instrument_id).collect();
        let seen = Seen {
            second: ctx.event_ts().micros() / SECOND,
            quote_balance: self
                .rows
                .iter()
                .map(|row| ctx.balance(row.quote_asset))
                .collect(),
            bid: instruments
                .iter()
                .map(|&instrument| {
                    ctx.resting(instrument, Side::Buy, QuoteLevel::ZERO)
                        .map(|order| order.state)
                })
                .collect(),
            ledger: instruments
                .iter()
                .map(|&instrument| {
                    (
                        ctx.position_base(instrument),
                        ctx.exposure_quote(instrument),
                        ctx.pnl_quote(instrument),
                    )
                })
                .collect(),
        };
        self.seen.lock().expect("seen mutex poisoned").push(seen);
        for instrument in instruments {
            ctx.quote(
                instrument,
                Side::Buy,
                QuoteLevel::ZERO,
                Some(DesiredQuote {
                    price: Price::from_mantissa(BID),
                    qty: QUOTE_QTY,
                }),
            );
        }
    }
}

/// One engine wired to one venue or more, with the lane of each, the order rows, and what the
/// strategy saw.
struct Venues {
    engine: HotEngine,
    rows: Vec<InstrumentRow>,
    lanes: Vec<Consumer<ExecLaneItem>>,
    persist: Consumer<PersistRecord>,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Venues {
    fn two(settings: [ExecSettings; 2]) -> Self {
        Self::new(settings.into_iter().zip([10, 11]).collect())
    }

    /// One venue for each pair of settings and run nonce, in venue order.
    fn new(venues: Vec<(ExecSettings, u32)>) -> Self {
        let rows = rows(venues.len());
        let (persistence, persist) =
            persist_ring_for(4_096, RecordedTables::new(&[TableKind::Orders]));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut lanes = Vec::new();
        let exec = venues
            .into_iter()
            .map(|(settings, run_nonce)| {
                let (producer, lane) = RingBuffer::<ExecLaneItem>::new(256);
                lanes.push(lane);
                ExecWiring {
                    sink: ExecSink::new(producer, Stamper::Off),
                    settings,
                    run_nonce,
                }
            })
            .collect();
        let strategy = Box::new(BidOnEach {
            rows: rows.clone(),
            seen: Arc::clone(&seen),
        });
        let engine = HotEngine::new(HotEngineSetup {
            exec,
            persistence: Some(persistence),
            ..detached_setup(&rows, strategy)
        });
        let mut run = Self {
            engine,
            rows,
            lanes,
            persist,
            seen,
        };
        for instrument in 0..run.rows.len() as u16 {
            let (bids, asks) = snapshot_pair(instrument, &[(BID, ONE)], &[(ASK, ONE)], 0);
            run.engine.dispatch(&book_reset(instrument, 0));
            run.dispatch(InboundMessage::new(ts(0), MessageBody::Book(bids)));
            run.dispatch(InboundMessage::new(ts(0), MessageBody::Book(asks)));
        }
        run
    }

    fn dispatch(&mut self, message: InboundMessage) {
        self.engine.dispatch(&message);
    }

    fn exec(&mut self, venue: usize, at: i64, event: ExecEvent) {
        let event = ExecEvent {
            venue: VenueId(venue as u8),
            ..event
        };
        self.dispatch(InboundMessage::new(ts(at), MessageBody::Exec(event)));
    }

    /// The account stream of the venue is up, its balances are known (USDT 1,000), and its open
    /// orders are known: the three facts that let it place.
    fn ready(&mut self, venue: usize, at: i64) {
        let instrument = InstrumentId(venue as u16);
        let stream = ExecEvent {
            kind: ExecKind::StreamReady,
            ..exec_event(instrument, ClientOrderId(0), Side::Buy, 0, at)
        };
        self.exec(venue, at, stream);
        self.balances(venue, AccountChunkKind::Snapshot, 1_000 * ONE, at);
        self.exec(venue, at, open_orders_snapshot_end(instrument, ts(at)));
    }

    fn balances(&mut self, venue: usize, kind: AccountChunkKind, quote_free: i64, at: i64) {
        let mut chunk = AccountChunk::new(kind, (at / 1_000) as u64, venue_ts(at));
        chunk.push_balances(&[AssetBalance {
            asset: self.rows[venue].quote_asset,
            free: quote_free,
            locked: 0,
        }]);
        chunk.is_last_chunk = true;
        chunk.venue = VenueId(venue as u8);
        self.dispatch(InboundMessage::new(ts(at), MessageBody::Account(chunk)));
    }

    /// One spin, and the commands that each venue's lane received from it.
    fn spin_at(&mut self, second: i64) -> Vec<Vec<ExecCommand>> {
        self.dispatch(spin(second as u64, second * SECOND));
        self.lanes.iter_mut().map(drain_commands).collect()
    }

    /// The venue acknowledges each placement on its lane, so the order rests.
    fn ack(&mut self, venue: usize, commands: &[ExecCommand], at: i64) {
        for command in commands {
            if let ExecCommand::Place {
                instrument,
                client_id,
                price,
                qty,
                side,
                ..
            } = *command
            {
                let ack = ExecEvent {
                    kind: ExecKind::AckPlaced,
                    status: Some(VenueOrderStatus::New),
                    qty,
                    ..exec_event(instrument, client_id, side, price.mantissa(), at)
                };
                self.exec(venue, at, ack);
            }
        }
    }

    /// The venue ends each order on its lane, as a cancel from elsewhere would, so the side quotes
    /// again at the next spin.
    fn end_orders(&mut self, venue: usize, commands: &[ExecCommand], at: i64) {
        for command in commands {
            if let ExecCommand::Place {
                instrument,
                client_id,
                price,
                side,
                ..
            } = *command
            {
                let ended = ExecEvent {
                    kind: ExecKind::ReportCanceled,
                    status: Some(VenueOrderStatus::Canceled),
                    ..exec_event(instrument, client_id, side, price.mantissa(), at)
                };
                self.exec(venue, at, ended);
            }
        }
    }

    /// A trade of `last` on the order that `place` sent, its first.
    fn fill(&mut self, venue: usize, place: &ExecCommand, last: i64, at: i64) {
        let ExecCommand::Place {
            instrument,
            client_id,
            side,
            price,
            qty,
            ..
        } = *place
        else {
            panic!("a fill answers a placement, not {place:?}");
        };
        let last = Qty::from_mantissa(last);
        let trade = ExecEvent {
            kind: ExecKind::ReportTrade,
            status: Some(VenueOrderStatus::PartiallyFilled),
            trade_id: Some(TradeId(1)),
            qty,
            last_price: price,
            last_qty: last,
            cumulative_qty: last,
            cumulative_quote: price.notional(last),
            ..exec_event(instrument, client_id, side, price.mantissa(), at)
        };
        self.exec(venue, at, trade);
    }

    fn seen_at(&self, second: i64) -> Seen {
        self.seen
            .lock()
            .expect("seen mutex poisoned")
            .iter()
            .rfind(|seen| seen.second == second)
            .cloned()
            .unwrap_or_else(|| panic!("no spin ran at second {second}"))
    }

    fn order_rows(&mut self) -> Vec<OrderRow> {
        std::iter::from_fn(|| self.persist.pop().ok())
            .filter_map(|record| match record {
                PersistRecord::Order(row) => Some(row),
                _ => None,
            })
            .collect()
    }

    /// The instruments of the order rows that a stream reset wrote.
    fn reset_rows(&mut self) -> Vec<InstrumentId> {
        self.order_rows()
            .into_iter()
            .filter(|row| row.transition == OrderTransition::StreamReset)
            .map(|row| row.instrument)
            .collect()
    }
}

fn places(commands: &[ExecCommand]) -> usize {
    commands
        .iter()
        .filter(|command| matches!(command, ExecCommand::Place { .. }))
        .count()
}

/// FITNESS: two venues that each name an asset USDT hold two balances that never mix. A balance
/// update of one venue moves no balance of the other and frees no reservation of the other, whose
/// money its own venue has not restated yet. A venue that freed money on another venue's update would
/// place against funds it had already committed.
#[test]
fn balances_of_two_venues_never_mix() {
    let mut two = Venues::two([exec_settings(), exec_settings()]);
    two.ready(0, 0);
    two.ready(1, 0);
    let placed = two.spin_at(1).swap_remove(0);
    assert_eq!(places(&placed), 1, "venue 0 places its bid: {placed:?}");
    two.ack(0, &placed, SECOND + 100_000);
    let reserved = QUOTE_QTY.mantissa() * BID / ONE;
    two.spin_at(2);
    assert_eq!(
        two.seen_at(2).quote_balance[0],
        Balance {
            free: 1_000 * ONE,
            locked: 0,
            reserved,
        },
        "venue 0 holds the cost of its bid against its own USDT"
    );

    two.balances(1, AccountChunkKind::Update, 50 * ONE, 3 * SECOND);
    two.spin_at(4);
    let seen = two.seen_at(4);
    assert_eq!(
        seen.quote_balance,
        [
            Balance {
                free: 1_000 * ONE,
                locked: 0,
                reserved,
            },
            Balance {
                free: 50 * ONE,
                locked: 0,
                reserved,
            },
        ],
        "an update of venue 1, stamped after venue 0's reservation, frees nothing at venue 0, and \
         venue 1 keeps its own bid's reservation against its own USDT"
    );
}

/// FITNESS: a spent rate budget, a readiness gap, and a stream reset of one venue each leave the
/// other venue quoting as before. The stream reset makes unknown exactly the working orders of its
/// own venue. A gate shared across venues would stop a healthy venue for the fault of another.
#[test]
fn spent_budget_on_one_venue_refuses_no_quote_on_other() {
    let spent = OrderBudget::of(&[OrderBudgetWindow {
        window: DurationUs::from_secs(600),
        max_places: 1,
    }])
    .expect("one window");
    let one_place = ExecSettings {
        order_budget: spent,
        ..exec_settings()
    };
    let mut two = Venues::two([one_place, exec_settings()]);
    two.ready(0, 0);
    two.ready(1, 0);
    let mut placed = [0; 2];
    for second in 1..=4 {
        let lanes = two.spin_at(second);
        for venue in 0..2 {
            placed[venue] += places(&lanes[venue]);
            two.end_orders(venue, &lanes[venue], second * SECOND + 100_000);
        }
    }
    assert_eq!(
        placed,
        [1, 4],
        "venue 0 spends its one placement, and venue 1 places at each spin"
    );

    let mut two = Venues::two([exec_settings(), exec_settings()]);
    two.ready(0, 0);
    two.ready(1, 0);
    let first = two.spin_at(1);
    for (venue, commands) in first.iter().enumerate() {
        two.ack(venue, commands, SECOND + 100_000);
    }
    two.reset_rows();
    let reset = ExecEvent {
        kind: ExecKind::StreamReset,
        instrument: None,
        ..exec_event(InstrumentId(0), ClientOrderId(0), Side::Buy, 0, 3 * SECOND)
    };
    two.exec(0, 3 * SECOND, reset);
    assert_eq!(
        two.reset_rows(),
        [InstrumentId(0)],
        "the reset of venue 0 makes unknown its own working order and no order of venue 1"
    );
    two.spin_at(4);
    assert_eq!(
        two.seen_at(4).bid,
        [Some(OrderState::Unknown), Some(OrderState::Live)],
        "venue 1's bid rests as it was through venue 0's reset"
    );
    two.end_orders(1, &first[1], 4 * SECOND + 500_000);
    let lanes = two.spin_at(5);
    assert_eq!(
        [places(&lanes[0]), places(&lanes[1])],
        [0, 1],
        "venue 0 waits for its stream and open orders, and venue 1 places its next bid: {lanes:?}"
    );
}

/// What one venue of a run gave, in that venue's own terms: its instrument is 0, and its client ids
/// count slots from its own first slot.
#[derive(Debug, Default, PartialEq)]
struct VenueTrace {
    commands: Vec<(i64, ExecCommand)>,
    seen: Vec<(i64, Balance, Option<OrderState>, Ledger)>,
    rows: Vec<OrderRow>,
}

fn local_client_id(id: ClientOrderId, instrument: InstrumentId) -> ClientOrderId {
    let first = side_base(instrument, Side::Buy) - side_base(InstrumentId(0), Side::Buy);
    let layout = ClientIdLayout {
        run_nonce: ClientIdLayout::nonce_of(id),
    };
    layout.encode(
        ClientIdLayout::slot_of(id) - first,
        ClientIdLayout::generation_of(id),
    )
}

fn local_command(command: ExecCommand) -> ExecCommand {
    let local = InstrumentId(0);
    match command {
        ExecCommand::Place {
            instrument,
            client_id,
            side,
            price,
            qty,
            style,
        } => ExecCommand::Place {
            instrument: local,
            client_id: local_client_id(client_id, instrument),
            side,
            price,
            qty,
            style,
        },
        ExecCommand::Cancel {
            instrument,
            client_id,
        } => ExecCommand::Cancel {
            instrument: local,
            client_id: local_client_id(client_id, instrument),
        },
        ExecCommand::AmendQty {
            instrument,
            client_id,
            qty,
        } => ExecCommand::AmendQty {
            instrument: local,
            client_id: local_client_id(client_id, instrument),
            qty,
        },
        ExecCommand::ReconcileOrder {
            instrument,
            client_id,
            recon_seq,
        } => ExecCommand::ReconcileOrder {
            instrument: local,
            client_id: local_client_id(client_id, instrument),
            recon_seq,
        },
        ExecCommand::ReconcileOpenOrders { recon_seq, .. } => ExecCommand::ReconcileOpenOrders {
            instrument: local,
            recon_seq,
        },
        ExecCommand::CancelOurs { reason, .. } => ExecCommand::CancelOurs {
            instrument: local,
            reason,
        },
        ExecCommand::CancelPriorRun { .. } => ExecCommand::CancelPriorRun { instrument: local },
        ExecCommand::ResubscribeStream => ExecCommand::ResubscribeStream,
    }
}

/// Plays one script for each venue of the run, in step: each venue is ready at once, places at each
/// spin, sees its placements rest, and at second 2 fills a part of its first bid and restates its
/// USDT. Script 0 then loses its stream at second 3. The two scripts differ in each amount, so a
/// value of one venue that reaches the other shows.
fn play(run: &mut Venues, scripts: &[usize]) -> Vec<VenueTrace> {
    let mut traces: Vec<VenueTrace> = scripts.iter().map(|_| VenueTrace::default()).collect();
    let mut first_bids = vec![None; scripts.len()];
    for venue in 0..scripts.len() {
        run.ready(venue, 0);
    }
    for second in 1..=5 {
        let at = second * SECOND;
        let lanes = run.spin_at(second);
        for (venue, commands) in lanes.iter().enumerate() {
            let local = commands
                .iter()
                .map(|command| (second, local_command(*command)));
            traces[venue].commands.extend(local);
            run.ack(venue, commands, at + 100_000);
            first_bids[venue] = first_bids[venue].or(commands.first().copied());
        }
        for (venue, &script) in scripts.iter().enumerate() {
            let scale = script as i64 + 1;
            if let (2, Some(place)) = (second, first_bids[venue]) {
                let last = scale * QUOTE_QTY.mantissa() / 4;
                run.fill(venue, &place, last, at + 300_000);
            }
            if second == 2 {
                let free = (700 - 100 * scale) * ONE;
                run.balances(venue, AccountChunkKind::Update, free, at + 400_000);
            }
            if second == 3 && script == 0 {
                let reset = ExecEvent {
                    kind: ExecKind::StreamReset,
                    instrument: None,
                    ..exec_event(
                        InstrumentId(0),
                        ClientOrderId(0),
                        Side::Buy,
                        0,
                        at + 200_000,
                    )
                };
                run.exec(venue, at + 200_000, reset);
            }
        }
    }
    let seen = run.seen.lock().expect("seen mutex poisoned").clone();
    let rows = run.order_rows();
    for (venue, trace) in traces.iter_mut().enumerate() {
        let instrument = InstrumentId(venue as u16);
        trace.seen = seen
            .iter()
            .map(|each| {
                let read = (
                    each.quote_balance[venue],
                    each.bid[venue],
                    each.ledger[venue],
                );
                (each.second, read.0, read.1, read.2)
            })
            .collect();
        trace.rows = rows
            .iter()
            .filter(|row| row.instrument == instrument)
            .map(|row| OrderRow {
                instrument: InstrumentId(0),
                client_id: local_client_id(row.client_id, instrument),
                ..*row
            })
            .collect();
    }
    traces
}

/// FITNESS: one run of two venues gives each venue the commands, the reads of the strategy (its
/// balance, its bid, its ledger row) and the order rows that a run of that venue alone gives it. A
/// fold that shared a field of one venue with the other would trade one venue on the other's facts.
#[test]
fn two_venues_equal_two_single_runs() {
    let settings = exec_settings();
    let mut both = Venues::new(vec![(settings, 10), (settings, 11)]);
    let mut merged = play(&mut both, &[0, 1]).into_iter();
    for script in 0..2 {
        let mut alone = Venues::new(vec![(settings, 10 + script as u32)]);
        let single = play(&mut alone, &[script]).swap_remove(0);
        let merged = merged.next().expect("one trace for each venue");
        assert!(
            !single.commands.is_empty() && single.rows.len() > single.commands.len(),
            "script {script} places, fills and rests orders: {single:?}"
        );
        assert_eq!(
            merged, single,
            "venue {script} of the run of two gives what its run alone gives"
        );
    }
}
