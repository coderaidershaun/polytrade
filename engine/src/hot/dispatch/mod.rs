//! The body of the hot loop: each message updates state, calls the strategy back, drains the
//! actions and runs the execution pass. It reads no clock, so state depends on messages alone.

mod digest;
mod exposure;
mod gate;
mod market;
mod setup;
mod telemetry;
mod ui_emit;

use exposure::{moves_position, restored_snapshot, warn_uncovered_instruments};
use gate::Warmup;

use crate::hot::book::Book;
use crate::hot::dispatch::ui_emit::UiEmitter;
use crate::hot::exec::{DesiredBook, ExecCallback, ExecEngine, QuoteLevel, SpinInput};
use crate::hot::ledger::PositionLedger;
use crate::hot::quant::volatility::EwmaVol;
use crate::hot::sink::{ExposureSink, LinkSink, PersistSink, StrategyLogSink};
use crate::hot::strategy::{Actions, ActionsSetup, DrainSinks, Strategy, StrategyCtx, WindowInfo};
use crate::hot::tracker::MicroTracker;
use crate::msg::exec::{AccountChunk, ExecEvent};
use crate::msg::ids::{InstrumentId, Price, Qty, Side};
use crate::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use crate::msg::run_state::RunAssertion;
use crate::msg::time::{FoldTime, LocalTs};
use crate::msg::ui::{DomQuote, UI_QUOTE_LEVELS};
use crate::runtime::config::RecordedTables;
use crate::runtime::exposure::{ExposureSnapshot, MAX_EXPOSURE_INSTRUMENTS};
use crate::runtime::shutdown::RunStateCell;

pub use digest::StateDigest;
pub use exposure::ExposureWiring;
pub use gate::LinkWiring;
pub use setup::{ExecWiring, HotEngineSetup, PersistWiring};

use setup::{Declarations, per_instrument_state};

/// Split for borrow: callback borrows immutably, engine holds strategy mutably.
struct HotState {
    books: Vec<Book>,
    trackers: Vec<MicroTracker>,
    ewma: Vec<Option<EwmaVol>>,
    windows: Vec<Option<WindowInfo>>,
    ledger: PositionLedger,
    /// Ledger's durable half (next boot restore).
    exposure: ExposureSink,
    /// Writer's state (seq is one fact with what changed).
    published_exposure: ExposureSnapshot,
    /// Spin of current declarations (on state for single-arg ctx building).
    spin_seq: u64,
    /// The latest received stamp folded so far: the time that deadlines and the strategy read.
    fold_time: FoldTime,
    /// Beside ledger not inside (fill moves both, one owner per state).
    exec: ExecEngine,
    /// Outside exec (callback &mut desired + & exec, can't coexist in same struct).
    desired: DesiredBook,
    actions: Actions,
    /// None if no persistence configured.
    sink: Option<PersistSink>,
    /// Bound `StrategyCtx::link_send` refuses a topic past.
    declared_link_topics: usize,
}

impl HotState {
    fn persist_dropped(&self) -> u64 {
        self.sink.as_ref().map_or(0, PersistSink::dropped)
    }

    fn retry_pending_seal(&mut self) {
        if let Some(sink) = self.sink.as_mut() {
            sink.retry_pending_seal();
        }
    }

    fn ctx(&mut self, event_ts: LocalTs) -> StrategyCtx<'_> {
        StrategyCtx {
            books: &self.books,
            trackers: &self.trackers,
            ewma: &self.ewma,
            windows: &self.windows,
            ledger: &self.ledger,
            exec: &self.exec,
            desired: &mut self.desired,
            actions: &mut self.actions,
            event_ts,
            fold_time: self.fold_time,
            spin_seq: self.spin_seq,
            declared_link_topics: self.declared_link_topics,
        }
    }
}

pub struct HotEngine {
    state: HotState,
    strategy: Box<dyn Strategy>,
    strategy_log_sink: StrategyLogSink,
    link_sink: Option<LinkSink>,
    run: RunAssertion,
    run_report: Option<RunStateCell>,
    unretained_volume_bars: u64,
    warmup: Warmup,
    feature_names: Vec<&'static str>,
    ui: UiEmitter,
}

impl HotEngine {
    pub fn new(setup: HotEngineSetup<'_>) -> Self {
        let HotEngineSetup {
            instruments,
            mut strategy,
            persistence,
            strategy_log_sink,
            ui_book_sink,
            ui_event_sink,
            link,
            warmup,
            exec,
            exposure,
        } = setup;
        if instruments.len() > MAX_EXPOSURE_INSTRUMENTS {
            warn_uncovered_instruments(instruments.len());
        }
        let (books, trackers, ewma) = per_instrument_state(instruments);
        let declarations = Declarations::resolve(strategy.as_mut(), instruments);
        let (persist_sink, tables) = match persistence {
            Some(PersistWiring { sink, tables }) => (Some(sink), tables),
            None => (None, RecordedTables::default()),
        };
        let (link_sink, run_report) = match link {
            Some(LinkWiring { sink, acknowledged }) => (Some(sink), Some(acknowledged)),
            None => (None, None),
        };
        Self {
            state: HotState {
                books,
                trackers,
                ewma,
                windows: vec![None; instruments.len()],
                ledger: PositionLedger::new(instruments.len(), exposure.restored),
                exposure: exposure.sink,
                published_exposure: restored_snapshot(exposure.restored),
                spin_seq: 0,
                fold_time: FoldTime::BEFORE_FIRST,
                exec: ExecEngine::new(ExecWiring::engine_setup(exec, instruments)),
                desired: DesiredBook::new(instruments.len()),
                actions: Actions::new(ActionsSetup {
                    tables,
                    link_schema_hash: declarations.link_schema_hash,
                }),
                sink: persist_sink,
                declared_link_topics: declarations.link_topics,
            },
            strategy,
            strategy_log_sink,
            link_sink,
            run: RunAssertion::INITIAL,
            run_report,
            unretained_volume_bars: 0,
            warmup: Warmup::new(warmup),
            feature_names: declarations.feature_names,
            ui: UiEmitter::new(ui_book_sink, ui_event_sink, instruments.len()),
        }
    }

    pub fn feature_names(&self) -> &[&'static str] {
        &self.feature_names
    }

    /// The fold time advances before anything reads it, so each deadline and each callback of the
    /// message reads it, while the records that the message writes keep its own stamp.
    pub fn dispatch(&mut self, message: &InboundMessage) {
        let received_ts_us = message.header.received_ts_us;
        self.state.fold_time = self.state.fold_time.advanced_to(received_ts_us);
        crate::log::set_message_time(received_ts_us);
        if self.warmup.observe(self.state.fold_time) {
            self.log_warmup_complete();
        }
        self.fold_message(message, received_ts_us);
    }

    /// The work of one message: the handler of its kind, the exposure that it moved, and the drain
    /// of what it banked. A spin runs its own steps instead.
    fn fold_message(&mut self, message: &InboundMessage, received_ts_us: LocalTs) {
        match &message.body {
            MessageBody::SpinTick(tick) => return self.fold_spin(tick, received_ts_us),
            MessageBody::Trade(event) => self.on_trade(event, received_ts_us),
            MessageBody::Book(chunk) => self.on_book(chunk, received_ts_us),
            MessageBody::BookReset(reset) => self.on_book_reset(reset, received_ts_us),
            MessageBody::MarketRotation(rotation) => {
                self.on_market_rotation(rotation, received_ts_us);
            }
            MessageBody::Kline(event) => self.on_kline(event, received_ts_us),
            MessageBody::Link(frame) => self.on_link(frame, received_ts_us),
            MessageBody::RunControl(control) => self.on_run_control(control, received_ts_us),
            MessageBody::Exec(event) => self.on_exec(event, received_ts_us),
            MessageBody::Account(chunk) => self.on_account(chunk),
        }
        if moves_position(message) {
            self.state.publish_exposure(received_ts_us);
        }
        self.drain_actions();
    }

    /// The steps of one spin, in the order that each relies on: the strategy reads the trackers
    /// that sampled this spin, the execution pass sees the declarations of this spin, and the
    /// execution state and the positions go out after the drain. The driver snapshots the
    /// counters after this returns.
    fn fold_spin(&mut self, tick: &SpinTick, received_ts_us: LocalTs) {
        self.state.spin_seq = tick.seq;
        self.sample_trackers();
        self.retry_latched_writes();
        self.spin_strategy(tick, received_ts_us);
        self.drain_actions();
        self.run_execution_pass(tick);
        self.emit_refusals();
        self.emit_execution_state(tick, received_ts_us);
        self.emit_positions(received_ts_us);
    }

    /// Post-drain per-spin (fills folded). Ahead of live gate (corrects late-attach UI).
    fn emit_positions(&mut self, event_ts: LocalTs) {
        for (instrument, row) in self.state.ledger.rows() {
            if row.has_mark() {
                self.ui.emit_position(instrument, row, event_ts);
            }
        }
    }

    /// Drain bank to sinks (fixed priority). No-emit fast path avoids cost.
    #[inline]
    fn drain_actions(&mut self) {
        if self.state.actions.is_empty() {
            return;
        }
        self.state.actions.drain(DrainSinks {
            persist: self.state.sink.as_mut(),
            log_sink: &mut self.strategy_log_sink,
            event_sink: &mut self.ui.event_sink,
            event_seq: &mut self.ui.event_seq,
            link_sink: self.link_sink.as_mut(),
        });
    }

    /// Apply ungated (fills during warmup = money). Callback gated only.
    fn on_exec(&mut self, event: &ExecEvent, received_ts_us: LocalTs) {
        let HotState {
            exec,
            ledger,
            actions,
            fold_time,
            ..
        } = &mut self.state;
        let callback = exec.on_exec_event(event, *fold_time, ledger, actions);
        // Tee ahead of live gate (operator sees fill/transition/refusal).
        match callback {
            ExecCallback::None => {}
            ExecCallback::Fill(fill) => {
                // Fill = two UI facts: tape delta + absolute state. State first (terminal>tape).
                if let Some(order) = exec.order(fill.client_id) {
                    self.ui
                        .emit_order_update(&order.updated_at(fill.event_ts_us));
                }
                self.ui.emit_fill(&fill);
            }
            ExecCallback::Update(update) => self.ui.emit_order_update(&update),
            ExecCallback::Reject(reject) => self.ui.emit_reject(&reject),
        }
        if !self.is_strategy_live() {
            return;
        }
        let mut ctx = self.state.ctx(received_ts_us);
        match callback {
            ExecCallback::None => {}
            ExecCallback::Fill(fill) => self.strategy.on_fill(&mut ctx, &fill),
            ExecCallback::Update(update) => self.strategy.on_order_update(&mut ctx, &update),
            ExecCallback::Reject(reject) => self.strategy.on_reject(&mut ctx, &reject),
        }
    }

    /// No callback (balance=read not event). UI re-states every spin.
    fn on_account(&mut self, chunk: &AccountChunk) {
        self.state.exec.on_account(chunk);
    }

    fn sample_trackers(&mut self) {
        for tracker in &mut self.state.trackers {
            tracker.on_spin();
        }
    }

    /// A parked run seals its files through a spin, so a seal or a snapshot that the ring refused
    /// is tried again on each spin.
    fn retry_latched_writes(&mut self) {
        self.state.retry_pending_seal();
        self.state.exposure.retry_pending();
    }

    fn spin_strategy(&mut self, tick: &SpinTick, received_ts_us: LocalTs) {
        if !self.is_strategy_live() {
            return;
        }
        let mut ctx = self.state.ctx(received_ts_us);
        self.strategy.on_spin(&mut ctx, tick);
    }

    /// Runs parked and warming too, so the engine still answers for the orders that it holds.
    fn run_execution_pass(&mut self, tick: &SpinTick) {
        let HotState {
            books,
            ledger,
            desired,
            windows,
            exec,
            actions,
            fold_time,
            ..
        } = &mut self.state;
        exec.on_spin(SpinInput {
            tick,
            now: *fold_time,
            books,
            desired,
            windows,
            ledger,
            bank: actions,
        });
    }

    /// The pass banks its refusals, and they go out only after it, so each one reads the state that
    /// the pass left.
    fn emit_refusals(&mut self) {
        let ui = &mut self.ui;
        self.state
            .exec
            .drain_refusals(&mut |reject| ui.emit_reject(reject));
    }

    /// Halt/balances/desired every spin (corrects late-attach). Desired expires this spin.
    fn emit_execution_state(&mut self, tick: &SpinTick, at: LocalTs) {
        let is_execution_wired = if let Some(halt) = self.state.exec.halt_state() {
            self.ui.emit_execution(halt, at);
            true
        } else {
            false
        };
        self.ui.emit_balances(self.state.exec.balances(), at);
        for index in 0..self.state.desired.instrument_count() {
            let instrument = InstrumentId(index as u16);
            if is_execution_wired {
                for side in [Side::Buy, Side::Sell] {
                    let exec = &self.state.exec;
                    let ui = &mut self.ui;
                    ui.emit_order_snapshot(
                        instrument,
                        side,
                        exec.working_orders(instrument, side),
                        at,
                    );
                }
            }
            let quote = DomQuote {
                bids: self.desired_ladder(instrument, Side::Buy, tick.seq),
                asks: self.desired_ladder(instrument, Side::Sell, tick.seq),
            };
            self.ui.emit_desired(instrument, quote, at);
        }
    }

    #[inline]
    fn desired_ladder(
        &self,
        instrument: InstrumentId,
        side: Side,
        spin_seq: u64,
    ) -> [Option<(Price, Qty)>; UI_QUOTE_LEVELS] {
        QuoteLevel::ALL.map(|level| {
            self.state
                .desired
                .quote(instrument, side, level, spin_seq)
                .map(|desired| (desired.price, desired.qty))
        })
    }
}

const _: () = assert!(UI_QUOTE_LEVELS == crate::hot::exec::MAX_QUOTE_LEVELS);
