//! Hot loop body: message -> state apply -> callback -> drain -> metrics. State = pure sequence function.

mod exposure;
mod gate;
mod market;
mod setup;
mod telemetry;

use exposure::{moves_position, restored_snapshot, warn_uncovered_instruments};
use gate::Warmup;

use crate::config::RecordedTables;
use crate::exposure::{ExposureSnapshot, MAX_EXPOSURE_INSTRUMENTS};
use crate::hot::book::Book;
use crate::hot::exec::{DesiredBook, ExecCallback, ExecEngine, QuoteLevel, SpinInput};
use crate::hot::ingress::QueueSample;
use crate::hot::ledger::PositionLedger;
use crate::hot::metrics::{HotMetrics, message_meta};
use crate::hot::quant::volatility::EwmaVol;
use crate::hot::strategy::{Actions, ActionsSetup, DrainSinks, Strategy, StrategyCtx, WindowInfo};
use crate::hot::tracker::MicroTracker;
use crate::hot::ui_emit::UiEmitter;
use crate::ids::{InstrumentId, Price, Qty, Side};
use crate::msg::exec::{AccountChunk, ExecEvent};
use crate::msg::inbound::{InboundMessage, SpinTick};
use crate::msg::ui::{DomQuote, UI_QUOTE_LEVELS};
use crate::shutdown::{RunAssertion, RunStateCell};
use crate::sink::{ExposureSink, LinkSink, MetricsSink, PersistSink, StrategyLogSink};
use crate::time::{EngineClock, TsUs};

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

    fn ctx(&mut self, event_ts: TsUs) -> StrategyCtx<'_> {
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
    clock: EngineClock,
    metrics: Box<HotMetrics>,
    metrics_sink: MetricsSink,
    ui: UiEmitter,
}

impl HotEngine {
    pub fn new(setup: HotEngineSetup<'_>) -> Self {
        let HotEngineSetup {
            instruments,
            mut strategy,
            persistence,
            strategy_log_sink,
            metrics_sink,
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
            clock: EngineClock::start(),
            metrics: Box::new(HotMetrics::new()),
            metrics_sink,
            ui: UiEmitter::new(ui_book_sink, ui_event_sink, instruments.len()),
        }
    }

    pub fn feature_names(&self) -> &[&'static str] {
        &self.feature_names
    }

    pub fn dispatch(&mut self, pop: QueueSample, message: &InboundMessage) {
        let dequeued_at = self.clock.now();
        self.metrics
            .record_occupancy(pop.queue_id, pop.depth, dequeued_at);
        if let Some(backlog) = pop.spin_backlog {
            self.metrics
                .record_spin_backlog(backlog, message.received_ts_us());
        }
        let meta = message_meta(message);
        if let Some(meta) = meta {
            self.record_ingress(meta, dequeued_at);
        }
        if self.warmup.observe(message.received_ts_us()) {
            self.log_warmup_complete();
        }
        self.handle(message);
        if moves_position(message) {
            self.state.publish_exposure(message.received_ts_us());
        }
        self.drain_actions();
        let processed_at = self.clock.now();
        if let Some(meta) = meta {
            self.record_processing(meta, dequeued_at, processed_at);
        }
        if let InboundMessage::SpinTick(tick) = message {
            self.exec_spin(tick);
            self.emit_positions(tick.received_ts_us);
            self.emit_snapshot(processed_at);
        }
    }

    /// Post-drain per-spin (fills folded). Ahead of live gate (corrects late-attach UI).
    fn emit_positions(&mut self, event_ts: TsUs) {
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

    fn handle(&mut self, message: &InboundMessage) {
        match message {
            InboundMessage::Trade(event) => self.on_trade(event),
            InboundMessage::Book(chunk) => self.on_book(chunk),
            InboundMessage::BookReset(reset) => self.on_book_reset(reset),
            InboundMessage::MarketRotation(rotation) => self.on_market_rotation(rotation),
            InboundMessage::Kline(event) => self.on_kline(event),
            InboundMessage::SpinTick(tick) => self.on_spin(tick),
            InboundMessage::Link(link) => self.on_link(link),
            InboundMessage::RunControl(control) => self.on_run_control(control),
            InboundMessage::Exec(event) => self.on_exec(event),
            InboundMessage::Account(chunk) => self.on_account(chunk),
        }
    }

    /// Apply ungated (fills during warmup = money). Callback gated only.
    fn on_exec(&mut self, event: &ExecEvent) {
        let HotState {
            exec,
            ledger,
            actions,
            ..
        } = &mut self.state;
        let callback = exec.on_exec_event(event, ledger, actions);
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
        let mut ctx = self.state.ctx(event.received_ts_us);
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

    fn on_spin(&mut self, tick: &SpinTick) {
        self.state.spin_seq = tick.seq;
        for tracker in &mut self.state.trackers {
            tracker.on_spin();
        }
        // Timer ticks parked (retry latched writes).
        self.state.retry_pending_seal();
        self.state.exposure.retry_pending();
        if !self.is_strategy_live() {
            return;
        }
        let mut ctx = self.state.ctx(tick.received_ts_us);
        self.strategy.on_spin(&mut ctx, tick);
    }

    /// After strategy drain (reconciler sees declarations). Separate (runs parked/warming).
    fn exec_spin(&mut self, tick: &SpinTick) {
        let HotState {
            books,
            ledger,
            desired,
            windows,
            exec,
            actions,
            ..
        } = &mut self.state;
        exec.on_spin(SpinInput {
            tick,
            books,
            desired,
            windows,
            ledger,
            bank: actions,
        });
        // Refusals banked, drain separate (state-first, not ring-first).
        let ui = &mut self.ui;
        exec.drain_refusals(&mut |reject| ui.emit_reject(reject));
        self.emit_exec_state(tick);
    }

    /// Halt/balances/desired every spin (corrects late-attach). Desired expires this spin.
    fn emit_exec_state(&mut self, tick: &SpinTick) {
        let at = tick.received_ts_us;
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
