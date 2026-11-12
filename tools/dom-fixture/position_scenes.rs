//! The risk scenes: a fixed stream of books and fills folded through the real chart and position
//! models, so no curve is drawn by hand. The risk chart takes the mid chart's window.

use polysim::ids::{FIXED_SCALE, InstrumentId, Price, Qty, Side};
use polysim::msg::ui::{UiBookState, UiEvent};
use polysim::time::DurationUs;
use polysim::time::TsUs;
use polysim::workstation::chart::model::{BookContinuity, ChartModel};
use polysim::workstation::chart::position_model::PositionModel;

use crate::fixture::{self, BASE_TS, INSTRUMENT, book, px, unit};

const SPIN: DurationUs = DurationUs::from_micros(1_000_000);
const WINDOW_BUCKETS: i64 = 300;

const COMMITS_PER_BUCKET: i64 = 2;

const TICK: Price = Price(FIXED_SCALE);

/// BTC scale, one dollar to a tick.
const BASE_BID: i64 = 65_990;

/// 0.01 of the base asset, so a handful of fills spans the axis at this price.
const ORDER_QTY: Qty = Qty(FIXED_SCALE / 100);

pub struct PositionScene {
    pub name: &'static str,
    pub check: &'static str,
    pub chart: ChartModel,
    pub positions: PositionModel,
    pub instrument: InstrumentId,
    pub tick: Price,
}

pub fn position_scenes() -> Vec<PositionScene> {
    vec![
        pnl_through_zero(),
        exposure_steps(),
        flat_at_zero(),
        dropped_frames(),
        rotation_clears_then_re_marks(),
        rotation_then_new_window(),
        no_tick_grid(),
    ]
}

/// A held position whose mark swings around the entry, so the PnL crosses zero. Switching series
/// rescales the axis more here than in any other scene.
fn pnl_through_zero() -> PositionScene {
    let mut feed = Feed::new(Some(TICK));
    feed.walk(0..20);
    feed.buy(20);
    feed.walk(20..WINDOW_BUCKETS);
    feed.finish(
        "pnl through zero — bought at 20 s, mark oscillates",
        "X rescales the axis from hundreds of dollars to single dollars; the PnL line crosses zero",
    )
}

/// Inventory goes from long to short. Exposure jumps at each fill and drifts with the mark between
/// them, giving about 243 levels rather than a staircase of 13.
fn exposure_steps() -> PositionScene {
    let mut feed = Feed::new(Some(TICK));
    for step in 0..WINDOW_BUCKETS {
        // The buying phase is a third of the scene on purpose: an even split never goes short, and
        // the negative colour would go unseen.
        if step % 24 == 0 && step > 0 {
            match step < WINDOW_BUCKETS / 3 {
                true => feed.buy(step),
                false => feed.sell(step),
            }
        }
        feed.bucket(step);
        feed.emit(step);
    }
    feed.finish(
        "exposure — long, through flat, into short",
        "line JUMPS at each fill and DRIFTS with the mark between them; it is flat ONLY where the \
         position is zero (spins 0-23 before the first fill, and 192-215 mid-scene) because zero \
         times any mark is zero. Crosses into short and the readout's colour flips with the sign",
    )
}

/// Marked but never traded, so the axis floor must stop it labelling in 1e-8 steps. Every run looks
/// like this for its first minutes.
fn flat_at_zero() -> PositionScene {
    let mut feed = Feed::new(Some(TICK));
    feed.walk(0..WINDOW_BUCKETS);
    feed.finish(
        "flat at zero — the minimum-span floor",
        "axis spans at least ONE WHOLE QUOTE UNIT, never 0.00000001 steps",
    )
}

/// A full event ring drops position frames while books still arrive. Dropping is safe because each
/// frame is an absolute state. The risk line splits at the hole while the mid line runs through it.
fn dropped_frames() -> PositionScene {
    let mut feed = Feed::new(Some(TICK));
    feed.walk(0..40);
    feed.buy(40);
    feed.walk(40..110);
    feed.walk_undelivered(110..170);
    feed.walk(170..WINDOW_BUCKETS);
    feed.finish(
        "dropped frames — a full event ring, books unaffected",
        "risk line SPLITS at the hole while the mid above keeps drawing straight through it",
    )
}

/// A rotation clears the series and the new mark draws a flat zero line, not nothing, which catches
/// a painter that confuses no data with zero. An exposure carried over would be a ghost.
fn rotation_clears_then_re_marks() -> PositionScene {
    let mut feed = Feed::new(Some(TICK));
    feed.walk(0..60);
    feed.buy(60);
    feed.walk(60..140);
    feed.rotate(140);
    feed.walk(141..WINDOW_BUCKETS);
    feed.finish(
        "rotation — clears, then re-marks at flat ZERO",
        "the series starts at the LEFT EDGE as a flat line ON zero — drawn, not absent; the empty \
         stretch to its right is what absent looks like. Nothing bridges back to the pre-rotation \
         series",
    )
}

/// The 70-bucket stagger is what makes this scene work: the mid restarts at 140 and the risk series
/// at 210, so a risk chart that derived its own window would visibly misalign. A one-bucket stagger
/// would look right and catch nothing.
fn rotation_then_new_window() -> PositionScene {
    let mut feed = Feed::new(Some(TICK));
    feed.walk(0..60);
    feed.buy(60);
    feed.walk(60..140);
    feed.rotate(140);
    feed.walk_undelivered(140..210);
    feed.buy(210);
    feed.walk(210..WINDOW_BUCKETS);
    feed.finish(
        "rotation, silence, new window",
        "crosshair sits on the same spin in both charts, though they start at different buckets",
    )
}

/// No tick grid, as on Polymarket: the mid chart is empty, so the risk chart that shares its window
/// is empty too.
fn no_tick_grid() -> PositionScene {
    let mut feed = Feed::new(None);
    feed.walk(0..60);
    feed.buy(60);
    feed.walk(60..WINDOW_BUCKETS);
    PositionScene {
        tick: Price(0),
        ..feed.finish(
            "no tick grid — no window, so no risk chart either",
            "KNOWN RULED LIMITATION, not a bug: no stamped tick = no window = no risk chart",
        )
    }
}

/// Keeps a small ledger and derives each position from it, rather than drawing one.
struct Feed {
    chart: ChartModel,
    positions: PositionModel,
    seq: u64,
    position_base: i64,
    cash_quote: i64,
    mark: Option<Price>,
}

impl Feed {
    fn new(tick: Option<Price>) -> Self {
        let mut chart = ChartModel::with_capacity(1, SPIN);
        chart.configure(&[tick], SPIN);
        let mut positions = PositionModel::with_capacity(1, SPIN);
        positions.configure(1, SPIN);
        Self {
            chart,
            positions,
            seq: 0,
            position_base: 0,
            cash_quote: 0,
            mark: None,
        }
    }

    fn walk(&mut self, steps: std::ops::Range<i64>) {
        for step in steps {
            self.bucket(step);
            self.emit(step);
        }
    }

    /// Books still arrive and set the mark, but the position frames are lost, as a full event ring
    /// loses them.
    fn walk_undelivered(&mut self, steps: std::ops::Range<i64>) {
        for step in steps {
            self.bucket(step);
        }
    }

    /// The mark follows the last mid, as the engine's does.
    fn bucket(&mut self, step: i64) {
        for sub in 0..COMMITS_PER_BUCKET {
            let bid = mid_bid(step) + wobble(step * COMMITS_PER_BUCKET + sub);
            self.commit(self.at(step, sub), bid);
            self.mark = Some(px(bid + 1));
        }
    }

    /// An absolute position, sent only once there is a mark. It becomes `f64` here, as on the link.
    fn emit(&mut self, step: i64) {
        let Some(mark) = self.mark else { return };
        let exposure = mark.notional(Qty(self.position_base));
        self.seq += 1;
        self.positions.apply_event(&UiEvent::Position {
            instrument: INSTRUMENT,
            seq: self.seq,
            event_ts_us: TsUs::from_micros(self.at(step, COMMITS_PER_BUCKET - 1)),
            exposure_quote: exposure as f64 / FIXED_SCALE as f64,
            pnl_quote: (self.cash_quote + exposure) as f64 / FIXED_SCALE as f64,
        });
    }

    fn buy(&mut self, step: i64) {
        self.fill(step, Side::Buy);
    }

    fn sell(&mut self, step: i64) {
        self.fill(step, Side::Sell);
    }

    fn fill(&mut self, step: i64, side: Side) {
        let price = px(mid_bid(step));
        let notional = price.notional(ORDER_QTY);
        match side {
            Side::Buy => {
                self.position_base += ORDER_QTY.0;
                self.cash_quote -= notional;
            }
            Side::Sell => {
                self.position_base -= ORDER_QTY.0;
                self.cash_quote += notional;
            }
        }
        self.seq += 1;
        self.chart.apply_event(&fixture::fill(
            self.seq,
            self.at(step, 0),
            side,
            price,
            ORDER_QTY,
        ));
    }

    /// A window handover resets the position, and nothing is sent until a new mark.
    fn rotate(&mut self, step: i64) {
        self.seq += 1;
        let event = UiEvent::Rotation {
            instrument: INSTRUMENT,
            seq: self.seq,
            event_ts_us: TsUs::from_micros(self.at(step, 0)),
        };
        self.chart.apply_event(&event);
        self.positions.apply_event(&event);
        self.position_base = 0;
        self.cash_quote = 0;
        self.mark = None;
    }

    fn at(&self, step: i64, sub: i64) -> i64 {
        BASE_TS + step * SPIN.micros() + sub * SPIN.micros() / COMMITS_PER_BUCKET
    }

    fn commit(&mut self, micros: i64, bid: i64) {
        self.seq += 1;
        let (bids, asks) = ([unit(bid)], [unit(bid + 2)]);
        self.chart.apply_book(
            &book(self.seq, micros, UiBookState::Valid, &bids, &asks),
            BookContinuity::Continuous,
        );
    }

    fn finish(self, name: &'static str, check: &'static str) -> PositionScene {
        PositionScene {
            name,
            check,
            chart: self.chart,
            positions: self.positions,
            instrument: INSTRUMENT,
            tick: TICK,
        }
    }
}

/// Two triangle waves: the slow one carries the mark away from the entry, and the fast one keeps it
/// from moving in one direction only.
fn mid_bid(step: i64) -> i64 {
    BASE_BID + wave(step, 137, 400) + wave(step, 23, 40)
}

fn wobble(commit: i64) -> i64 {
    wave(commit, 7, 3)
}

/// A triangle wave between minus and plus `amplitude`, with no randomness, so every run is the
/// same.
fn wave(step: i64, period: i64, amplitude: i64) -> i64 {
    let phase = step.rem_euclid(period);
    let rise = phase.min(period - phase);
    rise * amplitude * 4 / period - amplitude
}
