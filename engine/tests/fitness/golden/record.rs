//! Plays a tape into an engine of a shipped strategy and writes down, as text, the quotes that the
//! engine tees to the workstation at each spin: the ladder that the strategy declared.

use engine::hot::dispatch::HotEngine;
use engine::msg::exec::ExecLaneItem;
use engine::msg::ids::{FIXED_SCALE, InstrumentId, Price, Qty};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::ui::{DomQuote, UiEvent};
use rtrb::Consumer;

use crate::engine_support::PersistRings;
use crate::engine_support::spin;

/// The lines of one golden file, and how many of them hold at least one quote.
#[derive(Debug, Default)]
pub(super) struct DeclaredQuotes {
    pub(super) lines: Vec<String>,
    pub(super) quoted: usize,
}

impl DeclaredQuotes {
    fn push(&mut self, tape: &str, spin: u64, instrument: InstrumentId, quote: &DomQuote) {
        let bids = ladder_text(&quote.bids);
        let asks = ladder_text(&quote.asks);
        if bids != "-" || asks != "-" {
            self.quoted += 1;
        }
        self.lines.push(format!(
            "{tape} {spin} i{} bid {bids} ask {asks}",
            instrument.0
        ));
    }
}

/// An engine and the readers of each lane it writes. The persist and command lanes are emptied after
/// each message, so a full ring never changes what the engine does next.
pub(super) struct Recording {
    pub(super) tape: &'static str,
    pub(super) engine: HotEngine,
    pub(super) events: Consumer<UiEvent>,
    pub(super) persist: PersistRings,
    pub(super) commands: Consumer<ExecLaneItem>,
}

impl Recording {
    pub(super) fn play(&mut self, message: &InboundMessage, quotes: &mut DeclaredQuotes) {
        self.engine.dispatch(message);
        self.persist.audit();
        self.persist.research();
        while self.commands.pop().is_ok() {}
        let spin = match message.body {
            MessageBody::SpinTick(tick) => Some(tick.seq),
            _ => None,
        };
        while let Ok(event) = self.events.pop() {
            if let UiEvent::Quote {
                instrument, quote, ..
            } = event
            {
                let spin = spin.unwrap_or_else(|| {
                    panic!("tape {}: a quote was teed outside a spin", self.tape)
                });
                quotes.push(self.tape, spin, instrument, &quote);
            }
        }
    }

    pub(super) fn play_all(&mut self, tape: &[InboundMessage], quotes: &mut DeclaredQuotes) {
        for message in tape {
            self.play(message, quotes);
        }
    }
}

/// Market messages in receive order, with a spin each time a timer of period `spin_us` would fire
/// before a message, and one spin after the last message.
pub(super) fn with_spins(market: Vec<InboundMessage>, spin_us: i64) -> Vec<InboundMessage> {
    let first = market
        .first()
        .map_or(0, |message| message.header.received_ts_us.micros());
    let mut tape = Vec::with_capacity(market.len());
    let mut next_spin = first + spin_us;
    let mut spin_seq = 0;
    for message in market {
        let at = message.header.received_ts_us.micros();
        while next_spin <= at {
            tape.push(spin(spin_seq, next_spin));
            spin_seq += 1;
            next_spin += spin_us;
        }
        tape.push(message);
    }
    tape.push(spin(spin_seq, next_spin));
    tape
}

/// Each declared level as `<level>:<price>x<qty>`, or `-` for a side with no level.
fn ladder_text(levels: &[Option<(Price, Qty)>]) -> String {
    let text: Vec<String> = levels
        .iter()
        .enumerate()
        .filter_map(|(index, level)| {
            level.map(|(price, qty)| {
                format!(
                    "{index}:{}x{}",
                    decimal(price.mantissa()),
                    decimal(qty.mantissa())
                )
            })
        })
        .collect();
    match text.is_empty() {
        true => "-".to_owned(),
        false => text.join(","),
    }
}

/// The exact decimal of a fixed-point mantissa, with no trailing zeros, so a line reads as a price
/// and still holds each bit of it.
fn decimal(mantissa: i64) -> String {
    let sign = if mantissa < 0 { "-" } else { "" };
    let scale = FIXED_SCALE.unsigned_abs();
    let whole = mantissa.unsigned_abs() / scale;
    let fraction = mantissa.unsigned_abs() % scale;
    if fraction == 0 {
        return format!("{sign}{whole}");
    }
    let width = FIXED_SCALE.ilog10() as usize;
    let digits = format!("{fraction:0width$}");
    format!("{sign}{whole}.{}", digits.trim_end_matches('0'))
}
