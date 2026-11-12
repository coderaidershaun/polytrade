//! The monitor's channel rows as columns of words: what each row kind says and in what role, plus
//! the hover record that spells the same columns out untruncated.

use crate::hot::exec::{CloseReason, OrderState, RejectOrigin};
use crate::ids::{Price, Qty, Side};
use crate::msg::exec::Liquidity;
use crate::time::TsUs;
use crate::workstation::dom_view::tick_index;
use crate::workstation::format::{self, MISSING};
use crate::workstation::monitor::model::{
    FillRow, OrderEvent, OrderRow, SystemEvent, SystemNote, SystemRow, TradeRow,
};
use crate::workstation::monitor::view::{Channel, MonitorFrame};

use super::monitor::reject_tone;
use super::tone::Tone;

/// Six decimals, because a 10 bp fee on a $10 fill is about $0.0094 and fewer digits would show
/// every fee as zero.
const FEE_DECIMALS: usize = 6;

/// Stand-in for a lifecycle row's absent event time: the shape of a clock reading with no digits
/// in it, so the column stays aligned and nobody mistakes it for one.
const NO_EVENT_TIME: &str = "--:--:--.---";

/// The tag column: the venue's liquidity note, when it said one, and the side word beside it. One
/// cell because they are one column — a single space joins them in the record, as the eye reads them.
pub(crate) struct TagColumn {
    pub(crate) liquidity: Option<&'static str>,
    pub(crate) side: &'static str,
    pub(crate) tone: Tone,
}

impl TagColumn {
    fn side(side: &'static str, tone: Tone) -> Self {
        Self {
            liquidity: None,
            side,
            tone,
        }
    }

    fn fill(liquidity: Option<Liquidity>, side: &'static str, tone: Tone) -> Self {
        Self {
            liquidity: Some(liquidity_word(liquidity)),
            side,
            tone,
        }
    }
}

/// One column of a channel row. The painter and the hover record read this same list, so a column
/// cannot be drawn one way and reported another.
pub(crate) enum Cell {
    Time(String),
    Tag(TagColumn),
    /// Free text, cut with an ellipsis at its right end, running to the edge of the row. It starts
    /// past the tag when the row has one.
    Description {
        text: String,
        tone: Tone,
    },
    Price(String),
    Qty(String),
    /// Shown in its own column while the row is wide enough, and always part of the hover record.
    Detail(String),
}

impl Cell {
    fn time(at: TsUs) -> Self {
        Self::Time(format::render(|out| format::write_time_of_day(out, at)))
    }

    fn tick_price(price: Price, tick: Price) -> Self {
        Self::Price(price_text(price, tick))
    }

    fn quantity(qty: Qty, frame: &MonitorFrame<'_>) -> Self {
        Self::Qty(qty_text(qty, frame))
    }

    fn write_record(&self, record: &mut String) {
        match self {
            Cell::Tag(tag) => {
                if let Some(liquidity) = tag.liquidity {
                    record.push_str(liquidity);
                    record.push(' ');
                }
                record.push_str(tag.side);
            }
            Cell::Time(text)
            | Cell::Description { text, .. }
            | Cell::Price(text)
            | Cell::Qty(text)
            | Cell::Detail(text) => record.push_str(text),
        }
    }
}

/// A channel's row: where its history lives, what an empty channel says, and how a row draws.
/// Read by index newest-first, as the channel scrolls, so a band paints the rows it shows rather
/// than a copy of the whole retained history.
pub(crate) trait ChannelRow: Clone {
    const CHANNEL: Channel;
    const EMPTY_NOTE: &'static str;

    fn count(frame: &MonitorFrame<'_>) -> usize;

    /// The `index`-th newest row, counting from zero.
    fn at(frame: &MonitorFrame<'_>, index: usize) -> Option<Self>;

    /// Rows ever appended. It only grows, so a reader can count the rows it has not seen.
    fn appended(frame: &MonitorFrame<'_>) -> u64;

    fn cells(&self, frame: &MonitorFrame<'_>, out: &mut Vec<Cell>);
}

pub(crate) fn record_row<R: ChannelRow>(row: &R, frame: &MonitorFrame<'_>) -> String {
    let mut cells = Vec::new();
    row.cells(frame, &mut cells);
    let mut record = String::new();
    for (index, cell) in cells.iter().enumerate() {
        if index > 0 {
            record.push_str("  ");
        }
        cell.write_record(&mut record);
    }
    record
}

impl ChannelRow for SystemRow {
    const CHANNEL: Channel = Channel::System;
    const EMPTY_NOTE: &'static str = "no system events";

    fn count(frame: &MonitorFrame<'_>) -> usize {
        frame.model.monitor().system().count()
    }

    fn at(frame: &MonitorFrame<'_>, index: usize) -> Option<Self> {
        frame.model.monitor().system().nth(index).cloned()
    }

    fn appended(frame: &MonitorFrame<'_>) -> u64 {
        frame.model.monitor().system_appended()
    }

    fn cells(&self, frame: &MonitorFrame<'_>, out: &mut Vec<Cell>) {
        out.push(match self.at {
            Some(at) => Cell::time(at),
            None => Cell::Time(NO_EVENT_TIME.to_owned()),
        });
        let (text, tone) = system_text(&self.event, frame);
        out.push(Cell::Description { text, tone });
    }
}

impl ChannelRow for OrderRow {
    const CHANNEL: Channel = Channel::Orders;
    const EMPTY_NOTE: &'static str = "no orders";

    fn count(frame: &MonitorFrame<'_>) -> usize {
        frame.model.monitor().orders().count()
    }

    fn at(frame: &MonitorFrame<'_>, index: usize) -> Option<Self> {
        frame.model.monitor().orders().nth(index).copied()
    }

    fn appended(frame: &MonitorFrame<'_>) -> u64 {
        frame.model.monitor().orders_appended()
    }

    fn cells(&self, frame: &MonitorFrame<'_>, out: &mut Vec<Cell>) {
        let (word, side_tone) = side_word(self.side, Tone::SellFill);
        let (text, tone) = order_text(&self.event, frame);
        out.push(Cell::time(self.at));
        out.push(Cell::Tag(TagColumn::side(word, side_tone)));
        out.push(Cell::Description { text, tone });
    }
}

/// Public trades are the selected instrument's tape. The other channels cover every instrument.
impl ChannelRow for TradeRow {
    const CHANNEL: Channel = Channel::PubTrades;
    const EMPTY_NOTE: &'static str = "no public trades";

    fn count(frame: &MonitorFrame<'_>) -> usize {
        frame.model.monitor().trades(frame.instrument).count()
    }

    fn at(frame: &MonitorFrame<'_>, index: usize) -> Option<Self> {
        frame
            .model
            .monitor()
            .trades(frame.instrument)
            .nth(index)
            .copied()
    }

    fn appended(frame: &MonitorFrame<'_>) -> u64 {
        frame.model.monitor().trades_appended(frame.instrument)
    }

    fn cells(&self, frame: &MonitorFrame<'_>, out: &mut Vec<Cell>) {
        let (word, tone) = side_word(self.aggressor, Tone::Ask);
        out.push(Cell::time(self.at));
        out.push(Cell::Tag(TagColumn::side(word, tone)));
        out.push(Cell::tick_price(self.price, frame.tick));
        out.push(Cell::quantity(self.qty, frame));
    }
}

/// The liquidity tag is what the venue reported. When it said nothing the tag shows as missing
/// rather than as a guess.
impl ChannelRow for FillRow {
    const CHANNEL: Channel = Channel::TradeFills;
    const EMPTY_NOTE: &'static str = "no fills";

    fn count(frame: &MonitorFrame<'_>) -> usize {
        frame.model.monitor().fills().count()
    }

    fn at(frame: &MonitorFrame<'_>, index: usize) -> Option<Self> {
        frame.model.monitor().fills().nth(index).copied()
    }

    fn appended(frame: &MonitorFrame<'_>) -> u64 {
        frame.model.monitor().fills_appended()
    }

    fn cells(&self, frame: &MonitorFrame<'_>, out: &mut Vec<Cell>) {
        let (word, tone) = side_word(self.side, Tone::SellFill);
        let fee =
            format::render(|out| format::write_quote_amount(out, self.commission, FEE_DECIMALS));
        out.push(Cell::time(self.at));
        out.push(Cell::Tag(TagColumn::fill(self.liquidity, word, tone)));
        out.push(Cell::tick_price(self.price, frame.tick));
        out.push(Cell::quantity(self.qty, frame));
        out.push(Cell::Detail(format!(
            "fee {fee} a{}",
            self.commission_asset.0
        )));
    }
}

fn system_text(event: &SystemEvent, frame: &MonitorFrame<'_>) -> (String, Tone) {
    match event {
        SystemEvent::Lifecycle(note) => lifecycle_text(note),
        SystemEvent::Rotation { instrument } => {
            let name = format::render(|out| frame.instrument_label(*instrument, out));
            (format!("{name} window rotated"), Tone::Text)
        }
        SystemEvent::BookResynced { instrument } => {
            let name = format::render(|out| frame.instrument_label(*instrument, out));
            (format!("{name} book resynced"), Tone::Text2)
        }
        SystemEvent::EventsLost { count } => (format!("{count} ui events lost"), Tone::Invalid),
        SystemEvent::BooksLost { count } => (format!("{count} book snapshots lost"), Tone::Invalid),
    }
}

fn lifecycle_text(note: &SystemNote) -> (String, Tone) {
    match note {
        SystemNote::Starting => ("engine starting".to_owned(), Tone::Text2),
        SystemNote::Ready => ("engine ready".to_owned(), Tone::Positive),
    }
}

fn order_text(event: &OrderEvent, frame: &MonitorFrame<'_>) -> (String, Tone) {
    match *event {
        OrderEvent::Transition {
            client_id,
            state,
            price,
            qty,
            filled,
        } => {
            let text = format!(
                "{} #{:x}  {} of {} @ {}",
                state_word(state),
                client_id.0,
                qty_text(filled, frame),
                qty_text(qty, frame),
                price_text(price, frame.tick),
            );
            (text, state_tone(state))
        }
        OrderEvent::Refused { origin } => refusal_text(origin),
    }
}

fn refusal_text(origin: RejectOrigin) -> (String, Tone) {
    match origin {
        RejectOrigin::Local(reason) => (
            format!("refused by gate: {}", reason.label()),
            Tone::Warning,
        ),
        RejectOrigin::Venue { class, code } => (
            format!("venue refused {code} ({})", class.as_str()),
            reject_tone(class),
        ),
    }
}

/// The venue's answer in one word. A closed order says why it closed, because filled versus
/// cancelled is the distinction that matters most.
fn state_word(state: OrderState) -> &'static str {
    match state {
        OrderState::Free => "free",
        OrderState::PendingNew => "placing",
        OrderState::Live => "resting",
        OrderState::CancelInFlight => "cancelling",
        OrderState::AmendInFlight => "amending",
        OrderState::Unknown => "UNKNOWN",
        OrderState::Closed(CloseReason::Filled) => "FILLED",
        OrderState::Closed(CloseReason::Canceled) => "cancelled",
        OrderState::Closed(CloseReason::Rejected) => "rejected",
        OrderState::Closed(CloseReason::Expired) => "expired",
        OrderState::Closed(CloseReason::ReconciledGone) => "gone",
    }
}

fn state_tone(state: OrderState) -> Tone {
    match state {
        OrderState::Live => Tone::Text,
        OrderState::Closed(CloseReason::Filled) => Tone::Positive,
        // The venue's truth is lost, or the order left in a way we did not choose: worth finding
        // in the tape.
        OrderState::Unknown
        | OrderState::Closed(CloseReason::Expired)
        | OrderState::Closed(CloseReason::ReconciledGone) => Tone::Invalid,
        OrderState::Closed(CloseReason::Rejected) => Tone::Warning,
        _ => Tone::Text2,
    }
}

fn liquidity_word(liquidity: Option<Liquidity>) -> &'static str {
    match liquidity {
        Some(Liquidity::Maker) => "mkr",
        Some(Liquidity::Taker) => "tkr",
        None => MISSING,
    }
}

fn qty_text(qty: Qty, frame: &MonitorFrame<'_>) -> String {
    format::render(|out| format::write_qty(out, qty, frame.qty_scale, frame.qty_decimals))
}

fn price_text(price: Price, tick: Price) -> String {
    format::render(|out| write_tick_or_missing(out, price, tick))
}

/// A price off the tick grid shows as missing rather than rounded onto it.
fn write_tick_or_missing(buf: &mut String, price: Price, tick: Price) {
    format::write_opt(buf, tick_index(price, tick), format::write_tick_price);
}

/// The side word and the colour it wears. A public print takes ask red, an own fill takes the
/// orange reserved for sells we made — the words are the same, the reading is not.
fn side_word(side: Side, sell_tone: Tone) -> (&'static str, Tone) {
    match side {
        Side::Buy => ("BUY", Tone::Bid),
        Side::Sell => ("SELL", sell_tone),
    }
}
