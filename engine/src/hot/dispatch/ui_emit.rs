//! The hot thread's feed for the workstation. Committed book state and strategy events are copied
//! into fixed-size records, so nothing here allocates, and the link sends them on from the rings.

use crate::hot::book::{Book, BookState};
use crate::hot::digest::digest_fields;
use crate::hot::exec::{Balance, ExecHalt, Fill, OrderReject, OrderUpdate, WorkingOrderView};
use crate::hot::ledger::LedgerRow;
use crate::hot::sink::{UiBookSink, UiEventSink};
#[cfg(debug_assertions)]
use crate::msg::ids::ClientOrderId;
use crate::msg::ids::{AssetId, InstrumentId, Price, Qty, Side};
use crate::msg::inbound::{Level, MarketRotation, TradeEvent};
use crate::msg::time::LocalTs;
use crate::msg::ui::{
    DomQuote, UI_BOOK_LEVELS, UI_ORDER_SNAPSHOT_CAPACITY, UI_ORDER_SNAPSHOT_MAX_TOTAL,
    UiBookSnapshot, UiBookState, UiEvent, UiWorkingOrder,
};

pub(crate) struct UiEmitter {
    pub(crate) event_sink: UiEventSink,
    pub(crate) event_seq: u64,
    book_sink: UiBookSink,
    book_seqs: Vec<u64>,
}

impl UiEmitter {
    pub(crate) fn new(book_sink: UiBookSink, event_sink: UiEventSink, instruments: usize) -> Self {
        Self {
            event_sink,
            event_seq: 0,
            book_sink,
            book_seqs: vec![0; instruments],
        }
    }

    /// `seq` bumped always; ring-drop shows as gap.
    pub(crate) fn emit_book(
        &mut self,
        instrument: InstrumentId,
        books: &[Book],
        event_ts: LocalTs,
    ) {
        let index = usize::from(instrument.0);
        let book = &books[index];
        let state = match book.state() {
            BookState::AwaitingSnapshot => UiBookState::AwaitingSnapshot,
            BookState::Valid => UiBookState::Valid,
        };
        let mut bids = [EMPTY_LEVEL; UI_BOOK_LEVELS];
        let mut asks = [EMPTY_LEVEL; UI_BOOK_LEVELS];
        let bid_len = copy_top_levels(book.bids(), &mut bids);
        let ask_len = copy_top_levels(book.asks(), &mut asks);
        let seq = self.book_seqs[index];
        self.book_seqs[index] = seq + 1;
        self.book_sink.push(UiBookSnapshot {
            instrument,
            seq,
            event_ts_us: event_ts.to_peer(),
            state,
            bid_len,
            ask_len,
            bids,
            asks,
        });
    }

    pub(crate) fn emit_trade(&mut self, event: &TradeEvent, received_ts_us: LocalTs) {
        self.event_sink
            .push_stamped(&mut self.event_seq, |seq| UiEvent::Trade {
                instrument: event.instrument,
                seq,
                event_ts_us: received_ts_us.to_peer(),
                aggressor: event.side,
                price: event.price,
                qty: event.qty,
            });
    }

    pub(crate) fn emit_position(
        &mut self,
        instrument: InstrumentId,
        row: &LedgerRow,
        event_ts: LocalTs,
    ) {
        let exposure_quote_units = row.exposure_quote().to_f64();
        let pnl_quote_units = row.pnl_quote().to_f64();
        self.event_sink
            .push_stamped(&mut self.event_seq, |seq| UiEvent::Position {
                instrument,
                seq,
                event_ts_us: event_ts.to_peer(),
                exposure_quote: exposure_quote_units,
                pnl_quote: pnl_quote_units,
            });
    }

    pub(crate) fn emit_fill(&mut self, fill: &Fill) {
        self.event_sink
            .push_stamped(&mut self.event_seq, |seq| UiEvent::Fill {
                instrument: fill.instrument,
                seq,
                event_ts_us: fill.event_ts_us.to_peer(),
                quote_level: Some(fill.level),
                side: fill.side,
                price: fill.price,
                qty: fill.qty,
                commission: fill.commission,
                commission_asset: fill.commission_asset,
                liquidity: fill.liquidity,
            });
    }

    pub(crate) fn emit_order_update(&mut self, update: &OrderUpdate) {
        self.event_sink
            .push_stamped(&mut self.event_seq, |seq| UiEvent::OrderUpdate {
                instrument: update.instrument,
                seq,
                event_ts_us: update.event_ts_us.to_peer(),
                client_id: update.client_id,
                quote_level: Some(update.level),
                side: update.side,
                state: update.state,
                price: update.price,
                qty: update.qty,
                filled: update.filled,
            });
    }

    pub(crate) fn emit_order_snapshot(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        working: impl Iterator<Item = WorkingOrderView>,
        event_ts: LocalTs,
    ) {
        let mut orders = [UiWorkingOrder::EMPTY; UI_ORDER_SNAPSHOT_CAPACITY];
        // The duplicate check costs 2.3 KB of stack zeroed on entry and this runs twice per
        // instrument per spin, so release builds must not carry it at all rather than rely on the
        // optimiser noticing the writes are dead.
        #[cfg(debug_assertions)]
        let mut seen_ids = [ClientOrderId(0); UI_ORDER_SNAPSHOT_MAX_TOTAL as usize];
        let mut total_working = 0usize;
        for order in working {
            debug_assert_eq!(order.instrument, instrument);
            debug_assert_eq!(order.side, side);
            #[cfg(debug_assertions)]
            {
                assert!(
                    !seen_ids[..total_working.min(seen_ids.len())].contains(&order.client_id),
                    "working-order snapshot contains duplicate client id {:?}",
                    order.client_id
                );
                if let Some(seen) = seen_ids.get_mut(total_working) {
                    *seen = order.client_id;
                }
            }
            if let Some(cell) = orders.get_mut(total_working) {
                *cell = UiWorkingOrder {
                    client_id: order.client_id,
                    quote_level: order.level,
                    state: order.state,
                    price: order.price,
                    qty: order.qty,
                    filled: order.filled,
                };
            }
            total_working += 1;
        }
        debug_assert!(total_working <= usize::from(UI_ORDER_SNAPSHOT_MAX_TOTAL));
        let detail_len = total_working.min(UI_ORDER_SNAPSHOT_CAPACITY) as u8;
        let total_working =
            u16::try_from(total_working).expect("fixed OMS tables fit the snapshot total");
        self.event_sink
            .push_stamped(&mut self.event_seq, |seq| UiEvent::OrderSnapshot {
                instrument,
                seq,
                event_ts_us: event_ts.to_peer(),
                side,
                detail_len,
                total_working,
                orders,
            });
    }

    pub(crate) fn emit_reject(&mut self, reject: &OrderReject) {
        self.event_sink
            .push_stamped(&mut self.event_seq, |seq| UiEvent::Reject {
                instrument: reject.instrument,
                seq,
                event_ts_us: reject.event_ts_us.to_peer(),
                side: reject.side,
                origin: reject.origin,
            });
    }

    /// Re-stated each spin (absolute state); dropped frame costs one asset row, not a chunk boundary.
    pub(crate) fn emit_balances(
        &mut self,
        balances: impl Iterator<Item = (AssetId, Balance)>,
        event_ts: LocalTs,
    ) {
        for (asset, balance) in balances {
            self.event_sink
                .push_stamped(&mut self.event_seq, |seq| UiEvent::Balance {
                    asset,
                    seq,
                    event_ts_us: event_ts.to_peer(),
                    free: balance.free,
                    locked: balance.locked,
                });
        }
    }

    pub(crate) fn emit_execution(&mut self, halt: ExecHalt, event_ts: LocalTs) {
        self.event_sink
            .push_stamped(&mut self.event_seq, |seq| UiEvent::Execution {
                seq,
                event_ts_us: event_ts.to_peer(),
                halt,
            });
    }

    pub(crate) fn emit_desired(
        &mut self,
        instrument: InstrumentId,
        quote: DomQuote,
        event_ts: LocalTs,
    ) {
        self.event_sink
            .push_stamped(&mut self.event_seq, |seq| UiEvent::Quote {
                instrument,
                seq,
                event_ts_us: event_ts.to_peer(),
                quote,
            });
    }

    pub(crate) fn emit_rotation(&mut self, rotation: &MarketRotation, received_ts_us: LocalTs) {
        self.event_sink
            .push_stamped(&mut self.event_seq, |seq| UiEvent::Rotation {
                instrument: rotation.instrument,
                seq,
                event_ts_us: received_ts_us.to_peer(),
            });
    }

    pub(crate) fn dropped_books(&self) -> u64 {
        self.book_sink.dropped()
    }

    pub(crate) fn dropped_events(&self) -> u64 {
        self.event_sink.dropped()
    }
}

digest_fields! {
    UiEmitter { event_seq, book_seqs }
    not_state {
        // Output rings: whether a record fits depends on how fast the reader drains them. A drop
        // still advances its sequence, so the sequences above are state.
        event_sink,
        book_sink,
    }
}

const EMPTY_LEVEL: Level = Level {
    price: Price::from_mantissa(0),
    qty: Qty::from_mantissa(0),
};

fn copy_top_levels(levels: &[Level], out: &mut [Level; UI_BOOK_LEVELS]) -> u16 {
    let len = levels.len().min(UI_BOOK_LEVELS);
    out[..len].copy_from_slice(&levels[..len]);
    len as u16
}
