//! The channels that carry the workstation feed from the engine to the link actor. They are made
//! once at start, so the hot thread never allocates to publish.

use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use rtrb::{Consumer, RingBuffer};

use crate::hot::sink::{UiBookSink, UiEventSink};

use super::{UI_BOOK_RING_CAPACITY, UI_EVENT_RING_CAPACITY, UiBookSnapshot, UiCatalog, UiEvent};

/// A run sends only a handful of lifecycle messages, and each is sent with `try_send`, so a full
/// channel drops a message instead of stalling the engine.
const LIFECYCLE_CAPACITY: usize = 16;

/// The engine's own rare state changes. There is no starting phase, because a workstation only ever
/// attaches to an engine that is already running.
#[derive(Debug, Clone)]
pub(crate) enum UiLifecycle {
    Ready(UiCatalog),
    Draining,
}

/// The engine's end of the workstation feed.
pub(crate) struct UiWiring {
    pub lifecycle: SyncSender<UiLifecycle>,
    pub books: UiBookSink,
    pub events: UiEventSink,
}

/// The link actor's end of the workstation feed.
pub(crate) struct UiChannels {
    pub lifecycle: Receiver<UiLifecycle>,
    pub books: Consumer<UiBookSnapshot>,
    pub events: Consumer<UiEvent>,
}

/// Both rings are allocated here at startup, so the hot thread never allocates to publish.
pub(crate) fn ui_channel() -> (UiWiring, UiChannels) {
    let (lifecycle_tx, lifecycle_rx) = sync_channel(LIFECYCLE_CAPACITY);
    let (book_producer, book_consumer) = RingBuffer::<UiBookSnapshot>::new(UI_BOOK_RING_CAPACITY);
    let (event_producer, event_consumer) = RingBuffer::<UiEvent>::new(UI_EVENT_RING_CAPACITY);
    (
        UiWiring {
            lifecycle: lifecycle_tx,
            books: UiBookSink::new(book_producer),
            events: UiEventSink::new(event_producer),
        },
        UiChannels {
            lifecycle: lifecycle_rx,
            books: book_consumer,
            events: event_consumer,
        },
    )
}
