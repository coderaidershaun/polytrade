//! The channels that carry the workstation feed from the engine to the link actor. They are made
//! once at start, so the hot thread never allocates to publish.

use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use rtrb::{Consumer, Producer, RingBuffer};

use super::{UI_BOOK_RING_CAPACITY, UI_EVENT_RING_CAPACITY, UiBookSnapshot, UiCatalog, UiEvent};

/// A run sends only a handful of lifecycle messages, and each is sent with `try_send`, so a full
/// channel drops a message instead of stalling the engine.
const LIFECYCLE_CAPACITY: usize = 16;

/// The engine's own rare state changes. There is no starting phase, because a workstation only ever
/// attaches to an engine that is already running.
#[derive(Debug, Clone)]
pub enum UiLifecycle {
    Ready(UiCatalog),
    Draining,
}

/// The engine's end of the workstation feed. The hot thread wraps each ring in the sink of its
/// policy on a full ring.
pub struct UiWiring {
    pub lifecycle: SyncSender<UiLifecycle>,
    pub books: Producer<UiBookSnapshot>,
    pub events: Producer<UiEvent>,
}

/// The link actor's end of the workstation feed.
pub struct UiChannels {
    pub lifecycle: Receiver<UiLifecycle>,
    pub books: Consumer<UiBookSnapshot>,
    pub events: Consumer<UiEvent>,
}

/// Both rings are allocated here at startup, so the hot thread never allocates to publish.
pub fn ui_channel() -> (UiWiring, UiChannels) {
    let (lifecycle_tx, lifecycle_rx) = sync_channel(LIFECYCLE_CAPACITY);
    let (book_producer, book_consumer) = RingBuffer::<UiBookSnapshot>::new(UI_BOOK_RING_CAPACITY);
    let (event_producer, event_consumer) = RingBuffer::<UiEvent>::new(UI_EVENT_RING_CAPACITY);
    (
        UiWiring {
            lifecycle: lifecycle_tx,
            books: book_producer,
            events: event_producer,
        },
        UiChannels {
            lifecycle: lifecycle_rx,
            books: book_consumer,
            events: event_consumer,
        },
    )
}
