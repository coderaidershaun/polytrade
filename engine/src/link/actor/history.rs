//! The recent event frames of the engine, kept so a tab that opens during a run is sent them on the
//! history topic, a fixed count each flush, while live frames still leave on time.

use std::net::SocketAddr;
use std::num::NonZeroUsize;

use crate::msg::ui::UiEvent;

use super::super::subscribers::LINK_MAX_SUBSCRIBERS;

/// One subscriber that names the history topic, and the frames recorded before it came, by number.
/// A finished replay stays while its subscriber does, so a subscriber is sent its history once.
#[derive(Debug, Clone, Copy)]
struct Replay {
    to: SocketAddr,
    next: u64,
    end: u64,
}

/// A ring of the newest event frames, made at start with a capacity that never changes, and the
/// replay of each subscriber that names the history topic.
///
/// It lives in the link actor, never on the hot thread: the fold pays nothing for a late tab.
#[derive(Debug)]
pub struct HistoryRing {
    frames: Vec<UiEvent>,
    capacity: usize,
    /// Each event ever recorded. Event number `n` sits at slot `n % capacity` while it is held.
    recorded: u64,
    per_flush: usize,
    left_this_flush: usize,
    replays: Vec<Replay>,
    skipped: u64,
}

impl HistoryRing {
    /// Each slot is reserved here, so recording never allocates.
    pub fn new(capacity: NonZeroUsize, per_flush: NonZeroUsize) -> Self {
        Self {
            frames: Vec::with_capacity(capacity.get()),
            capacity: capacity.get(),
            recorded: 0,
            per_flush: per_flush.get(),
            left_this_flush: 0,
            replays: Vec::with_capacity(LINK_MAX_SUBSCRIBERS),
            skipped: 0,
        }
    }

    /// Keeps `event` in place of the oldest frame once the ring is full.
    pub fn record(&mut self, event: UiEvent) {
        if self.frames.len() < self.capacity {
            self.frames.push(event);
        } else {
            let slot = (self.recorded % self.capacity as u64) as usize;
            self.frames[slot] = event;
        }
        self.recorded += 1;
    }

    /// Opens one flush. `subscribers` are the addresses that name the history topic now: each new
    /// one is sent each frame held at this moment, and a replay to an address that left stops.
    pub fn begin_flush(&mut self, subscribers: &[SocketAddr]) {
        self.replays
            .retain(|replay| subscribers.contains(&replay.to));
        let oldest = self.oldest();
        for address in subscribers {
            let is_known = self.replays.iter().any(|replay| replay.to == *address);
            let has_room = self.replays.len() < self.replays.capacity();
            if !is_known && has_room {
                self.replays.push(Replay {
                    to: *address,
                    next: oldest,
                    end: self.recorded,
                });
            }
        }
        self.left_this_flush = self.per_flush;
    }

    /// The next history frame of this flush and its address, until the count of a flush is spent.
    /// A replay that eviction overtook goes on from the oldest frame held, and the frames it missed
    /// are counted.
    pub fn next_frame(&mut self) -> Option<(SocketAddr, UiEvent)> {
        if self.left_this_flush == 0 {
            return None;
        }
        let oldest = self.oldest();
        for replay in &mut self.replays {
            if replay.next < oldest {
                let resume = oldest.min(replay.end);
                self.skipped += resume - replay.next;
                replay.next = resume;
            }
            if replay.next >= replay.end {
                continue;
            }
            let frame = self.frames[(replay.next % self.capacity as u64) as usize];
            replay.next += 1;
            self.left_this_flush -= 1;
            return Some((replay.to, frame));
        }
        None
    }

    /// The frames held, oldest first.
    pub fn held(&self) -> impl Iterator<Item = &UiEvent> {
        let start = (self.oldest() % self.capacity as u64) as usize;
        let (newer, older) = self.frames.split_at(start.min(self.frames.len()));
        older.iter().chain(newer)
    }

    pub fn capacity(&self) -> usize {
        self.frames.capacity()
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Frames that eviction removed before their replay could send them.
    pub fn skipped(&self) -> u64 {
        self.skipped
    }

    fn oldest(&self) -> u64 {
        self.recorded - self.frames.len() as u64
    }
}
