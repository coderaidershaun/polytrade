//! Reconstructs every slot leg's book from the hot ring and watches the rotation lifecycle, so the
//! live test can assert on physics the adapter never states outright: zero-gap handover (a new
//! window's book reaches Valid while the old still streams) and teardown latency (the old window's
//! final `BookReset` relative to its nominal close). The adapter surfaces neither on the ring; both
//! are inferred from the message sequence here, off any production path.

use polysim::hot::book::{Book, BookState};
use polysim::msg::inbound::{BookChunkKind, InboundMessage, Level};
use polysim::registry::Registry;

use crate::US;

/// A leg counts as live if it produced a frame within this window (the venue re-emits books ~150ms).
const LIVE_WINDOW_US: i64 = 5_000_000;

/// One slot leg (an Up or Down outcome instrument): its reconstructed book plus the liveness and
/// validity stamps the handover/teardown logic reads.
struct Leg {
    label: String,
    book: Book,
    last_msg_us: Option<i64>,
    valid_since_rotation: Option<i64>,
    /// A `BookReset` for this leg without a preceding `MarketRotation` is a teardown/reconnect reset,
    /// not the rotation's own. Set by `MarketRotation`, consumed by the next `BookReset`.
    pending_rotation_reset: bool,
}

impl Leg {
    fn new(label: String, capacity: usize) -> Self {
        Self {
            label,
            book: Book::new(capacity),
            last_msg_us: None,
            valid_since_rotation: None,
            pending_rotation_reset: false,
        }
    }

    fn is_live(&self, now_us: i64) -> bool {
        self.last_msg_us
            .is_some_and(|last| now_us - last < LIVE_WINDOW_US)
    }

    fn is_valid(&self) -> bool {
        matches!(self.book.state(), BookState::Valid)
    }

    /// Fully empty — both sides gone. The unambiguous size-0 collapse-burst signature; a merely
    /// thin (one-sided) book is normal for these 0..1 outcome markets and is not a collapse.
    fn is_empty(&self) -> bool {
        self.book.best_bid().is_none() && self.book.best_ask().is_none()
    }
}

#[derive(Default)]
struct Slot {
    window_open_us: Option<i64>,
    window_close_us: Option<i64>,
    /// A size-0 collapse emptied a leg after nominal close — the burst fast-path's ring evidence.
    collapsed_after_close_us: Option<i64>,
    /// The latest past-close non-rotation `BookReset` seen for the current window. A shadow-divergence
    /// re-snapshot during grace also resets, so the REAL teardown is the LAST such reset before the
    /// leg goes silent — finalised when the next window rotates in, or at run end.
    teardown_candidate: Option<TeardownCandidate>,
}

struct TeardownCandidate {
    confirmed_at_us: i64,
    silence_us: i64,
}

/// One observed handover into a slot: when its window rotated in, when both its legs reached Valid,
/// and what the sibling (old) window was doing at that instant — the zero-gap evidence.
pub struct RotationObs {
    pub slot: usize,
    pub window_open_us: i64,
    pub window_close_us: i64,
    pub rotated_at_us: i64,
    pub ready_us: Option<i64>,
    pub sibling_window_open_us: Option<i64>,
    pub sibling_age_at_ready_us: Option<i64>,
    pub sibling_live_at_ready: bool,
}

impl RotationObs {
    /// A handover (as opposed to the boot subscribe) is a rotation whose new book reached Valid while
    /// the previous window's book was still streaming — the whole zero-gap guarantee in one predicate.
    pub fn is_zero_gap_handover(&self) -> bool {
        self.ready_us.is_some() && self.sibling_live_at_ready
    }
}

/// One observed teardown: the old window's final `BookReset` relative to its nominal close, and the
/// path inferred from ring evidence (a collapse burst seen vs the leg simply going silent).
pub struct TeardownObs {
    pub slot: usize,
    pub window_open_us: i64,
    pub confirmed_at_us: i64,
    pub latency_us: i64,
    pub silence_at_teardown_us: i64,
    pub collapse_seen: bool,
}

impl TeardownObs {
    /// Inferred, not read from the adapter: the ring shows a collapse burst emptying the book (fast
    /// path) or only silence before the reset (the grace `/book` 404 probe must have confirmed it).
    pub fn inferred_path(&self) -> &'static str {
        if self.collapse_seen {
            "burst-collapse (book emptied on the ring before teardown)"
        } else {
            "probe-404 (leg went silent; only the REST probe could confirm)"
        }
    }
}

pub struct RotationObserver {
    reference_boundary_us: f64,
    legs: Vec<Leg>,
    slots: [Slot; 2],
    rotations: Vec<RotationObs>,
    teardowns: Vec<TeardownObs>,
    max_live_legs: usize,
    max_tails: usize,
    total_messages: u64,
}

impl RotationObserver {
    pub fn new(registry: &Registry, reference_boundary_us: f64) -> Self {
        let legs = registry
            .instruments()
            .iter()
            .map(|row| Leg::new(slot_label(&row.venue_symbol), row.book_capacity))
            .collect();
        Self {
            reference_boundary_us,
            legs,
            slots: [Slot::default(), Slot::default()],
            rotations: Vec::new(),
            teardowns: Vec::new(),
            max_live_legs: 0,
            max_tails: 0,
            total_messages: 0,
        }
    }

    pub fn observe(&mut self, message: InboundMessage) {
        self.total_messages += 1;
        let now = message.received_ts_us().micros();
        match message {
            InboundMessage::MarketRotation(rotation) => {
                let index = rotation.instrument.0 as usize;
                self.legs[index].last_msg_us = Some(now);
                self.legs[index].pending_rotation_reset = true;
                self.on_rotation(
                    index,
                    now,
                    rotation.window_open_ts_us.micros(),
                    rotation.window_close_ts_us.micros(),
                );
            }
            InboundMessage::BookReset(reset) => {
                let index = reset.instrument.0 as usize;
                self.on_book_reset(index, now);
            }
            InboundMessage::Book(chunk) => {
                let index = chunk.instrument.0 as usize;
                self.legs[index].last_msg_us = Some(now);
                match chunk.kind {
                    BookChunkKind::Snapshot => {
                        let _ = self.legs[index].book.apply_snapshot_chunk(&chunk);
                    }
                    BookChunkKind::Delta => self.legs[index].book.apply_delta_chunk(&chunk),
                }
                self.note_valid(index, now);
                self.note_collapse(index, now);
            }
            InboundMessage::Trade(trade) => {
                let index = trade.instrument.0 as usize;
                self.legs[index].last_msg_us = Some(now);
            }
            InboundMessage::Kline(_)
            | InboundMessage::SpinTick(_)
            | InboundMessage::Link(_)
            | InboundMessage::RunControl(_)
            | InboundMessage::Exec(_)
            | InboundMessage::Account(_) => {}
        }
    }

    fn on_book_reset(&mut self, index: usize, now: i64) {
        let prior_last = self.legs[index].last_msg_us;
        self.legs[index].last_msg_us = Some(now);
        self.legs[index].valid_since_rotation = None;
        if self.legs[index].pending_rotation_reset {
            self.legs[index].pending_rotation_reset = false;
            self.legs[index].book.apply_reset();
            return;
        }
        self.legs[index].book.apply_reset();
        self.note_teardown_candidate(index, now, prior_last);
    }

    fn on_rotation(&mut self, index: usize, now: i64, open: i64, close: i64) {
        let slot = index / 2;
        if self.slots[slot].window_open_us == Some(open) {
            return;
        }
        // The slot's previous window is done; seal its teardown (if it tore down) before the new
        // window overwrites the slot's bounds and collapse state.
        self.finalize_teardown(slot);
        self.slots[slot].window_open_us = Some(open);
        self.slots[slot].window_close_us = Some(close);
        self.slots[slot].collapsed_after_close_us = None;
        self.legs[slot * 2].valid_since_rotation = None;
        self.legs[slot * 2 + 1].valid_since_rotation = None;
        let sibling = 1 - slot;
        self.rotations.push(RotationObs {
            slot,
            window_open_us: open,
            window_close_us: close,
            rotated_at_us: now,
            ready_us: None,
            sibling_window_open_us: self.slots[sibling].window_open_us,
            sibling_age_at_ready_us: None,
            sibling_live_at_ready: false,
        });
    }

    /// A book chunk landed on `index`: mark the leg Valid if it just became so, then close out the
    /// slot's pending handover once both legs are Valid, capturing the sibling window's state now.
    fn note_valid(&mut self, index: usize, now: i64) {
        if self.legs[index].is_valid() && self.legs[index].valid_since_rotation.is_none() {
            self.legs[index].valid_since_rotation = Some(now);
        }
        let slot = index / 2;
        let both_valid = self.legs[slot * 2].valid_since_rotation.is_some()
            && self.legs[slot * 2 + 1].valid_since_rotation.is_some();
        if !both_valid {
            return;
        }
        let sibling = 1 - slot;
        let sibling_last = self.legs[sibling * 2]
            .last_msg_us
            .max(self.legs[sibling * 2 + 1].last_msg_us);
        let sibling_live =
            self.legs[sibling * 2].is_live(now) || self.legs[sibling * 2 + 1].is_live(now);
        if let Some(observation) = self
            .rotations
            .iter_mut()
            .rev()
            .find(|obs| obs.slot == slot && obs.ready_us.is_none())
        {
            observation.ready_us = Some(now);
            observation.sibling_age_at_ready_us = sibling_last.map(|last| now - last);
            observation.sibling_live_at_ready = sibling_live;
        }
    }

    /// After nominal close a leg emptying entirely is the size-0 collapse burst — the fast-path
    /// teardown evidence. Recorded per slot so the following `BookReset` can name the path.
    fn note_collapse(&mut self, index: usize, now: i64) {
        let slot = index / 2;
        let Some(close) = self.slots[slot].window_close_us else {
            return;
        };
        if now > close
            && self.legs[index].is_empty()
            && self.slots[slot].collapsed_after_close_us.is_none()
        {
            self.slots[slot].collapsed_after_close_us = Some(now);
        }
    }

    /// A non-rotation `BookReset` past nominal close is teardown evidence (a reset at-or-before close
    /// is a reconnect re-baseline). Keep only the LATEST such reset per window: a grace-tail shadow
    /// divergence also resets, so the real teardown is the last one before the leg goes silent.
    /// `prior_last` is the leg's last frame BEFORE this reset — the silence gap the FSM waited on.
    fn note_teardown_candidate(&mut self, index: usize, now: i64, prior_last: Option<i64>) {
        let slot = index / 2;
        let Some(close) = self.slots[slot].window_close_us else {
            return;
        };
        if now <= close {
            return;
        }
        let silence = prior_last.map_or(0, |last| now - last).max(0);
        self.slots[slot].teardown_candidate = Some(TeardownCandidate {
            confirmed_at_us: now,
            silence_us: silence,
        });
    }

    /// Seal the slot's current window's teardown candidate into an observation, if it tore down.
    fn finalize_teardown(&mut self, slot: usize) {
        let Some(candidate) = self.slots[slot].teardown_candidate.take() else {
            return;
        };
        let (Some(open), Some(close)) = (
            self.slots[slot].window_open_us,
            self.slots[slot].window_close_us,
        ) else {
            return;
        };
        self.teardowns.push(TeardownObs {
            slot,
            window_open_us: open,
            confirmed_at_us: candidate.confirmed_at_us,
            latency_us: candidate.confirmed_at_us - close,
            silence_at_teardown_us: candidate.silence_us,
            collapse_seen: self.slots[slot].collapsed_after_close_us.is_some(),
        });
    }

    /// Seal any still-pending teardown — the last window(s) whose successor never rotated in before
    /// the capture ended. Call once after the run loop, before reading [`RotationObserver::teardowns`].
    pub fn finalize(&mut self) {
        for slot in 0..2 {
            self.finalize_teardown(slot);
        }
    }

    pub fn sample(&mut self, now_us: i64) {
        let live = self.legs.iter().filter(|leg| leg.is_live(now_us)).count();
        self.max_live_legs = self.max_live_legs.max(live);
        let mut tails = 0;
        for (slot, state) in self.slots.iter().enumerate() {
            let Some(close) = state.window_close_us else {
                continue;
            };
            let slot_live =
                self.legs[slot * 2].is_live(now_us) || self.legs[slot * 2 + 1].is_live(now_us);
            if now_us > close && slot_live {
                tails += 1;
            }
        }
        self.max_tails = self.max_tails.max(tails);
    }

    pub fn total_messages(&self) -> u64 {
        self.total_messages
    }

    pub fn max_tails(&self) -> usize {
        self.max_tails
    }

    pub fn rotations(&self) -> &[RotationObs] {
        &self.rotations
    }

    pub fn teardowns(&self) -> &[TeardownObs] {
        &self.teardowns
    }

    pub fn zero_gap_handovers(&self) -> usize {
        self.rotations
            .iter()
            .filter(|obs| obs.is_zero_gap_handover())
            .count()
    }

    /// How long the old window kept streaming after the new one went Valid: the teardown of the
    /// window the handover overlapped, minus that handover's ready instant. `None` when the old
    /// window's teardown fell outside the capture (e.g. the final handover).
    pub fn overlap_us(&self, handover: &RotationObs) -> Option<i64> {
        let ready = handover.ready_us?;
        let sibling_open = handover.sibling_window_open_us?;
        self.teardowns
            .iter()
            .find(|teardown| teardown.window_open_us == sibling_open)
            .map(|teardown| teardown.confirmed_at_us - ready)
    }

    pub fn leg_book(&self, index: usize) -> Option<&Book> {
        self.legs.get(index).map(|leg| &leg.book)
    }

    pub fn leg_is_valid(&self, index: usize) -> bool {
        self.legs.get(index).is_some_and(Leg::is_valid)
    }

    pub fn leg_label(&self, index: usize) -> &str {
        self.legs.get(index).map_or("?", |leg| leg.label.as_str())
    }

    fn relative_secs(&self, us: i64) -> f64 {
        (us as f64 - self.reference_boundary_us) / US
    }

    pub fn print_interval(&self, elapsed_secs: u64) {
        println!(
            "\n--- t+{elapsed_secs}s  msgs={} live_legs_peak={} tails_peak={} ---",
            self.total_messages, self.max_live_legs, self.max_tails
        );
        for leg in &self.legs {
            if leg.is_valid() {
                println!("  [{}] {}", leg.label, book_line(&leg.book));
            }
        }
    }

    pub fn print_report(&self) {
        println!("\n======== poly_rotation integration report ========");
        println!(
            "messages={} peak live legs={} (4 = both windows' up+down on one connection) max tails={}",
            self.total_messages, self.max_live_legs, self.max_tails
        );

        println!("\nrotations (times relative to the run's reference boundary):");
        for obs in &self.rotations {
            let name = if obs.slot == 0 { 'A' } else { 'B' };
            let ready = obs.ready_us.map_or_else(
                || "never ready".to_owned(),
                |ready| format!("ready@{:+.0}s", self.relative_secs(ready)),
            );
            let overlap = self
                .overlap_us(obs)
                .map(|overlap| {
                    format!(
                        ", old window streamed {:.1}s past new-Valid",
                        overlap as f64 / US
                    )
                })
                .unwrap_or_default();
            println!(
                "  slot {name} open@{:+.0}s close@{:+.0}s rotated@{:+.0}s {ready} — {}{overlap}",
                self.relative_secs(obs.window_open_us),
                self.relative_secs(obs.window_close_us),
                self.relative_secs(obs.rotated_at_us),
                handover_note(obs),
            );
        }

        println!("\nteardowns (latency from each window's nominal close):");
        for teardown in &self.teardowns {
            let name = if teardown.slot == 0 { 'A' } else { 'B' };
            println!(
                "  slot {name} window@{:+.0}s torn down {:.1}s after close (silent {:.1}s) via {}",
                self.relative_secs(teardown.window_open_us),
                teardown.latency_us as f64 / US,
                teardown.silence_at_teardown_us as f64 / US,
                teardown.inferred_path(),
            );
        }
        println!("==================================================");
    }
}

fn handover_note(obs: &RotationObs) -> String {
    match (obs.ready_us, obs.sibling_age_at_ready_us) {
        (Some(_), Some(age)) if obs.sibling_live_at_ready => format!(
            "HANDOVER: old window still streaming (last frame {:.1}s ago) when new book went Valid",
            age as f64 / US
        ),
        (Some(_), _) => "boot / no live sibling (not a handover)".to_owned(),
        (None, _) => "book never reached Valid".to_owned(),
    }
}

pub fn book_line(book: &Book) -> String {
    let side = |levels: &[Level]| {
        levels
            .iter()
            .take(5)
            .map(|level| format!("{:.3}x{:.2}", level.price.to_f64(), level.qty.to_f64()))
            .collect::<Vec<_>>()
            .join(",")
    };
    let spread = match (book.best_bid(), book.best_ask()) {
        (Some(bid), Some(ask)) => format!("spread {:.3}", ask.price.to_f64() - bid.price.to_f64()),
        _ => "one side empty".to_owned(),
    };
    format!(
        "asks {} | bids {} | {spread}",
        side(book.asks()),
        side(book.bids())
    )
}

/// Turns `btc-updown-5m-a-up` into `a-up`; the venue-symbol prefix is constant across the four slots.
fn slot_label(venue_symbol: &str) -> String {
    venue_symbol
        .strip_prefix("btc-updown-5m-")
        .unwrap_or(venue_symbol)
        .to_owned()
}
