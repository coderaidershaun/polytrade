//! Where the crosshair stands: under a mouse, a dragging finger, or pinned where a finger lifted
//! until the pane lets go of it. It uses no egui type, so it reads alone.

use crate::workstation::chart::view::ChartDomain;

/// One pointer event, already resolved against the plots. `finger` is the browser's id for one
/// finger, which stays the same from touch down to lift. `LetGo` is a click or tap in the pane that
/// is not a finger landing on a plot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Touch {
    OnPlot { finger: u64, bucket: u64 },
    LetGo,
    Moved { finger: u64, bucket: u64 },
    Lifted { finger: u64 },
    Cancelled { finger: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Scrub {
    finger: u64,
    bucket: u64,
}

/// A mouse never pins, so on a desktop with no touch screen nothing here is ever set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct CrosshairPin {
    scrub: Option<Scrub>,
    pinned: Option<u64>,
}

impl CrosshairPin {
    pub(super) fn on_touch(&mut self, touch: Touch) {
        match touch {
            // A lift that landed off the page never arrives, so a new touch takes over a stale
            // scrub instead of waiting for it.
            Touch::OnPlot { finger, bucket } => self.scrub = Some(Scrub { finger, bucket }),
            // A click lets go of the pin but not of a scrub in progress, which ends when its own
            // finger lifts.
            Touch::LetGo => self.pinned = None,
            Touch::Moved { finger, bucket } => {
                if let Some(scrub) = self.scrub.as_mut().filter(|scrub| scrub.finger == finger) {
                    scrub.bucket = bucket;
                }
            }
            Touch::Lifted { finger } => {
                if let Some(scrub) = self.scrub.filter(|scrub| scrub.finger == finger) {
                    self.pinned = Some(scrub.bucket);
                    self.scrub = None;
                }
            }
            Touch::Cancelled { finger } => {
                if self.scrub.is_some_and(|scrub| scrub.finger == finger) {
                    self.scrub = None;
                }
            }
        }
    }

    /// The bucket to mark: under a scrubbing finger, else under the mouse, else the pin.
    pub(super) fn bucket(&self, hovered: Option<u64>) -> Option<u64> {
        self.scrub
            .map(|scrub| scrub.bucket)
            .or(hovered)
            .or(self.pinned)
    }

    /// A scrub whose finger is no longer down never gets its lift, as when the chart was not painted
    /// in that frame. Left alone, it would keep the crosshair from following the mouse.
    pub(super) fn end_scrub(&mut self) {
        self.scrub = None;
    }

    /// A pin whose bucket has left the window marks nothing, so it is dropped.
    pub(super) fn forget_outside(&mut self, domain: ChartDomain) {
        self.pinned = self.pinned.filter(|bucket| domain.contains(*bucket));
    }
}
