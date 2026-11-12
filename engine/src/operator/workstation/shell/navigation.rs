//! What the operator chose to see: the size class a canvas falls in, the selection, and the URL
//! fragment that stores it.

const MEDIUM_MIN_WIDTH: f32 = 720.0;
const WIDE_MIN_WIDTH: f32 = 1_200.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SizeClass {
    Compact,
    Medium,
    Wide,
}

impl SizeClass {
    pub(super) fn of_width(width: f32) -> Self {
        if width >= WIDE_MIN_WIDTH {
            Self::Wide
        } else if width >= MEDIUM_MIN_WIDTH {
            Self::Medium
        } else {
            Self::Compact
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum View {
    Chart,
    Ladder,
    Monitor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Selection {
    #[default]
    Desk,
    View(View),
}

impl Selection {
    /// The switcher's order, which is also the key map: digit `n` selects `ALL[n]`.
    pub const ALL: [Self; 4] = [
        Self::Desk,
        Self::View(View::Chart),
        Self::View(View::Ladder),
        Self::View(View::Monitor),
    ];

    /// The leading `#` is optional. Anything unknown, including an empty fragment, means the desk.
    pub fn from_fragment(fragment: &str) -> Self {
        let name = fragment.strip_prefix('#').unwrap_or(fragment);
        Self::ALL
            .into_iter()
            .find(|selection| selection.fragment().strip_prefix('#') == Some(name))
            .unwrap_or_default()
    }

    pub fn fragment(self) -> &'static str {
        match self {
            Self::Desk => "#desk",
            Self::View(View::Chart) => "#chart",
            Self::View(View::Ladder) => "#ladder",
            Self::View(View::Monitor) => "#monitor",
        }
    }
}

/// The selection plus the single view a compact canvas falls back to, because the desk does not
/// exist there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Navigation {
    selection: Selection,
    last_view: View,
}

impl Navigation {
    pub fn new(selection: Selection) -> Self {
        let last_view = match selection {
            Selection::View(view) => view,
            Selection::Desk => View::Chart,
        };
        Self {
            selection,
            last_view,
        }
    }

    /// What the URL must show. It can be the desk while a compact canvas shows a view.
    pub(crate) fn selection(&self) -> Selection {
        self.selection
    }

    pub(super) fn shown(&self, class: SizeClass) -> Selection {
        match (self.selection, class) {
            (Selection::Desk, SizeClass::Compact) => Selection::View(self.last_view),
            (selection, _) => selection,
        }
    }

    /// The desk is refused on a compact canvas, so key `0` does nothing there.
    pub fn select(&mut self, selection: Selection, class: SizeClass) {
        match selection {
            Selection::Desk if class == SizeClass::Compact => {}
            Selection::Desk => self.selection = selection,
            Selection::View(view) => {
                self.selection = selection;
                self.last_view = view;
            }
        }
    }
}
