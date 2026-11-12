//! One attached trading engine as the workstation holds it, toolkit-free: link session, status,
//! screen, model, and the rule that a new session starts them over.

use crate::link::RunState;
use crate::msg::ui::UiCatalog;
use crate::time::TsUs;

use super::dom_view::FeedStatus;
use super::link_model::{ConnectionState, LinkStatus};
use super::link_session::{Delivery, LinkSession};
use super::model::UiModel;
use super::monitor::model::SystemNote;
use super::monitor::view::MonitorUiState;

/// `Starting` means no engine has announced its catalog yet, which the workstation decides for
/// itself rather than being told.
pub(crate) enum Screen {
    Starting,
    Live,
}

pub(crate) struct Attachment {
    session: LinkSession,
    status: LinkStatus,
    screen: Screen,
    model: UiModel,
    monitor_state: MonitorUiState,
}

impl Attachment {
    pub(crate) fn new(session: LinkSession, now: TsUs) -> Self {
        let status = session.status(now);
        Self {
            session,
            status,
            screen: Screen::Starting,
            model: UiModel::new(),
            monitor_state: MonitorUiState::new(),
        }
    }

    /// Folds one datagram. Returns true when the screen has something new to show.
    pub(crate) fn on_datagram(&mut self, bytes: &[u8], now: TsUs) -> bool {
        let delivery = self.session.on_datagram(bytes, now);
        // A frame can raise the session without yielding a delivery, so the status is read
        // first and the delivery lands in the model that is already starting over.
        let is_new_status = self.refresh(now);
        let Some(delivery) = delivery else {
            return is_new_status;
        };
        match delivery {
            Delivery::Book(snapshot) => self.model.apply_book(snapshot),
            Delivery::Event(event) => self.model.apply_event(event),
            Delivery::Catalog(catalog) => self.adopt_catalog(catalog),
        }
        true
    }

    /// Reads the session's status afresh, because silence moves it with no datagram to say so.
    /// Returns true when it changed. A status that names a different session starts everything
    /// this attachment holds over.
    pub(crate) fn refresh(&mut self, now: TsUs) -> bool {
        let status = self.session.status(now);
        if status == self.status {
            return false;
        }
        if status.session != self.status.session {
            self.screen = Screen::Starting;
            self.model = UiModel::new();
            self.monitor_state = MonitorUiState::new();
        }
        self.status = status;
        true
    }

    pub(crate) fn assert_run_state(&mut self, state: RunState, now: TsUs) {
        self.session.assert_run_state(state);
        self.refresh(now);
    }

    /// `false`, with nothing changed, when there is no other engine to move to.
    pub(crate) fn attach_to_next_peer(&mut self, now: TsUs) -> bool {
        let has_moved = self.session.attach_to_next_peer();
        self.refresh(now);
        has_moved
    }

    /// The datagram to send to the active engine, when one is due. Call it only while the socket
    /// can carry it, because a due subscribe is spent when returned.
    pub(crate) fn subscribe_due(&mut self, now: TsUs) -> Option<&[u8]> {
        self.session.subscribe_due(now)
    }

    fn adopt_catalog(&mut self, catalog: UiCatalog) {
        self.model.note_lifecycle(SystemNote::Ready);
        self.screen = Screen::Live;
        self.model.set_catalog(catalog);
    }

    /// What the link says about the feed, before the ladder checks how fresh the book is.
    pub(crate) fn feed_status(&self) -> FeedStatus {
        let is_attached = self.status.connection == ConnectionState::Live;
        if is_attached { FeedStatus::Live } else { FeedStatus::Disconnected }
    }

    pub(crate) fn active_peer(&self) -> usize {
        self.session.active_peer()
    }

    pub(crate) fn model(&self) -> &UiModel {
        &self.model
    }

    /// Both parts of what the panels paint, borrowed at once because the painters take them together.
    pub(crate) fn panels(&mut self) -> (&mut UiModel, &mut MonitorUiState) {
        (&mut self.model, &mut self.monitor_state)
    }

    pub(crate) fn screen(&self) -> &Screen {
        &self.screen
    }

    pub(crate) fn link_status(&self) -> &LinkStatus {
        &self.status
    }
}
