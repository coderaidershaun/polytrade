//! Fixed statuses for the link bar, chosen so each one makes the bar say something different, plus
//! two with the run button armed.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use eframe::egui;

use polysim::link::{RunPhase, RunState};
use polysim::time::DurationUs;
use polysim::workstation::browser::link_bar::{self, LinkAction, LinkBarFrame};
use polysim::workstation::link_model::{ConnectionState, ControlVerdict, LinkStatus};
use polysim::workstation::link_view::RunConfirm;

const LOCAL_ENGINE: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9310);
const LOST_EPOCH: u64 = 1_761_508_800_123_457;

/// An armed scene reads the clock one second into the confirm window, so its button says CONFIRM
/// and never runs out.
const ARMED_READING: DurationUs = DurationUs::from_secs(1);

enum Bar {
    /// `None` is a page that has attached to no engine.
    Live(Option<LinkStatus>),
    Armed(LinkStatus),
}

pub struct LinkScene {
    pub name: &'static str,
    bar: Bar,
}

impl LinkScene {
    fn live(name: &'static str, status: LinkStatus) -> Self {
        Self {
            name,
            bar: Bar::Live(Some(status)),
        }
    }

    fn armed(name: &'static str, status: LinkStatus) -> Self {
        Self {
            name,
            bar: Bar::Armed(status),
        }
    }

    /// A live scene is the real bar, so a click arms it for real. An armed scene is armed again
    /// every frame, so a click on it cannot disarm it.
    pub fn paint(&self, ui: &mut egui::Ui, rect: egui::Rect) {
        match &self.bar {
            Bar::Live(status) => {
                // The fixture has no engine to send an assertion or a peer switch to.
                let _action = link_bar::paint(ui, rect, status.as_ref());
            }
            Bar::Armed(status) => paint_armed(ui, rect, status),
        }
    }
}

fn paint_armed(ui: &mut egui::Ui, rect: egui::Rect, status: &LinkStatus) {
    let mut confirm = RunConfirm::default();
    confirm.press(status, DurationUs::ZERO);
    let frame = LinkBarFrame {
        status: Some(status),
        ui_time: ARMED_READING,
    };
    // A click on the armed button sends and disarms it, so the next frame has to arm it again.
    if let LinkAction::Assert(_) = link_bar::paint_with_confirm(ui, rect, &frame, &mut confirm) {
        ui.ctx().request_repaint();
    }
}

pub fn link_scenes() -> Vec<LinkScene> {
    vec![
        LinkScene::live("live, running, 2 peers", running(LOCAL_ENGINE, 0, 2)),
        LinkScene::live("connecting, 1 peer", engine(LOCAL_ENGINE, 0, 1)),
        LinkScene::live(
            "asserting IDLE over a running engine",
            LinkStatus {
                asserted_state: Some(RunState::Idle),
                control: ControlVerdict::Pending,
                ..running(LOCAL_ENGINE, 0, 2)
            },
        ),
        LinkScene::live(
            "control LOST to another page",
            LinkStatus {
                asserted_state: Some(RunState::Idle),
                control: ControlVerdict::Lost {
                    holder_epoch: LOST_EPOCH,
                },
                ..running(LOCAL_ENGINE, 0, 2)
            },
        ),
        LinkScene::live(
            "stale, stopped, idle, applied",
            LinkStatus {
                connection: ConnectionState::Stale,
                phase: Some(RunPhase::Stopped),
                reported_state: Some(RunState::Idle),
                asserted_state: Some(RunState::Idle),
                control: ControlVerdict::Applied,
                ..engine(LOCAL_ENGINE, 0, 2)
            },
        ),
        LinkScene::live(
            "starting phase, 1 peer",
            LinkStatus {
                connection: ConnectionState::Live,
                phase: Some(RunPhase::Starting),
                reported_state: Some(RunState::Idle),
                ..engine(LOCAL_ENGINE, 0, 1)
            },
        ),
        LinkScene::live(
            "draining phase, applied RUNNING",
            LinkStatus {
                phase: Some(RunPhase::Draining),
                asserted_state: Some(RunState::Running),
                control: ControlVerdict::Applied,
                ..running(LOCAL_ENGINE, 0, 2)
            },
        ),
        LinkScene::live("3 peers, second selected", running(LOCAL_ENGINE, 1, 3)),
        LinkScene::live(
            "IPv6 peer",
            LinkStatus {
                asserted_state: Some(RunState::Running),
                control: ControlVerdict::Applied,
                ..running(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9310), 0, 2)
            },
        ),
        LinkScene {
            name: "no engine attached",
            bar: Bar::Live(None),
        },
        LinkScene::armed(
            "armed: running engine, CONFIRM STOP",
            running(LOCAL_ENGINE, 0, 2),
        ),
        LinkScene::armed(
            "armed: idle engine, CONFIRM START",
            LinkStatus {
                reported_state: Some(RunState::Idle),
                asserted_state: Some(RunState::Idle),
                control: ControlVerdict::Applied,
                ..running(LOCAL_ENGINE, 0, 2)
            },
        ),
        LinkScene::live(
            "stress: 9 peers, long address, LOST",
            LinkStatus {
                connection: ConnectionState::Stale,
                phase: Some(RunPhase::Draining),
                asserted_state: Some(RunState::Idle),
                control: ControlVerdict::Lost {
                    holder_epoch: LOST_EPOCH,
                },
                ..running(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 100, 100, 100)), 65535),
                    8,
                    9,
                )
            },
        ),
    ]
}

fn engine(peer: SocketAddr, peer_index: usize, peer_count: usize) -> LinkStatus {
    LinkStatus {
        peer,
        peer_index,
        peer_count,
        session: 0,
        connection: ConnectionState::Connecting,
        phase: None,
        reported_state: None,
        asserted_state: None,
        control: ControlVerdict::NoOpinion,
    }
}

fn running(peer: SocketAddr, peer_index: usize, peer_count: usize) -> LinkStatus {
    LinkStatus {
        connection: ConnectionState::Live,
        phase: Some(RunPhase::Ready),
        reported_state: Some(RunState::Running),
        ..engine(peer, peer_index, peer_count)
    }
}
