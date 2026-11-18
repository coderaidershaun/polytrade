//! The sending half of the link: it drains the UI rings and the latency reports to subscribers,
//! sends a late subscriber the recent events, and builds the lifecycle and catalog frames that each
//! heartbeat repeats, so a late subscriber catches up.

use std::net::SocketAddr;

use crate::msg::persist::FeatureId;
use crate::msg::time::LocalTs;
use crate::msg::ui::UiCatalog;
use crate::msg::ui::channel::UiLifecycle;

use super::super::control::{CatalogFeature, CatalogInstrument, Lifecycle, RunPhase, Subscribe};
use super::super::envelope::{Envelope, TopicId, WireName};
use super::super::frame::{LinkBody, LinkDatagram};
use super::Actor;
use crate::msg::run_state::{ControllerEpoch, RunState};

impl Actor {
    /// History goes first, so a live event recorded in this flush cannot evict a frame that its
    /// replay is about to send.
    pub(super) fn flush_feeds(&mut self, now: LocalTs) {
        self.send_history(now);
        while let Ok(outbound) = self.outbound.pop() {
            self.send(outbound.topic, LinkBody::Payload(outbound.payload), now);
        }
        while let Ok(snapshot) = self.channels.books.pop() {
            self.send(TopicId::BOOKS, LinkBody::Book(snapshot), now);
        }
        while let Ok(event) = self.channels.events.pop() {
            self.send(TopicId::EVENTS, LinkBody::Event(event), now);
            self.history.record(event);
        }
        while let Ok(report) = self.latency.pop() {
            self.send(TopicId::LATENCY, LinkBody::Latency(report), now);
        }
    }

    fn send_history(&mut self, now: LocalTs) {
        self.recipients.clear();
        self.recipients
            .extend(self.subscribers.naming(TopicId::HISTORY, now));
        self.history.begin_flush(&self.recipients);
        while let Some((address, event)) = self.history.next_frame() {
            let len = self.encode(TopicId::HISTORY, LinkBody::History(event));
            if self.try_transmit(address, len) {
                self.counts.history_sent += 1;
            } else {
                self.counts.history_failed += 1;
            }
        }
    }

    pub(super) fn lifecycle(&self) -> Lifecycle {
        let acknowledged = self.control.acknowledged();
        Lifecycle {
            phase: self.phase,
            run_state: acknowledged.state,
            execution_mode: self.execution_mode,
            acknowledged_epoch: acknowledged.epoch,
            spin_interval_us: self.spin_interval,
            feature_count: self.feature_count,
        }
    }

    pub(super) fn poll_lifecycle(&mut self, now: LocalTs) {
        while let Ok(message) = self.channels.lifecycle.try_recv() {
            match message {
                UiLifecycle::Ready(catalog) => {
                    self.phase = RunPhase::Ready;
                    self.execution_mode = catalog.execution_mode;
                    self.build_catalog_frames(&catalog, now);
                }
                UiLifecycle::Draining => self.phase = RunPhase::Draining,
            }
        }
    }

    /// # Panics
    /// When a name is longer than `LINK_NAME_LEN` bytes, which the engine refuses at startup before
    /// this can run.
    fn build_catalog_frames(&mut self, catalog: &UiCatalog, now: LocalTs) {
        let catalog_ts_us = now.to_peer();
        self.catalog_frames.clear();
        let instrument_count = catalog.instruments.len() as u16;
        for row in &catalog.instruments {
            self.catalog_frames.push((
                TopicId::CATALOG_INSTRUMENTS,
                LinkBody::CatalogInstrument(CatalogInstrument {
                    catalog_ts_us,
                    total_count: instrument_count,
                    instrument: row.instrument_id,
                    display: WireName::new(&row.display),
                    tick_size: row.tick_size,
                    lot_size: row.lot_size,
                    qty_scale: row.qty_scale,
                    base_asset: row.base_asset,
                    quote_asset: row.quote_asset,
                    base: WireName::new(&row.base),
                    quote: WireName::new(&row.quote),
                }),
            ));
        }
        let feature_count = catalog.feature_names.len() as u16;
        for (index, name) in catalog.feature_names.iter().enumerate() {
            self.catalog_frames.push((
                TopicId::CATALOG_FEATURES,
                LinkBody::CatalogFeature(CatalogFeature {
                    catalog_ts_us,
                    total_count: feature_count,
                    feature: FeatureId(index as u16),
                    name: WireName::new(name),
                }),
            ));
        }
    }

    pub(super) fn refresh_subscriptions(&mut self) {
        for index in 0..self.peers.len() {
            let address = self.peers[index].feed.address;
            let topics = self.peers[index].feed.topics;
            let body = LinkBody::Subscribe(Subscribe {
                topics,
                desired_state: RunState::Running,
                desired_epoch: ControllerEpoch::ZERO,
            });
            self.send_to(address, TopicId::SUBSCRIBE, body);
        }
    }

    pub(super) fn send(&mut self, topic: TopicId, body: LinkBody, now: LocalTs) {
        self.recipients.clear();
        self.recipients
            .extend(self.subscribers.recipients(topic, now));
        if self.recipients.is_empty() {
            return;
        }
        let len = self.encode(topic, body);
        for index in 0..self.recipients.len() {
            let address = self.recipients[index];
            self.transmit(address, len);
        }
    }

    fn send_to(&mut self, address: SocketAddr, topic: TopicId, body: LinkBody) {
        let len = self.encode(topic, body);
        self.transmit(address, len);
    }

    fn encode(&mut self, topic: TopicId, body: LinkBody) -> usize {
        let index = usize::from(topic.0);
        self.outbound_seq_by_topic[index] += 1;
        let envelope = Envelope::new(self.identity, topic, self.outbound_seq_by_topic[index]);
        LinkDatagram { envelope, body }.encode(&mut self.buffer)
    }

    /// Never waits, so a peer whose socket is full cannot stall the others. A dropped send is
    /// counted, and the far side sees it as a sequence gap.
    fn transmit(&mut self, address: SocketAddr, len: usize) {
        if !self.try_transmit(address, len) {
            self.counts.send_failed += 1;
        }
    }

    fn try_transmit(&self, address: SocketAddr, len: usize) -> bool {
        self.socket
            .try_send_to(&self.buffer[..len], address)
            .is_ok()
    }
}
