//! The link's sending half. It drains the hot thread's UI rings to subscribers, repeats the catalog
//! and lifecycle every heartbeat so a late subscriber catches up, and renews its own subscriptions.

use std::net::SocketAddr;

use crate::msg::persist::FeatureId;
use crate::msg::ui::{UiCatalog, UiLifecycle};
use crate::time::TsUs;

use super::super::control::{
    CatalogFeature, CatalogInstrument, Lifecycle, RunPhase, RunState, Subscribe,
};
use super::super::envelope::{Envelope, TopicId, WireName};
use super::super::frame::{LinkBody, LinkDatagram};
use super::{Actor, REPORT_TICKS};

impl Actor {
    pub(super) fn flush_feeds(&mut self, now: TsUs) {
        while let Ok(outbound) = self.outbound.pop() {
            self.send(outbound.topic, LinkBody::Payload(outbound.payload), now);
        }
        while let Ok(snapshot) = self.channels.books.pop() {
            self.send(TopicId::BOOKS, LinkBody::Book(snapshot), now);
        }
        while let Ok(event) = self.channels.events.pop() {
            self.send(TopicId::EVENTS, LinkBody::Event(event), now);
        }
    }

    pub(super) fn announce(&mut self, now: TsUs) {
        self.poll_lifecycle(now);
        let lifecycle = LinkBody::Lifecycle(self.lifecycle());
        self.send(TopicId::LIFECYCLE, lifecycle, now);
        for index in 0..self.catalog_frames.len() {
            let (topic, body) = self.catalog_frames[index];
            self.send(topic, body, now);
        }
        self.check_controller(now);
        self.push_marker_if_needed(now);
        self.check_peer_silence(now);
        self.announce_ticks += 1;
        if self.announce_ticks.is_multiple_of(REPORT_TICKS) {
            self.report_counts();
        }
    }

    fn lifecycle(&self) -> Lifecycle {
        let acknowledged = self.control.acknowledged().load();
        Lifecycle {
            phase: self.phase,
            run_state: acknowledged.state,
            execution_mode: self.execution_mode,
            acknowledged_epoch: acknowledged.epoch,
            spin_interval_us: self.spin_interval,
            feature_count: self.feature_count,
        }
    }

    fn poll_lifecycle(&mut self, now: TsUs) {
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
    fn build_catalog_frames(&mut self, catalog: &UiCatalog, now: TsUs) {
        let catalog_ts_us = now;
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
                desired_epoch: 0,
            });
            self.send_to(address, TopicId::SUBSCRIBE, body);
        }
    }

    fn send(&mut self, topic: TopicId, body: LinkBody, now: TsUs) {
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
        if self
            .socket
            .try_send_to(&self.buffer[..len], address)
            .is_err()
        {
            self.counts.send_failed += 1;
        }
    }
}
