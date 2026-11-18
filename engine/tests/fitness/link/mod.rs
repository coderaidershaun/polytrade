//! Tests of the link between engines: frames on the wire, the control path, and peer frames that
//! reach the hot thread.

mod control_plane;
mod controller;
mod history;
mod link_ingress;
mod link_input;
mod link_wire;
mod loopback;
mod marker_queue;
mod wire_digest;
mod wire_frames;
