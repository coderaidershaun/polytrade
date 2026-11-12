//! The workstation's side of the link to one engine: the session that speaks the wire, the model of
//! its status, the view of the link bar, and the attachment that ties them to the page.

pub(crate) mod attachment;
pub mod model;
pub mod session;
pub mod view;
