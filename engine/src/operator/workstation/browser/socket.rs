//! The browser's WebSocket to the relay, as one value that owns its callbacks. Dropping it detaches
//! them first, so a replaced socket's late events never call them.

use js_sys::{ArrayBuffer, Uint8Array};
use web_sys::wasm_bindgen::closure::Closure;
use web_sys::wasm_bindgen::{JsCast, JsValue};
use web_sys::{BinaryType, Document, MessageEvent, Url, WebSocket};

use crate::msg::relay::{PEER_KEY, RELAY_PATH};
use crate::warn;

/// The relay sends one text message, its hello, then only binary ones, each exactly one datagram.
pub(super) enum Frame {
    Hello(String),
    Datagram(Vec<u8>),
}

pub(super) struct LinkSocket {
    socket: WebSocket,
    _on_message: Closure<dyn FnMut(MessageEvent)>,
    _on_state_change: Closure<dyn FnMut()>,
}

impl LinkSocket {
    pub(super) fn open(
        document: &Document,
        peer: usize,
        mut on_frame: impl FnMut(Frame) + 'static,
        on_state_change: impl FnMut() + 'static,
    ) -> Result<Self, JsValue> {
        let base = document.base_uri()?.unwrap_or_default();
        let url = Url::new_with_base(&format!("{RELAY_PATH}?{PEER_KEY}={peer}"), &base)?;
        url.set_protocol(if url.protocol() == "https:" { "wss:" } else { "ws:" });
        let socket = WebSocket::new(&url.href())?;
        socket.set_binary_type(BinaryType::Arraybuffer);

        let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
            let Some(frame) = Frame::of(event.data()) else { return };
            on_frame(frame);
        });
        let on_state_change = Closure::<dyn FnMut()>::new(on_state_change);
        socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        socket.set_onopen(Some(on_state_change.as_ref().unchecked_ref()));
        socket.set_onclose(Some(on_state_change.as_ref().unchecked_ref()));
        socket.set_onerror(Some(on_state_change.as_ref().unchecked_ref()));
        Ok(Self {
            socket,
            _on_message: on_message,
            _on_state_change: on_state_change,
        })
    }

    pub(super) fn is_open(&self) -> bool {
        self.socket.ready_state() == WebSocket::OPEN
    }

    pub(super) fn is_closed(&self) -> bool {
        self.socket.ready_state() == WebSocket::CLOSED
    }

    pub(super) fn send(&self, datagram: &[u8]) {
        if let Err(error) = self.socket.send_with_u8_array(datagram) {
            warn!("cannot send a datagram to the relay: {error:?}");
        }
    }
}

impl Drop for LinkSocket {
    fn drop(&mut self) {
        self.socket.set_onmessage(None);
        self.socket.set_onopen(None);
        self.socket.set_onclose(None);
        self.socket.set_onerror(None);
        if let Err(error) = self.socket.close() {
            warn!("cannot close the relay socket: {error:?}");
        }
    }
}

impl Frame {
    fn of(data: JsValue) -> Option<Self> {
        if let Some(text) = data.as_string() {
            return Some(Self::Hello(text));
        }
        let buffer = data.dyn_into::<ArrayBuffer>().ok()?;
        Some(Self::Datagram(Uint8Array::new(&buffer).to_vec()))
    }
}
