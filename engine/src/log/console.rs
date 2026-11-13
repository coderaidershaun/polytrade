//! The browser build's log output. A page cannot start the drain thread, so each log macro prints
//! one line straight to the console, without a backtrace.

use core::fmt::Arguments;

use web_sys::console;
use web_sys::wasm_bindgen::JsValue;

use super::record::{Callsite, Level, WORDS_PER_RECORD};
use super::render::{self, LogArgs};

pub fn register_thread(_tag: &'static str) {}

pub(crate) fn register_hot_lane(_tag: &'static str) {}

#[doc(hidden)]
pub fn text_message(callsite: &'static Callsite, args: Arguments<'_>) {
    emit(callsite, &args.to_string());
}

/// Renders at once with the drain's own renderer, so a line reads the same in the page and in a file.
#[doc(hidden)]
pub fn hot_message<A: LogArgs>(callsite: &'static Callsite, args: A) {
    let mut words = [0; WORDS_PER_RECORD];
    args.write_words(&mut words);
    let mut text = String::new();
    // A String accepts every write, so rendering into it cannot fail.
    render::render(callsite, &words, &mut text).ok();
    emit(callsite, &text);
}

fn emit(callsite: &Callsite, text: &str) {
    let level = callsite.level();
    let message = format!(
        "{:<5} {} ({}:{}) {text}",
        level.as_str(),
        callsite.module(),
        callsite.file(),
        callsite.line()
    );
    let text = JsValue::from_str(&message);
    match level {
        Level::Info => console::info_1(&text),
        Level::Warn => console::warn_1(&text),
        Level::Error => console::error_1(&text),
    }
}
