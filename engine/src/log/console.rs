//! The browser build's log output. A page cannot start the drain thread, so each log macro prints
//! one line straight to the console, without a backtrace.

use core::fmt::Arguments;

use web_sys::console;
use web_sys::wasm_bindgen::JsValue;

use super::record::Level;

pub fn register_thread(_tag: &'static str) {}

#[doc(hidden)]
pub fn info_message(module: &'static str, file: &'static str, line: u32, args: Arguments<'_>) {
    emit(Level::Info, module, file, line, args);
}

#[doc(hidden)]
pub fn warn_message(module: &'static str, file: &'static str, line: u32, args: Arguments<'_>) {
    emit(Level::Warn, module, file, line, args);
}

#[doc(hidden)]
pub fn error_message(module: &'static str, file: &'static str, line: u32, args: Arguments<'_>) {
    emit(Level::Error, module, file, line, args);
}

fn emit(level: Level, module: &str, file: &str, line: u32, args: Arguments<'_>) {
    let message = format!("{:<5} {module} ({file}:{line}) {args}", level.as_str());
    let text = JsValue::from_str(&message);
    match level {
        Level::Info => console::info_1(&text),
        Level::Warn => console::warn_1(&text),
        Level::Error => console::error_1(&text),
    }
}
