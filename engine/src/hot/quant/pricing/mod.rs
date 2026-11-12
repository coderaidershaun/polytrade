//! Pricing: closed-form optimal-quote calculators (tick-space, zero alloc).

mod gueant;

pub use gueant::{GueantParams, Objective, QuoteCoefficients, QuoteInputs, Quotes};
