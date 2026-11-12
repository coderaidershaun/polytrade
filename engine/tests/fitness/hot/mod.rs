//! Tests of the hot thread: the fold of ordered messages into books, ledger, orders, and the
//! strategy.

mod alloc;
mod exec;
/// The quant calculator pins. They live here rather than beside the calculators because `src/`
/// carries production code only. One module per calculator, named for the concept rather than the
/// source path.
mod quant;
