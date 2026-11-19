//! Random draws for strategy code. Their only seed is a param, so a replay of the same messages with
//! the same params draws the same numbers and gives the same orders.

use serde::{Deserialize, Serialize};

/// splitmix64's step: it walks the state across the whole word.
const GOLDEN_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

/// The seed of a strategy's draws, read from its params like any other knob.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Seed(pub u64);

/// A splitmix64 generator. It has no constructor without a [`Seed`], so no draw comes from a clock
/// or from the host.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Draws {
    state: u64,
}

impl Draws {
    pub fn from_seed(seed: Seed) -> Self {
        Self { state: seed.0 }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GOLDEN_GAMMA);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    /// A draw in `[0, 1)`, from the top 53 bits, which an `f64` holds exactly.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}
