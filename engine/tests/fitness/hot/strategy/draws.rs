//! A replay must draw the numbers that the run drew, or it sends other orders. So the only seed of
//! the draws of a strategy is a param, and one seed gives one sequence.

use engine::hot::strategy::{Draws, Seed, StrategyParams};
use engine::runtime::config::Config;
use proptest::prelude::*;

use crate::runtime::config_guards::config_with_params;

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, Default)]
#[serde(default)]
struct SeededParams {
    seed: Seed,
}

impl StrategyParams for SeededParams {}

fn first_draws(seed: Seed) -> Vec<f64> {
    let mut draws = Draws::from_seed(seed);
    (0..256).map(|_| draws.unit()).collect()
}

proptest! {
    /// CONTRACT: the seed that a config gives its params draws the same units on each read of that
    /// config, each unit lies in [0, 1), and another seed draws another sequence.
    #[test]
    fn draws_repeat_for_the_seed_that_params_give(raw in any::<u64>()) {
        let document = config_with_params(&format!("    seed: {raw}\n"));
        let config = Config::<SeededParams>::from_yaml(&document).expect("the seed parses");
        prop_assert_eq!(config.strategy.params.seed, Seed(raw));

        let run = first_draws(config.strategy.params.seed);
        let replay = first_draws(Seed(raw));
        prop_assert_eq!(&run, &replay);
        prop_assert!(run.iter().all(|unit| (0.0..1.0).contains(unit)));
        prop_assert_ne!(run, first_draws(Seed(raw.wrapping_add(1))));
    }
}
