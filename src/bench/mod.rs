//! Bench runner on top of [`crate::test_utils::harness::VolgaCluster`].
//!
//! Not a second cluster API: no `EngineAdapter`. Flink is a sibling
//! [`flink::FlinkCluster`] later, not a wrapper around Volga. A long soak is
//! the same runner with a long duration and stability oracles.

pub mod cli;
pub mod config;
pub mod dump;
pub mod flink;
pub mod oracles;
pub mod run;
pub mod spec;

pub use run::run_bench;
pub use spec::{lag_p99_bound, OracleConfig, Scenario, BenchSpec};
