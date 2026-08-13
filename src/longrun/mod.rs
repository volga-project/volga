//! Long-run soak (and later bench) on top of [`crate::test_utils::harness::VolgaCluster`].
//!
//! Not a second cluster API: no `EngineAdapter` / `RunSpec`. Flink is a sibling
//! [`flink::FlinkCluster`] later, not a wrapper around Volga.

pub mod cli;
pub mod dump;
pub mod flink;
pub mod job;
pub mod oracles;
pub mod run;
pub mod spec;

pub use run::run_soak;
pub use spec::{SoakOracleConfig, SoakScenario, SoakSpec};
