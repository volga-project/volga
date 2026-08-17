//! Sibling of [`crate::test_utils::harness::VolgaCluster`]. Not implemented in v1.
//!
//! Do not wrap Volga in an engine adapter. When Flink lands, it gets its own
//! submit / kill / teardown, sharing soak scenarios and Prom oracles only.

/// Placeholder for the later Flink engine cluster.
pub struct FlinkCluster {
    _private: (),
}

impl FlinkCluster {
    pub fn not_implemented() -> anyhow::Result<Self> {
        anyhow::bail!(
            "FlinkCluster is not implemented in v1; use `volga-longrun soak` with VolgaCluster"
        )
    }
}
