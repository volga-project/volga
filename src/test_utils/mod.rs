//! Shared test / soak helpers. Always compiled so `volga-longrun` can link the cluster.

#[cfg(test)]
pub mod common;
pub mod checkpoint;
pub mod harness;
pub mod launch_specs;
#[cfg(test)]
pub mod many_to_many_harness;
#[cfg(test)]
pub mod parquet;
#[cfg(test)]
pub mod pipeline_exec;
pub mod recovery;
#[cfg(test)]
pub mod smoke;
#[cfg(test)]
pub mod support;
#[cfg(test)]
pub mod transport;
#[cfg(test)]
pub mod window;
#[cfg(test)]
pub mod window_aggs;
