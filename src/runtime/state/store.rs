//! Shared operator store port (data plane + per-task maintenance).

use anyhow::Result;
use async_trait::async_trait;

use super::task_state::OperatorTaskState;

/// Worker-shared store for one [`crate::runtime::operators::OperatorKind`].
///
/// Multi-ns physically; [`Self::maintain`] is per task state (one ns).
#[async_trait]
pub trait OperatorStore: Send + Sync + std::fmt::Debug {
    /// Run maintenance for a single task state (read cutoff from `state`).
    async fn maintain(&self, state: &dyn OperatorTaskState) -> Result<()>;
}
