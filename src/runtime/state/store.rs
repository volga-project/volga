//! Shared operator store port (data plane + per-task maintenance).

use std::any::Any;

use anyhow::Result;
use async_trait::async_trait;

use crate::runtime::metrics::MetricsLabels;

use super::task_state::OperatorTaskState;

/// Worker-shared store for one [`crate::runtime::operators::OperatorKind`].
///
/// Multi-ns physically; [`Self::maintain`] is per task state (one ns).
#[async_trait]
pub trait OperatorStore: Any + Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;

    /// Worker-scoped Prom/registry labels (set when the store is opened).
    fn metrics_labels(&self) -> Option<&MetricsLabels> {
        None
    }

    /// Run maintenance for a single task / namespace.
    ///
    /// Namespace and task id come from `state`; labels from [`Self::metrics_labels`].
    async fn maintain(&self, state: &dyn OperatorTaskState) -> Result<()>;
}
