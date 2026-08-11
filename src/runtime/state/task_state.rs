//! Per-task operator state registered on the worker [`super::StateRegistry`].

use std::any::Any;

use async_trait::async_trait;

use crate::runtime::observability::snapshot_types::TaskOperatorMetrics;
use crate::runtime::operators::window::model::StateNamespace;
use crate::runtime::operators::OperatorKind;

/// Worker-local state for one operator task (ns, watermarks, metrics hooks, …).
#[async_trait]
pub trait OperatorTaskState: Send + Sync + std::fmt::Debug {
    fn state_namespace(&self) -> &StateNamespace;
    fn kind(&self) -> OperatorKind;
    fn as_any(&self) -> &dyn Any;

    /// Sampled operator metrics for the worker poll path (API snapshot + Prom).
    async fn sample_metrics(&self) -> Option<TaskOperatorMetrics> {
        None
    }
}
