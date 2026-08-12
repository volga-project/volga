//! Per-task operator state registered on the worker [`super::StateRegistry`].

use std::any::Any;

use async_trait::async_trait;

use crate::runtime::metrics::MetricsLabels;
use crate::runtime::observability::snapshot_types::TaskOperatorMetrics;
use crate::runtime::operators::window::model::StateNamespace;
use crate::runtime::operators::OperatorKind;

/// Worker-local state for one operator task (ns, watermarks, …).
#[async_trait]
pub trait OperatorTaskState: Send + Sync + std::fmt::Debug {
    fn state_namespace(&self) -> &StateNamespace;
    fn kind(&self) -> OperatorKind;
    fn as_any(&self) -> &dyn Any;

    /// Operator metrics for the worker poll path (API snapshot).
    ///
    /// Identity (`task_id` / labels) is supplied by the caller — the worker already
    /// has both; state does not store a second copy.
    async fn task_operator_metrics(
        &self,
        task_id: &str,
        labels: Option<&MetricsLabels>,
    ) -> Option<TaskOperatorMetrics> {
        let _ = (task_id, labels);
        None
    }
}
