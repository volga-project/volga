//! Per-task operator state registered on the worker [`super::StateRegistry`].

use std::any::Any;

use async_trait::async_trait;

use crate::runtime::observability::snapshot_types::TaskOperatorMetrics;
use crate::runtime::operators::window::model::StateNamespace;
use crate::runtime::operators::OperatorKind;

/// Worker-local state for one operator task (ns, watermarks, …).
#[async_trait]
pub trait OperatorTaskState: Send + Sync + std::fmt::Debug {
    fn state_namespace(&self) -> &StateNamespace;
    fn kind(&self) -> OperatorKind;
    fn as_any(&self) -> &dyn Any;

    /// Runtime task id (same string used as the registry / Prom `task_id` label).
    fn task_id(&self) -> &str;

    /// Operator metrics for the worker poll path (API snapshot).
    async fn task_operator_metrics(&self) -> Option<TaskOperatorMetrics> {
        None
    }
}
