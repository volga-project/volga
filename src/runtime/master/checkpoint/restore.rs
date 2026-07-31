use std::collections::HashMap;

use anyhow::{ensure, Result};

use crate::runtime::checkpoint::{
    CompletedCheckpoint, RestorePlan, SerializedCheckpoint, SerializedRestore, TaskKey,
};
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::operators::operator::{operator_config_requires_checkpoint, OperatorConfig};

#[derive(Debug, Default)]
pub struct RestorePlanner;

impl RestorePlanner {
    pub fn plan(
        checkpoint: CompletedCheckpoint,
        target_graph: &ExecutionGraph,
    ) -> Result<RestorePlan> {
        let checkpoint_id = checkpoint.checkpoint_id;
        let mut checkpoint_tasks = checkpoint.tasks;
        let target_tasks = target_graph
            .get_vertices()
            .values()
            .filter(|vertex| operator_config_requires_checkpoint(&vertex.operator_config))
            .map(|vertex| TaskKey {
                vertex_id: vertex.vertex_id.as_ref().to_string(),
                task_index: vertex.task_index,
            })
            .collect::<Vec<_>>();
        // Stable parallelism requires the same stateful task set on restore.
        // Rescaling will replace this with operator-specific source-to-target task mapping.
        ensure!(
            checkpoint_tasks.len() == target_tasks.len(),
            "checkpoint {} has {} stateful tasks, target assignment expects {}",
            checkpoint_id,
            checkpoint_tasks.len(),
            target_tasks.len()
        );

        let mut tasks = HashMap::with_capacity(target_tasks.len());
        for task in &target_tasks {
            let checkpoint_data = checkpoint_tasks.remove(task).ok_or_else(|| {
                anyhow::anyhow!(
                    "checkpoint {} has no state for task {} index={}",
                    checkpoint_id,
                    task.vertex_id,
                    task.task_index
                )
            })?;
            let vertex = target_graph
                .get_vertex(&task.vertex_id)
                .ok_or_else(|| anyhow::anyhow!("target task {} not found", task.vertex_id))?;
            ensure!(
                vertex.task_index == task.task_index,
                "target task {} index does not match checkpoint",
                task.vertex_id
            );
            tasks.insert(
                task.clone(),
                Self::plan_operator(&vertex.operator_config, checkpoint_data)?,
            );
        }

        Ok(RestorePlan { tasks })
    }

    fn plan_operator(
        config: &OperatorConfig,
        checkpoint: SerializedCheckpoint,
    ) -> Result<SerializedRestore> {
        match config {
            OperatorConfig::SourceConfig(_) | OperatorConfig::WindowConfig(_) => {
                Ok(Self::passthrough(checkpoint))
            }
            OperatorConfig::ChainedConfig(_) => {
                anyhow::bail!("chained restore is not implemented")
            }
            _ => anyhow::bail!("operator is not checkpointable"),
        }
    }

    fn passthrough(checkpoint: SerializedCheckpoint) -> SerializedRestore {
        // Same-assignment state is directly restorable. Rescaling will replace this with
        // operator-specific decode, redistribution, and encoding.
        SerializedRestore::new(checkpoint.into_bytes())
    }
}
