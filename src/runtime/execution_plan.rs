use std::collections::HashMap;

use crate::api::spec::pipeline::PipelineSpec;
use crate::api::compile_logical_graph;
use crate::executor::placement::{
    build_task_placement_mapping,
    strategy_from_name,
    worker_to_task_ids,
    TaskPlacementMapping,
    TaskPlacementStrategyName,
    WorkerEndpoint,
    WorkerTaskPlacement,
};
use crate::runtime::execution_graph::ExecutionGraph;
use crate::api::spec::worker_runtime::WorkerRuntimeSpec;

#[derive(Clone, Debug)]
pub struct ExecutionPlan {
    pub pipeline_spec: PipelineSpec,
    pub execution_graph: ExecutionGraph,
    pub worker_endpoints: Vec<WorkerEndpoint>,
    pub task_placement_mapping: TaskPlacementMapping,
    pub worker_task_ids: HashMap<String, Vec<String>>,
    pub worker_runtime: WorkerRuntimeSpec,
}

impl ExecutionPlan {
    pub fn resolve_worker_runtime(spec: &PipelineSpec) -> WorkerRuntimeSpec {
        let mut worker_runtime = spec.worker_runtime.clone();
        worker_runtime.transport_overrides_queue_records = spec.transport_overrides_queue_records();
        worker_runtime.operator_type_storage_overrides = spec.operator_type_storage_overrides();
        worker_runtime
    }

    pub fn from_spec(spec: &PipelineSpec, mut worker_endpoints: Vec<WorkerEndpoint>) -> Self {
        let mut execution_graph = if spec.sql.is_none() && spec.logical_graph.is_none() {
            ExecutionGraph::new()
        } else {
            let logical_graph = compile_logical_graph(spec);
            logical_graph.to_execution_graph()
        };

        let worker_runtime = Self::resolve_worker_runtime(spec);

        let placement_strategy = spec
            .execution_profile
            .task_placement_strategy()
            .cloned()
            .unwrap_or(TaskPlacementStrategyName::SingleWorker);
        let mut placements = strategy_from_name(&placement_strategy).place_tasks(&execution_graph);
        if placements.is_empty() {
            placements.push(WorkerTaskPlacement::new(Vec::new()));
        }

        if worker_endpoints.is_empty() {
            let count = placements.len().max(1);
            worker_endpoints = (0..count)
                .map(|idx| {
                    let worker_id = if count == 1 {
                        "single_worker".to_string()
                    } else {
                        format!("worker-{idx}")
                    };
                    WorkerEndpoint::new(worker_id, "127.0.0.1".to_string(), 0)
                })
                .collect();
        }

        let task_placement_mapping = build_task_placement_mapping(&placements, &worker_endpoints);
        execution_graph.update_channels_with_node_mapping_and_transport(
            Some(&task_placement_mapping),
            &worker_runtime.transport,
            &worker_runtime.transport_overrides_queue_records,
        );

        let worker_task_ids = worker_to_task_ids(&task_placement_mapping);

        Self {
            pipeline_spec: spec.clone(),
            execution_graph,
            worker_endpoints,
            task_placement_mapping,
            worker_task_ids,
            worker_runtime,
        }
    }
}
