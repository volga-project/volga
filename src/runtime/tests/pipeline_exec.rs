use std::collections::HashMap as StdHashMap;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{LogicalGraph, PipelineSpec};
use crate::common::types::PipelineId;
use crate::runtime::observability::{PipelineSnapshot, WorkerSnapshot};
use crate::runtime::worker::{Worker, WorkerConfig};
use crate::transport::transport_backend_actor::TransportBackendType;

pub async fn execute_with_state_updates(
    spec: PipelineSpec,
    logical_graph: LogicalGraph,
    state_updates_sender: Option<mpsc::Sender<PipelineSnapshot>>,
) -> Result<PipelineSnapshot> {
    let mut execution_graph = logical_graph.to_execution_graph();
    let pipeline_id = PipelineId(uuid::Uuid::new_v4().to_string());
    execution_graph.configure_channels(None, Some(&spec));
    let vertex_ids = execution_graph.get_vertices().keys().cloned().collect();
    let worker_id = "single_worker".to_string();

    let num_threads_per_task = match spec.execution_profile.clone().unwrap() {
        ExecutionProfile::SingleWorker { num_threads_per_task } => num_threads_per_task,
        _ => panic!("Execution profile must be SingleWorker"),
    };

    let mut worker_config = WorkerConfig::new(
        worker_id.clone(),
        pipeline_id,
        execution_graph,
        vertex_ids,
        num_threads_per_task,
        TransportBackendType::Grpc,
    );
    worker_config.window_state_namespace = spec.worker_runtime.window_state_namespace.clone();
    let mut worker = Worker::from_config(worker_config);

    if let Some(pipeline_state_sender) = state_updates_sender {
        let (worker_state_sender, mut worker_state_receiver) = mpsc::channel::<WorkerSnapshot>(100);
        let pipeline_sender = pipeline_state_sender.clone();
        let worker_id_clone = worker_id.clone();
        tokio::spawn(async move {
            while let Some(worker_state) = worker_state_receiver.recv().await {
                let mut worker_states = StdHashMap::new();
                worker_states.insert(worker_id_clone.clone(), worker_state);
                let _ = pipeline_sender.send(PipelineSnapshot::new(worker_states)).await;
            }
        });
        worker
            .execute_worker_lifecycle_for_testing_with_state_updates(worker_state_sender)
            .await;
    } else {
        worker.execute_worker_lifecycle_for_testing().await;
    }

    let worker_state = worker.get_state().await;
    worker.close();

    let mut worker_states = StdHashMap::new();
    worker_states.insert(worker_id, worker_state);
    Ok(PipelineSnapshot::new(worker_states))
}

pub async fn execute(spec: PipelineSpec, logical_graph: LogicalGraph) -> Result<PipelineSnapshot> {
    execute_with_state_updates(spec, logical_graph, None).await
}
