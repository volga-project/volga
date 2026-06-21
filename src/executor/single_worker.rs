use std::collections::HashMap as StdHashMap;

use anyhow::Result;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{compile_logical_graph, PipelineSpec};
use crate::common::types::PipelineId;
use crate::runtime::observability::{PipelineSnapshot, WorkerSnapshot};
use crate::runtime::worker::{Worker, WorkerConfig};
use crate::transport::transport_backend_actor::TransportBackendType;

// Simulates a single worker executing a pipeline, used locally

pub async fn execute_with_state_updates(
    spec: PipelineSpec,
    state_updates_sender: Option<mpsc::Sender<PipelineSnapshot>>,
) -> Result<PipelineSnapshot> {
    let logical_graph = compile_logical_graph(&spec);
    let mut execution_graph = logical_graph.to_execution_graph();
    let pipeline_id = PipelineId(Uuid::new_v4());
    execution_graph.update_channels_with_node_mapping_and_transport(
        None,
        &spec.worker_runtime.transport,
        &spec.transport_overrides_queue_records(),
    );
    let vertex_ids = execution_graph.get_vertices().keys().cloned().collect();
    let worker_id = "single_worker".to_string();

    let execution_profile = spec.execution_profile.clone().unwrap();
    let num_threads_per_task = match execution_profile {
        ExecutionProfile::SingleWorker { num_threads_per_task } => num_threads_per_task,
        _ => panic!("Execution profile must be SingleWorker"),
    };

    let mut worker_config = WorkerConfig::new(
        worker_id.clone(),
        pipeline_id,
        execution_graph,
        vertex_ids,
        num_threads_per_task,
        TransportBackendType::InMemory,
    );
    worker_config.storage_budgets = spec.worker_runtime.storage.budgets.clone();
    worker_config.inmem_store_lock_pool_size = spec.worker_runtime.storage.inmem_store_lock_pool_size;
    worker_config.inmem_store_bucket_granularity = spec.worker_runtime.storage.inmem_store_bucket_granularity;
    worker_config.inmem_store_max_batch_size = spec.worker_runtime.storage.inmem_store_max_batch_size;
    worker_config.operator_type_storage_overrides = spec.operator_type_storage_overrides();
    let mut worker = Worker::new(worker_config);

    if let Some(pipeline_state_sender) = state_updates_sender {
        let (worker_state_sender, mut worker_state_receiver) = mpsc::channel::<WorkerSnapshot>(100);
        let pipeline_sender = pipeline_state_sender.clone();
        let worker_id_clone = worker_id.clone();
        tokio::spawn(async move {
            while let Some(worker_state) = worker_state_receiver.recv().await {
                let mut worker_states = StdHashMap::new();
                worker_states.insert(worker_id_clone.clone(), worker_state);
                let pipeline_state = PipelineSnapshot::new(worker_states);
                let _ = pipeline_sender.send(pipeline_state).await;
            }
        });

        worker
            .execute_worker_lifecycle_for_testing_with_state_updates(worker_state_sender)
            .await;
    } else {
        worker.execute_worker_lifecycle_for_testing().await;
    }

    let worker_state = worker.get_state().await;
    worker.close().await;

    let mut worker_states = StdHashMap::new();
    worker_states.insert(worker_id, worker_state);
    Ok(PipelineSnapshot::new(worker_states))
}

pub async fn execute(spec: PipelineSpec) -> Result<PipelineSnapshot> {
    execute_with_state_updates(spec, None).await
}