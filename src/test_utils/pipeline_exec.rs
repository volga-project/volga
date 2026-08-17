use std::collections::HashMap as StdHashMap;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{LogicalGraph, PipelineSpec};
use crate::common::types::PipelineId;
use crate::runtime::observability::{PipelineSnapshot, WorkerSnapshot};
use crate::runtime::worker::{Close, GetState, RunTestLifecycle, Worker, WorkerConfig};
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

    // Maintenance on by default (StateSpec); use a short interval so inproc
    // tests actually tick the cleaner before finishing.
    let mut state = spec.state.clone();
    state.maintenance_enabled = true;
    if state.maintenance_interval_ms >= 1_000 {
        state.maintenance_interval_ms = 50;
    }
    let worker_config = WorkerConfig::new(
        worker_id.clone(),
        pipeline_id,
        execution_graph,
        vertex_ids,
        num_threads_per_task,
        TransportBackendType::Grpc,
    )
    .with_state_spec(&state);
    let worker = Worker::spawn_configured(worker_config).await;

    if let Some(pipeline_state_sender) = state_updates_sender {
        let (worker_state_sender, mut worker_state_receiver) = mpsc::channel::<WorkerSnapshot>(100);
        let pipeline_sender = pipeline_state_sender.clone();
        let worker_id_clone = worker_id.clone();
        tokio::spawn(async move {
            while let Some(worker_state) = worker_state_receiver.recv().await {
                let mut worker_states = StdHashMap::new();
                worker_states.insert(worker_id_clone.clone(), worker_state);
                let _ = pipeline_sender
                    .send(PipelineSnapshot::new(worker_states))
                    .await;
            }
        });
        worker
            .ask(RunTestLifecycle {
                state_updates: Some(worker_state_sender),
            })
            .await
            .map_err(|e| anyhow::anyhow!("RunTestLifecycle: {e:?}"))?;
    } else {
        worker
            .ask(RunTestLifecycle {
                state_updates: None,
            })
            .await
            .map_err(|e| anyhow::anyhow!("RunTestLifecycle: {e:?}"))?;
    }

    let worker_state = worker
        .ask(GetState {
            execution_attempt_id: 0,
        })
        .await
        .map_err(|e| anyhow::anyhow!("GetState: {e:?}"))?;
    let _ = worker.ask(Close).await;
    let _ = worker.stop_gracefully().await;

    let mut worker_states = StdHashMap::new();
    worker_states.insert(worker_id, worker_state);
    Ok(PipelineSnapshot::new(worker_states))
}

pub async fn execute(spec: PipelineSpec, logical_graph: LogicalGraph) -> Result<PipelineSnapshot> {
    execute_with_state_updates(spec, logical_graph, None).await
}
