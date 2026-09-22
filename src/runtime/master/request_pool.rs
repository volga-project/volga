//! Configure and start request workers when they register.
//!
//! Process lifetime belongs to the orchestrator. The master does not heartbeat
//! these workers, delete their pods, or shut them down.

use std::sync::Arc;

use crate::orchestrator::orchestrator::{WorkerNode, WorkerRole};
use crate::orchestrator::task_assignment::TaskWorkerMapping;
use crate::runtime::consts::{runtime_consts, MASTER_DISCOVERY_TIMEOUT};

use super::attempt::session::{map_session_error, Configure, StartWorker, WorkerSession};
use super::events::LifecycleEvent;
use super::state::{MasterState, PipelineContext};
use super::worker_client::WorkerClient;

pub(super) async fn start_request_workers(
    state: &MasterState,
    pipeline: Arc<PipelineContext>,
) -> Result<(), String> {
    state.set_running_pipeline(pipeline.clone()).await;
    if pipeline.expected_request_workers == 0 {
        return Ok(());
    }
    if pipeline.request_graph.is_none() {
        return Err("expected request workers but compiled spec has no request graph".to_string());
    }
    let timeout = runtime_consts().duration(MASTER_DISCOVERY_TIMEOUT);
    let nodes = state
        .wait_for_ready_workers(
            pipeline.expected_request_workers,
            timeout,
            WorkerRole::Request,
        )
        .await
        .map_err(|error| error.to_string())?;
    for node in nodes.values() {
        start_one(state, &pipeline, node).await?;
    }
    let worker_ids: Vec<String> = nodes.keys().cloned().collect();
    state
        .record_lifecycle_event(LifecycleEvent::RequestWorkersStarted { worker_ids })
        .await;
    Ok(())
}

/// A request worker registered after the pipeline was already running (process restart).
pub(super) async fn configure_registered(state: &MasterState, worker_id: &str) {
    let Some(pipeline) = state.running_pipeline().await else {
        return;
    };
    if pipeline.expected_request_workers == 0 || pipeline.request_graph.is_none() {
        return;
    }
    let nodes = state.orchestrator.get_worker_nodes().await;
    let Some(node) = nodes.get(worker_id).filter(|node| node.is_request()) else {
        return;
    };
    if let Err(error) = start_one(state, &pipeline, node).await {
        println!("[MASTER] request worker {worker_id} configure failed: {error}");
    }
}

async fn start_one(
    state: &MasterState,
    pipeline: &PipelineContext,
    node: &WorkerNode,
) -> Result<(), String> {
    let _gate = state.request_configure_lock().lock().await;
    let epoch = {
        let (epoch, started) = state.request_start_status(&node.worker_id).await;
        if started {
            return Ok(());
        }
        epoch
    };
    let addr = format!("{}:{}", node.worker_ip, node.worker_port);
    let client = WorkerClient::open(&node.worker_id, addr, 0)
        .await
        .map_err(|error| format!("{}: {error}", node.worker_id))?;
    let session = WorkerSession::spawn(node.worker_id.clone(), client);
    let bind = format!("{}:{}", node.worker_ip, node.transport_port);
    session
        .ask(Configure {
            pipeline_id: pipeline.pipeline_id.clone(),
            spec: pipeline.spec.clone(),
            vertex_ids: Vec::new(),
            mapping: TaskWorkerMapping::new(),
            task_restore_data: Vec::new(),
            restoring: false,
            role: WorkerRole::Request,
            request_bind_address: Some(bind),
        })
        .await
        .map_err(map_session_error)
        .map_err(|error| format!("{}: {error}", node.worker_id))?;
    session
        .ask(StartWorker)
        .await
        .map_err(map_session_error)
        .map_err(|error| format!("{}: {error}", node.worker_id))?;
    let _ = session.stop_gracefully().await;
    state.mark_request_started(&node.worker_id, epoch).await;
    Ok(())
}
