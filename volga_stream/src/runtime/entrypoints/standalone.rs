use anyhow::Result;
use tokio::signal;

use crate::api::compile_logical_graph;
use crate::executor::bootstrap::MasterBootstrapPayload;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::master::Master;
use crate::runtime::master_server::{MasterServer, TaskKey};
use crate::runtime::operators::operator::operator_config_requires_checkpoint;
use crate::runtime::worker::WorkerConfig;
use crate::runtime::worker_server::WorkerServer;
use crate::runtime::master_server::master_service::GetWorkerBootstrapRequest;
use crate::runtime::master_server::master_service::master_service_client::MasterServiceClient;
use crate::transport::transport_backend_actor::TransportBackendType;
use crate::transport::channel::Channel;

pub async fn run_master(
    bind_addr: String,
    worker_addrs: Vec<String>,
    bootstrap: MasterBootstrapPayload,
) -> Result<()> {
    let plan = &bootstrap.plan;
    let mut execution_graph = if plan.pipeline_spec.sql.is_none()
        && plan.pipeline_spec.logical_graph.is_none()
    {
        ExecutionGraph::new()
    } else {
        let logical_graph = compile_logical_graph(&plan.pipeline_spec);
        logical_graph.to_execution_graph()
    };

    let expected_tasks = execution_graph
        .get_vertices()
        .values()
        .filter(|v| operator_config_requires_checkpoint(&v.operator_config))
        .map(|v| TaskKey {
            vertex_id: v.vertex_id.as_ref().to_string(),
            task_index: v.task_index,
        })
        .collect::<Vec<_>>();

    let mut master_server = MasterServer::new();
    master_server.set_checkpointable_tasks(expected_tasks).await;
    master_server.set_bootstrap_payload(bootstrap).await;
    master_server.start(&bind_addr).await?;

    let master_snapshot_sink = master_server.snapshot_sink();
    let mut master = Master::new().with_snapshot_sink(master_snapshot_sink);
    master.execute(worker_addrs).await?;

    master_server.stop().await;
    Ok(())
}

pub async fn run_worker(master_addr: String, bind_addr: String, worker_id: String) -> Result<()> {
    let mut client = MasterServiceClient::connect(format!("http://{}", master_addr)).await?;
    let resp = client
        .get_worker_bootstrap(tonic::Request::new(GetWorkerBootstrapRequest {
            worker_id: worker_id.clone(),
        }))
        .await?
        .into_inner();
    let bootstrap: crate::executor::bootstrap::WorkerBootstrapPayload =
        bincode::deserialize(&resp.bootstrap_bytes)?;
    if bootstrap.worker_id != worker_id {
        return Err(anyhow::anyhow!(
            "worker_id mismatch: env={} bootstrap={}",
            worker_id,
            bootstrap.worker_id
        ));
    }

    let plan = bootstrap.plan;
    let mut execution_graph = if plan.pipeline_spec.sql.is_none()
        && plan.pipeline_spec.logical_graph.is_none()
    {
        ExecutionGraph::new()
    } else {
        let logical_graph = compile_logical_graph(&plan.pipeline_spec);
        logical_graph.to_execution_graph()
    };

    execution_graph.update_channels_with_node_mapping_and_transport(
        Some(&plan.task_placement_mapping),
        &plan.worker_runtime.transport,
        &plan.worker_runtime.transport_overrides_queue_records,
    );

    let vertex_ids = bootstrap.assigned_task_ids.clone();

    let has_remote_channels = vertex_ids.iter().any(|vertex_id| {
        if let Some((in_edges, out_edges)) = execution_graph.get_edges_for_vertex(vertex_id) {
            in_edges
                .iter()
                .chain(out_edges.iter())
                .any(|e| matches!(e.channel.as_ref(), Some(Channel::Remote { .. })))
        } else {
            false
        }
    });
    let transport_backend_type = if has_remote_channels {
        TransportBackendType::Grpc
    } else {
        TransportBackendType::InMemory
    };

    let worker_config = WorkerConfig::new(
        worker_id.clone(),
        bootstrap.pipeline_execution_context.clone(),
        execution_graph,
        vertex_ids
            .into_iter()
            .map(|v| std::sync::Arc::<str>::from(v))
            .collect(),
        transport_backend_type,
        plan.worker_runtime.clone(),
    )
    .with_master_addr(master_addr.clone());

    let mut worker_server = WorkerServer::new(worker_config);
    worker_server.start(&bind_addr).await?;

    signal::ctrl_c().await?;
    worker_server.stop().await;
    Ok(())
}
