use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{compile_logical_graph, PipelineSpec};
use crate::cluster::cluster_provider::ClusterProvider;
use crate::cluster::node_assignment::{node_to_vertex_ids, NodeAssignStrategy};
use crate::common::test_utils::gen_unique_grpc_port;
use crate::common::types::PipelineId;
use crate::runtime::master::Master;
use crate::runtime::master_server::{MasterServer, TaskKey};
use crate::runtime::observability::PipelineSnapshot;
use crate::runtime::operators::operator::operator_config_requires_checkpoint;
use crate::runtime::worker::WorkerConfig;
use crate::runtime::worker_server::WorkerServer;
use crate::transport::channel::Channel;
use crate::transport::transport_backend_actor::TransportBackendType;

// Simulates a master and worker executing a pipeline

pub async fn execute(
    spec: PipelineSpec,
    cluster_provider: Arc<dyn ClusterProvider>,
    node_assign: Arc<dyn NodeAssignStrategy>,
) -> Result<PipelineSnapshot> {
    let logical_graph = compile_logical_graph(&spec);
    let mut execution_graph = logical_graph.to_execution_graph();
    let pipeline_id = PipelineId(Uuid::new_v4());

    let cluster_nodes = cluster_provider
        .get_all_nodes()
        .values()
        .cloned()
        .collect::<Vec<_>>();

    let vertex_to_node = node_assign.assign_nodes(&execution_graph, &cluster_nodes);
    execution_graph.update_channels_with_node_mapping_and_transport(
        Some(&vertex_to_node),
        &spec.worker_runtime.transport,
        &spec.transport_overrides_queue_records(),
    );

    let master_port = gen_unique_grpc_port();
    let master_addr = format!("127.0.0.1:{}", master_port);
    let mut master_server = MasterServer::new();
    let checkpointable_tasks = execution_graph
        .get_vertices()
        .values()
        .filter(|v| operator_config_requires_checkpoint(&v.operator_config))
        .map(|v| TaskKey {
            vertex_id: v.vertex_id.as_ref().to_string(),
            task_index: v.task_index,
        })
        .collect::<Vec<_>>();
    master_server.set_checkpointable_tasks(checkpointable_tasks).await;
    master_server.start(&master_addr).await?;
    let master_snapshot_sink = master_server.snapshot_sink();

    let node_to_vertex_ids = node_to_vertex_ids(&vertex_to_node);
    let mut worker_servers = Vec::new();
    let mut worker_addrs = Vec::new();

    for node_id in node_to_vertex_ids.keys() {
        let port = gen_unique_grpc_port();
        let addr = format!("127.0.0.1:{}", port);

        let vertex_ids = node_to_vertex_ids.get(node_id).unwrap().clone();

        // Select data-plane transport backend based on actual channels:
        // - if any edge touching this worker's vertices is remote, use gRPC transport backend
        // - otherwise, prefer in-memory transport backend (single-node / single-worker fast path)
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

        // assert that the execution profile is MasterWorker
        let execution_profile = spec.execution_profile.clone().unwrap();
        let num_threads_per_task = match execution_profile {
            ExecutionProfile::MasterWorker { num_threads_per_task } => num_threads_per_task,
            _ => panic!("Execution profile must be MasterWorker"),
        };

        let mut worker_config = WorkerConfig::new(
            node_id.clone(),
            pipeline_id,
            execution_graph.clone(),
            vertex_ids
                .into_iter()
                .map(Arc::<str>::from)
                .collect(),
            num_threads_per_task,
            transport_backend_type,
        )
        .with_master_addr(master_addr.clone());
        worker_config.storage_budgets = spec.worker_runtime.storage.budgets.clone();
        worker_config.inmem_store_lock_pool_size = spec.worker_runtime.storage.inmem_store_lock_pool_size;
        worker_config.inmem_store_bucket_granularity = spec.worker_runtime.storage.inmem_store_bucket_granularity;
        worker_config.inmem_store_max_batch_size = spec.worker_runtime.storage.inmem_store_max_batch_size;
        worker_config.operator_type_storage_overrides = spec.operator_type_storage_overrides();

        let mut worker_server = WorkerServer::new(worker_config);
        worker_server.start(&addr).await?;
        worker_servers.push(worker_server);
        worker_addrs.push(addr);
    }

    let worker_addrs_for_join = worker_addrs.clone();

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let mut master = Master::new().with_snapshot_sink(master_snapshot_sink);
    master.execute(worker_addrs_for_join).await?;

    let worker_states = master.get_worker_states().await;
    let mut servers = worker_servers;
    for s in servers.iter_mut() {
        s.stop().await;
    }
    let mut ms = master_server;
    ms.stop().await;
    Ok(PipelineSnapshot::new(worker_states))
}