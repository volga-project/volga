use anyhow::Result;
use async_trait::async_trait;

use crate::cluster::node_assignment::node_to_vertex_ids;
use crate::common::test_utils::gen_unique_grpc_port;
use crate::executor::runtime_adapter::{AttemptHandle, RuntimeAdapter, StartAttemptRequest};
use crate::runtime::master::Master;
use crate::runtime::master_server::{MasterServer, TaskKey};
use crate::runtime::worker::WorkerConfig;
use crate::runtime::worker_server::WorkerServer;
use crate::runtime::operators::operator::operator_config_requires_checkpoint;
use crate::transport::transport_backend_actor::TransportBackendType;

pub struct LocalRuntimeAdapter;

impl LocalRuntimeAdapter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl RuntimeAdapter for LocalRuntimeAdapter {
    async fn start_attempt(&self, mut req: StartAttemptRequest) -> Result<AttemptHandle> {
        let cluster_nodes = req
            .cluster_provider
            .get_all_nodes()
            .values()
            .cloned()
            .collect::<Vec<_>>();

        let vertex_to_node = req.node_assign.assign_nodes(&req.execution_graph, &cluster_nodes);
        req.execution_graph
            .update_channels_with_node_mapping_and_transport(
                Some(&vertex_to_node),
                &req.worker_runtime.transport,
                &req.transport_overrides_queue_records,
            );

        let master_port = gen_unique_grpc_port();
        let master_addr = format!("127.0.0.1:{}", master_port);
        let mut master_server = MasterServer::new();
        let expected_tasks = req
            .execution_graph
            .get_vertices()
            .values()
            .filter(|v| operator_config_requires_checkpoint(&v.operator_config))
            .map(|v| TaskKey {
                vertex_id: v.vertex_id.as_ref().to_string(),
                task_index: v.task_index,
            })
            .collect::<Vec<_>>();
        master_server.set_checkpointable_tasks(expected_tasks).await;
        master_server.start(&master_addr).await?;

        let node_to_vertex_ids = node_to_vertex_ids(&vertex_to_node);
        let mut worker_servers = Vec::new();
        let mut worker_addrs = Vec::new();

        for node_id in node_to_vertex_ids.keys() {
            let port = gen_unique_grpc_port();
            let addr = format!("127.0.0.1:{}", port);

            let vertex_ids = node_to_vertex_ids.get(node_id).unwrap().clone();
            let mut worker_config = WorkerConfig::new(
                node_id.clone(),
                req.execution_ids.clone(),
                req.execution_graph.clone(),
                vertex_ids
                    .into_iter()
                    .map(|v| std::sync::Arc::<str>::from(v))
                    .collect(),
                1,
                TransportBackendType::Grpc,
            )
            .with_master_addr(master_addr.clone());
            worker_config.storage_budgets = req.worker_runtime.storage.budgets.clone();
            worker_config.inmem_store_lock_pool_size = req.worker_runtime.storage.inmem_store_lock_pool_size;
            worker_config.inmem_store_bucket_granularity = req.worker_runtime.storage.inmem_store_bucket_granularity;
            worker_config.inmem_store_max_batch_size = req.worker_runtime.storage.inmem_store_max_batch_size;
            worker_config.operator_type_storage_overrides = req.operator_type_storage_overrides.clone();

            let mut worker_server = WorkerServer::new(worker_config);
            worker_server.start(&addr).await?;
            worker_servers.push(worker_server);
            worker_addrs.push(addr);
        }

        let execution_ids = req.execution_ids.clone();
        let worker_addrs_for_join = worker_addrs.clone();
        let join = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

            let mut master = Master::new();
            master.execute(worker_addrs_for_join.clone()).await?;

            let worker_states = master.get_worker_states().await;
            let mut servers = worker_servers;
            for s in servers.iter_mut() {
                s.stop().await;
            }
            let mut ms = master_server;
            ms.stop().await;
            Ok(crate::runtime::master::PipelineState::new(worker_states))
        });

        Ok(AttemptHandle {
            execution_ids,
            master_addr,
            worker_addrs,
            join,
        })
    }
}

