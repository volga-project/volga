use anyhow::Result;
use async_trait::async_trait;
use crate::executor::placement::{
    build_task_placement_mapping,
    strategy_from_name,
    worker_to_task_ids,
    TaskPlacementStrategyName,
    WorkerEndpoint,
    WorkerTaskPlacement,
};
use crate::common::test_utils::gen_unique_grpc_port;
use crate::executor::runtime_adapter::{AttemptHandle, RuntimeAdapter, StartAttemptRequest};
use crate::runtime::master::Master;
use crate::runtime::master_server::{MasterServer, TaskKey};
use crate::runtime::worker::WorkerConfig;
use crate::runtime::worker_server::WorkerServer;
use crate::runtime::operators::operator::operator_config_requires_checkpoint;
use crate::transport::transport_backend_actor::TransportBackendType;
use crate::transport::channel::Channel;
use crate::api::spec::runtime_adapter::RuntimeAdapterKind;

pub struct LocalRuntimeAdapter;

impl LocalRuntimeAdapter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl RuntimeAdapter for LocalRuntimeAdapter {
    async fn start_attempt(&self, mut req: StartAttemptRequest) -> Result<AttemptHandle> {
        let task_placement_strategy = req
            .pipeline_spec
            .execution_profile
            .task_placement_strategy()
            .cloned()
            .unwrap_or_else(|| panic!("execution profile does not define task placement strategy"));
        if !self
            .supported_task_placement_strategies()
            .iter()
            .any(|s| s == &task_placement_strategy)
        {
            panic!(
                "LocalRuntimeAdapter does not support task placement strategy {:?}",
                task_placement_strategy
            );
        }
        let placement_strategy = strategy_from_name(&task_placement_strategy);
        let mut placements = placement_strategy.place_tasks(&req.execution_graph);
        if placements.is_empty() {
            placements.push(WorkerTaskPlacement::new(Vec::new()));
        }
        let worker_endpoints = placements
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                let worker_id = if placements.len() == 1 {
                    "local".to_string()
                } else {
                    format!("local-{idx}")
                };
                WorkerEndpoint::new(worker_id, "127.0.0.1".to_string(), 0)
            })
            .collect::<Vec<_>>();
        let task_placement_mapping =
            build_task_placement_mapping(&placements, &worker_endpoints);
        req.execution_graph
            .update_channels_with_node_mapping_and_transport(
                Some(&task_placement_mapping),
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
        let master_snapshot_sink = master_server.snapshot_sink();

        let worker_to_task_ids = worker_to_task_ids(&task_placement_mapping);
        let mut worker_servers = Vec::new();
        let mut worker_addrs = Vec::new();

        for worker_id in worker_to_task_ids.keys() {
            let port = gen_unique_grpc_port();
            let addr = format!("127.0.0.1:{}", port);

            let vertex_ids = worker_to_task_ids.get(worker_id).unwrap().clone();

            // Select data-plane transport backend based on actual channels:
            // - if any edge touching this worker's vertices is remote, use gRPC transport backend
            // - otherwise, prefer in-memory transport backend (single-node / single-worker fast path)
            let has_remote_channels = vertex_ids.iter().any(|vertex_id| {
                if let Some((in_edges, out_edges)) = req.execution_graph.get_edges_for_vertex(vertex_id) {
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

            let mut worker_config = WorkerConfig::new(
                worker_id.clone(),
                req.execution_ids.clone(),
                req.execution_graph.clone(),
                vertex_ids
                    .into_iter()
                    .map(|v| std::sync::Arc::<str>::from(v))
                    .collect(),
                1,
                transport_backend_type,
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
        let (stop_sender, stop_receiver) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            tokio::select! {
                _ = stop_receiver => {
                    let mut servers = worker_servers;
                    for s in servers.iter_mut() {
                        s.stop().await;
                    }
                    let mut ms = master_server;
                    ms.stop().await;
                    Ok(crate::runtime::observability::PipelineSnapshot::new(std::collections::HashMap::new()))
                }
                result = async {
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

                    let mut master = Master::new().with_snapshot_sink(master_snapshot_sink);
                    master.execute(worker_addrs_for_join.clone()).await?;

                    let worker_states = master.get_worker_states().await;
                    let mut servers = worker_servers;
                    for s in servers.iter_mut() {
                        s.stop().await;
                    }
                    let mut ms = master_server;
                    ms.stop().await;
                    Ok(crate::runtime::observability::PipelineSnapshot::new(worker_states))
                } => {
                    result
                }
            }
        });

        Ok(AttemptHandle {
            execution_ids,
            master_addr,
            worker_addrs,
            join,
            stop_sender: Some(stop_sender),
        })
    }

    async fn stop_attempt(&self, mut handle: AttemptHandle) -> Result<()> {
        if let Some(stop_sender) = handle.stop_sender.take() {
            let _ = stop_sender.send(());
        }
        let _ = handle.join.await;
        Ok(())
    }

    fn supported_task_placement_strategies(&self) -> &[TaskPlacementStrategyName] {
        static SUPPORTED: [TaskPlacementStrategyName; 1] = [TaskPlacementStrategyName::SingleNode];
        &SUPPORTED
    }

    fn runtime_kind(&self) -> RuntimeAdapterKind {
        RuntimeAdapterKind::Local
    }
}

