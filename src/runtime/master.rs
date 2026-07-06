use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use crate::api::PipelineSpec;
use crate::orchestrator::orchestrator::{MasterOrchestrator, WorkerNode};
use crate::orchestrator::task_assignment::{assign_tasks_with_strategy, worker_to_tasks};
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::master_checkpoint::{MasterCheckpointRegistry, TaskKey};
use crate::runtime::observability::snapshot_types::{PipelineSnapshot, WorkerSnapshot};
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::operator_config_requires_checkpoint;
use crate::runtime::worker_config_utils::WorkerInitPayload;
use crate::runtime::worker_server::worker_service::worker_service_client::WorkerServiceClient as WorkerConfigServiceClient;

// PipelineState moved to runtime/observability/snapshot_types.rs

// Include the generated protobuf client code
pub mod worker_service {
    tonic::include_proto!("worker_service");
}

use worker_service::{
    worker_service_client::WorkerServiceClient,
    GetWorkerStateRequest, StartWorkerRequest, RunWorkerTasksRequest, CloseWorkerTasksRequest, CloseWorkerRequest, TriggerCheckpointRequest,
};

/// Client for communicating with a worker server
pub struct WorkerClient {
    client: WorkerServiceClient<tonic::transport::Channel>,
    worker_ip: String,
}

impl WorkerClient {
    pub async fn connect(worker_ip: String) -> anyhow::Result<Self> {
        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY_MS: u64 = 1000;
        
        let addr = format!("http://{}", worker_ip);
        let mut last_error = None;
        
        for attempt in 0..MAX_RETRIES {
            match WorkerServiceClient::connect(addr.clone()).await {
                Ok(client) => {
                    println!("[MASTER] Successfully connected to worker {} on attempt {}", worker_ip, attempt + 1);
                    return Ok(Self {
                        client,
                        worker_ip,
                    });
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    println!("[MASTER] Connection attempt {} to worker {} failed: {}", attempt + 1, worker_ip, e);
                    
                    // Don't sleep on the last attempt
                    if attempt < MAX_RETRIES - 1 {
                        sleep(Duration::from_millis(RETRY_DELAY_MS * (attempt + 1) as u64)).await;
                    }
                }
            }
        }
        
        Err(anyhow::anyhow!(
            "Failed to connect to worker {} after {} attempts. Last error: {:?}", 
            worker_ip, 
            MAX_RETRIES, 
            last_error
        ))
    }

    pub async fn start_worker(&mut self) -> anyhow::Result<bool> {
        let request = tonic::Request::new(StartWorkerRequest {});
        let response = self.client.start_worker(request).await?;
        
        let success = response.get_ref().success;
        if !success {
            let error_msg = &response.get_ref().error_message;
            println!("[MASTER] Failed to start worker {}: {}", self.worker_ip, error_msg);
        } else {
            println!("[MASTER] Successfully started worker {}", self.worker_ip);
        }
        
        Ok(success)
    }

    pub async fn run_worker_tasks(&mut self) -> anyhow::Result<bool> {
        let request = tonic::Request::new(RunWorkerTasksRequest {});
        let response = self.client.run_worker_tasks(request).await?;
        let success = response.get_ref().success;
        if !success {
            let error_msg = &response.get_ref().error_message;
            println!("[MASTER] Failed to run tasks on worker {}: {}", self.worker_ip, error_msg);
        } else {
            println!("[MASTER] Successfully ran tasks on worker {}", self.worker_ip);
        }
        Ok(success)
    }

    pub async fn close_worker_tasks(&mut self) -> anyhow::Result<bool> {
        let request = tonic::Request::new(CloseWorkerTasksRequest {});
        let response = self.client.close_worker_tasks(request).await?;
        let success = response.get_ref().success;
        if !success {
            let error_msg = &response.get_ref().error_message;
            println!("[MASTER] Failed to close tasks on worker {}: {}", self.worker_ip, error_msg);
        } else {
            println!("[MASTER] Successfully closed tasks on worker {}", self.worker_ip);
        }
        Ok(success)
    }

    pub async fn close_worker(&mut self) -> anyhow::Result<bool> {
        let request = tonic::Request::new(CloseWorkerRequest {});
        let response = self.client.close_worker(request).await?;
        
        let success = response.get_ref().success;
        if !success {
            let error_msg = &response.get_ref().error_message;
            println!("[MASTER] Failed to close worker {}: {}", self.worker_ip, error_msg);
        } else {
            println!("[MASTER] Successfully closed worker {}", self.worker_ip);
        }
        
        Ok(success)
    }

    pub async fn get_worker_state(&mut self) -> anyhow::Result<WorkerSnapshot> {
        let request = tonic::Request::new(GetWorkerStateRequest {});
        let response = self.client.get_worker_state(request).await?;
        
        let state_bytes = &response.get_ref().worker_state_bytes;
        if state_bytes.is_empty() {
            return Err(anyhow::anyhow!("Worker state is empty"));
        }
        
        let worker_state = WorkerSnapshot::from_bytes(state_bytes)?;
        Ok(worker_state)
    }

    pub async fn trigger_checkpoint(&mut self, checkpoint_id: u64) -> anyhow::Result<bool> {
        let request = tonic::Request::new(TriggerCheckpointRequest { checkpoint_id });
        let response = self.client.trigger_checkpoint(request).await?;
        let success = response.get_ref().success;
        if !success {
            let error_msg = &response.get_ref().error_message;
            println!("[MASTER] Failed to trigger checkpoint {} on worker {}: {}", checkpoint_id, self.worker_ip, error_msg);
        }
        Ok(success)
    }
}

/// Master that orchestrates multiple worker servers
pub struct Master {
    orchestrator: Arc<dyn MasterOrchestrator>,
    config: Arc<Mutex<Option<MasterConfig>>>,
    worker_clients: Arc<Mutex<HashMap<String, WorkerClient>>>,
    registered_workers: Arc<Mutex<HashSet<String>>>,
    checkpoint_registry: Arc<Mutex<MasterCheckpointRegistry>>,
    worker_states: Arc<Mutex<HashMap<String, WorkerSnapshot>>>,
    worker_endpoints_by_id: Arc<Mutex<HashMap<String, String>>>,
    state_polling_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    running: Arc<std::sync::atomic::AtomicBool>,
    lates_pipeline_snapshot: Arc<Mutex<Option<PipelineSnapshot>>>,
}

#[derive(Clone)]
pub struct MasterConfig {
    pub spec: Option<PipelineSpec>,
    pub execution_graph: ExecutionGraph,
    pub expected_workers: usize,
}

impl MasterConfig {
    pub fn with_spec(spec: PipelineSpec, execution_graph: ExecutionGraph, expected_workers: usize) -> Self {
        Self {
            spec: Some(spec),
            execution_graph,
            expected_workers,
        }
    }

    pub fn with_graph(execution_graph: ExecutionGraph, expected_workers: usize) -> Self {
        Self {
            spec: None,
            execution_graph,
            expected_workers,
        }
    }
}

impl Master {
    pub fn new(orchestrator: Arc<dyn MasterOrchestrator>) -> Self {
        Self {
            orchestrator,
            config: Arc::new(Mutex::new(None)),
            worker_clients: Arc::new(Mutex::new(HashMap::new())),
            registered_workers: Arc::new(Mutex::new(HashSet::new())),
            checkpoint_registry: Arc::new(Mutex::new(MasterCheckpointRegistry::default())),
            worker_states: Arc::new(Mutex::new(HashMap::new())),
            worker_endpoints_by_id: Arc::new(Mutex::new(HashMap::new())),
            state_polling_handle: Arc::new(Mutex::new(None)),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            lates_pipeline_snapshot: Arc::new(Mutex::new(None)),
        }
    }

    /// Connect to all workers
    pub async fn connect_to_workers(&self, worker_ips: Vec<String>) -> anyhow::Result<()> {
        println!("[MASTER] Connecting to {} workers", worker_ips.len());
        
        let mut clients_guard = self.worker_clients.lock().await;
        
        for worker_ip in worker_ips {
            match WorkerClient::connect(worker_ip.clone()).await {
                Ok(client) => {
                    clients_guard.insert(worker_ip.clone(), client);
                    println!("[MASTER] Connected to worker {}", worker_ip);
                }
                Err(e) => {
                    println!("[MASTER] Failed to connect to worker {}: {}", worker_ip, e);
                    return Err(e);
                }
            }
        }
        
        println!("[MASTER] Successfully created clients for all workers");
        Ok(())
    }

    pub async fn register_worker(&self, worker_id: String) {
        let mut workers = self.registered_workers.lock().await;
        workers.insert(worker_id);
    }

    pub fn checkpointable_tasks_for_graph(execution_graph: &ExecutionGraph) -> Vec<TaskKey> {
        execution_graph
            .get_vertices()
            .values()
            .filter(|v| operator_config_requires_checkpoint(&v.operator_config))
            .map(|v| TaskKey {
                vertex_id: v.vertex_id.as_ref().to_string(),
                task_index: v.task_index,
            })
            .collect::<Vec<_>>()
    }

    pub async fn configure(&self, config: MasterConfig) {
        let mut registry = self.checkpoint_registry.lock().await;
        registry.expected_tasks = Self::checkpointable_tasks_for_graph(&config.execution_graph)
            .into_iter()
            .collect();
        drop(registry);

        let mut guard = self.config.lock().await;
        *guard = Some(config);
    }

    pub async fn report_checkpoint(
        &self,
        checkpoint_id: u64,
        task: TaskKey,
        blobs: Vec<(String, Vec<u8>)>,
    ) {
        let mut registry = self.checkpoint_registry.lock().await;
        registry.store.put(checkpoint_id, task.clone(), blobs);
        let expected_count = registry.expected_tasks.len();
        registry.coordinator.ack(checkpoint_id, task, expected_count);
    }

    pub async fn get_task_checkpoint(
        &self,
        checkpoint_id: u64,
        task: TaskKey,
    ) -> Vec<(String, Vec<u8>)> {
        let registry = self.checkpoint_registry.lock().await;
        registry
            .store
            .checkpoint_snapshots
            .get(&(checkpoint_id, task))
            .cloned()
            .unwrap_or_default()
    }

    pub async fn get_latest_complete_checkpoint(&self) -> Option<u64> {
        let registry = self.checkpoint_registry.lock().await;
        registry.coordinator.latest_complete()
    }

    async fn wait_for_workers_registration(
        &self,
        expected_workers: usize,
    ) -> anyhow::Result<Vec<String>> {
        println!(
            "[MASTER] Waiting for worker registration: expected={}",
            expected_workers
        );
        let mut last_count = usize::MAX;
        loop {
            let workers = self.registered_workers.lock().await;

            if workers.len() != last_count {
                println!(
                    "[MASTER] Registered workers progress: {}/{}",
                    workers.len(), expected_workers
                );
                last_count = workers.len();
            }
            if workers.len() >= expected_workers {
                let mut worker_ids = workers.iter().cloned().collect::<Vec<_>>();
                worker_ids.sort();
                println!("[MASTER] Registration complete: workers={:?}", worker_ids);
                return Ok(worker_ids);
            }
            drop(workers);
            sleep(Duration::from_millis(500)).await;
        }
    }

    async fn wait_for_worker_nodes(
        &self,
        worker_ids: &[String],
        timeout: Duration,
    ) -> anyhow::Result<HashMap<String, WorkerNode>> {
        let start = tokio::time::Instant::now();
        let mut last_missing: Vec<String> = Vec::new();
        loop {
            let nodes = self.orchestrator.get_worker_nodes().await;
            let mut missing = worker_ids
                .iter()
                .filter(|id| !nodes.contains_key(*id))
                .cloned()
                .collect::<Vec<_>>();
            missing.sort();
            if missing.is_empty() {
                return Ok(nodes);
            }
            if missing != last_missing {
                println!(
                    "[MASTER] Waiting for worker nodes discovery: missing={:?}",
                    missing
                );
                last_missing = missing.clone();
            }
            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "timed out waiting for worker nodes discovery; missing worker_ids={:?}",
                    missing
                ));
            }
            sleep(Duration::from_millis(500)).await;
        }
    }

    async fn configure_workers(
        &self,
        spec: &PipelineSpec,
        base_graph: &ExecutionGraph,
        pipeline_id: &str,
        worker_ids_sorted: &[String],
        worker_nodes: &HashMap<String, WorkerNode>,
    ) -> anyhow::Result<Vec<String>> {
        println!(
            "[MASTER] Configuring workers: count={} pipeline_id={}",
            worker_ids_sorted.len(),
            pipeline_id
        );
        let mut node_list = Vec::<WorkerNode>::new();
        for worker_id in worker_ids_sorted {
            let node = worker_nodes
                .get(worker_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Worker node not found for worker_id={}", worker_id))?;
            println!(
                "[MASTER] Worker node resolved: worker_id={} addr={}:{}",
                worker_id, node.worker_ip, node.worker_port
            );
            node_list.push(node);
        }
        let assignment_strategy = spec
            .node_assignment_strategy
            .as_ref()
            .unwrap_or_else(|| panic!("node_assignment_strategy must be set for master-worker configuration"));
        let vertex_to_node = assign_tasks_with_strategy(
            assignment_strategy,
            base_graph,
            &node_list,
        );

        let worker_to_tasks = worker_to_tasks(&vertex_to_node);
        let mut configure_futures = Vec::new();
        for worker_id in worker_ids_sorted {
            let worker_id = worker_id.clone();
            let node = worker_nodes
                .get(&worker_id)
                .ok_or_else(|| anyhow::anyhow!("Worker node not found for worker_id={}", worker_id))?;
            let worker_addr = format!("{}:{}", node.worker_ip, node.worker_port);
            let vertex_ids = worker_to_tasks
                .get(&worker_id)
                .cloned()
                .unwrap_or_default();
            println!(
                "[MASTER] Sending configure request: worker_id={} addr={} assigned_vertices={}",
                worker_id,
                worker_addr,
                vertex_ids.len()
            );
            let payload = WorkerInitPayload {
                worker_id: worker_id.clone(),
                pipeline_id: pipeline_id.to_string(),
                pipeline_spec: spec.clone(),
                vertex_ids,
                task_worker_mapping: vertex_to_node.clone(),
            };

            let future = async move {
                let init_payload_bytes = serde_json::to_vec(&payload)?;
                let mut client = WorkerConfigServiceClient::connect(format!("http://{}", worker_addr))
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to connect to worker {}: {}", worker_id, e))?;
                let req = crate::runtime::worker_server::worker_service::ConfigureWorkerRequest {
                    init_payload_bytes,
                };
                let resp = client
                    .configure_worker(tonic::Request::new(req))
                    .await
                    .map_err(|e| anyhow::anyhow!("configure_worker RPC failed for {}: {}", worker_id, e))?
                    .into_inner();
                println!(
                    "[MASTER] Received configure response: worker_id={} success={}",
                    worker_id, resp.success
                );
                if !resp.success {
                    return Err(anyhow::anyhow!(
                        "Worker {} rejected configure: {}",
                        worker_id,
                        resp.error_message
                    ));
                }
                
                Ok::<(String, String), anyhow::Error>((worker_id, resp.execution_graph_signature))
            };
            configure_futures.push(future);
        }

        let configure_results = futures::future::join_all(configure_futures).await;
        
        // verify all workers consistently compiled execution graph
        let mut expected_execution_graph_signature: Option<String> = None;
        for result in configure_results {
            let (worker_id, worker_signature) = result?;
            if worker_signature.is_empty() {
                panic!(
                    "Worker {} returned empty execution graph signature during configure",
                    worker_id
                );
            }
            if let Some(expected_signature) = &expected_execution_graph_signature {
                if expected_signature != &worker_signature {
                    panic!(
                        "Execution graph signature mismatch. Worker {} has {}, expected {}",
                        worker_id,
                        worker_signature,
                        expected_signature
                    );
                }
            } else {
                expected_execution_graph_signature = Some(worker_signature);
            }
        }

        Ok(worker_ids_sorted.to_vec())
    }

    /// Start all workers
    pub async fn start_all_workers(&self) -> anyhow::Result<()> {
        println!("[MASTER] Starting all workers");
        
        // Collect worker IPs first
        let worker_ips: Vec<String> = {
            let clients_guard = self.worker_clients.lock().await;
            clients_guard.keys().cloned().collect()
        };
        
        let mut start_futures = Vec::new();
        for worker_ip in worker_ips {
            let worker_clients = self.worker_clients.clone();
            let future = async move {
                let mut clients_guard = worker_clients.lock().await;
                if let Some(client) = clients_guard.get_mut(&worker_ip) {
                    let result = client.start_worker().await;
                    (worker_ip, result)
                } else {
                    (worker_ip, Err(anyhow::anyhow!("Worker client not found")))
                }
            };
            start_futures.push(future);
        }
        
        let results = futures::future::join_all(start_futures).await;
        
        for (worker_ip, result) in results {
            match result {
                Ok(success) => {
                    if !success {
                        return Err(anyhow::anyhow!("Failed to start worker {}", worker_ip));
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Error starting worker {}: {}", worker_ip, e));
                }
            }
        }
        
        println!("[MASTER] All workers started successfully");
        Ok(())
    }

    /// Start periodic state polling
    pub async fn start_state_polling(&self) {
        println!("[MASTER] Starting state polling task");
        
        let worker_clients = self.worker_clients.clone();
        let worker_states = self.worker_states.clone();
        let worker_endpoints_by_id = self.worker_endpoints_by_id.clone();
        let running = self.running.clone();
        let lates_pipeline_snapshot = self.lates_pipeline_snapshot.clone();
        
        let handle = tokio::spawn(async move {
            while running.load(std::sync::atomic::Ordering::Relaxed) {
                let mut new_states_by_worker_id = HashMap::new();
                let mut new_endpoints_by_worker_id = HashMap::new();
                
                let mut clients_guard = worker_clients.lock().await;
                for (worker_ip, client) in clients_guard.iter_mut() {
                    match client.get_worker_state().await {
                        Ok(state) => {
                            new_endpoints_by_worker_id.insert(state.worker_id.clone(), worker_ip.clone());
                            new_states_by_worker_id.insert(state.worker_id.clone(), state);
                        }
                        Err(e) => {
                            println!("[MASTER] Failed to get state from worker {}: {}", worker_ip, e);
                        }
                    }
                }
                drop(clients_guard);
                
                let snapshot_states = new_states_by_worker_id.clone();

                // Update shared state
                {
                    let mut states_guard = worker_states.lock().await;
                    *states_guard = new_states_by_worker_id;
                }
                {
                    let mut endpoints_guard = worker_endpoints_by_id.lock().await;
                    *endpoints_guard = new_endpoints_by_worker_id;
                }

                let snapshot = PipelineSnapshot::new(snapshot_states.clone());
                let mut guard = lates_pipeline_snapshot.lock().await;
                *guard = Some(snapshot);
                
                sleep(Duration::from_millis(100)).await;
            }
        });
        
        let mut handle_guard = self.state_polling_handle.lock().await;
        *handle_guard = Some(handle);
    }

    /// Wait for all workers to have tasks in the specified status
    pub async fn wait_for_all_tasks_status(&self, target_status: StreamTaskStatus, timeout_duration: Option<Duration>) -> anyhow::Result<()> {
        println!("[MASTER] Waiting for all workers to have tasks in status: {:?}", target_status);
        
        let start_time = std::time::Instant::now();
        // let timeout_duration = Duration::from_secs(5);
        
        loop {
            // Check timeout
            if timeout_duration.is_some() && start_time.elapsed() > timeout_duration.unwrap() {
                return Err(anyhow::anyhow!("Timeout waiting for all tasks to be {:?} after {:?}", target_status, timeout_duration));
            }
            
            let all_ready = {
                let states_guard = self.worker_states.lock().await;
                if states_guard.is_empty() {
                    false
                } else {
                    states_guard.values().all(|state| state.all_tasks_have_status(target_status))
                }
            };
            
            if all_ready {
                println!("[MASTER] All workers have tasks in status: {:?}", target_status);
                break;
            }
            
            sleep(Duration::from_millis(100)).await;
        }
        
        Ok(())
    }


    /// Start all tasks on all workers
    pub async fn run_all_tasks(&self) -> anyhow::Result<()> {
        println!("[MASTER] Running all tasks on all workers");
        let worker_ips: Vec<String> = {
            let clients_guard = self.worker_clients.lock().await;
            clients_guard.keys().cloned().collect()
        };
        let mut run_futures = Vec::new();
        for worker_ip in worker_ips {
            let worker_clients = self.worker_clients.clone();
            let future = async move {
                let mut clients_guard = worker_clients.lock().await;
                if let Some(client) = clients_guard.get_mut(&worker_ip) {
                    let result = client.run_worker_tasks().await;
                    (worker_ip, result)
                } else {
                    (worker_ip, Err(anyhow::anyhow!("Worker client not found")))
                }
            };
            run_futures.push(future);
        }
        let results = futures::future::join_all(run_futures).await;
        for (worker_ip, result) in results {
            match result {
                Ok(success) => {
                    if !success {
                        return Err(anyhow::anyhow!("Failed to run tasks on worker {}", worker_ip));
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Error running tasks on worker {}: {}", worker_ip, e));
                }
            }
        }
        println!("[MASTER] All tasks started successfully");
        Ok(())
    }

    /// Close all tasks on all workers
    pub async fn close_all_tasks(&self) -> anyhow::Result<()> {
        println!("[MASTER] Closing all tasks on all workers");
        
        // Collect worker IPs first
        let worker_ips: Vec<String> = {
            let clients_guard = self.worker_clients.lock().await;
            clients_guard.keys().cloned().collect()
        };
        
        let mut close_futures = Vec::new();
        for worker_ip in worker_ips {
            let worker_clients = self.worker_clients.clone();
            let future = async move {
                let mut clients_guard = worker_clients.lock().await;
                if let Some(client) = clients_guard.get_mut(&worker_ip) {
                    let result = client.close_worker_tasks().await;
                    (worker_ip, result)
                } else {
                    (worker_ip, Err(anyhow::anyhow!("Worker client not found")))
                }
            };
            close_futures.push(future);
        }
        
        let results = futures::future::join_all(close_futures).await;
        
        for (worker_ip, result) in results {
            match result {
                Ok(success) => {
                    if !success {
                        return Err(anyhow::anyhow!("Failed to close tasks on worker {}", worker_ip));
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Error closing tasks on worker {}: {}", worker_ip, e));
                }
            }
        }
        
        println!("[MASTER] All tasks closed successfully");
        Ok(())
    }

    /// Close all workers
    pub async fn close_all_workers(&self) -> anyhow::Result<()> {
        println!("[MASTER] Closing all workers");
        
        // Collect worker IPs first
        let worker_ips: Vec<String> = {
            let clients_guard = self.worker_clients.lock().await;
            clients_guard.keys().cloned().collect()
        };
        
        let mut close_futures = Vec::new();
        for worker_ip in worker_ips {
            let worker_clients = self.worker_clients.clone();
            let future = async move {
                let mut clients_guard = worker_clients.lock().await;
                if let Some(client) = clients_guard.get_mut(&worker_ip) {
                    let result = client.close_worker().await;
                    (worker_ip, result)
                } else {
                    (worker_ip, Err(anyhow::anyhow!("Worker client not found")))
                }
            };
            close_futures.push(future);
        }
        
        let results = futures::future::join_all(close_futures).await;
        
        for (worker_ip, result) in results {
            match result {
                Ok(success) => {
                    if !success {
                        return Err(anyhow::anyhow!("Failed to close worker {}", worker_ip));
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Error closing worker {}: {}", worker_ip, e));
                }
            }
        }
        
        println!("[MASTER] All workers closed successfully");
        Ok(())
    }

    /// Execute the complete pipeline lifecycle
    pub async fn execute(&self) -> anyhow::Result<()> {
        println!("[MASTER] Starting execution lifecycle");
        let (spec, execution_graph, expected_workers) = {
            let config = self.config.lock().await;
            let config = config.clone().ok_or_else(|| {
                anyhow::anyhow!("Master is not configured: call configure before execute")
            })?;
            let spec = config.spec.ok_or_else(|| {
                anyhow::anyhow!("Master is configured without spec: provide spec in MasterConfig before execute")
            })?;
            (spec, config.execution_graph, config.expected_workers)
        };
        
        self.running.store(true, std::sync::atomic::Ordering::Relaxed);
        
        // 1. Wait for workers to register.
        let worker_ids_sorted = self.wait_for_workers_registration(expected_workers).await?;
        let pipeline_id = self.orchestrator.get_pipeline_id().await;
        let worker_nodes = self
            .wait_for_worker_nodes(&worker_ids_sorted, Duration::from_secs(30))
            .await?;

        // 2. Configure workers from pipeline spec.
        let worker_ids_sorted = self
            .configure_workers(&spec, &execution_graph, &pipeline_id, &worker_ids_sorted, &worker_nodes)
            .await?;
        let worker_ips = worker_ids_sorted
            .iter()
            .map(|id| {
                let node = worker_nodes
                    .get(id)
                    .unwrap_or_else(|| panic!("worker node not found for worker_id={}", id));
                format!("{}:{}", node.worker_ip, node.worker_port)
            })
            .collect::<Vec<_>>();

        // 3. Master connects to all workers.
        self.connect_to_workers(worker_ips).await?;
        
        // 4. Master starts all workers.
        self.start_all_workers().await?;
        
        // 5. Start state polling to monitor worker states.
        self.start_state_polling().await;
        
        // 6. Master waits for all tasks on all workers to be opened.
        self.wait_for_all_tasks_status(StreamTaskStatus::Opened, Some(Duration::from_secs(30))).await?;
        
        // 7. Master runs all tasks on all workers.
        self.run_all_tasks().await?;
        
        // 8. Master waits for all tasks on all workers to be finished.
        self.wait_for_all_tasks_status(StreamTaskStatus::Finished, None).await?;
        
        // 9. Master closes all tasks on all workers.
        self.close_all_tasks().await?;
        
        // 10. Master waits for all tasks on all workers to be closed.
        self.wait_for_all_tasks_status(StreamTaskStatus::Closed, Some(Duration::from_secs(30))).await?;
        
        // 11. Master closes workers.
        self.close_all_workers().await?;
        
        // Stop state polling
        self.running.store(false, std::sync::atomic::Ordering::Relaxed);
        let mut handle_guard = self.state_polling_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            let _ = handle.await;
        }
        
        println!("[MASTER] Execution completed successfully");
        Ok(())
    }

    /// Get current worker states
    pub async fn get_worker_states(&self) -> HashMap<String, WorkerSnapshot> {
        let states_guard = self.worker_states.lock().await;
        states_guard.clone()
    }

    pub async fn get_latest_pipeline_snapshot(&self) -> Option<PipelineSnapshot> {
        let guard = self.lates_pipeline_snapshot.lock().await;
        guard.clone()
    }
} 
