use crate::runtime::{
    execution_graph::ExecutionGraph, functions::source::request_source::{extract_request_source_config, RequestSourceProcessor}, metrics::WorkerMetrics, runtime_context::RuntimeContext, stream_task::{StreamTask, StreamTaskStatus}, stream_task_actor::{StreamTaskActor, StreamTaskMessage}, state::OperatorStates
};
use crate::runtime::VertexId;
use crate::runtime::metrics::{MetricsLabels, StreamTaskMetrics};
use crate::transport::{transport_backend_actor::TransportBackendType, GrpcTransportBackend, InMemoryTransportBackend, TransportBackend};
use crate::transport::transport_backend_actor::{TransportBackendActor, TransportBackendActorMessage};
use std::{collections::HashMap};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use kameo::{spawn, prelude::ActorRef};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::time::{sleep, Duration};
use futures::future::join_all;
use serde::{Serialize, Deserialize};
use anyhow::Result;
use crate::runtime::operators::operator::OperatorType;
use crate::runtime::operators::operator::operator_config_requires_checkpoint;
use serde_json::Value;
use crate::storage::{StorageBudgetConfig, WorkerStorageContext};
use crate::storage::batch_store::{BatchStore, InMemBatchStore};
use crate::runtime::operators::window::TimeGranularity;
use crate::control_plane::types::ExecutionIds;

use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub execution_ids: ExecutionIds,
    pub graph: ExecutionGraph,
    pub vertex_ids: Vec<VertexId>,
    pub num_threads_per_task: usize,
    pub transport_backend_type: TransportBackendType,
    pub master_addr: Option<String>,
    pub restore_checkpoint_id: Option<u64>,
    pub storage_budgets: StorageBudgetConfig,
    pub inmem_store_lock_pool_size: usize,
    pub inmem_store_bucket_granularity: TimeGranularity,
    pub inmem_store_max_batch_size: usize,
}

impl WorkerConfig {
    pub fn new(
        worker_id: String,
        execution_ids: ExecutionIds,
        graph: ExecutionGraph,
        vertex_ids: Vec<VertexId>,
        num_threads_per_task: usize,
        transport_backend_type: TransportBackendType,
    ) -> Self {
        Self {
            worker_id,
            execution_ids,
            graph,
            vertex_ids,
            num_threads_per_task,
            transport_backend_type,
            master_addr: None,
            restore_checkpoint_id: None,
            storage_budgets: StorageBudgetConfig::default(),
            inmem_store_lock_pool_size: 4096,
            inmem_store_bucket_granularity: TimeGranularity::Seconds(1),
            inmem_store_max_batch_size: 1024,
        }
    }

    pub fn with_master_addr(mut self, master_addr: String) -> Self {
        self.master_addr = Some(master_addr);
        self
    }

    pub fn with_restore_checkpoint_id(mut self, checkpoint_id: u64) -> Self {
        self.restore_checkpoint_id = Some(checkpoint_id);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerState {
    pub execution_ids: ExecutionIds,
    pub worker_id: String,
    pub task_statuses: HashMap<VertexId, StreamTaskStatus>,
    pub worker_metrics: Option<WorkerMetrics>,
}

impl WorkerState {
    pub fn new(worker_id: String, execution_ids: ExecutionIds) -> Self {
        Self {
            execution_ids,
            worker_id,
            task_statuses: HashMap::new(),
            worker_metrics: None,
        }
    }

    pub fn all_tasks_have_status(&self, status: StreamTaskStatus) -> bool {
        self.task_statuses.values().all(|_status| *_status == status)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }

    pub fn set_metrics(&mut self, worker_metrics: WorkerMetrics) {
        self.worker_metrics = Some(worker_metrics);
    }
}

pub struct Worker {
    worker_id: String,
    execution_ids: ExecutionIds,
    graph: ExecutionGraph,
    vertex_ids: Vec<VertexId>,
    transport_backend_type: TransportBackendType,
    master_addr: Option<String>,
    restore_checkpoint_id: Option<u64>,
    storage_budgets: StorageBudgetConfig,
    inmem_store_lock_pool_size: usize,
    inmem_store_bucket_granularity: TimeGranularity,
    inmem_store_max_batch_size: usize,
    task_actors: HashMap<VertexId, ActorRef<StreamTaskActor>>,
    backend_actor: Option<ActorRef<TransportBackendActor>>,
    task_runtimes: HashMap<VertexId, Runtime>,
    transport_backend_runtime: Option<Runtime>,
    worker_state: Arc<tokio::sync::Mutex<WorkerState>>,
    operator_states: Arc<OperatorStates>,
    running: Arc<AtomicBool>,
    tasks_state_polling_handle: Option<tokio::task::JoinHandle<()>>,

    // if RequestSource/Sink is configured, run processor - shared between tasks
    request_source_processor: Option<RequestSourceProcessor>,
    request_source_processor_runtime: Option<Runtime>,

    // TODO separate backend runtime for request mode channels
}

impl Worker {
    pub fn new(config: WorkerConfig) -> Self {
        let mut task_runtimes = HashMap::new();
        for vertex_id in &config.vertex_ids {
            let task_runtime = Builder::new_multi_thread()
                .worker_threads(config.num_threads_per_task)
                .enable_all()
                .thread_name(format!("task-runtime-{}", vertex_id))
                .build().unwrap();

            task_runtimes.insert(vertex_id.clone(), task_runtime);
        }

        // Set request_source_processor_runtime if needed
        let request_source_config = extract_request_source_config(&config.graph);
        let request_source_processor_runtime = if request_source_config.is_some() {
            Some(Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .thread_name("request-source-processor-runtime")
                .build().unwrap())
        } else {
            None
        };

        Self {
            worker_id: config.worker_id.clone(),
            execution_ids: config.execution_ids.clone(),
            graph: config.graph,
            vertex_ids: config.vertex_ids.clone(),
            task_actors: HashMap::new(),
            transport_backend_type: config.transport_backend_type,
            master_addr: config.master_addr.clone(),
            restore_checkpoint_id: config.restore_checkpoint_id,
            storage_budgets: config.storage_budgets,
            inmem_store_lock_pool_size: config.inmem_store_lock_pool_size,
            inmem_store_bucket_granularity: config.inmem_store_bucket_granularity,
            inmem_store_max_batch_size: config.inmem_store_max_batch_size,
            backend_actor: None,
            task_runtimes,
            transport_backend_runtime: Some(
                Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .thread_name("transport-backend-runtime")
                .build().unwrap()),
            request_source_processor_runtime,
            worker_state: Arc::new(tokio::sync::Mutex::new(WorkerState::new(
                config.worker_id,
                config.execution_ids,
            ))),
            operator_states: Arc::new(OperatorStates::new()),
            running: Arc::new(AtomicBool::new(false)),
            tasks_state_polling_handle: None,
            request_source_processor: None
        }
    }

    async fn poll_and_update_tasks_state(
        worker_id: String,
        execution_ids: ExecutionIds,
        task_runtimes: HashMap<VertexId, Handle>,
        task_actors: HashMap<VertexId, ActorRef<StreamTaskActor>>,
        graph: ExecutionGraph,
        state: Arc<tokio::sync::Mutex<WorkerState>>,
        state_update_sender: Option<mpsc::Sender<WorkerState>>
    ) {
        let mut task_futures = Vec::new();
        for (vertex_id, runtime) in &task_runtimes {
            let vertex_id = vertex_id.clone();
            let task_ref = task_actors.get(&vertex_id).unwrap().clone();

            let task_ref = task_ref.clone();
            let fut = runtime.spawn(async move {
                (vertex_id.clone(), task_ref.ask(StreamTaskMessage::GetState).await.unwrap())
            });
            task_futures.push(fut);
        }
        let task_results = join_all(task_futures).await;

        let mut task_statuses: HashMap<VertexId, StreamTaskStatus> = HashMap::new();
        let mut task_metrics: HashMap<VertexId, StreamTaskMetrics> = HashMap::new();

        for result in task_results {
            if let Ok((vertex_id, state)) = result {
                task_statuses.insert(vertex_id.clone(), state.status.clone());
                task_metrics.insert(vertex_id.clone(), state.metrics.clone());
            }
        }

        let task_metrics_str: HashMap<String, StreamTaskMetrics> = task_metrics
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v))
            .collect();
        let worker_metrics = WorkerMetrics::new(worker_id, execution_ids, task_metrics_str, &graph);
        worker_metrics.record();

        // Update shared WorkerState
        {
            let mut state_guard = state.lock().await;
            state_guard.task_statuses = task_statuses;
            state_guard.set_metrics(worker_metrics);
            // state_guard.worker_metrics.set_tasks_metrics(task_metrics.clone());
            if state_update_sender.is_some() {
                state_update_sender.unwrap().send(state_guard.clone()).await.unwrap();
            }
        } // Release lock before sleep
    }

    pub async fn wait_for_all_tasks_status(
        state: Arc<tokio::sync::Mutex<WorkerState>>,
        running: Arc<AtomicBool>,
        target_status: StreamTaskStatus,
        timeout_s: Option<u64>
    ) {
        println!("[WORKER] Waiting for all tasks to be {:?}", target_status);
        
        let start_time = std::time::Instant::now();
        
        while running.load(Ordering::SeqCst) {
            // Check timeout if needed
            if timeout_s.is_some() && start_time.elapsed() > Duration::from_secs(timeout_s.unwrap()) {
                // Print statuses that are different from expected
                let state_guard = state.lock().await;
                let mut different_statuses = Vec::new();
                for (task_id, status) in &state_guard.task_statuses {
                    if *status != target_status {
                        different_statuses.push((task_id.clone(), *status));
                    }
                }
                
                if !different_statuses.is_empty() {
                    println!("[WORKER] Timeout waiting for {:?}. Tasks with different statuses:", target_status);
                    for (task_id, status) in different_statuses {
                        println!("  - {}: {:?}", task_id, status);
                    }
                }
                
                panic!("Timeout waiting for all tasks to be {:?} after {:?}s", target_status, timeout_s.unwrap());
            }
            
            let all_ready = {
                let state_guard = state.lock().await;
                state_guard.all_tasks_have_status(target_status)
            };
            
            if all_ready {
                println!("[WORKER] All tasks are {:?}", target_status);
                break;
            }
            
            // TODO configure this
            sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn spawn_actors(&mut self) {
        println!("[WORKER] Spawning actors");

        // Worker-level shared storage context (used by window/request operators).
        // Backend selection (SlateDB) will be added later; for now use InMemBatchStore.
        let budgets = self.storage_budgets.clone();
        let store = Arc::new(InMemBatchStore::new(
            self.inmem_store_lock_pool_size,
            self.inmem_store_bucket_granularity,
            self.inmem_store_max_batch_size,
        )) as Arc<dyn BatchStore>;
        let worker_storage = WorkerStorageContext::new(store, budgets).expect("worker storage ctx");

        let mut backend: Box<dyn TransportBackend> = match self.transport_backend_type {
            TransportBackendType::Grpc => Box::new(GrpcTransportBackend::new()),
            TransportBackendType::InMemory => Box::new(InMemoryTransportBackend::new()),
        };
        let mut transport_client_configs = backend.init_channels(&self.graph, self.vertex_ids.clone());

        let backend_actor_task = self.transport_backend_runtime.as_ref().unwrap().spawn(async{
            return spawn(TransportBackendActor::new(backend))
        });
        let backend_actor_ref = backend_actor_task.await.unwrap();
        self.backend_actor = Some(backend_actor_ref);

        let vertex_ids = self.vertex_ids.clone();
        for vertex_id in &vertex_ids {
            let vertex = self
                .graph
                .get_vertex(vertex_id.as_ref())
                .expect("Vertex should exist");
            let task_runtime = self.task_runtimes.get(vertex_id).expect("Task runtime should exist");

            // Create runtime context for the vertex
            let mut runtime_context = RuntimeContext::new(
                vertex_id.clone(),
                vertex.task_index,
                vertex.parallelism,
                {
                    let mut cfg = HashMap::<String, Value>::new();
                    if let Some(master_addr) = &self.master_addr {
                        cfg.insert("master_addr".to_string(), Value::String(master_addr.clone()));
                    }
                    if let Some(restore_checkpoint_id) = self.restore_checkpoint_id {
                        cfg.insert("restore_checkpoint_id".to_string(), Value::from(restore_checkpoint_id));
                    }
                    cfg.insert("pipeline_spec_id".to_string(), Value::String(self.execution_ids.pipeline_spec_id.0.to_string()));
                    cfg.insert("pipeline_id".to_string(), Value::String(self.execution_ids.pipeline_id.0.to_string()));
                    cfg.insert("attempt_id".to_string(), Value::from(self.execution_ids.attempt_id.0));
                    cfg.insert("worker_id".to_string(), Value::String(self.worker_id.clone()));
                    Some(cfg)
                },
                Some(self.operator_states.clone()),
                Some(self.graph.clone()),
            );
            if let Some(request_source_processor) = &self.request_source_processor {
                runtime_context.set_request_sink_source_request_receiver(request_source_processor.get_shared_request_receiver().clone());
                runtime_context.set_request_sink_source_response_sender(request_source_processor.get_response_sender());
            }
            runtime_context.set_worker_storage_context(worker_storage.clone());

            // Create the task and its actor in the task's runtime
            let mut transport_cfg = transport_client_configs.remove(vertex_id).unwrap();
            transport_cfg.set_metrics_labels(MetricsLabels {
                pipeline_spec_id: self.execution_ids.pipeline_spec_id.0.to_string(),
                pipeline_id: self.execution_ids.pipeline_id.0.to_string(),
                attempt_id: self.execution_ids.attempt_id.0,
                worker_id: self.worker_id.clone(),
            });
            let task = StreamTask::new(
                vertex_id.clone(),
                vertex.operator_config.clone(),
                transport_cfg,
                runtime_context,
                self.graph.clone(),
            );
            let task_actor = StreamTaskActor::new(task);
            let task_ref = task_runtime.spawn(async{
                return spawn(task_actor)
            });
            let task_actor_ref = task_ref.await.unwrap();
            self.task_actors.insert(vertex_id.clone(), task_actor_ref);
        }

        println!("[WORKER] Actors spawned");
    }

    async fn start_tasks(
        &mut self, 
        state_updates_sender: Option<mpsc::Sender<WorkerState>>
    ) {
        println!("[WORKER] Starting tasks");

        // Start all tasks
        let mut start_futures = Vec::new();
        for (vertex_id, runtime) in &self.task_runtimes {
            let vertex_id = vertex_id.clone();
            let task_ref = self.task_actors.get(&vertex_id).unwrap().clone();

            let task_ref = task_ref.clone();
            let fut = runtime.spawn(async move {
                if let Err(e) = task_ref.ask(crate::runtime::stream_task_actor::StreamTaskMessage::Start).await {
                    eprintln!("Error starting task {}: {}", vertex_id, e);
                }
            });
            start_futures.push(fut);
        }
        
        for f in start_futures {
            let _ = f.await.unwrap();
        }

        // Start tasks state polling loop
        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let task_actors = self.task_actors.clone();
        let graph = self.graph.clone();
        let state = self.worker_state.clone();
        let worker_id = self.worker_id.clone();
        let execution_ids = self.execution_ids.clone();
        
        let task_runtime_handles: HashMap<VertexId, Handle> = self.task_runtimes.iter()
            .map(|(k, v)| (k.clone(), v.handle().clone()))
            .collect();
        
        let polling_handle = tokio::spawn(async move { 
            while running.load(Ordering::SeqCst) {
                Self::poll_and_update_tasks_state(
                    worker_id.clone(),
                    execution_ids.clone(),
                    task_runtime_handles.clone(),
                    task_actors.clone(),
                    graph.clone(),
                    state.clone(),
                    state_updates_sender.clone(),
                ).await;
                sleep(Duration::from_millis(100)).await;
            }
            // final poll
            Self::poll_and_update_tasks_state(
                worker_id.clone(),
                execution_ids.clone(),
                task_runtime_handles,
                task_actors,
                graph,
                state,
                state_updates_sender.clone(),
            ).await;
        });

        self.tasks_state_polling_handle = Some(polling_handle);

        println!("[WORKER] Started all tasks");
    }

    async fn start_transport_backend(&mut self) {
        let backend_actor_ref = self.backend_actor.as_ref().unwrap().clone();
        self.transport_backend_runtime.as_ref().unwrap().spawn(async move {
            backend_actor_ref.ask(TransportBackendActorMessage::Start).await.unwrap()
        }).await.unwrap();
    }

    async fn start_request_source_processor_if_needed(&mut self) {
        if let Some(runtime) = &self.request_source_processor_runtime {
            let request_source_config = extract_request_source_config(&self.graph).expect("request_source_config should be set");
            println!("[WORKER] Starting request source processor");
            
            let mut processor = RequestSourceProcessor::new(request_source_config);
            
            // Start the processor in its dedicated runtime
            let (processor, start_result) = runtime.spawn(async move {
                let result = processor.start().await;
                (processor, result)
            }).await.unwrap();
            
            self.request_source_processor = Some(processor);
            
            if let Err(e) = start_result {
                panic!("Failed to start request source processor: {}", e);
            }
        }
    }

    async fn stop_request_source_processor_if_needed(&mut self) {
        if let Some(mut processor) = self.request_source_processor.take() {
            let runtime = self.request_source_processor_runtime.as_ref().expect("request_source_processor_runtime should be set");
            println!("[WORKER] Stopping request source processor");
            
            let stop_result = runtime.spawn(async move {
                processor.stop().await
            }).await.unwrap();
            
            if let Err(e) = stop_result {
                panic!("Failed to stop request source processor: {}", e);
            }
            
            println!("[WORKER] Request source processor stopped");
        }
    }

    async fn send_signal_to_task_actors(&mut self, signal: StreamTaskMessage) {
        println!("[WORKER] Sending {:?} signal to all task actors", signal);
        
        for (vertex_id, runtime) in &self.task_runtimes {
            let vertex_id = vertex_id.clone();
            let task_ref = self.task_actors.get(&vertex_id).unwrap().clone();
            let signal_clone = signal.clone();
            let signal_for_error = signal.clone();
            let fut = runtime.spawn(async move {
                if let Err(e) = task_ref.ask(signal_clone).await {
                    eprintln!("Error sending {:?} signal to task {}: {}", signal_for_error, vertex_id, e);
                }
            });
            let _ = fut.await;
        }

        println!("[WORKER] {:?} signal sent to all task actors", signal);
    }

    pub async fn get_state(&self) -> WorkerState {
        if self.running.load(Ordering::SeqCst) {
        let task_runtime_handles: HashMap<VertexId, Handle> = self.task_runtimes.iter()
                .map(|(k, v)| (k.clone(), v.handle().clone()))
                .collect();
            let task_actors = self.task_actors.clone();
            let graph = self.graph.clone();
            let state = self.worker_state.clone();
            Self::poll_and_update_tasks_state(
                self.worker_id.clone(),
                self.execution_ids.clone(),
                task_runtime_handles,
                task_actors,
                graph,
                state,
                None,
            ).await;
        }
        self.worker_state.lock().await.clone()
    }

    pub fn operator_states(&self) -> Arc<OperatorStates> {
        self.operator_states.clone()
    }

    async fn cleanup(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.tasks_state_polling_handle.take() {
            if let Err(e) = handle.await {
                eprintln!("Polling task failed: {:?}", e);
            }
        }

        // Shutdown transport backend runtime
        if let Some(runtime) = self.transport_backend_runtime.take() {
            runtime.shutdown_background();
        }

        // Shutdown request source processor runtime
        if let Some(runtime) = self.request_source_processor_runtime.take() {
            runtime.shutdown_background();
        }

        // Shutdown task runtimes
        for (_, runtime) in self.task_runtimes.drain() {
            runtime.shutdown_background();
        }

        // Clear actor references
        // TODO destroy actors
        self.backend_actor = None;
        self.task_actors.clear();

        println!("[WORKER] Cleanup completed");
    }

    // control functions
    pub async fn start(&mut self) {
        self.start_request_source_processor_if_needed().await;
        self.spawn_actors().await;
        self.start_tasks(None).await;
    }

    pub async fn signal_tasks_run(&mut self) {
        self.start_transport_backend().await;
        self.send_signal_to_task_actors(crate::runtime::stream_task_actor::StreamTaskMessage::Run).await;
    }

    pub async fn signal_tasks_close(&mut self) {
        self.send_signal_to_task_actors(crate::runtime::stream_task_actor::StreamTaskMessage::Close).await;
    }

    pub async fn trigger_checkpoint(&mut self, checkpoint_id: u64) {
        println!("[WORKER] Triggering checkpoint {} on source tasks", checkpoint_id);
        for (vertex_id, runtime) in &self.task_runtimes {
            let vertex_id = vertex_id.clone();
            let vertex_type = self.graph.get_vertex_type(vertex_id.as_ref());
            if vertex_type != OperatorType::Source && vertex_type != OperatorType::ChainedSourceSink {
                continue;
            }

            // Only trigger sources that actually participate in checkpointing.
            if let Some(v) = self.graph.get_vertices().get(vertex_id.as_ref()) {
                if !operator_config_requires_checkpoint(&v.operator_config) {
                    continue;
                }
            }

            let task_ref = self.task_actors.get(&vertex_id).unwrap().clone();
            let fut = runtime.spawn(async move {
                let _ = task_ref.ask(StreamTaskMessage::TriggerCheckpoint(checkpoint_id)).await;
            });
            let _ = fut.await;
        }
    }

    pub async fn close(&mut self) {
        self.stop_request_source_processor_if_needed().await;
        self.cleanup().await;
    }

    // Test-only "crash": abort runtimes without graceful close.
    pub async fn kill_for_testing(&mut self) {
        self.running.store(false, Ordering::SeqCst);

        // Best-effort: stop request source processor (if any) to avoid background noise in tests.
        self.stop_request_source_processor_if_needed().await;

        // Drop actors + abort task runtimes quickly.
        self.task_actors.clear();
        self.backend_actor = None;

        for (_id, rt) in self.task_runtimes.drain() {
            // Important: do not use shutdown_timeout() here (it blocks), as this is called from async tests.
            rt.shutdown_background();
        }
        if let Some(rt) = self.transport_backend_runtime.take() {
            rt.shutdown_background();
        }
    }

    // This should only be used for testing - simulates worker execution
    // In real environment master is used to coordinate worker lifecycle
    pub async fn execute_worker_lifecycle_for_testing(
        &mut self,
    ) {
        self._execute_worker_lifecycle_for_testing(None).await
    }

    pub async fn execute_worker_lifecycle_for_testing_with_state_updates(
        &mut self,
        state_udpates_sender: mpsc::Sender<WorkerState>
    ) {
        self._execute_worker_lifecycle_for_testing(Some(state_udpates_sender)).await
    }

    async fn _execute_worker_lifecycle_for_testing(
        &mut self,
        state_updates_sender: Option<mpsc::Sender<WorkerState>>
    ) {
        println!("[WORKER] Starting worker execution");
        
        if state_updates_sender.is_none() {
            self.start().await;
        } else {
            self.start_request_source_processor_if_needed().await;
            self.spawn_actors().await;
            self.start_tasks(state_updates_sender).await;
        }

        println!("[WORKER] Worker started, waiting for all tasks to be opened");

        Self::wait_for_all_tasks_status(
            self.worker_state.clone(),
            self.running.clone(),
            StreamTaskStatus::Opened,
            Some(10)
        ).await;

        println!("[WORKER] All tasks opened, running tasks");

        self.signal_tasks_run().await;

        println!("[WORKER] Tasks running, waiting for all tasks to be finished");

        // Wait for tasks to finish
        Self::wait_for_all_tasks_status(
            self.worker_state.clone(),
            self.running.clone(),
            StreamTaskStatus::Finished,
            None
        ).await;
        
        println!("[WORKER] All tasks finished, sending close signal");
        // Send close signal
        self.signal_tasks_close().await;

        println!("[WORKER] Waiting for all tasks to be closed");

        // Wait for tasks to be closed
        Self::wait_for_all_tasks_status(
            self.worker_state.clone(),
            self.running.clone(),
            StreamTaskStatus::Closed,
            Some(10)
        ).await;

        println!("[WORKER] All tasks closed, cleaning up");
        
        // Cleanup
        self.close().await;

        println!("[WORKER] Worker execution completed");
    }
}