use crate::{common::types::PipelineId, runtime::{
    execution_graph::ExecutionGraph, functions::source::request_source::{RequestSourceProcessor, extract_request_source_config}, metrics::emit_poll_derived_gauges, observability::snapshot_types::{TaskOperatorMetrics, WorkerSnapshot}, runtime_context::RuntimeContext, state::OperatorStates, stream_task::StreamTask, stream_task_actor::{StreamTaskActor, StreamTaskMessage}
}};
use crate::runtime::VertexId;
use crate::runtime::health::WorkerHealth;
use crate::runtime::metrics::{MetricsLabels, TaskMetrics, WorkerAggregateMetrics};
use crate::runtime::observability::snapshot_types::StreamTaskStatus;
use crate::transport::{transport_backend_actor::TransportBackendType, TransportBackend, TransportBackendTrait};
use crate::transport::transport_backend_actor::{TransportBackendActor, TransportBackendActorMessage};
use std::{collections::HashMap};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use kameo::{spawn, prelude::ActorRef};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::time::{sleep, Duration};
use futures::future::join_all;
// (serde/Result imports removed; this module does not serialize Worker directly)
use crate::runtime::operators::operator::OperatorType;
use crate::runtime::operators::operator::operator_config_requires_checkpoint;
use serde_json::Value;
use crate::runtime::operators::source::SourceHandles;
use crate::storage::{InMemSortedKV, SortedKV};
use crate::runtime::operators::window::store::StateNamespace;

use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub pipeline_id: PipelineId,
    pub execution_attempt_id: u64,
    pub graph: ExecutionGraph,
    pub vertex_ids: Vec<VertexId>,
    pub num_threads_per_task: usize,
    pub transport_backend_type: TransportBackendType,
    pub master_addr: Option<String>,
    pub restore_checkpoint_id: Option<u64>,
    pub window_state_namespace: String,
}

impl WorkerConfig {
    pub fn new(
        worker_id: String,
        pipeline_id: PipelineId,
        graph: ExecutionGraph,
        vertex_ids: Vec<VertexId>,
        num_threads_per_task: usize,
        transport_backend_type: TransportBackendType,
    ) -> Self {
        Self {
            worker_id,
            pipeline_id,
            execution_attempt_id: 0,
            graph,
            vertex_ids,
            num_threads_per_task,
            transport_backend_type,
            master_addr: None,
            restore_checkpoint_id: None,
            window_state_namespace: "window_state".to_string(),
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

// WorkerState moved to runtime/observability/snapshot_types.rs

pub struct Worker {
    worker_id: String,
    health: Arc<WorkerHealth>,
    config: Option<WorkerConfig>,
    task_actors: HashMap<VertexId, ActorRef<StreamTaskActor>>,
    backend_actor: Option<ActorRef<TransportBackendActor>>,
    task_runtimes: HashMap<VertexId, Runtime>,
    transport_backend_runtime: Option<Runtime>,
    worker_state: Arc<tokio::sync::Mutex<WorkerSnapshot>>,
    operator_states: Arc<OperatorStates>,
    running: Arc<AtomicBool>,
    tasks_state_polling_handle: Option<tokio::task::JoinHandle<()>>,
    // Watches the process health bus; on a fatal event it quiesces tasks so a broken
    // worker stops producing while the master drives a recovery reset.
    fatal_watcher_handle: Option<tokio::task::JoinHandle<()>>,

    // if RequestSource/Sink is configured, run processor - shared between tasks
    request_source_processor: Option<RequestSourceProcessor>,
    request_source_processor_runtime: Option<Runtime>,

    source_handles: Arc<SourceHandles>,

    // TODO separate backend runtime for request mode channels
}

impl Worker {
    pub fn new(worker_id: String) -> Self {
        Self {
            worker_id: worker_id.clone(),
            health: Arc::new(WorkerHealth::new()),
            config: None,
            task_actors: HashMap::new(),
            backend_actor: None,
            task_runtimes: HashMap::new(),
            transport_backend_runtime: None,
            worker_state: Arc::new(tokio::sync::Mutex::new(WorkerSnapshot::new(
                worker_id,
                PipelineId(String::new()),
            ))),
            operator_states: Arc::new(OperatorStates::new()),
            running: Arc::new(AtomicBool::new(false)),
            tasks_state_polling_handle: None,
            fatal_watcher_handle: None,
            request_source_processor: None,
            request_source_processor_runtime: None,
            source_handles: Arc::new(SourceHandles::new()),
        }
    }

    pub fn from_config(config: WorkerConfig) -> Self {
        let mut worker = Self::new(config.worker_id.clone());
        worker.configure(config);
        worker
    }

    pub fn configure(&mut self, config: WorkerConfig) {
        if self.running.load(Ordering::SeqCst) {
            panic!("Cannot configure worker while it is running");
        }
        // Fresh incarnation: drop any sticky fatal from a previous execution attempt so this
        // worker is not immediately reported unhealthy after a recovery reset.
        self.health.clear();
        self.source_handles.clear();
        let mut config = config;
        config.worker_id = self.worker_id.clone();
        println!(
            "[WORKER] Configuring worker_id={} pipeline_id={} vertices={} threads_per_task={}",
            config.worker_id,
            config.pipeline_id.0.as_str(),
            config.vertex_ids.len(),
            config.num_threads_per_task
        );

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

        self.task_actors = HashMap::new();
        self.backend_actor = None;
        self.task_runtimes = task_runtimes;
        self.transport_backend_runtime = Some(
            Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .thread_name("transport-backend-runtime")
                .build()
                .unwrap(),
        );
        self.worker_state = Arc::new(tokio::sync::Mutex::new(WorkerSnapshot::new(
            self.worker_id.clone(),
            config.pipeline_id.clone(),
        )));
        self.operator_states = Arc::new(OperatorStates::new());
        self.running = Arc::new(AtomicBool::new(false));
        self.tasks_state_polling_handle = None;
        self.fatal_watcher_handle = None;
        self.request_source_processor = None;
        self.request_source_processor_runtime = request_source_processor_runtime;
        self.config = Some(config);
    }

    pub fn health(&self) -> Arc<WorkerHealth> {
        self.health.clone()
    }

    pub fn is_configured(&self) -> bool {
        self.config.is_some()
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn worker_id(&self) -> String {
        self.worker_id.clone()
    }

    pub fn pipeline_id(&self) -> Option<String> {
        self.config
            .as_ref()
            .map(|cfg| cfg.pipeline_id.0.clone())
    }

    pub fn execution_attempt_id(&self) -> u64 {
        self.config
            .as_ref()
            .map(|cfg| cfg.execution_attempt_id)
            .unwrap_or_default()
    }

    async fn poll_and_update_tasks_state(
        worker_id: String,
        pipeline_id: PipelineId,
        task_runtimes: HashMap<VertexId, Handle>,
        task_actors: HashMap<VertexId, ActorRef<StreamTaskActor>>,
        graph: ExecutionGraph,
        operator_states: Arc<OperatorStates>,
        state: Arc<tokio::sync::Mutex<WorkerSnapshot>>,
        state_update_sender: Option<mpsc::Sender<WorkerSnapshot>>
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
        let mut task_metrics: HashMap<VertexId, TaskMetrics> = HashMap::new();
        let mut task_operator_metrics: HashMap<VertexId, TaskOperatorMetrics> = HashMap::new();
        let mut task_metadata: HashMap<VertexId, HashMap<String, String>> = HashMap::new();

        for result in task_results {
            if let Ok((vertex_id, state)) = result {
                task_statuses.insert(vertex_id.clone(), state.status.clone());
                task_metrics.insert(vertex_id.clone(), state.metrics.clone());
                if !state.metadata.is_empty() {
                    task_metadata.insert(vertex_id.clone(), state.metadata.clone());
                }

                if let Some(op_state) = operator_states.get_operator_state(vertex_id.as_ref()) {
                    if let Some(m) = op_state.task_operator_metrics() {
                        task_operator_metrics.insert(vertex_id.clone(), m);
                    }
                }
            }
        }

        let task_metrics_str: HashMap<String, TaskMetrics> = task_metrics
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v))
            .collect();
        let worker_metrics = WorkerAggregateMetrics::new(worker_id, pipeline_id, task_metrics_str, &graph);
        worker_metrics.record();
        emit_poll_derived_gauges(&worker_metrics);

        // Update shared WorkerState
        {
            let mut state_guard = state.lock().await;
            state_guard.task_statuses = task_statuses;
            state_guard.set_metrics(worker_metrics);
            state_guard.task_operator_metrics = task_operator_metrics;
            state_guard.task_metadata = task_metadata;
            // state_guard.worker_metrics.set_tasks_metrics(task_metrics.clone());
            if state_update_sender.is_some() {
                state_update_sender.unwrap().send(state_guard.clone()).await.unwrap();
            }
        } // Release lock before sleep
    }

    pub async fn wait_for_all_tasks_status(
        state: Arc<tokio::sync::Mutex<WorkerSnapshot>>,
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
        let config = self
            .config
            .as_ref()
            .expect("Worker must be configured before use")
            .clone();

        // Shared SortedKV for all window operators on this worker (each op gets a clone/handle).
        let window_sorted_kv: Arc<dyn SortedKV> = Arc::new(InMemSortedKV::new());
        let window_state_namespace =
            StateNamespace::new(config.window_state_namespace.as_bytes());

        let mut backend: Box<dyn TransportBackendTrait> = match config.transport_backend_type {
            TransportBackendType::Grpc => Box::new(TransportBackend::new(self.health.clone())),
        };
        let mut transport_client_configs = backend.init_channels(&config.graph, config.vertex_ids.clone());

        let backend_actor_task = self.transport_backend_runtime.as_ref().unwrap().spawn(async{
            return spawn(TransportBackendActor::new(backend))
        });
        let backend_actor_ref = backend_actor_task.await.unwrap();
        self.backend_actor = Some(backend_actor_ref);

        let vertex_ids = config.vertex_ids.clone();
        for vertex_id in &vertex_ids {
            let vertex = config
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
                    if let Some(master_addr) = &config.master_addr {
                        cfg.insert("master_addr".to_string(), Value::String(master_addr.clone()));
                    }
                    if let Some(restore_checkpoint_id) = config.restore_checkpoint_id {
                        cfg.insert("restore_checkpoint_id".to_string(), Value::from(restore_checkpoint_id));
                    }
                    cfg.insert(
                        "execution_attempt_id".to_string(),
                        Value::from(config.execution_attempt_id),
                    );
                    cfg.insert("pipeline_id".to_string(), Value::String(config.pipeline_id.0.clone()));
                    cfg.insert("worker_id".to_string(), Value::String(config.worker_id.clone()));
                    Some(cfg)
                },
                Some(self.operator_states.clone()),
                Some(config.graph.clone()),
            );
            runtime_context.set_source_handles(self.source_handles.clone());
            if let Some(request_source_processor) = &self.request_source_processor {
                runtime_context.set_request_sink_source_request_receiver(request_source_processor.get_shared_request_receiver().clone());
                runtime_context.set_request_sink_source_response_sender(request_source_processor.get_response_sender());
            }
            if matches!(
                &vertex.operator_config,
                crate::runtime::operators::operator::OperatorConfig::WindowConfig(_)
                    | crate::runtime::operators::operator::OperatorConfig::WindowRequestConfig(_)
            ) {
                runtime_context.set_sorted_kv(window_sorted_kv.clone());
                runtime_context.set_window_state_namespace(window_state_namespace.clone());
            }

            // Create the task and its actor in the task's runtime
            let mut transport_cfg = transport_client_configs.remove(vertex_id).unwrap();
            transport_cfg.set_metrics_labels(MetricsLabels {
                pipeline_id: config.pipeline_id.0.clone(),
                worker_id: config.worker_id.clone(),
            });
            let task = StreamTask::new(
                vertex_id.clone(),
                vertex.operator_config.clone(),
                transport_cfg,
                runtime_context,
                config.graph.clone(),
                self.health.clone(),
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
        state_updates_sender: Option<mpsc::Sender<WorkerSnapshot>>
    ) {
        println!("[WORKER] Starting tasks");
        let config = self
            .config
            .as_ref()
            .expect("Worker must be configured before use")
            .clone();

        // Start all tasks
        let mut start_futures = Vec::new();
        for (vertex_id, task_runtime) in &self.task_runtimes {
            let vertex_id = vertex_id.clone();
            let task_ref = self.task_actors.get(&vertex_id).unwrap().clone();

            let task_ref = task_ref.clone();
            let fut = task_runtime.spawn(async move {
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
        let graph = config.graph.clone();
        let state = self.worker_state.clone();
        let operator_states = self.operator_states.clone();
        let worker_id = config.worker_id.clone();
        let pipeline_id = config.pipeline_id.clone();
        
        let task_runtime_handles: HashMap<VertexId, Handle> = self.task_runtimes.iter()
            .map(|(k, v)| (k.clone(), v.handle().clone()))
            .collect();
        
        let polling_handle = tokio::spawn(async move { 
            while running.load(Ordering::SeqCst) {
                Self::poll_and_update_tasks_state(
                    worker_id.clone(),
                    pipeline_id.clone(),
                    task_runtime_handles.clone(),
                    task_actors.clone(),
                    graph.clone(),
                    operator_states.clone(),
                    state.clone(),
                    state_updates_sender.clone(),
                ).await;
                sleep(Duration::from_millis(100)).await;
            }
            // final poll
            Self::poll_and_update_tasks_state(
                worker_id.clone(),
                pipeline_id.clone(),
                task_runtime_handles,
                task_actors,
                graph,
                operator_states.clone(),
                state,
                state_updates_sender.clone(),
            ).await;
        });

        self.tasks_state_polling_handle = Some(polling_handle);

        self.spawn_fatal_watcher();

        println!("[WORKER] Started all tasks");
    }

    /// Watch the worker-scoped health bus. On the first fatal event, quiesce all tasks
    /// (send Close) so a broken worker stops producing. The master observes the worker
    /// as unhealthy (via heartbeat) and drives a recovery reset independently.
    fn spawn_fatal_watcher(&mut self) {
        let task_actors = self.task_actors.clone();
        let task_runtime_handles: HashMap<VertexId, Handle> = self
            .task_runtimes
            .iter()
            .map(|(k, v)| (k.clone(), v.handle().clone()))
            .collect();

        let health = self.health.clone();
        let handle = tokio::spawn(async move {
            let mut fatal_events = health.subscribe();
            // A fatal may already be present (e.g. reported just before subscribing).
            if health.last_fatal().is_none() {
                loop {
                    match fatal_events.recv().await {
                        Ok(_) => break,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    }
                }
            }

            println!("[WORKER] Fatal detected, quiescing tasks");
            for (vertex_id, handle) in &task_runtime_handles {
                if let Some(task_ref) = task_actors.get(vertex_id) {
                    let task_ref = task_ref.clone();
                    let _ = handle
                        .spawn(async move {
                            let _ = task_ref.ask(StreamTaskMessage::Close).await;
                        })
                        .await;
                }
            }
        });

        self.fatal_watcher_handle = Some(handle);
    }

    async fn start_transport_backend(&mut self) {
        let backend_actor_ref = self.backend_actor.as_ref().unwrap().clone();
        self.transport_backend_runtime.as_ref().unwrap().spawn(async move {
            backend_actor_ref.ask(TransportBackendActorMessage::Start).await.unwrap()
        }).await.unwrap();
    }

    async fn start_request_source_processor_if_needed(&mut self) {
        let config = self
            .config
            .as_ref()
            .expect("Worker must be configured before use")
            .clone();
        if let Some(request_runtime) = &self.request_source_processor_runtime {
            let request_source_config = extract_request_source_config(&config.graph).expect("request_source_config should be set");
            println!("[WORKER] Starting request source processor");
            
            let mut processor = RequestSourceProcessor::new(request_source_config);
            
            // Start the processor in its dedicated runtime
            let (processor, start_result) = request_runtime.spawn(async move {
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
            let request_runtime = self.request_source_processor_runtime.as_ref().expect("request_source_processor_runtime should be set");
            println!("[WORKER] Stopping request source processor");
            
            let stop_result = request_runtime.spawn(async move {
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
        
        for (vertex_id, task_runtime) in &self.task_runtimes {
            let vertex_id = vertex_id.clone();
            let task_ref = self.task_actors.get(&vertex_id).unwrap().clone();
            let signal_clone = signal.clone();
            let signal_for_error = signal.clone();
            let fut = task_runtime.spawn(async move {
                if let Err(e) = task_ref.ask(signal_clone).await {
                    eprintln!("Error sending {:?} signal to task {}: {}", signal_for_error, vertex_id, e);
                }
            });
            let _ = fut.await;
        }

        println!("[WORKER] {:?} signal sent to all task actors", signal);
    }

    pub async fn get_state(&self) -> WorkerSnapshot {
        if self.running.load(Ordering::SeqCst) {
            let config = self
                .config
                .as_ref()
                .expect("Worker must be configured before use");
            let task_runtime_handles: HashMap<VertexId, Handle> = self.task_runtimes.iter()
                .map(|(k, v)| (k.clone(), v.handle().clone()))
                .collect();
            let task_actors = self.task_actors.clone();
            let graph = config.graph.clone();
            let state = self.worker_state.clone();
            Self::poll_and_update_tasks_state(
                config.worker_id.clone(),
                config.pipeline_id.clone(),
                task_runtime_handles,
                task_actors,
                graph,
                self.operator_states.clone(),
                state,
                None,
            ).await;
        }
        self.worker_state.lock().await.clone()
    }

    pub fn operator_states(&self) -> Arc<OperatorStates> {
        self.operator_states.clone()
    }

    /// Tear down nested runtimes. Safe to call from async (blocking work uses
    /// `block_in_place` / a disposer thread). Idempotent.
    /// Prefer replacing with `Worker::new` when possible — `Drop` calls this.
    pub fn close(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.fatal_watcher_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.tasks_state_polling_handle.take() {
            handle.abort();
        }
        self.request_source_processor.take();
        self.task_actors.clear();
        self.config = None;
        self.health.clear();

        // Run transport Close on its runtime before disposing it — backend cleanup
        // (stop gRPC server, drain client/reader/writer tasks) lives on that message.
        if let Some(backend_runtime) = self.transport_backend_runtime.as_ref() {
            if let Some(backend_actor) = self.backend_actor.take() {
                let handle = backend_runtime.handle().clone();
                let close = async move {
                    let _ = backend_actor
                        .ask(TransportBackendActorMessage::Close)
                        .await;
                };
                // Cannot `block_in_place` on current-thread runtimes (e.g. default
                // `#[tokio::test]`); always offload when a Tokio context is entered.
                if tokio::runtime::Handle::try_current().is_ok() {
                    if let Ok(join) = std::thread::Builder::new()
                        .name("worker-transport-close".into())
                        .spawn(move || handle.block_on(close))
                    {
                        let _ = join.join();
                    }
                } else {
                    handle.block_on(close);
                }
            }
        }
        self.backend_actor = None;

        let mut runtimes = Vec::new();
        if let Some(runtime) = self.transport_backend_runtime.take() {
            runtimes.push(runtime);
        }
        if let Some(runtime) = self.request_source_processor_runtime.take() {
            runtimes.push(runtime);
        }
        for (_, runtime) in self.task_runtimes.drain() {
            runtimes.push(runtime);
        }
        if runtimes.is_empty() {
            return;
        }
        // Never Drop a Runtime while a Tokio runtime is entered on this thread.
        let Ok(handle) = std::thread::Builder::new()
            .name("worker-runtime-dispose".into())
            .spawn(move || {
                for runtime in runtimes {
                    runtime.shutdown_background();
                }
            })
        else {
            return;
        };
        let _ = handle.join();
        println!("[WORKER] Cleanup completed");
    }

    // control functions
    pub async fn start(&mut self) {
        if !self.is_configured() {
            panic!("Worker must be configured before start");
        }
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

    /// Cooperative source stop for harness-driven pipeline finish.
    /// Returns `false` if the worker is not configured.
    pub fn stop_sources(&mut self) -> bool {
        if !self.is_configured() {
            println!("[WORKER] Rejecting stop_sources: worker not configured");
            return false;
        }
        println!("[WORKER] Stopping sources (cooperative finish)");
        self.source_handles.stop_all();
        true
    }

    /// Inject checkpoint barriers on checkpointable source tasks.
    /// Returns `false` if the worker is not configured (e.g. reset/closed); callers
    /// must treat that as rejection so master aborts the in-flight checkpoint.
    pub async fn trigger_checkpoint_barrier(&mut self, checkpoint_id: u64) -> bool {
        let Some(config) = self.config.as_ref().cloned() else {
            println!(
                "[WORKER] Rejecting checkpoint barrier {}: worker not configured",
                checkpoint_id
            );
            return false;
        };
        println!(
            "[WORKER] Triggering checkpoint barrier {} on source tasks",
            checkpoint_id
        );
        for (vertex_id, task_runtime) in &self.task_runtimes {
            let vertex_id = vertex_id.clone();
            let vertex_type = config.graph.get_vertex_type(vertex_id.as_ref());
            if vertex_type != OperatorType::Source && vertex_type != OperatorType::ChainedSourceSink {
                continue;
            }

            // Only trigger sources that actually participate in checkpointing.
            if let Some(v) = config.graph.get_vertices().get(vertex_id.as_ref()) {
                if !operator_config_requires_checkpoint(&v.operator_config) {
                    continue;
                }
            }

            self.source_handles.cancel(&vertex_id);
            let task_ref = self.task_actors.get(&vertex_id).unwrap().clone();
            let fut = task_runtime.spawn(async move {
                let _ = task_ref
                    .ask(StreamTaskMessage::TriggerCheckpointBarrier(checkpoint_id))
                    .await;
            });
            let _ = fut.await;
        }
        true
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
        state_udpates_sender: mpsc::Sender<WorkerSnapshot>
    ) {
        self._execute_worker_lifecycle_for_testing(Some(state_udpates_sender)).await
    }

    async fn _execute_worker_lifecycle_for_testing(
        &mut self,
        state_updates_sender: Option<mpsc::Sender<WorkerSnapshot>>
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
        
        self.close();

        println!("[WORKER] Worker execution completed");
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.close();
    }
}