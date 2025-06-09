use crate::{runtime::{
    execution_graph::ExecutionGraph, runtime_context::RuntimeContext, stream_task::{StreamTask, StreamTaskStatus, StreamTaskMetrics}, stream_task_actor::{StreamTaskActor, StreamTaskMessage}
}, transport::{InMemoryTransportBackend, TransportBackend}};
use crate::transport::transport_backend_actor::{TransportBackendActor, TransportBackendActorMessage};
use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use kameo::{spawn, prelude::ActorRef};
use tokio::runtime::{Builder, Runtime};
use tokio::time::{sleep, Duration};
use futures::future::join_all;

#[derive(Debug, Clone)]
pub struct WorkerState {
    pub task_statuses: HashMap<String, StreamTaskStatus>,
    pub task_metrics: HashMap<String, StreamTaskMetrics>,
    pub aggregated_metrics: AggregatedMetrics,
}

#[derive(Debug, Clone)]
pub struct AggregatedMetrics {
    pub total_messages: u64,
    pub total_records: u64,
    pub latency_histogram: Vec<u64>, // Combined histogram from all tasks
}

impl AggregatedMetrics {
    pub fn new() -> Self {
        Self {
            total_messages: 0,
            total_records: 0,
            latency_histogram: vec![0, 0, 0, 0, 0], // 5 buckets
        }
    }

    pub fn aggregate_from_sink_metrics(&mut self, sink_metrics: &[StreamTaskMetrics]) {
        self.total_messages = 0;
        self.total_records = 0;
        self.latency_histogram = vec![0, 0, 0, 0, 0];

        for metrics in sink_metrics {
            self.total_messages += metrics.num_messages;
            self.total_records += metrics.num_records;
            
            for (i, &count) in metrics.latency_histogram.iter().enumerate() {
                self.latency_histogram[i] += count;
            }
        }
    }
}

impl WorkerState {
    pub fn new() -> Self {
        Self {
            task_statuses: HashMap::new(),
            task_metrics: HashMap::new(),
            aggregated_metrics: AggregatedMetrics::new(),
        }
    }

    pub fn all_tasks_closed(&self) -> bool {
        self.task_statuses.values().all(|status| *status == StreamTaskStatus::Closed)
    }
}

pub struct Worker {
    graph: ExecutionGraph,
    vertex_ids: Vec<String>,
    task_actors: HashMap<String, ActorRef<StreamTaskActor>>,
    backend_actor: Option<ActorRef<TransportBackendActor>>,
    task_runtimes: HashMap<String, Runtime>,
    backend_runtime: Option<Runtime>,
    num_io_threads: usize,
    state: Arc<std::sync::Mutex<WorkerState>>,
    running: Arc<AtomicBool>,
    polling_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Worker {
    pub fn new(
        graph: ExecutionGraph, 
        vertex_ids: Vec<String>,
        num_io_threads: usize,
    ) -> Self {
        Self {
            graph,
            vertex_ids,
            task_actors: HashMap::new(),
            backend_actor: None,
            task_runtimes: HashMap::new(),
            backend_runtime: None,
            num_io_threads,
            state: Arc::new(std::sync::Mutex::new(WorkerState::new())),
            running: Arc::new(AtomicBool::new(false)),
            polling_handle: None,
        }
    }

    async fn tasks_state_polling_loop(
        running: Arc<AtomicBool>,
        task_actors: HashMap<String, ActorRef<StreamTaskActor>>,
        graph: ExecutionGraph,
        state: Arc<std::sync::Mutex<WorkerState>>,
    ) {
        while running.load(Ordering::SeqCst) {
            // Collect states and metrics from all tasks
            let mut task_statuses = HashMap::new();
            let mut task_metrics = HashMap::new();
            let mut sink_metrics = Vec::new();

            for (vertex_id, task_ref) in &task_actors {
                if let Ok(state) = task_ref.ask(StreamTaskMessage::GetState).await {
                    task_statuses.insert(vertex_id.clone(), state.status.clone());
                    task_metrics.insert(vertex_id.clone(), state.metrics.clone());
                    
                    // Check if this is a sink vertex
                    if let Some(operator_type) = graph.get_vertex_type(vertex_id) {
                        if operator_type == crate::runtime::operator::OperatorType::SINK {
                            sink_metrics.push(state.metrics);
                        }
                    }
                }
            }

            // Update shared WorkerState
            {
                let mut state_guard = state.lock().unwrap();
                state_guard.task_statuses = task_statuses;
                state_guard.task_metrics = task_metrics;
                state_guard.aggregated_metrics.aggregate_from_sink_metrics(&sink_metrics);
            } // Release lock before sleep

            // Sleep before next poll
            sleep(Duration::from_millis(1000)).await;
        }
    }

    pub fn start(&mut self) {
        println!("Starting worker");

        let backend_runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("transport-backend-runtime")
            .build().unwrap();

        let mut backend = InMemoryTransportBackend::new();
        let mut transport_client_configs = backend.init_channels(&self.graph, self.vertex_ids.clone());

        let backend_actor = backend_runtime.block_on(async {
            spawn(TransportBackendActor::new(backend))
        });

        self.backend_runtime = Some(backend_runtime);
        self.backend_actor = Some(backend_actor.clone());

        for vertex_id in &self.vertex_ids {
            let vertex = self.graph.get_vertex(vertex_id).expect("Vertex should exist");
            
            // Create runtime for this task
            let runtime = Builder::new_multi_thread()
                .worker_threads(self.num_io_threads)
                .enable_all()
                .thread_name(format!("task-runtime-{}", vertex_id))
                .build().unwrap();

            // Create runtime context for the vertex
            let runtime_context = RuntimeContext::new(
                vertex_id.clone(),
                vertex.task_index,
                vertex.parallelism,
                None,
            );

            // Create the task and its actor in the task's runtime
            let task = StreamTask::new(
                vertex_id.clone(),
                vertex.operator_config.clone(),
                transport_client_configs.remove(&vertex_id.clone()).unwrap(),
                runtime_context,
                self.graph.clone(),
            );
            let task_actor = StreamTaskActor::new(task);
            let task_ref = runtime.block_on(async {
                spawn(task_actor)
            });
            self.task_actors.insert(vertex_id.clone(), task_ref.clone());
            self.task_runtimes.insert(vertex_id.clone(), runtime);
        }

        // Start the backend
        self.backend_runtime.as_ref().unwrap().block_on(async {
            self.backend_actor.as_ref().unwrap().ask(TransportBackendActorMessage::Start).await
        }).unwrap();

        // Start all tasks
        let mut start_futures = Vec::new();
        for (vertex_id, runtime) in &self.task_runtimes {
            let vertex_id = vertex_id.clone();
            let task_ref = self.task_actors.get(&vertex_id).unwrap().clone();

            let task_ref = task_ref.clone();
            let fut = runtime.spawn(async move {
                if let Err(e) = task_ref.ask(crate::runtime::stream_task_actor::StreamTaskMessage::Run).await {
                    eprintln!("Error running task {}: {}", vertex_id, e);
                }
            });
            start_futures.push(fut);
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for f in start_futures {
                let _ = f.await?;
            }
            Ok::<(), anyhow::Error>(())
        }).unwrap();

        // Start tasks state polling loop
        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let task_actors = self.task_actors.clone();
        let graph = self.graph.clone();
        let shared_state = self.state.clone();
        
        let polling_handle = tokio::spawn(async move {
            Self::tasks_state_polling_loop(running, task_actors, graph, shared_state).await;
        });

        self.polling_handle = Some(polling_handle);

        println!("Started worker");
    }

    pub fn close(&mut self) {
        println!("Closing worker");

        // Stop polling first
        self.running.store(false, Ordering::SeqCst);

        // Close all tasks in parallel
        let mut close_futures = Vec::new();
        for (vertex_id, runtime) in &self.task_runtimes {
            let task_ref = self.task_actors.get(&vertex_id.clone()).unwrap().clone();
            let fut = runtime.spawn(async move {
                task_ref.ask(crate::runtime::stream_task_actor::StreamTaskMessage::Close).await?;
                Ok::<(), anyhow::Error>(())
            });
            close_futures.push(fut);
        }

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for f in close_futures {
                let _ = f.await?;
            }
            Ok::<(), anyhow::Error>(())
        }).unwrap();

        // Close the backend
        if let Some(backend_actor) = &self.backend_actor {
            self.backend_runtime.as_ref().unwrap().block_on(async {
                backend_actor.ask(TransportBackendActorMessage::Close).await
            }).unwrap();
        }

        // Wait for polling task to finish
        if let Some(handle) = self.polling_handle.take() {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                if let Err(e) = handle.await {
                    eprintln!("Polling task failed: {:?}", e);
                }
            });
        }

        // Shutdown all runtimes
        for (vertex_id, runtime) in self.task_runtimes.drain() {
            println!("Shutting down runtime for task {}", vertex_id);
            runtime.shutdown_timeout(std::time::Duration::from_secs(1));
        }

        // Shutdown backend runtime
        if let Some(runtime) = self.backend_runtime.take() {
            println!("Shutting down transport backend runtime");
            runtime.shutdown_timeout(std::time::Duration::from_secs(1));
        }

        println!("Closed worker");
    }

    async fn wait_for_completion(&mut self) {
        // Wait for completion by checking the shared state
        while self.running.load(Ordering::SeqCst) {
            let all_tasks_closed = {
                let state_guard = self.state.lock().unwrap();
                state_guard.all_tasks_closed()
            };
            if all_tasks_closed {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        // Stop polling
        self.running.store(false, Ordering::SeqCst);
        
        // Wait for polling task to finish
        if let Some(handle) = self.polling_handle.take() {
            if let Err(e) = handle.await {
                eprintln!("Polling task failed: {:?}", e);
            }
        }
        
        println!("All tasks have completed");
    }

    pub fn get_state(&self) -> WorkerState {
        self.state.lock().unwrap().clone()
    }

    pub fn execute(&mut self) {
        println!("Starting worker execution");
        self.start();
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            self.wait_for_completion().await;
            println!("Worker execution completed");
        });
    }
}
