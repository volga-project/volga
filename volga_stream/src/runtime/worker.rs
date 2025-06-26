use crate::{runtime::{
    execution_graph::ExecutionGraph, operators::operator::OperatorType, runtime_context::RuntimeContext, stream_task::{StreamTask, StreamTaskMetrics, StreamTaskStatus}, stream_task_actor::{StreamTaskActor, StreamTaskMessage}
}, transport::{transport_backend_actor::TransportBackendType, GrpcTransportBackend, InMemoryTransportBackend, TransportBackend}};
use crate::transport::transport_backend_actor::{TransportBackendActor, TransportBackendActorMessage};
use std::{collections::HashMap};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use kameo::{spawn, prelude::ActorRef};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::time::{sleep, Duration};
use futures::future::join_all;
use serde::{Serialize, Deserialize};
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub graph: ExecutionGraph,
    pub vertex_ids: Vec<String>,
    pub num_io_threads: usize,
    pub transport_backend_type: TransportBackendType,
}

impl WorkerConfig {
    pub fn new(
        graph: ExecutionGraph,
        vertex_ids: Vec<String>,
        num_io_threads: usize,
        transport_backend_type: TransportBackendType,
    ) -> Self {
        Self {
            graph,
            vertex_ids,
            num_io_threads,
            transport_backend_type,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerState {
    pub task_statuses: HashMap<String, StreamTaskStatus>,
    pub task_metrics: HashMap<String, StreamTaskMetrics>,
    pub aggregated_metrics: AggregatedMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
            latency_histogram: vec![0, 0, 0, 0, 0],
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

    pub fn all_tasks_have_status(&self, status: StreamTaskStatus) -> bool {
        self.task_statuses.values().all(|_status| *_status == status)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }
}

pub struct Worker {
    graph: ExecutionGraph,
    vertex_ids: Vec<String>,
    transport_backend_type: TransportBackendType,
    task_actors: HashMap<String, ActorRef<StreamTaskActor>>,
    backend_actor: Option<ActorRef<TransportBackendActor>>,
    task_runtimes: HashMap<String, Runtime>,
    transport_backend_runtime: Option<Runtime>,
    state: Arc<tokio::sync::Mutex<WorkerState>>,
    running: Arc<AtomicBool>,
    tasks_state_polling_handle: Option<tokio::task::JoinHandle<()>>
}

impl Worker {
    pub fn new(config: WorkerConfig) -> Self {
        let mut task_runtimes = HashMap::new();
        for vertex_id in &config.vertex_ids {
            let task_runtime = Builder::new_multi_thread()
                .worker_threads(config.num_io_threads)
                .enable_all()
                .thread_name(format!("task-runtime-{}", vertex_id))
                .build().unwrap();

            task_runtimes.insert(vertex_id.clone(), task_runtime);
        }

        Self {
            graph: config.graph,
            vertex_ids: config.vertex_ids.clone(),
            task_actors: HashMap::new(),
            transport_backend_type: config.transport_backend_type,
            backend_actor: None,
            task_runtimes,
            transport_backend_runtime: Some(
                Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .thread_name("transport-backend-runtime")
                .build().unwrap()),
            state: Arc::new(tokio::sync::Mutex::new(WorkerState::new())),
            running: Arc::new(AtomicBool::new(false)),
            tasks_state_polling_handle: None
        }
    }

    async fn poll_and_update_tasks_state(
        task_runtimes: HashMap<String, Handle>,
        task_actors: HashMap<String, ActorRef<StreamTaskActor>>,
        graph: ExecutionGraph,
        state: Arc<tokio::sync::Mutex<WorkerState>>,
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

        let mut task_statuses = HashMap::new();
        let mut task_metrics = HashMap::new();
        let mut sink_metrics = Vec::new();

        for result in task_results {
            if let Ok((vertex_id, state)) = result {
                task_statuses.insert(vertex_id.clone(), state.status.clone());
                task_metrics.insert(vertex_id.clone(), state.metrics.clone());
                
                // Check if this is a sink vertex
                let operator_type = graph.get_vertex_type(&vertex_id);
                if operator_type == OperatorType::Sink || operator_type == OperatorType::ChainedSourceSink {
                    sink_metrics.push(state.metrics);
                }
            }
        }

        // Update shared WorkerState
        {
            let mut state_guard = state.lock().await;
            state_guard.task_statuses = task_statuses;
            state_guard.task_metrics = task_metrics;
            state_guard.aggregated_metrics.aggregate_from_sink_metrics(&sink_metrics);
        } // Release lock before sleep
    }

    pub async fn wait_for_all_tasks_status(
        state: Arc<tokio::sync::Mutex<WorkerState>>,
        running: Arc<AtomicBool>,
        target_status: StreamTaskStatus
    ) {
        println!("[WORKER] Waiting for all tasks to be {:?}", target_status);
        
        let start_time = std::time::Instant::now();
        let timeout_duration = Duration::from_secs(5);
        
        while running.load(Ordering::SeqCst) {
            // Check timeout
            if start_time.elapsed() > timeout_duration {
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
                
                panic!("Timeout waiting for all tasks to be {:?} after {:?}", target_status, timeout_duration);
            }
            
            let all_ready = {
                let state_guard = state.lock().await;
                state_guard.all_tasks_have_status(target_status)
            };
            
            if all_ready {
                println!("[WORKER] All tasks are {:?}", target_status);
                break;
            }
            
            sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn spawn_actors(&mut self) {
        println!("[WORKER] Spawning actors");

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
            let vertex = self.graph.get_vertex(vertex_id).expect("Vertex should exist");
            let task_runtime = self.task_runtimes.get(vertex_id).expect("Task runtime should exist");

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
            let task_ref = task_runtime.spawn(async{
                return spawn(task_actor)
            });
            let task_actor_ref = task_ref.await.unwrap();
            self.task_actors.insert(vertex_id.clone(), task_actor_ref);
        }

        println!("[WORKER] Actors spawned");
    }

    async fn start_tasks(&mut self) {
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
        let state = self.state.clone();
        
        let task_runtime_handles: HashMap<String, Handle> = self.task_runtimes.iter()
            .map(|(k, v)| (k.clone(), v.handle().clone()))
            .collect();
        
        let polling_handle = tokio::spawn(async move { 
            while running.load(Ordering::SeqCst) {
                Self::poll_and_update_tasks_state(task_runtime_handles.clone(), task_actors.clone(), graph.clone(), state.clone()).await;
                sleep(Duration::from_millis(100)).await;
            }
            // final poll
            Self::poll_and_update_tasks_state(task_runtime_handles, task_actors, graph, state).await;
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
            let task_runtime_handles: HashMap<String, Handle> = self.task_runtimes.iter()
                .map(|(k, v)| (k.clone(), v.handle().clone()))
                .collect();
            let task_actors = self.task_actors.clone();
            let graph = self.graph.clone();
            let state = self.state.clone();
            Self::poll_and_update_tasks_state(task_runtime_handles, task_actors, graph, state).await;
        }
        self.state.lock().await.clone()
    }

    async fn cleanup(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.tasks_state_polling_handle.take() {
            if let Err(e) = handle.await {
                eprintln!("Polling task failed: {:?}", e);
            }
        }

        // TODO destroy actors and runtimes

        println!("[WORKER] Cleanup completed");
    }

    // control functions
    pub async fn start(&mut self) {
        self.spawn_actors().await;
        self.start_tasks().await;
    }

    pub async fn run_tasks(&mut self) {
        self.start_transport_backend().await;
        self.send_signal_to_task_actors(crate::runtime::stream_task_actor::StreamTaskMessage::Run).await;
    }

    pub async fn close_tasks(&mut self) {
        self.send_signal_to_task_actors(crate::runtime::stream_task_actor::StreamTaskMessage::Close).await;
    }

    pub async fn close(&mut self) {
        self.cleanup().await;
    }

    // This should only be used for testing - simulates worker execution
    // In real environment master is used to coordinate worker lifecycle
    pub async fn execute_worker_lifecycle_for_testing(&mut self) {
        println!("[WORKER] Starting worker execution");
        
        self.start().await;

        Self::wait_for_all_tasks_status(
            self.state.clone(),
            self.running.clone(),
            StreamTaskStatus::Opened
        ).await;

        self.run_tasks().await;

        println!("[WORKER] Worker started");
        
        // Wait for tasks to finish
        Self::wait_for_all_tasks_status(
            self.state.clone(),
            self.running.clone(),
            StreamTaskStatus::Finished
        ).await;
        
        // Send close signal
        self.close_tasks().await;

        // Wait for tasks to be closed
        Self::wait_for_all_tasks_status(
            self.state.clone(),
            self.running.clone(),
            StreamTaskStatus::Closed
        ).await;
        
        // Cleanup
        self.close().await;

        println!("[WORKER] Worker execution completed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::stream_task::{StreamTaskStatus, StreamTaskMetrics};

    #[test]
    fn test_worker_state_serialization() {
        // Create a test WorkerState
        let mut worker_state = WorkerState::new();
        
        // Add some task statuses
        worker_state.task_statuses.insert("task1".to_string(), StreamTaskStatus::Running);
        worker_state.task_statuses.insert("task2".to_string(), StreamTaskStatus::Opened);
        
        // Add some task metrics
        let mut metrics1 = StreamTaskMetrics::new("task1".to_string());
        metrics1.num_messages = 100;
        metrics1.num_records = 500;
        metrics1.latency_histogram = vec![10, 20, 30, 40, 50];
        
        let mut metrics2 = StreamTaskMetrics::new("task2".to_string());
        metrics2.num_messages = 200;
        metrics2.num_records = 1000;
        metrics2.latency_histogram = vec![15, 25, 35, 45, 55];
        
        worker_state.task_metrics.insert("task1".to_string(), metrics1);
        worker_state.task_metrics.insert("task2".to_string(), metrics2);
        
        // Update aggregated metrics
        worker_state.aggregated_metrics.total_messages = 300;
        worker_state.aggregated_metrics.total_records = 1500;
        worker_state.aggregated_metrics.latency_histogram = vec![25, 45, 65, 85, 105];

        // Test serialization and deserialization
        let bytes = worker_state.to_bytes().unwrap();
        let deserialized_state = WorkerState::from_bytes(&bytes).unwrap();

        // Verify the state was correctly deserialized
        assert_eq!(worker_state.task_statuses, deserialized_state.task_statuses);
        assert_eq!(worker_state.task_metrics.len(), deserialized_state.task_metrics.len());
        
        // Compare task metrics
        for (key, original_metrics) in &worker_state.task_metrics {
            let deserialized_metrics = deserialized_state.task_metrics.get(key).unwrap();
            assert_eq!(original_metrics.vertex_id, deserialized_metrics.vertex_id);
            assert_eq!(original_metrics.num_messages, deserialized_metrics.num_messages);
            assert_eq!(original_metrics.num_records, deserialized_metrics.num_records);
            assert_eq!(original_metrics.latency_histogram, deserialized_metrics.latency_histogram);
        }
        
        // Compare aggregated metrics
        assert_eq!(worker_state.aggregated_metrics.total_messages, deserialized_state.aggregated_metrics.total_messages);
        assert_eq!(worker_state.aggregated_metrics.total_records, deserialized_state.aggregated_metrics.total_records);
        assert_eq!(worker_state.aggregated_metrics.latency_histogram, deserialized_state.aggregated_metrics.latency_histogram);
    }
}
