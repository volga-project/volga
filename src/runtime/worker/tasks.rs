use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::future::join_all;
use kameo::prelude::{spawn, ActorRef};
use serde_json::Value;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use crate::common::types::PipelineId;
use crate::runtime::checkpoint::TaskKey;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::functions::source::request_source::{
    extract_request_source_config, RequestSourceProcessor,
};
use crate::runtime::metrics::{
    collect_stream_task_metrics, MetricsLabels, TaskMetrics, WorkerAggregateMetrics,
};
use crate::runtime::observability::snapshot_types::{TaskOperatorMetrics, WorkerSnapshot};
use crate::runtime::observability::{StreamTaskStatus, TaskMetadata};
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::state::StateRegistry;
use crate::runtime::stream_task::StreamTask;
use crate::runtime::stream_task_actor::{StreamTaskActor, StreamTaskMessage};
use crate::runtime::VertexId;
use crate::transport::transport_backend_actor::{
    TransportBackendActor, TransportBackendActorMessage,
};
use crate::transport::{TransportBackend, TransportBackendTrait};

use super::inner::WorkerInner;
use super::Worker;

impl WorkerInner {
    pub(crate) async fn poll_and_update_tasks_state(
        worker_id: String,
        pipeline_id: PipelineId,
        task_runtimes: HashMap<VertexId, Handle>,
        task_actors: HashMap<VertexId, ActorRef<StreamTaskActor>>,
        graph: ExecutionGraph,
        state_registry: Arc<StateRegistry>,
        state: Arc<tokio::sync::Mutex<WorkerSnapshot>>,
        state_update_sender: Option<mpsc::Sender<WorkerSnapshot>>,
    ) {
        let mut task_futures = Vec::new();
        for (vertex_id, runtime) in &task_runtimes {
            let vertex_id = vertex_id.clone();
            let task_ref = task_actors.get(&vertex_id).unwrap().clone();

            let task_ref = task_ref.clone();
            let fut = runtime.spawn(async move {
                (
                    vertex_id.clone(),
                    task_ref.ask(StreamTaskMessage::GetState).await.unwrap(),
                )
            });
            task_futures.push(fut);
        }
        let task_results = join_all(task_futures).await;

        let mut task_statuses: HashMap<VertexId, StreamTaskStatus> = HashMap::new();
        let mut task_operator_metrics: HashMap<VertexId, TaskOperatorMetrics> = HashMap::new();
        let mut task_metadata: HashMap<VertexId, TaskMetadata> = HashMap::new();

        for result in task_results {
            if let Ok((vertex_id, state)) = result {
                task_statuses.insert(vertex_id.clone(), state.status.clone());
                if !state.metadata.is_empty() {
                    task_metadata.insert(vertex_id.clone(), state.metadata.clone());
                }

                if let Some(op_state) = state_registry.get_task_state(vertex_id.as_ref()) {
                    if let Some(m) = op_state.task_operator_metrics().await {
                        task_operator_metrics.insert(vertex_id.clone(), m);
                    }
                }
            }
        }

        // One in-process registry visit for all tasks (not per-task GetState / Prom parse).
        let labels = MetricsLabels {
            pipeline_id: pipeline_id.0.clone(),
            worker_id: worker_id.clone(),
        };
        let collected = collect_stream_task_metrics(Some(&labels));
        let task_metrics_str: HashMap<String, TaskMetrics> = task_statuses
            .keys()
            .map(|vertex_id| {
                let key = vertex_id.as_ref().to_string();
                let metrics = collected
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| TaskMetrics::empty(key.clone()));
                (key, metrics)
            })
            .collect();
        let worker_metrics =
            WorkerAggregateMetrics::new(worker_id, pipeline_id, task_metrics_str, &graph);
        worker_metrics.publish_poll_gauges(&labels);

        {
            let mut state_guard = state.lock().await;
            state_guard.task_statuses = task_statuses;
            state_guard.set_metrics(worker_metrics);
            state_guard.task_operator_metrics = task_operator_metrics;
            state_guard.task_metadata = task_metadata;
            if state_update_sender.is_some() {
                state_update_sender
                    .unwrap()
                    .send(state_guard.clone())
                    .await
                    .unwrap();
            }
        }
    }

    pub async fn wait_for_all_tasks_status(
        state: Arc<tokio::sync::Mutex<WorkerSnapshot>>,
        running: Arc<AtomicBool>,
        target_status: StreamTaskStatus,
        timeout_s: Option<u64>,
    ) {
        println!("[WORKER] Waiting for all tasks to be {:?}", target_status);

        let start_time = std::time::Instant::now();

        while running.load(Ordering::SeqCst) {
            if timeout_s.is_some() && start_time.elapsed() > Duration::from_secs(timeout_s.unwrap())
            {
                let state_guard = state.lock().await;
                let mut different_statuses = Vec::new();
                for (task_id, status) in &state_guard.task_statuses {
                    if *status != target_status {
                        different_statuses.push((task_id.clone(), *status));
                    }
                }

                if !different_statuses.is_empty() {
                    println!(
                        "[WORKER] Timeout waiting for {:?}. Tasks with different statuses:",
                        target_status
                    );
                    for (task_id, status) in different_statuses {
                        println!("  - {}: {:?}", task_id, status);
                    }
                }

                panic!(
                    "Timeout waiting for all tasks to be {:?} after {:?}s",
                    target_status,
                    timeout_s.unwrap()
                );
            }

            let all_ready = {
                let state_guard = state.lock().await;
                state_guard.all_tasks_in(&[target_status])
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
        let config = self.config.clone();

        let mut backend: Box<dyn TransportBackendTrait> =
            Box::new(TransportBackend::new_with_labels(
                self.health.clone(),
                Some(MetricsLabels {
                    pipeline_id: config.pipeline_id.0.clone(),
                    worker_id: config.worker_id.clone(),
                }),
            ));
        let mut transport_client_configs =
            backend.init_channels(&config.graph, config.vertex_ids.clone());

        let backend_actor_task = self
            .transport_backend_runtime
            .as_ref()
            .unwrap()
            .spawn(async { spawn(TransportBackendActor::new(backend)) });
        let backend_actor_ref = backend_actor_task.await.unwrap();
        self.backend_actor = Some(backend_actor_ref);

        let vertex_ids = config.vertex_ids.clone();
        for vertex_id in &vertex_ids {
            let vertex = config
                .graph
                .get_vertex(vertex_id.as_ref())
                .expect("Vertex should exist");
            let task_runtime = self
                .task_runtimes
                .get(vertex_id)
                .expect("Task runtime should exist");

            let mut runtime_context = RuntimeContext::new(
                vertex_id.clone(),
                vertex.task_index,
                vertex.parallelism,
                {
                    let mut cfg = HashMap::<String, Value>::new();
                    if let Some(master_addr) = &config.master_addr {
                        cfg.insert(
                            "master_addr".to_string(),
                            Value::String(master_addr.clone()),
                        );
                    }
                    cfg.insert(
                        "execution_attempt_id".to_string(),
                        Value::from(config.execution_attempt_id),
                    );
                    cfg.insert(
                        "pipeline_id".to_string(),
                        Value::String(config.pipeline_id.0.clone()),
                    );
                    cfg.insert(
                        "worker_id".to_string(),
                        Value::String(config.worker_id.clone()),
                    );
                    Some(cfg)
                },
                Some(self.state_registry.clone()),
                Some(config.graph.clone()),
            )
            .with_max_parallelism(vertex.max_parallelism)
            .with_state_config(
                config.operator_state_backend.clone(),
                config.request_store.clone(),
                config.pipeline_id.clone(),
                vertex.operator_id.clone(),
            );
            runtime_context.set_source_handles(self.source_handles.clone());
            if let Some(request_source_processor) = &self.request_source_processor {
                runtime_context.set_request_sink_source_request_receiver(
                    request_source_processor
                        .get_shared_request_receiver()
                        .clone(),
                );
                runtime_context.set_request_sink_source_response_sender(
                    request_source_processor.get_response_sender(),
                );
            }
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
                config
                    .task_restore_data
                    .get(&TaskKey {
                        vertex_id: vertex_id.as_ref().to_string(),
                        task_index: vertex.task_index,
                    })
                    .cloned(),
            );
            let task_actor = StreamTaskActor::new(task);
            let task_ref = task_runtime.spawn(async { spawn(task_actor) });
            let task_actor_ref = task_ref.await.unwrap();
            self.task_actors.insert(vertex_id.clone(), task_actor_ref);
        }

        println!("[WORKER] Actors spawned");
    }

    pub(crate) async fn start_tasks(
        &mut self,
        state_updates_sender: Option<mpsc::Sender<WorkerSnapshot>>,
    ) {
        println!("[WORKER] Starting tasks");
        let config = self.config.clone();

        let mut start_futures = Vec::new();
        for (vertex_id, task_runtime) in &self.task_runtimes {
            let vertex_id = vertex_id.clone();
            let task_ref = self.task_actors.get(&vertex_id).unwrap().clone();

            let task_ref = task_ref.clone();
            let fut = task_runtime.spawn(async move {
                if let Err(e) = task_ref
                    .ask(crate::runtime::stream_task_actor::StreamTaskMessage::Start)
                    .await
                {
                    eprintln!("Error starting task {}: {}", vertex_id, e);
                }
            });
            start_futures.push(fut);
        }

        for f in start_futures {
            let _ = f.await.unwrap();
        }

        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let task_actors = self.task_actors.clone();
        let graph = config.graph.clone();
        let state = self.worker_state.clone();
        let state_registry = self.state_registry.clone();
        let worker_id = config.worker_id.clone();
        let pipeline_id = config.pipeline_id.clone();

        let task_runtime_handles: HashMap<VertexId, Handle> = self
            .task_runtimes
            .iter()
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
                    state_registry.clone(),
                    state.clone(),
                    state_updates_sender.clone(),
                )
                .await;
                sleep(Duration::from_millis(100)).await;
            }
            Self::poll_and_update_tasks_state(
                worker_id.clone(),
                pipeline_id.clone(),
                task_runtime_handles,
                task_actors,
                graph,
                state_registry.clone(),
                state,
                state_updates_sender.clone(),
            )
            .await;
        });

        self.tasks_state_polling_handle = Some(polling_handle);

        if self.config.state_maintenance_enabled {
            let registry = self.state_registry.clone();
            let running = self.running.clone();
            let interval_ms = self.config.state_maintenance_interval_ms.max(1);
            self.state_maintenance_handle = Some(tokio::spawn(async move {
                while running.load(Ordering::SeqCst) {
                    if let Err(err) = registry.run_maintenance_once().await {
                        eprintln!("[WORKER] state maintenance tick failed: {err:#}");
                    }
                    sleep(Duration::from_millis(interval_ms)).await;
                }
            }));
        }

        self.spawn_fatal_watcher();

        println!("[WORKER] Started all tasks");
    }

    pub(crate) fn spawn_fatal_watcher(&mut self) {
        let task_actors = self.task_actors.clone();
        let task_runtime_handles: HashMap<VertexId, Handle> = self
            .task_runtimes
            .iter()
            .map(|(k, v)| (k.clone(), v.handle().clone()))
            .collect();

        let health = self.health.clone();
        let handle = tokio::spawn(async move {
            let mut fatal_events = health.subscribe();
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

    pub(crate) async fn start_transport_backend(&mut self) {
        let backend_actor_ref = self.backend_actor.as_ref().unwrap().clone();
        self.transport_backend_runtime
            .as_ref()
            .unwrap()
            .spawn(async move {
                backend_actor_ref
                    .ask(TransportBackendActorMessage::Start)
                    .await
                    .unwrap()
            })
            .await
            .unwrap();
    }

    pub(crate) async fn start_request_source_processor_if_needed(&mut self) {
        let config = self.config.clone();
        if let Some(request_runtime) = &self.request_source_processor_runtime {
            let request_source_config = extract_request_source_config(&config.graph)
                .expect("request_source_config should be set");
            println!("[WORKER] Starting request source processor");

            let mut processor = RequestSourceProcessor::new(request_source_config);

            let (processor, start_result) = request_runtime
                .spawn(async move {
                    let result = processor.start().await;
                    (processor, result)
                })
                .await
                .unwrap();

            self.request_source_processor = Some(processor);

            if let Err(e) = start_result {
                panic!("Failed to start request source processor: {}", e);
            }
        }
    }

    pub(crate) async fn stop_request_source_processor_if_needed(&mut self) {
        if let Some(mut processor) = self.request_source_processor.take() {
            let request_runtime = self
                .request_source_processor_runtime
                .as_ref()
                .expect("request_source_processor_runtime should be set");
            println!("[WORKER] Stopping request source processor");

            let stop_result = request_runtime
                .spawn(async move { processor.stop().await })
                .await
                .unwrap();

            if let Err(e) = stop_result {
                panic!("Failed to stop request source processor: {}", e);
            }

            println!("[WORKER] Request source processor stopped");
        }
    }

    pub(crate) async fn send_signal_to_task_actors(&mut self, signal: StreamTaskMessage) {
        println!("[WORKER] Sending {:?} signal to all task actors", signal);

        let futs: Vec<_> = self
            .task_runtimes
            .iter()
            .map(|(vertex_id, task_runtime)| {
                let vertex_id = vertex_id.clone();
                let task_ref = self.task_actors.get(&vertex_id).unwrap().clone();
                let signal_clone = signal.clone();
                let signal_for_error = signal.clone();
                task_runtime.spawn(async move {
                    if let Err(e) = task_ref.ask(signal_clone).await {
                        eprintln!(
                            "Error sending {:?} signal to task {}: {}",
                            signal_for_error, vertex_id, e
                        );
                    }
                })
            })
            .collect();
        let _ = join_all(futs).await;

        println!("[WORKER] {:?} signal sent to all task actors", signal);
    }

    pub async fn get_state(&self) -> WorkerSnapshot {
        if self.running.load(Ordering::SeqCst) {
            let task_runtime_handles: HashMap<VertexId, Handle> = self
                .task_runtimes
                .iter()
                .map(|(k, v)| (k.clone(), v.handle().clone()))
                .collect();
            let task_actors = self.task_actors.clone();
            let graph = self.config.graph.clone();
            let state = self.worker_state.clone();
            Self::poll_and_update_tasks_state(
                self.config.worker_id.clone(),
                self.config.pipeline_id.clone(),
                task_runtime_handles,
                task_actors,
                graph,
                self.state_registry.clone(),
                state,
                None,
            )
            .await;
        }
        self.worker_state.lock().await.clone()
    }
}

impl Worker {
    pub async fn get_state(&self) -> WorkerSnapshot {
        match &self.inner {
            Some(inner) => inner.get_state().await,
            None => self.last_snapshot.clone(),
        }
    }
}
