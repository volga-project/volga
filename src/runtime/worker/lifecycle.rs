use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::runtime::Builder;
use tokio::sync::mpsc;

use crate::common::types::PipelineId;
use crate::runtime::functions::source::request_source::extract_request_source_config;
use crate::runtime::observability::snapshot_types::WorkerSnapshot;
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::operator_config_requires_checkpoint;
use crate::runtime::operators::operator::OperatorType;
use crate::runtime::operators::source::SourceHandles;
use crate::runtime::state::OperatorStates;
use crate::runtime::stream_task_actor::StreamTaskMessage;
use crate::transport::transport_backend_actor::TransportBackendActorMessage;

use super::config::WorkerConfig;
use super::Worker;

impl Worker {
    pub(crate) fn configure(&mut self, config: WorkerConfig) {
        if self.running.load(Ordering::SeqCst) {
            panic!("Cannot configure worker while it is running");
        }
        // Bind health to this attempt so stale tasks from a prior incarnation cannot
        // report_fatal into the live heartbeat (they still hold the old attempt id).
        self.health
            .set_execution_attempt(config.execution_attempt_id);
        self.source_handles.clear();
        assert_eq!(
            config.worker_id, self.worker_id,
            "configure worker_id must match process worker_id"
        );
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

    pub(crate) fn close_sync_dispose_runtimes(&mut self) {
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

    pub(crate) async fn close_async(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.fatal_watcher_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.tasks_state_polling_handle.take() {
            handle.abort();
        }
        if !self.task_actors.is_empty() {
            self.signal_tasks_close().await;
        }
        self.request_source_processor.take();
        self.task_actors.clear();
        self.config = None;

        // Transport backend uses a 1-thread runtime; awaiting Close on that runtime from
        // within itself deadlocks (Close joins tasks spawned on the same runtime). Match
        // drain-tip `close()`: run the ask on a dedicated thread via `block_on`.
        if let Some(backend_runtime) = self.transport_backend_runtime.as_ref() {
            if let Some(backend_actor) = self.backend_actor.take() {
                let handle = backend_runtime.handle().clone();
                let close = async move {
                    let _ = backend_actor
                        .ask(TransportBackendActorMessage::Close)
                        .await;
                };
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
        } else {
            self.backend_actor = None;
        }

        self.close_sync_dispose_runtimes();
    }

    /// Close then restore empty shell fields (same ActorRef + health Arc).
    /// Sticky fatal is cleared here; attempt fencing advances on the next configure.
    pub(crate) async fn reset_async(&mut self) {
        self.close_async().await;
        self.health.clear();
        self.worker_state = Arc::new(tokio::sync::Mutex::new(WorkerSnapshot::new(
            self.worker_id.clone(),
            PipelineId(String::new()),
        )));
        self.operator_states = Arc::new(OperatorStates::new());
        self.running = Arc::new(AtomicBool::new(false));
        self.source_handles = Arc::new(SourceHandles::new());
    }

    pub(crate) async fn start(&mut self) -> Result<(), String> {
        if !self.is_configured() {
            return Err("Worker is not configured yet".to_string());
        }
        self.start_request_source_processor_if_needed().await;
        self.spawn_actors().await;
        self.start_tasks(None).await;
        Ok(())
    }

    pub(crate) async fn signal_tasks_run(&mut self) {
        self.start_transport_backend().await;
        self.send_signal_to_task_actors(crate::runtime::stream_task_actor::StreamTaskMessage::Run).await;
    }

    pub(crate) async fn signal_tasks_close(&mut self) {
        self.send_signal_to_task_actors(crate::runtime::stream_task_actor::StreamTaskMessage::Close).await;
    }

    /// Cooperative source stop for harness-driven pipeline finish.
    /// Returns `false` if the worker is not configured.
    pub(crate) fn stop_sources(&mut self) -> bool {
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

    /// In-process test lifecycle (master normally coordinates this).
    pub(crate) async fn run_test_lifecycle(
        &mut self,
        state_updates_sender: Option<mpsc::Sender<WorkerSnapshot>>,
    ) {
        println!("[WORKER] Starting worker execution");

        if state_updates_sender.is_none() {
            self.start()
                .await
                .expect("test lifecycle requires configured worker");
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

        self.close_async().await;

        println!("[WORKER] Worker execution completed");
    }
}
