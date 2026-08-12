//! Configure-scoped worker incarnation. Dropped on reset/close; rebuilt on configure.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use kameo::prelude::ActorRef;
use tokio::runtime::{Builder, Runtime};

use crate::runtime::functions::source::request_source::extract_request_source_config;
use crate::runtime::functions::source::request_source::RequestSourceProcessor;
use crate::runtime::health::WorkerHealth;
use crate::runtime::metrics::MetricsLabels;
use crate::runtime::observability::snapshot_types::WorkerSnapshot;
use crate::runtime::operators::source::SourceHandles;
use crate::runtime::state::{StateRegistry, StateResourceTracker, StateSessionHandle};
use crate::runtime::stream_task_actor::StreamTaskActor;
use crate::runtime::VertexId;
use crate::transport::transport_backend_actor::{
    TransportBackendActor, TransportBackendActorMessage,
};

use super::config::WorkerConfig;

pub(crate) struct WorkerInner {
    pub(crate) config: WorkerConfig,
    pub(crate) health: Arc<WorkerHealth>,
    pub(crate) task_actors: HashMap<VertexId, ActorRef<StreamTaskActor>>,
    pub(crate) backend_actor: Option<ActorRef<TransportBackendActor>>,
    pub(crate) task_runtimes: HashMap<VertexId, Runtime>,
    pub(crate) transport_backend_runtime: Option<Runtime>,
    pub(crate) worker_state: Arc<tokio::sync::Mutex<WorkerSnapshot>>,
    pub(crate) state_registry: Arc<StateRegistry>,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) tasks_state_polling_handle: Option<tokio::task::JoinHandle<()>>,
    pub(crate) state_maintenance_handle: Option<tokio::task::JoinHandle<()>>,
    pub(crate) fatal_watcher_handle: Option<tokio::task::JoinHandle<()>>,
    pub(crate) request_source_processor: Option<RequestSourceProcessor>,
    pub(crate) request_source_processor_runtime: Option<Runtime>,
    pub(crate) source_handles: Arc<SourceHandles>,
}

impl WorkerInner {
    pub(crate) fn from_config(config: WorkerConfig) -> Self {
        let health = Arc::new(WorkerHealth::new());
        let mut task_runtimes = HashMap::new();
        for vertex_id in &config.vertex_ids {
            let task_runtime = Builder::new_multi_thread()
                .worker_threads(config.num_threads_per_task)
                .enable_all()
                .thread_name(format!("task-runtime-{}", vertex_id))
                .build()
                .unwrap();
            task_runtimes.insert(vertex_id.clone(), task_runtime);
        }

        let request_source_config = extract_request_source_config(&config.graph);
        let request_source_processor_runtime = if request_source_config.is_some() {
            Some(
                Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .thread_name("request-source-processor-runtime")
                    .build()
                    .unwrap(),
            )
        } else {
            None
        };

        let worker_state = Arc::new(tokio::sync::Mutex::new(WorkerSnapshot::new(
            config.worker_id.clone(),
            config.pipeline_id.clone(),
        )));

        let session = StateSessionHandle::connect(&config.operator_state_backend)
            .expect("state session init");
        let state_registry = Arc::new(StateRegistry::new(
            session,
            Arc::new(StateResourceTracker::new()),
            Some(MetricsLabels {
                pipeline_id: config.pipeline_id.0.clone(),
                worker_id: config.worker_id.clone(),
            }),
        ));
        state_registry.set_maintenance_enabled(config.state_maintenance_enabled);

        Self {
            config,
            health,
            task_actors: HashMap::new(),
            backend_actor: None,
            task_runtimes,
            transport_backend_runtime: Some(
                Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .thread_name("transport-backend-runtime")
                    .build()
                    .unwrap(),
            ),
            worker_state,
            state_registry,
            running: Arc::new(AtomicBool::new(false)),
            tasks_state_polling_handle: None,
            state_maintenance_handle: None,
            fatal_watcher_handle: None,
            request_source_processor: None,
            request_source_processor_runtime,
            source_handles: Arc::new(SourceHandles::new()),
        }
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub(crate) fn execution_attempt_id(&self) -> u64 {
        self.config.execution_attempt_id
    }

    /// Drop nested Tokio runtimes after tasks/transport have been signaled/aborted.
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
        for runtime in runtimes {
            runtime.shutdown_background();
        }
        println!("[WORKER] Cleanup completed");
    }

    /// Tear down this incarnation. Joins tasks and transport before disposing runtimes.
    pub(crate) async fn close(mut self) -> WorkerSnapshot {
        // Stop background watchers first so they cannot race Close.
        if let Some(handle) = self.fatal_watcher_handle.take() {
            handle.abort();
        }
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.tasks_state_polling_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.state_maintenance_handle.take() {
            handle.abort();
        }
        // Final cleaner tick so short-lived runs still apply retention.
        if self.config.state_maintenance_enabled {
            if let Err(err) = self.state_registry.run_maintenance_once().await {
                eprintln!("[WORKER] final state maintenance failed: {err:#}");
            }
        }

        // Close: signal after Finished; abort mid-run so dispose cannot hang.
        if !self.task_actors.is_empty() {
            self.signal_tasks_close().await;
        }
        self.request_source_processor.take();
        self.task_actors.clear();

        let snapshot = self.worker_state.lock().await.clone();

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
        snapshot
    }
}

impl Drop for WorkerInner {
    fn drop(&mut self) {
        if !self.task_actors.is_empty() || self.backend_actor.is_some() {
            panic!(
                "WorkerInner dropped with live tasks/transport — reset must call close() first"
            );
        }
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.fatal_watcher_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.tasks_state_polling_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.state_maintenance_handle.take() {
            handle.abort();
        }
        self.request_source_processor.take();
        self.close_sync_dispose_runtimes();
    }
}
