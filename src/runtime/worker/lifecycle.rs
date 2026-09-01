use tokio::sync::mpsc;

use crate::runtime::observability::snapshot_types::WorkerSnapshot;
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::operator_config_requires_checkpoint;
use crate::runtime::operators::operator::OperatorType;
use crate::runtime::stream_task::StreamTaskMessage;

use super::config::WorkerConfig;
use super::inner::WorkerInner;
use super::Worker;

impl Worker {
    pub(crate) fn configure(&mut self, config: WorkerConfig) {
        if self.is_running() {
            panic!("Cannot configure worker while it is running");
        }
        assert_eq!(
            config.worker_id, self.worker_id,
            "configure worker_id must match process worker_id"
        );
        assert!(self.inner.is_none(), "configure requires prior reset/close");
        println!(
            "[WORKER] Configuring worker_id={} pipeline_id={} vertices={} threads_per_task={}",
            config.worker_id,
            config.pipeline_id.0.as_str(),
            config.vertex_ids.len(),
            config.num_threads_per_task
        );

        self.inner = Some(WorkerInner::from_config(config));
    }

    pub(crate) async fn close_async(&mut self) {
        if let Some(inner) = self.inner.take() {
            self.last_snapshot = inner.close().await;
        }
    }

    pub(crate) async fn reset_async(&mut self) {
        self.close_async().await;
    }

    pub(crate) async fn start(&mut self) -> Result<(), String> {
        let inner = self.require_inner()?;
        inner.start_request_source_processor_if_needed().await;
        inner.spawn_actors().await;
        inner.start_tasks(None).await;
        Ok(())
    }

    pub(crate) async fn signal_tasks_run(&mut self) -> Result<(), String> {
        let inner = self.require_inner()?;
        inner.start_transport_backend().await;
        inner
            .send_signal_to_task_actors(crate::runtime::stream_task::StreamTaskMessage::Run)
            .await;
        Ok(())
    }

    pub(crate) async fn signal_tasks_close(&mut self) -> Result<(), String> {
        let inner = self.require_inner()?;
        inner.signal_tasks_close().await;
        Ok(())
    }

    /// Cooperative source stop for harness-driven pipeline finish.
    /// Returns `false` if the worker is not configured.
    pub(crate) fn stop_sources(&mut self) -> bool {
        let Some(inner) = self.inner.as_mut() else {
            println!("[WORKER] Rejecting stop_sources: worker not configured");
            return false;
        };
        println!("[WORKER] Stopping sources (cooperative finish)");
        inner.source_handles.stop_all();
        true
    }

    /// Inject checkpoint barriers on checkpointable source tasks.
    /// Returns `false` if the worker is not configured (e.g. reset/closed); callers
    /// must treat that as rejection so master aborts the in-flight checkpoint.
    pub async fn trigger_checkpoint_barrier(&mut self, checkpoint_id: u64) -> bool {
        let Some(inner) = self.inner.as_mut() else {
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
        let config = inner.config.clone();
        for (vertex_id, task_runtime) in &inner.task_runtimes {
            let vertex_id = vertex_id.clone();
            let vertex_type = config.graph.get_vertex_type(vertex_id.as_ref());
            if vertex_type != OperatorType::Source {
                continue;
            }

            if let Some(v) = config.graph.get_vertices().get(vertex_id.as_ref()) {
                if !operator_config_requires_checkpoint(&v.operator_config) {
                    continue;
                }
            }

            inner.source_handles.cancel(&vertex_id);
            let task_ref = inner.task_actors.get(&vertex_id).unwrap().clone();
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
            let inner = self
                .inner
                .as_mut()
                .expect("test lifecycle requires configured worker");
            inner.start_request_source_processor_if_needed().await;
            inner.spawn_actors().await;
            inner.start_tasks(state_updates_sender).await;
        }

        println!("[WORKER] Worker started, waiting for all tasks to be opened");

        let (worker_state, running) = {
            let inner = self.inner.as_ref().expect("configured");
            (inner.worker_state.clone(), inner.running.clone())
        };

        WorkerInner::wait_for_all_tasks_status(
            worker_state,
            running.clone(),
            StreamTaskStatus::Opened,
            Some(10),
        )
        .await;

        println!("[WORKER] All tasks opened, running tasks");

        self.signal_tasks_run()
            .await
            .expect("signal run after open");

        println!("[WORKER] Tasks running, waiting for all tasks to be finished");

        let (worker_state, running) = {
            let inner = self.inner.as_ref().expect("configured");
            (inner.worker_state.clone(), inner.running.clone())
        };

        WorkerInner::wait_for_all_tasks_status(
            worker_state,
            running,
            StreamTaskStatus::Finished,
            None,
        )
        .await;

        println!("[WORKER] All tasks finished, sending close signal");
        self.signal_tasks_close()
            .await
            .expect("signal close after finish");

        println!("[WORKER] Waiting for all tasks to be closed");

        let (worker_state, running) = {
            let inner = self.inner.as_ref().expect("configured");
            (inner.worker_state.clone(), inner.running.clone())
        };

        WorkerInner::wait_for_all_tasks_status(
            worker_state,
            running,
            StreamTaskStatus::Closed,
            Some(10),
        )
        .await;

        println!("[WORKER] All tasks closed, cleaning up");

        self.close_async().await;

        println!("[WORKER] Worker execution completed");
    }
}

impl WorkerInner {
    pub(crate) async fn signal_tasks_close(&mut self) {
        self.send_signal_to_task_actors(crate::runtime::stream_task::StreamTaskMessage::Close)
            .await;
    }
}
