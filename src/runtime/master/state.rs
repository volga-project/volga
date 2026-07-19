use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration, Instant};

use crate::api::PipelineSpec;
use crate::orchestrator::orchestrator::{MasterOrchestrator, WorkerNode};
use crate::runtime::consts::{
    runtime_consts, MASTER_CHECKPOINT_RETENTION, MASTER_REGISTRY_WAIT_TICK,
};
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::observability::snapshot_types::PipelineSnapshot;
use crate::runtime::operators::operator::operator_config_requires_checkpoint;

use super::checkpoint::{
    CheckpointAckOutcome, CheckpointStartError, Checkpoints, TaskKey,
};
use super::events::{
    CheckpointPropagationPhase, LifecycleEvent, LifecycleEventRecord, LifecycleJournal,
};
use super::MasterConfig;

pub(super) struct PipelineContext {
    pub pipeline_id: String,
    pub spec: PipelineSpec,
    pub execution_graph: ExecutionGraph,
    pub expected_workers: usize,
}

#[derive(Default)]
struct WorkerRecord {
    discovered: Option<WorkerNode>,
    registered: bool,
    replacing: bool,
    /// Execution attempt this worker is assigned to while running; cleared on recover/finish.
    execution_attempt_id: Option<u64>,
}

#[derive(Default)]
struct WorkerRegistry {
    workers: HashMap<String, WorkerRecord>,
}

enum WorkerReadiness {
    Ready(HashMap<String, WorkerNode>),
    Waiting {
        replacement_candidates: HashSet<String>,
    },
}

impl WorkerRegistry {
    fn register(&mut self, worker_id: String) {
        let record = self.workers.entry(worker_id).or_default();
        record.registered = true;
        record.replacing = false;
    }

    fn reconcile_readiness(
        &mut self,
        nodes: HashMap<String, WorkerNode>,
        expected: usize,
    ) -> WorkerReadiness {
        for record in self.workers.values_mut() {
            record.discovered = None;
        }
        for (worker_id, node) in nodes {
            self.workers.entry(worker_id).or_default().discovered = Some(node);
        }

        let mut ready: Vec<_> = self
            .workers
            .iter()
            .filter_map(|(worker_id, record)| {
                if record.registered && !record.replacing {
                    record
                        .discovered
                        .clone()
                        .map(|node| (worker_id.clone(), node))
                } else {
                    None
                }
            })
            .collect();
        if ready.len() < expected {
            let replacement_candidates = self
                .workers
                .iter()
                .filter(|(_, record)| {
                    record.discovered.is_some() && (!record.registered || record.replacing)
                })
                .map(|(worker_id, _)| worker_id.clone())
                .collect();
            return WorkerReadiness::Waiting {
                replacement_candidates,
            };
        }
        ready.sort_by(|(left, _), (right, _)| left.cmp(right));
        ready.truncate(expected);
        WorkerReadiness::Ready(ready.into_iter().collect())
    }

    fn mark_replacing(&mut self, worker_ids: &[String]) {
        for worker_id in worker_ids {
            let record = self.workers.entry(worker_id.clone()).or_default();
            record.registered = false;
            record.replacing = true;
            record.execution_attempt_id = None;
        }
    }

    /// Assign exactly `worker_ids` to `execution_attempt_id` (clears others).
    fn set_execution_attempt(&mut self, execution_attempt_id: u64, worker_ids: &[String]) {
        let selected: HashSet<&str> = worker_ids.iter().map(String::as_str).collect();
        for (worker_id, record) in self.workers.iter_mut() {
            record.execution_attempt_id = if selected.contains(worker_id.as_str()) {
                Some(execution_attempt_id)
            } else {
                None
            };
        }
    }

    fn clear_execution_attempt(&mut self) {
        for record in self.workers.values_mut() {
            record.execution_attempt_id = None;
        }
    }
}

pub(super) struct WorkerReadinessError {
    pub replacement_candidates: HashSet<String>,
    expected: usize,
}

impl fmt::Display for WorkerReadinessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "timed out waiting for {} ready workers; unready={:?}",
            self.expected, self.replacement_candidates
        )
    }
}

pub(super) struct MasterState {
    config: Mutex<Option<MasterConfig>>,
    checkpoints: Mutex<Checkpoints>,
    pub orchestrator: Arc<dyn MasterOrchestrator>,
    workers: Mutex<WorkerRegistry>,
    latest_pipeline_snapshot: Mutex<Option<PipelineSnapshot>>,
    lifecycle_events: Mutex<LifecycleJournal>,
    lifecycle_event_tx: broadcast::Sender<LifecycleEventRecord>,
    current_attempt_id: AtomicU64,
}

impl MasterState {
    pub(super) fn new(orchestrator: Arc<dyn MasterOrchestrator>) -> Self {
        let (lifecycle_event_tx, _) = broadcast::channel(256);
        Self {
            config: Mutex::new(None),
            checkpoints: Mutex::new(Checkpoints::default()),
            orchestrator,
            workers: Mutex::new(WorkerRegistry::default()),
            latest_pipeline_snapshot: Mutex::new(None),
            lifecycle_events: Mutex::new(LifecycleJournal::default()),
            lifecycle_event_tx,
            current_attempt_id: AtomicU64::new(0),
        }
    }

    /// Assign the scheduled worker set to `execution_attempt_id` (registry SoT).
    pub(super) async fn set_workers_execution_attempt(
        &self,
        execution_attempt_id: u64,
        worker_ids: &[String],
    ) {
        self.workers
            .lock()
            .await
            .set_execution_attempt(execution_attempt_id, worker_ids);
    }

    pub(super) async fn clear_workers_execution_attempt(&self) {
        self.workers.lock().await.clear_execution_attempt();
    }

    /// Workers on the current execution attempt, with control-plane endpoints.
    pub(super) async fn current_execution_worker_endpoints(
        &self,
    ) -> Option<(u64, Vec<(String, String)>)> {
        let execution_attempt_id = self.current_attempt_id();
        let workers = self.workers.lock().await;
        let mut endpoints: Vec<_> = workers
            .workers
            .iter()
            .filter_map(|(worker_id, record)| {
                if record.execution_attempt_id != Some(execution_attempt_id) {
                    return None;
                }
                let node = record.discovered.as_ref()?;
                Some((
                    worker_id.clone(),
                    format!("{}:{}", node.worker_ip, node.worker_port),
                ))
            })
            .collect();
        if endpoints.is_empty() {
            return None;
        }
        endpoints.sort_by(|(left, _), (right, _)| left.cmp(right));
        Some((execution_attempt_id, endpoints))
    }

    pub(super) fn checkpointable_tasks_for_graph(execution_graph: &ExecutionGraph) -> Vec<TaskKey> {
        execution_graph
            .get_vertices()
            .values()
            .filter(|vertex| operator_config_requires_checkpoint(&vertex.operator_config))
            .map(|vertex| TaskKey {
                vertex_id: vertex.vertex_id.as_ref().to_string(),
                task_index: vertex.task_index,
            })
            .collect()
    }

    pub(super) async fn configure(&self, config: MasterConfig) {
        let expected_acks = Self::checkpointable_tasks_for_graph(&config.execution_graph);
        let expected_aligns = config
            .execution_graph
            .all_tasks()
            .map(|(vertex_id, task_index)| TaskKey {
                vertex_id: vertex_id.as_ref().to_string(),
                task_index,
            })
            .collect();
        let retention = runtime_consts().u64(MASTER_CHECKPOINT_RETENTION) as usize;
        self.checkpoints.lock().await.configure(
            expected_acks.into_iter().collect(),
            expected_aligns,
            retention,
        );
        *self.config.lock().await = Some(config);
    }

    pub(super) async fn pipeline_context(&self) -> anyhow::Result<PipelineContext> {
        let config = self.config.lock().await.clone().ok_or_else(|| {
            anyhow::anyhow!("Master is not configured: call configure before execute")
        })?;
        let pipeline_id = self.orchestrator.get_pipeline_id().await;
        Ok(PipelineContext {
            pipeline_id,
            spec: config.spec,
            execution_graph: config.execution_graph,
            expected_workers: config.expected_workers,
        })
    }

    pub(super) async fn register_worker(&self, worker_id: String) {
        self.workers.lock().await.register(worker_id.clone());
        self.record_lifecycle_event(LifecycleEvent::WorkerRegistered { worker_id })
            .await;
    }

    pub(super) fn set_current_attempt_id(&self, attempt_id: u64) {
        self.current_attempt_id.store(attempt_id, Ordering::SeqCst);
    }

    pub(super) fn current_attempt_id(&self) -> u64 {
        self.current_attempt_id.load(Ordering::SeqCst)
    }

    pub(super) async fn begin_checkpoint(
        &self,
        attempt_id: u64,
    ) -> Result<u64, CheckpointStartError> {
        let checkpoint_id = self.checkpoints.lock().await.start()?;
        self.record_lifecycle_event(LifecycleEvent::CheckpointStarted {
            checkpoint_id,
            attempt_id,
        })
        .await;
        Ok(checkpoint_id)
    }

    pub(super) async fn abort_in_flight_checkpoint(
        &self,
        attempt_id: u64,
        detail: String,
    ) -> Option<u64> {
        let checkpoint_id = self.checkpoints.lock().await.abort_in_flight();
        if let Some(checkpoint_id) = checkpoint_id {
            self.record_lifecycle_event(LifecycleEvent::CheckpointFailed {
                checkpoint_id,
                attempt_id,
                detail,
            })
            .await;
        }
        checkpoint_id
    }

    pub(super) async fn in_flight_checkpoint_timed_out(&self, timeout: Duration) -> Option<u64> {
        self.checkpoints
            .lock()
            .await
            .in_flight_timed_out(timeout)
    }

    /// Journal barrier progress and count it toward completion (with state acks).
    /// Drops stale attempts and unknown ids; rejects are soft so tasks are not failed.
    pub(super) async fn report_checkpoint_propagation(
        &self,
        checkpoint_id: u64,
        task: TaskKey,
        execution_attempt_id: u64,
        phase: CheckpointPropagationPhase,
    ) -> Result<(), String> {
        let current = self.current_attempt_id();
        if execution_attempt_id != current {
            return Ok(());
        }

        let outcome = {
            let mut cps = self.checkpoints.lock().await;
            // Only in-flight CPs accept barrier progress (Completed implies align already done).
            if cps.in_flight_id() != Some(checkpoint_id) {
                return Ok(());
            }
            cps.note_barrier_progress(checkpoint_id, task.clone())
        };

        self.record_lifecycle_event(LifecycleEvent::CheckpointPropagation {
            checkpoint_id,
            attempt_id: execution_attempt_id,
            vertex_id: task.vertex_id,
            task_index: task.task_index,
            phase,
        })
        .await;

        if matches!(outcome, CheckpointAckOutcome::Completed) {
            self.record_lifecycle_event(LifecycleEvent::CheckpointCompleted { checkpoint_id })
                .await;
        }
        Ok(())
    }

    pub(super) async fn report_checkpoint(
        &self,
        checkpoint_id: u64,
        task: TaskKey,
        blobs: Vec<(String, Vec<u8>)>,
        execution_attempt_id: u64,
    ) -> Result<(), String> {
        let current = self.current_attempt_id();
        if execution_attempt_id != current {
            return Err(format!(
                "stale checkpoint report attempt={execution_attempt_id} current={current}"
            ));
        }
        if blobs.is_empty() {
            return Err(format!(
                "empty checkpoint blob list for {}:{}",
                task.vertex_id, task.task_index
            ));
        }
        if let Some((name, _)) = blobs.iter().find(|(_, bytes)| bytes.is_empty()) {
            return Err(format!(
                "empty checkpoint blob '{name}' for {}:{}",
                task.vertex_id, task.task_index
            ));
        }

        let outcome = self
            .checkpoints
            .lock()
            .await
            .report(checkpoint_id, task, blobs);

        match outcome {
            CheckpointAckOutcome::Completed => {
                self.record_lifecycle_event(LifecycleEvent::CheckpointCompleted { checkpoint_id })
                    .await;
                Ok(())
            }
            CheckpointAckOutcome::Pending => Ok(()),
            CheckpointAckOutcome::Rejected(reason) => Err(format!(
                "checkpoint ack rejected for checkpoint_id={checkpoint_id}: {reason:?}"
            )),
        }
    }

    pub(super) async fn task_checkpoint(
        &self,
        checkpoint_id: u64,
        task: TaskKey,
    ) -> Vec<(String, Vec<u8>)> {
        self.checkpoints.lock().await.get(checkpoint_id, &task)
    }

    pub(super) async fn latest_complete_checkpoint(&self) -> Option<u64> {
        self.checkpoints.lock().await.latest_complete()
    }

    pub(super) async fn publish_snapshot(&self, snapshot: PipelineSnapshot) {
        *self.latest_pipeline_snapshot.lock().await = Some(snapshot);
    }

    pub(super) async fn latest_pipeline_snapshot(&self) -> Option<PipelineSnapshot> {
        self.latest_pipeline_snapshot.lock().await.clone()
    }

    pub(super) async fn record_lifecycle_event(&self, event: LifecycleEvent) {
        let record = self.lifecycle_events.lock().await.record(event);
        let _ = self.lifecycle_event_tx.send(record.clone());
        if let Ok(event_json) = serde_json::to_string(&record.event) {
            let orchestrator = self.orchestrator.clone();
            tokio::spawn(async move {
                let _ = orchestrator
                    .record_lifecycle_event(record.sequence, &event_json)
                    .await;
            });
        }
    }

    pub(super) async fn lifecycle_events_since(&self, sequence: u64) -> Vec<LifecycleEventRecord> {
        self.lifecycle_events.lock().await.since(sequence)
    }

    pub(super) fn subscribe_lifecycle_events(
        &self,
    ) -> broadcast::Receiver<LifecycleEventRecord> {
        self.lifecycle_event_tx.subscribe()
    }

    pub(super) async fn wait_for_ready_workers(
        &self,
        expected: usize,
        timeout: Duration,
    ) -> Result<HashMap<String, WorkerNode>, WorkerReadinessError> {
        println!("[MASTER] Waiting for {} ready workers", expected);
        let start = Instant::now();
        loop {
            let discovered = self.orchestrator.get_worker_nodes().await;
            let readiness = {
                let mut workers = self.workers.lock().await;
                workers.reconcile_readiness(discovered, expected)
            };
            match readiness {
                WorkerReadiness::Ready(workers) => return Ok(workers),
                WorkerReadiness::Waiting {
                    replacement_candidates,
                } if start.elapsed() > timeout => {
                    return Err(WorkerReadinessError {
                        replacement_candidates,
                        expected,
                    });
                }
                WorkerReadiness::Waiting { .. } => {}
            }
            sleep(runtime_consts().duration(MASTER_REGISTRY_WAIT_TICK)).await;
        }
    }

    pub(super) async fn request_replacement(&self, worker_ids: &[String]) -> anyhow::Result<()> {
        self.workers.lock().await.mark_replacing(worker_ids);
        self.record_lifecycle_event(LifecycleEvent::ReplacementRequested {
            worker_ids: worker_ids.to_vec(),
        })
        .await;
        self.orchestrator.request_replacement(worker_ids).await
    }
}
