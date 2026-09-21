use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use kameo::actor::ActorRef;
use kameo::spawn;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration, Instant};

use crate::api::PipelineSpec;
use crate::common::types::PipelineId;
use crate::orchestrator::orchestrator::{MasterOrchestrator, WorkerNode};
use crate::runtime::checkpoint::{RestorePlan, SerializedCheckpoint};
use crate::runtime::consts::{runtime_consts, MASTER_REGISTRY_WAIT_TICK};
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::metrics::{
    increment_pipeline_counter, record_pipeline_histogram, METRIC_CHECKPOINT_COMPLETED,
    METRIC_CHECKPOINT_DURATION_MS, METRIC_CHECKPOINT_FAILED,
};
use crate::runtime::observability::snapshot_types::PipelineSnapshot;
use crate::runtime::operators::operator::{
    operator_config_requires_checkpoint, OperatorType,
};

use super::attempt::ExecutionAttempt;
use super::checkpoint::{
    create_checkpoint_store, AbortInFlightCheckpoint, CheckpointAckOutcome, CheckpointCoordinator,
    CheckpointStartError, ConfigureCheckpoints, InFlightCheckpointId, InFlightCheckpointTimedOut,
    LatestCompleteCheckpoint, LoadCheckpoint, NoteBarrierProgress, ReportCheckpoint,
    RestorePlanner, StartCheckpoint, TaskKey,
};
use super::events::{
    CheckpointPropagationPhase, LifecycleEvent, LifecycleEventRecord, LifecycleJournal,
};
use super::lifecycle::MasterLifecycle;
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
    checkpoints: ActorRef<CheckpointCoordinator>,
    pub orchestrator: Arc<dyn MasterOrchestrator>,
    workers: Mutex<WorkerRegistry>,
    latest_pipeline_snapshot: Mutex<Option<PipelineSnapshot>>,
    lifecycle_events: Mutex<LifecycleJournal>,
    lifecycle_event_tx: broadcast::Sender<LifecycleEventRecord>,
    current_attempt_id: AtomicU64,
    /// Live attempt (lifecycle supervises; Drain goes through lifecycle intent).
    current_attempt: Mutex<Option<ActorRef<ExecutionAttempt>>>,
    /// Job supervisor actor (`RequestFinish` / attempt loop).
    lifecycle: Mutex<Option<ActorRef<MasterLifecycle>>>,
}

impl MasterState {
    pub(super) fn new(orchestrator: Arc<dyn MasterOrchestrator>) -> Self {
        let (lifecycle_event_tx, _) = broadcast::channel(256);
        Self {
            config: Mutex::new(None),
            checkpoints: spawn(CheckpointCoordinator::default()),
            orchestrator,
            workers: Mutex::new(WorkerRegistry::default()),
            latest_pipeline_snapshot: Mutex::new(None),
            lifecycle_events: Mutex::new(LifecycleJournal::default()),
            lifecycle_event_tx,
            current_attempt_id: AtomicU64::new(0),
            current_attempt: Mutex::new(None),
            lifecycle: Mutex::new(None),
        }
    }

    pub(super) async fn set_current_attempt(&self, attempt: ActorRef<ExecutionAttempt>) {
        *self.current_attempt.lock().await = Some(attempt);
    }

    pub(super) async fn clear_current_attempt(&self) {
        *self.current_attempt.lock().await = None;
    }

    pub(super) async fn current_attempt(&self) -> Option<ActorRef<ExecutionAttempt>> {
        self.current_attempt.lock().await.clone()
    }

    pub(super) async fn set_lifecycle(&self, lifecycle: ActorRef<MasterLifecycle>) {
        *self.lifecycle.lock().await = Some(lifecycle);
    }

    pub(super) async fn clear_lifecycle(&self) {
        *self.lifecycle.lock().await = None;
    }

    pub(super) async fn lifecycle(&self) -> Option<ActorRef<MasterLifecycle>> {
        self.lifecycle.lock().await.clone()
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

    /// Tasks a barrier can reach: the downstream closure of checkpointable sources.
    ///
    /// Request-mode read-path vertices (HTTP source, WRO, request sink) sit in a
    /// disjoint component and never see a barrier (#301).
    pub(super) fn alignable_tasks_for_graph(execution_graph: &ExecutionGraph) -> Vec<TaskKey> {
        let mut pending: Vec<crate::runtime::VertexId> = execution_graph
            .get_vertices()
            .values()
            .filter(|vertex| {
                operator_config_requires_checkpoint(&vertex.operator_config)
                    && vertex.operator_config.role() == OperatorType::Source
            })
            .map(|vertex| vertex.vertex_id.clone())
            .collect();

        let mut reachable: HashSet<crate::runtime::VertexId> =
            pending.iter().cloned().collect();
        while let Some(vertex_id) = pending.pop() {
            let Some((_, outputs)) = execution_graph.get_edges_for_vertex(vertex_id.as_ref()) else {
                continue;
            };
            for edge in outputs {
                if reachable.insert(edge.target_vertex_id.clone()) {
                    pending.push(edge.target_vertex_id.clone());
                }
            }
        }

        execution_graph
            .get_vertices()
            .values()
            .filter(|vertex| reachable.contains(&vertex.vertex_id))
            .map(|vertex| TaskKey {
                vertex_id: vertex.vertex_id.as_ref().to_string(),
                task_index: vertex.task_index,
            })
            .collect()
    }

    pub(super) async fn configure(&self, config: MasterConfig) {
        let pipeline_id = PipelineId(self.orchestrator.get_pipeline_id().await);
        let checkpoint_store = create_checkpoint_store(&config.spec.state.checkpoint_store);
        let expected_acks = Self::checkpointable_tasks_for_graph(&config.execution_graph);
        let expected_aligns = Self::alignable_tasks_for_graph(&config.execution_graph)
            .into_iter()
            .collect();
        let retention = config.spec.state.checkpoint.retention().max(1);
        self.checkpoints
            .ask(ConfigureCheckpoints {
                pipeline_id,
                expected_acks: expected_acks.into_iter().collect(),
                expected_aligns,
                retention,
                store: checkpoint_store,
            })
            .await
            .expect("checkpoint coordinator");
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
        // kameo flattens `Result` replies: Ok(id) / Err(SendError::HandlerError(start_err)).
        let checkpoint_id = match self.checkpoints.ask(StartCheckpoint).await {
            Ok(checkpoint_id) => checkpoint_id,
            Err(error) => return Err(error.unwrap_err()),
        };
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
    ) -> Result<Option<u64>, String> {
        let aborted = self
            .checkpoints
            .ask(AbortInFlightCheckpoint)
            .await
            .map_err(|error| format!("checkpoint coordinator: {error}"))?;
        if let Some(checkpoint_id) = aborted {
            let pipeline_id = self.orchestrator.get_pipeline_id().await;
            // Duration stays success-only so abort/timeouts do not inflate the histogram.
            increment_pipeline_counter(METRIC_CHECKPOINT_FAILED, 1, &pipeline_id);
            self.record_lifecycle_event(LifecycleEvent::CheckpointFailed {
                checkpoint_id,
                attempt_id,
                detail,
            })
            .await;
            Ok(Some(checkpoint_id))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn in_flight_checkpoint_timed_out(&self, timeout: Duration) -> Option<u64> {
        self.checkpoints
            .ask(InFlightCheckpointTimedOut(timeout))
            .await
            .ok()
            .flatten()
    }

    pub(super) async fn in_flight_checkpoint_id(&self) -> Option<u64> {
        self.checkpoints
            .ask(InFlightCheckpointId)
            .await
            .ok()
            .flatten()
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

        let outcome = self
            .checkpoints
            .ask(NoteBarrierProgress {
                checkpoint_id,
                task: task.clone(),
            })
            .await
            .map_err(|error| format!("failed to update checkpoint store: {error}"))?;
        let Some(outcome) = outcome else {
            return Ok(());
        };

        self.record_lifecycle_event(LifecycleEvent::CheckpointPropagation {
            checkpoint_id,
            attempt_id: execution_attempt_id,
            vertex_id: task.vertex_id,
            task_index: task.task_index,
            phase,
        })
        .await;

        if let CheckpointAckOutcome::Completed { duration_ms } = outcome {
            let pipeline_id = self.orchestrator.get_pipeline_id().await;
            record_pipeline_histogram(
                METRIC_CHECKPOINT_DURATION_MS,
                duration_ms as f64,
                &pipeline_id,
            );
            increment_pipeline_counter(METRIC_CHECKPOINT_COMPLETED, 1, &pipeline_id);
            self.record_lifecycle_event(LifecycleEvent::CheckpointCompleted { checkpoint_id })
                .await;
        }
        Ok(())
    }

    pub(super) async fn report_checkpoint(
        &self,
        checkpoint_id: u64,
        task: TaskKey,
        checkpoint: SerializedCheckpoint,
        execution_attempt_id: u64,
    ) -> Result<(), String> {
        let current = self.current_attempt_id();
        if execution_attempt_id != current {
            return Err(format!(
                "stale checkpoint report attempt={execution_attempt_id} current={current}"
            ));
        }
        {
            let config = self.config.lock().await;
            let config = config
                .as_ref()
                .ok_or_else(|| "master is not configured".to_string())?;
            let vertex = config
                .execution_graph
                .get_vertex(&task.vertex_id)
                .ok_or_else(|| format!("unknown checkpoint task {}", task.vertex_id))?;
            if vertex.task_index != task.task_index
                || !operator_config_requires_checkpoint(&vertex.operator_config)
            {
                return Err(format!(
                    "checkpoint data does not match task {} index={}",
                    task.vertex_id, task.task_index
                ));
            }
        }
        if checkpoint.is_empty() {
            return Err(format!(
                "empty checkpoint data for {} index={}",
                task.vertex_id, task.task_index
            ));
        }
        let outcome = self
            .checkpoints
            .ask(ReportCheckpoint {
                checkpoint_id,
                task,
                checkpoint,
            })
            .await
            .map_err(|error| format!("failed to update checkpoint store: {error}"))?;

        match outcome {
            CheckpointAckOutcome::Completed { duration_ms } => {
                let pipeline_id = self.orchestrator.get_pipeline_id().await;
                record_pipeline_histogram(
                    METRIC_CHECKPOINT_DURATION_MS,
                    duration_ms as f64,
                    &pipeline_id,
                );
                increment_pipeline_counter(METRIC_CHECKPOINT_COMPLETED, 1, &pipeline_id);
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

    pub(super) async fn plan_restore(&self, checkpoint_id: u64) -> Result<RestorePlan, String> {
        let completed = self
            .checkpoints
            .ask(LoadCheckpoint(checkpoint_id))
            .await
            .map_err(|error| format!("failed to load checkpoint: {error}"))?;
        let target_graph = {
            let config = self.config.lock().await;
            let config = config
                .as_ref()
                .ok_or_else(|| "master is not configured".to_string())?;
            config.execution_graph.clone()
        };
        RestorePlanner::plan(completed, &target_graph)
            .map_err(|error| format!("failed to plan restore: {error}"))
    }

    pub(super) async fn latest_complete_checkpoint(&self) -> Option<u64> {
        self.checkpoints
            .ask(LatestCompleteCheckpoint)
            .await
            .ok()
            .flatten()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::prelude::SessionContext;

    use crate::api::planner::{Planner, PlanningContext};
    use crate::api::ExecutionMode;
    use crate::runtime::functions::source::datagen_source::{DatagenSourceConfig, DatagenSpec};
    use crate::runtime::functions::source::RequestSourceSinkSpec;
    use crate::runtime::operators::operator::OperatorConfig;
    use crate::runtime::operators::sink::sink_operator::SinkConfig;
    use crate::runtime::operators::source::source_operator::SourceConfig;

    const WINDOW_SQL: &str = "SELECT
            event_time,
            key,
            value,
            SUM(value) OVER (
                PARTITION BY key
                ORDER BY event_time
                RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW
            ) as sum_value
        FROM events";

    fn window_graph(mode: ExecutionMode) -> ExecutionGraph {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let datagen = DatagenSourceConfig::new(
            schema.clone(),
            DatagenSpec {
                rate: Some(1.0),
                limit: None,
                run_for_s: Some(1.0),
                batch_size: 1,
                fields: HashMap::new(),
                replayable: true,
            },
        );
        let mut planner = Planner::new(
            PlanningContext::new(SessionContext::new())
                .with_parallelism(2)
                .with_execution_mode(mode),
        );
        planner.register_source(
            "events".to_string(),
            SourceConfig::DatagenSourceConfig(datagen),
            schema,
        );
        if mode == ExecutionMode::Request {
            planner.register_request_source_sink(
                SourceConfig::HttpRequestSourceConfig(
                    crate::runtime::functions::source::RequestSourceConfig::new(
                        RequestSourceSinkSpec {
                            bind_address: "127.0.0.1:0".to_string(),
                            max_pending_requests: 8,
                            request_timeout_ms: 1_000,
                            schema_json: None,
                            sink: None,
                        },
                    ),
                ),
                Some(SinkConfig::RequestSinkConfig),
            );
        }
        planner.sql_to_graph(WINDOW_SQL).unwrap().to_execution_graph()
    }

    fn task_keys(tasks: Vec<TaskKey>) -> HashSet<String> {
        tasks.into_iter().map(|t| t.vertex_id).collect()
    }

    fn is_request_io(config: &OperatorConfig) -> bool {
        matches!(
            config,
            OperatorConfig::SourceConfig(SourceConfig::HttpRequestSourceConfig(_))
                | OperatorConfig::SinkConfig(SinkConfig::RequestSinkConfig)
                | OperatorConfig::WindowRequestConfig(_)
        )
    }

    #[test]
    fn streaming_align_set_is_the_whole_graph() {
        let graph = window_graph(ExecutionMode::Streaming);
        let all: HashSet<_> = graph
            .all_tasks()
            .map(|(id, _)| id.as_ref().to_string())
            .collect();
        let aligns = task_keys(MasterState::alignable_tasks_for_graph(&graph));
        let acks = task_keys(MasterState::checkpointable_tasks_for_graph(&graph));
        assert_eq!(aligns, all);
        assert!(acks.is_subset(&aligns));
        assert!(!acks.is_empty());
    }

    #[test]
    fn request_mode_align_set_excludes_read_path() {
        let graph = window_graph(ExecutionMode::Request);
        let all: HashSet<_> = graph
            .all_tasks()
            .map(|(id, _)| id.as_ref().to_string())
            .collect();
        let aligns = task_keys(MasterState::alignable_tasks_for_graph(&graph));
        let acks = task_keys(MasterState::checkpointable_tasks_for_graph(&graph));

        let request_vertices: HashSet<_> = graph
            .get_vertices()
            .values()
            .filter(|v| is_request_io(&v.operator_config))
            .map(|v| v.vertex_id.as_ref().to_string())
            .collect();
        assert!(!request_vertices.is_empty());
        assert!(request_vertices.is_disjoint(&aligns));
        assert!(aligns.is_subset(&all));
        assert!(aligns.len() < all.len());
        assert!(acks.is_subset(&aligns));
        assert!(!acks.is_empty());
    }
}
