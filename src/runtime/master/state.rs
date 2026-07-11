use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, Instant};

use crate::api::PipelineSpec;
use crate::orchestrator::orchestrator::{MasterOrchestrator, WorkerNode};
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::observability::snapshot_types::{PipelineSnapshot, WorkerSnapshot};
use crate::runtime::operators::operator::operator_config_requires_checkpoint;

use super::checkpoint::{MasterCheckpointRegistry, TaskKey};
use super::MasterConfig;

pub(super) const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);
pub(super) const REPLACEMENT_TIMEOUT: Duration = Duration::from_secs(120);
const WAIT_TICK: Duration = Duration::from_millis(500);

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
    checkpoint_registry: Mutex<MasterCheckpointRegistry>,
    pub orchestrator: Arc<dyn MasterOrchestrator>,
    workers: Mutex<WorkerRegistry>,
    latest_pipeline_snapshot: Mutex<Option<PipelineSnapshot>>,
}

impl MasterState {
    pub(super) fn new(orchestrator: Arc<dyn MasterOrchestrator>) -> Self {
        Self {
            config: Mutex::new(None),
            checkpoint_registry: Mutex::new(MasterCheckpointRegistry::default()),
            orchestrator,
            workers: Mutex::new(WorkerRegistry::default()),
            latest_pipeline_snapshot: Mutex::new(None),
        }
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
        let expected_tasks = Self::checkpointable_tasks_for_graph(&config.execution_graph);
        self.checkpoint_registry.lock().await.expected_tasks = expected_tasks.into_iter().collect();
        *self.config.lock().await = Some(config);
    }

    pub(super) async fn pipeline_context(&self) -> anyhow::Result<PipelineContext> {
        let config = self.config.lock().await.clone().ok_or_else(|| {
            anyhow::anyhow!("Master is not configured: call configure before execute")
        })?;
        let spec = config.spec.ok_or_else(|| {
            anyhow::anyhow!(
                "Master is configured without spec: provide spec in MasterConfig before execute"
            )
        })?;
        let pipeline_id = self.orchestrator.get_pipeline_id().await;
        Ok(PipelineContext {
            pipeline_id,
            spec,
            execution_graph: config.execution_graph,
            expected_workers: config.expected_workers,
        })
    }

    pub(super) async fn register_worker(&self, worker_id: String) {
        self.workers.lock().await.register(worker_id);
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
            sleep(WAIT_TICK).await;
        }
    }

    pub(super) async fn request_replacement(&self, worker_ids: &[String]) -> anyhow::Result<()> {
        self.workers.lock().await.mark_replacing(worker_ids);
        self.orchestrator.request_replacement(worker_ids).await
    }

    pub(super) async fn publish_snapshot(&self, snapshot: PipelineSnapshot) {
        *self.latest_pipeline_snapshot.lock().await = Some(snapshot);
    }

    pub(super) async fn worker_states(&self) -> HashMap<String, WorkerSnapshot> {
        self.latest_pipeline_snapshot
            .lock()
            .await
            .as_ref()
            .map(|snapshot| snapshot.worker_states.clone())
            .unwrap_or_default()
    }

    pub(super) async fn latest_pipeline_snapshot(&self) -> Option<PipelineSnapshot> {
        self.latest_pipeline_snapshot.lock().await.clone()
    }

    pub(super) async fn report_checkpoint(
        &self,
        checkpoint_id: u64,
        task: TaskKey,
        blobs: Vec<(String, Vec<u8>)>,
    ) {
        let mut registry = self.checkpoint_registry.lock().await;
        registry.store.put(checkpoint_id, task.clone(), blobs);
        let expected_count = registry.expected_tasks.len();
        registry
            .coordinator
            .ack(checkpoint_id, task, expected_count);
    }

    pub(super) async fn task_checkpoint(
        &self,
        checkpoint_id: u64,
        task: TaskKey,
    ) -> Vec<(String, Vec<u8>)> {
        self.checkpoint_registry
            .lock()
            .await
            .store
            .checkpoint_snapshots
            .get(&(checkpoint_id, task))
            .cloned()
            .unwrap_or_default()
    }

    pub(super) async fn latest_complete_checkpoint(&self) -> Option<u64> {
        self.checkpoint_registry
            .lock()
            .await
            .coordinator
            .latest_complete()
    }
}
