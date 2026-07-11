use std::collections::{HashMap, HashSet};

use crate::orchestrator::orchestrator::WorkerNode;
use crate::orchestrator::task_assignment::{build_mapping, worker_to_tasks, TaskWorkerMapping};
use crate::runtime::observability::StreamTaskStatus;

use super::super::state::{DISCOVERY_TIMEOUT, REPLACEMENT_TIMEOUT};
use super::super::worker_client::{WorkerCallError, WorkerClient};
use super::{ExecutionAttempt, ScheduleError};

impl ExecutionAttempt {
    pub(in crate::runtime::master) async fn schedule(&mut self) -> Result<(), ScheduleError> {
        let readiness_timeout = if self.id == 0 {
            DISCOVERY_TIMEOUT
        } else {
            REPLACEMENT_TIMEOUT
        };
        let nodes = match self
            .state
            .wait_for_ready_workers(self.pipeline.expected_workers, readiness_timeout)
            .await
        {
            Ok(nodes) => nodes,
            Err(error) if !error.replacement_candidates.is_empty() => {
                let detail = error.to_string();
                return Err(ScheduleError::Recoverable {
                    replace: error.replacement_candidates,
                    detail,
                });
            }
            Err(error) => return Err(ScheduleError::Terminal(error.to_string())),
        };
        println!(
            "[MASTER] Scheduling workers={} attempt={} restore={:?}",
            nodes.len(),
            self.id,
            self.restore_checkpoint_id
        );

        let mapping = build_mapping(&self.pipeline.spec, &self.pipeline.execution_graph, &nodes)
            .map_err(|error| ScheduleError::Terminal(error.to_string()))?;
        self.configure_all(&nodes, &mapping).await?;

        self.clients.clear();
        self.connect_all(&nodes).await?;
        self.start_all().await?;
        self.wait_opened().await?;
        self.run_all().await
    }

    async fn configure_all(
        &self,
        nodes: &HashMap<String, WorkerNode>,
        mapping: &TaskWorkerMapping,
    ) -> Result<(), ScheduleError> {
        let worker_tasks = worker_to_tasks(mapping);
        let futures = nodes.iter().map(|(worker_id, node)| {
            let worker_id = worker_id.clone();
            let worker_addr = format!("{}:{}", node.worker_ip, node.worker_port);
            let pipeline_id = self.pipeline.pipeline_id.clone();
            let spec = self.pipeline.spec.clone();
            let vertex_ids = worker_tasks.get(&worker_id).cloned().unwrap_or_default();
            let mapping = mapping.clone();
            async move {
                let result = WorkerClient::configure(
                    worker_id.clone(),
                    worker_addr,
                    pipeline_id,
                    self.id,
                    spec,
                    vertex_ids,
                    mapping,
                    self.restore_checkpoint_id,
                )
                .await;
                (worker_id, result)
            }
        });

        let mut signatures = HashSet::new();
        let mut errors = Vec::new();
        for (worker_id, result) in futures::future::join_all(futures).await {
            match result {
                Ok(signature) if signature.is_empty() => {
                    return Err(ScheduleError::Terminal(
                        "empty execution graph signature".into(),
                    ));
                }
                Ok(signature) => {
                    signatures.insert(signature);
                }
                Err(WorkerCallError::Rejected(detail)) => {
                    return Err(ScheduleError::Terminal(format!(
                        "configure rejected by {}: {}",
                        worker_id, detail
                    )));
                }
                Err(error) => errors.push(Err(recoverable(&worker_id, error))),
            }
        }
        merge_errors(errors)?;

        if signatures.len() != 1 {
            return Err(ScheduleError::Terminal(
                "execution graph signature mismatch across workers".into(),
            ));
        }
        Ok(())
    }

    async fn connect_all(
        &mut self,
        nodes: &HashMap<String, WorkerNode>,
    ) -> Result<(), ScheduleError> {
        let failure_tx = self.failure_tx.clone();
        let execution_attempt_id = self.id;
        let futures = nodes.iter().map(|(worker_id, node)| {
            let worker_id = worker_id.clone();
            let worker_addr = format!("{}:{}", node.worker_ip, node.worker_port);
            let failure_tx = failure_tx.clone();
            async move {
                let result = WorkerClient::connect(
                    worker_id.clone(),
                    worker_addr,
                    execution_attempt_id,
                    failure_tx,
                )
                .await;
                (worker_id, result)
            }
        });

        let mut errors = Vec::new();
        for (worker_id, result) in futures::future::join_all(futures).await {
            match result {
                Ok(client) => {
                    self.clients.insert(worker_id, client);
                }
                Err(error) => errors.push(Err(recoverable(&worker_id, error))),
            }
        }
        merge_errors(errors)
    }

    async fn start_all(&self) -> Result<(), ScheduleError> {
        let futures = self.clients.iter().map(|(worker_id, client)| async move {
            client
                .start_worker()
                .await
                .map_err(|error| recoverable(worker_id, error))
        });
        merge_errors(futures::future::join_all(futures).await)
    }

    async fn wait_opened(&self) -> Result<(), ScheduleError> {
        let Err(error) = self.wait_status(StreamTaskStatus::Opened).await else {
            return Ok(());
        };

        let poll = self.poll_states().await;
        let replace = poll
            .failures
            .iter()
            .filter(|(_, error)| error.requires_replacement())
            .map(|(worker_id, _)| worker_id.clone())
            .collect();
        Err(ScheduleError::Recoverable {
            replace,
            detail: error.to_string(),
        })
    }

    async fn run_all(&self) -> Result<(), ScheduleError> {
        let futures = self.clients.iter().map(|(worker_id, client)| async move {
            client
                .run_worker_tasks()
                .await
                .map_err(|error| recoverable(worker_id, error))
        });
        merge_errors(futures::future::join_all(futures).await)
    }
}

fn recoverable(worker_id: &str, error: WorkerCallError) -> ScheduleError {
    let mut replace = HashSet::new();
    if error.requires_replacement() {
        replace.insert(worker_id.to_string());
    }
    ScheduleError::Recoverable {
        replace,
        detail: format!("{}: {}", worker_id, error),
    }
}

fn merge_errors(results: Vec<Result<(), ScheduleError>>) -> Result<(), ScheduleError> {
    let mut replace = HashSet::new();
    let mut details = Vec::new();
    for result in results {
        match result {
            Ok(()) => {}
            Err(ScheduleError::Terminal(message)) => {
                return Err(ScheduleError::Terminal(message));
            }
            Err(ScheduleError::Recoverable {
                replace: failed,
                detail,
            }) => {
                replace.extend(failed);
                details.push(detail);
            }
        }
    }
    if details.is_empty() {
        return Ok(());
    }
    Err(ScheduleError::Recoverable {
        replace,
        detail: details.join("; "),
    })
}
