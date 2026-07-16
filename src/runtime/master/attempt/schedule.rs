use std::collections::{HashMap, HashSet};

use crate::orchestrator::orchestrator::WorkerNode;
use crate::orchestrator::task_assignment::{build_mapping, worker_to_tasks, TaskWorkerMapping};
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::consts::{runtime_consts, MASTER_DISCOVERY_TIMEOUT, MASTER_REPLACEMENT_TIMEOUT};

use super::super::events::LifecycleEvent;
use super::super::worker_client::{WorkerCallError, WorkerClient};
use super::{ExecutionAttempt, ScheduleError};

impl ExecutionAttempt {
    pub(in crate::runtime::master) async fn schedule(&mut self) -> Result<(), ScheduleError> {
        let readiness_timeout = if self.id == 0 {
            runtime_consts().duration(MASTER_DISCOVERY_TIMEOUT)
        } else {
            runtime_consts().duration(MASTER_REPLACEMENT_TIMEOUT)
        };
        let nodes = match self
            .state
            .wait_for_ready_workers(self.pipeline.expected_workers, readiness_timeout)
            .await
        {
            Ok(nodes) => nodes,
            // Always recoverable: named peers get replaced; empty candidates means
            // "nobody Ready yet" (e.g. kube STS gap) — retry another wait window
            // under the recovery budget instead of failing the pipeline.
            Err(error) => {
                let detail = error.to_string();
                return Err(ScheduleError::Recoverable {
                    replace: error.replacement_candidates,
                    detail,
                });
            }
        };
        println!(
            "[MASTER] Scheduling workers={} attempt={} restore={:?}",
            nodes.len(),
            self.id,
            self.restore_checkpoint_id
        );
        let mapping = build_mapping(&self.pipeline.spec, &self.pipeline.execution_graph, &nodes)
            .map_err(|error| ScheduleError::Terminal(error.to_string()))?;
        self.connect_all(&nodes).await?;
        let endpoints = nodes
            .iter()
            .map(|(worker_id, node)| super::super::state::ActiveWorkerEndpoint {
                worker_id: worker_id.clone(),
                worker_ip: format!("{}:{}", node.worker_ip, node.worker_port),
            })
            .collect();
        self.state.set_active_worker_endpoints(endpoints).await;
        self.configure_all(&mapping).await?;
        self.start_all().await?;
        self.wait_opened().await?;
        self.start_heartbeats();
        self.run_all().await?;
        let worker_ids: Vec<String> = nodes.keys().cloned().collect();
        self.state
            .record_lifecycle_event(LifecycleEvent::AttemptScheduled {
                attempt_id: self.id,
                worker_ids: worker_ids.clone(),
            })
            .await;
        self.state
            .record_lifecycle_event(LifecycleEvent::AttemptRunning {
                attempt_id: self.id,
                worker_ids,
            })
            .await;
        Ok(())
    }

    async fn configure_all(
        &self,
        mapping: &TaskWorkerMapping,
    ) -> Result<(), ScheduleError> {
        let worker_tasks = worker_to_tasks(mapping);
        let futures = self.clients.iter().map(|(worker_id, client)| {
            let worker_id = worker_id.clone();
            let pipeline_id = self.pipeline.pipeline_id.clone();
            let spec = self.pipeline.spec.clone();
            let vertex_ids = worker_tasks.get(&worker_id).cloned().unwrap_or_default();
            let mapping = mapping.clone();
            async move {
                let result = client.configure(
                    worker_id.clone(),
                    pipeline_id,
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
                Err(error) => errors.push(Err(ScheduleError::Recoverable {
                    replace: HashSet::from([worker_id.clone()]),
                    detail: format!("{}: {}", worker_id, error),
                })),
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
        self.clients.clear();
        let execution_attempt_id = self.id;
        let futures = nodes.iter().map(|(worker_id, node)| {
            let worker_id = worker_id.clone();
            let worker_addr = format!("{}:{}", node.worker_ip, node.worker_port);
            async move {
                let result = WorkerClient::open(
                    &worker_id,
                    worker_addr,
                    execution_attempt_id,
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
                Err(error) => errors.push(Err(ScheduleError::Recoverable {
                    replace: HashSet::from([worker_id.clone()]),
                    detail: format!("{}: {}", worker_id, error),
                })),
            }
        }
        merge_errors(errors)
    }

    fn start_heartbeats(&mut self) {
        for (worker_id, client) in &mut self.clients {
            client.start_heartbeat(worker_id.clone(), self.failure_tx.clone());
        }
    }

    async fn start_all(&self) -> Result<(), ScheduleError> {
        let futures = self.clients.iter().map(|(worker_id, client)| async move {
            client
                .start_worker()
                .await
                .map_err(|error| ScheduleError::Recoverable {
                    replace: HashSet::from([worker_id.clone()]),
                    detail: format!("{}: {}", worker_id, error),
                })
        });
        merge_errors(futures::future::join_all(futures).await)
    }

    async fn wait_opened(&self) -> Result<(), ScheduleError> {
        match self.wait_status(StreamTaskStatus::Opened).await {
            Ok(()) => Ok(()),
            Err(workers) => Err(ScheduleError::Recoverable {
                replace: workers,
                detail: format!("workers did not reach {:?}", StreamTaskStatus::Opened),
            }),
        }
    }

    async fn run_all(&self) -> Result<(), ScheduleError> {
        let futures = self.clients.iter().map(|(worker_id, client)| async move {
            client
                .run_worker_tasks()
                .await
                .map_err(|error| ScheduleError::Recoverable {
                    replace: HashSet::from([worker_id.clone()]),
                    detail: format!("{}: {}", worker_id, error),
                })
        });
        merge_errors(futures::future::join_all(futures).await)
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
