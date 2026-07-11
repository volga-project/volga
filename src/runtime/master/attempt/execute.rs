use std::collections::{HashMap, HashSet};

use tokio::time::{interval_at, sleep, Duration, Instant};

use crate::runtime::observability::snapshot_types::{PipelineSnapshot, WorkerSnapshot};
use crate::runtime::observability::StreamTaskStatus;

use super::super::state::MasterState;
use super::super::worker_client::{WorkerCallError, WorkerClient};
use super::{AttemptOutcome, ExecutionAttempt};

const POLL: Duration = Duration::from_millis(100);
const STATUS_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct StatePoll {
    pub states: HashMap<String, WorkerSnapshot>,
    pub failures: Vec<(String, WorkerCallError)>,
}

impl ExecutionAttempt {
    pub(super) async fn poll_states(&self) -> StatePoll {
        poll_client_states(&self.clients, &self.state).await
    }

    pub(super) async fn wait_status(&self, status: StreamTaskStatus) -> anyhow::Result<()> {
        let start = Instant::now();
        loop {
            let poll = self.poll_states().await;
            if !poll.failures.is_empty() {
                return Err(anyhow::anyhow!(
                    "worker state polling failed: {}",
                    format_failures(&poll.failures)
                ));
            }
            if all_have_status(&poll.states, &self.clients, status) {
                return Ok(());
            }
            if start.elapsed() > STATUS_TIMEOUT {
                return Err(anyhow::anyhow!(
                    "timed out waiting for {:?} on {:?}",
                    status,
                    self.clients.keys().collect::<Vec<_>>()
                ));
            }
            sleep(POLL).await;
        }
    }

    pub(in crate::runtime::master) async fn run(&mut self) -> anyhow::Result<AttemptOutcome> {
        let mut poll = interval_at(Instant::now() + POLL, POLL);
        loop {
            tokio::select! {
                biased;

                failure = self.failure_rx.recv() => {
                    let failure =
                        failure.ok_or_else(|| anyhow::anyhow!("failure channel closed"))?;
                    // TODO: We may have a race where pipeline is finished and at the same time we somehow get failure signal
                    // resulting in unnecesery pipeline restart
                    println!(
                        "[MASTER] Failure worker={} attempt={} ({})",
                        failure.worker_id,
                        self.id,
                        failure.detail
                    );
                    let mut replace = HashSet::new();
                    if failure.kind.requires_replacement() {
                        self.clients.remove(&failure.worker_id);
                        replace.insert(failure.worker_id);
                    }
                    return Ok(AttemptOutcome::Recover(replace));
                }
                state_poll = async {
                    poll.tick().await;
                    poll_client_states(&self.clients, &self.state).await
                } => {
                    if !state_poll.failures.is_empty() {
                        let replace = state_poll
                            .failures
                            .iter()
                            .filter(|(_, error)| error.requires_replacement())
                            .map(|(worker_id, _)| worker_id.clone())
                            .collect();
                        println!(
                            "[MASTER] Worker state polling failed attempt={}: {}",
                            self.id,
                            format_failures(&state_poll.failures)
                        );
                        return Ok(AttemptOutcome::Recover(replace));
                    }
                    if all_have_status(
                        &state_poll.states,
                        &self.clients,
                        StreamTaskStatus::Finished,
                    ) {
                        return Ok(AttemptOutcome::Finished);
                    }
                }
            }
        }
    }
}

async fn poll_client_states(
    clients: &HashMap<String, WorkerClient>,
    state: &MasterState,
) -> StatePoll {
    let futures = clients.iter().map(|(worker_id, client)| {
        let worker_id = worker_id.clone();
        async move { (worker_id, client.get_worker_state().await) }
    });
    let mut states = HashMap::new();
    let mut failures = Vec::new();
    for (worker_id, result) in futures::future::join_all(futures).await {
        match result {
            Ok(worker_state) => {
                states.insert(worker_state.worker_id.clone(), worker_state);
            }
            Err(error) => failures.push((worker_id, error)),
        }
    }
    state
        .publish_snapshot(PipelineSnapshot::new(states.clone()))
        .await;
    StatePoll { states, failures }
}

fn all_have_status(
    states: &HashMap<String, WorkerSnapshot>,
    clients: &HashMap<String, WorkerClient>,
    status: StreamTaskStatus,
) -> bool {
    !clients.is_empty()
        && clients.keys().all(|worker_id| {
            states
                .get(worker_id)
                .map(|state| !state.task_statuses.is_empty() && state.all_tasks_have_status(status))
                .unwrap_or(false)
        })
}

fn format_failures(failures: &[(String, WorkerCallError)]) -> String {
    failures
        .iter()
        .map(|(worker_id, error)| format!("{}: {}", worker_id, error))
        .collect::<Vec<_>>()
        .join("; ")
}
