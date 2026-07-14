use std::collections::{HashMap, HashSet};

use tokio::time::{interval_at, sleep, timeout, Duration, Instant};

use crate::runtime::consts::{runtime_consts, MASTER_FAILURE_AGGREGATION_WINDOW};
use crate::runtime::observability::snapshot_types::{PipelineSnapshot, WorkerSnapshot};
use crate::runtime::observability::StreamTaskStatus;

use super::super::failure::{workers_to_replace, FailureEvent, FailureKind};
use super::super::state::MasterState;
use super::super::events::LifecycleEvent;
use super::super::worker_client::{WorkerCallError, WorkerClient};
use super::{AttemptOutcome, ExecutionAttempt};

const POLL: Duration = Duration::from_millis(100);
const STATUS_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct StatePoll {
    pub states: HashMap<String, WorkerSnapshot>,
    pub failures: Vec<(String, WorkerCallError)>,
}

impl ExecutionAttempt {
    pub(super) async fn wait_status(
        &self,
        status: StreamTaskStatus,
    ) -> Result<(), HashSet<String>> {
        wait_for_status(
            &self.clients,
            &self.state,
            status,
            Some(STATUS_TIMEOUT),
        )
        .await
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
                    return Ok(self.await_failure_window_and_recover(failure).await);
                }
                state_poll = async {
                    poll.tick().await;
                    poll_client_states(&self.clients, &self.state).await
                } => {
                    if !state_poll.failures.is_empty() {
                        for (worker_id, error) in &state_poll.failures {
                            self.record_failure(&FailureEvent {
                                worker_id: worker_id.clone(),
                                kind: FailureKind::StatePoll,
                                detail: error.to_string(),
                            })
                            .await;
                        }
                        return Ok(execution_poll_outcome(&state_poll.failures));
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

    /// Record the first failure, wait the aggregation window for cascade fatals, then decide.
    async fn await_failure_window_and_recover(
        &mut self,
        first: FailureEvent,
    ) -> AttemptOutcome {
        let mut events = Vec::new();
        self.record_failure(&first).await;
        events.push(first);

        let window = runtime_consts().duration(MASTER_FAILURE_AGGREGATION_WINDOW);
        let deadline = Instant::now() + window;
        while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, self.failure_rx.recv()).await {
                Ok(Some(failure)) => {
                    self.record_failure(&failure).await;
                    events.push(failure);
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        let replace = workers_to_replace(&events);
        let involved: HashSet<_> = events.iter().map(|e| e.worker_id.clone()).collect();
        let reused: Vec<_> = involved
            .into_iter()
            .filter(|id| !replace.contains(id))
            .collect();
        println!(
            "[MASTER] Failure window done attempt={} events={} replace={:?} reusable={:?}",
            self.id,
            events.len(),
            replace,
            reused
        );
        for worker_id in &replace {
            self.clients.remove(worker_id);
        }
        AttemptOutcome::Recover(replace)
    }

    async fn record_failure(&self, failure: &FailureEvent) {
        self.state
            .record_lifecycle_event(LifecycleEvent::WorkerFailure {
                attempt_id: self.id,
                worker_id: failure.worker_id.clone(),
                kind: format!("{:?}", failure.kind),
                detail: failure.detail.clone(),
            })
            .await;
        println!(
            "[MASTER] Failure worker={} attempt={} kind={:?} ({})",
            failure.worker_id, self.id, failure.kind, failure.detail
        );
    }
}

async fn wait_for_status(
    clients: &HashMap<String, WorkerClient>,
    state: &MasterState,
    status: StreamTaskStatus,
    timeout: Option<Duration>,
) -> Result<(), HashSet<String>> {
    let start = Instant::now();
    loop {
        let poll = poll_client_states(clients, state).await;
        if !poll.failures.is_empty() {
            return Err(poll
                .failures
                .into_iter()
                .map(|(worker_id, _)| worker_id)
                .collect());
        }
        if all_have_status(&poll.states, clients, status) {
            return Ok(());
        }
        if let Some(timeout) = timeout {
            if start.elapsed() > timeout {
                return Err(clients
                    .keys()
                    .filter(|worker_id| {
                        !poll
                            .states
                            .get(*worker_id)
                            .map(|worker_state| {
                                !worker_state.task_statuses.is_empty()
                                    && worker_state.all_tasks_have_status(status)
                            })
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect());
            }
        }
        sleep(POLL).await;
    }
}

fn execution_poll_outcome(failures: &[(String, WorkerCallError)]) -> AttemptOutcome {
    let replace = failures
        .iter()
        .map(|(worker_id, _)| worker_id.clone())
        .collect();
    AttemptOutcome::Recover(replace)
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
