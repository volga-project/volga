use std::collections::{HashMap, HashSet};

use tokio::time::{interval_at, sleep, timeout, Duration, Instant};

use crate::runtime::consts::{
    runtime_consts, MASTER_CHECKPOINT_INTERVAL, MASTER_CHECKPOINT_TIMEOUT,
    MASTER_FAILURE_AGGREGATION_WINDOW, MASTER_STATE_POLL_INTERVAL,
};
use crate::runtime::observability::snapshot_types::{PipelineSnapshot, WorkerSnapshot};
use crate::runtime::observability::StreamTaskStatus;

use crate::common::failure::{workers_to_replace, FailureEvent, FailureKind};
use super::super::checkpoint::CheckpointStartError;
use super::super::state::MasterState;
use super::super::events::LifecycleEvent;
use super::super::worker_client::{WorkerCallError, WorkerClient};
use super::{AttemptOutcome, ExecutionAttempt};

const STATUS_POLL: Duration = Duration::from_millis(100);
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
        let _health_poll = self.state.orchestrator.run_health_poll(
            self.clients.keys().cloned().collect(),
            self.failure_tx.clone(),
        );
        let poll_interval = runtime_consts().duration(MASTER_STATE_POLL_INTERVAL);
        let mut poll = interval_at(Instant::now() + poll_interval, poll_interval);
        let checkpoint_interval = runtime_consts().duration(MASTER_CHECKPOINT_INTERVAL);
        let checkpoint_timeout = runtime_consts().duration(MASTER_CHECKPOINT_TIMEOUT);
        let mut checkpoint_tick = if checkpoint_interval.is_zero() {
            None
        } else {
            Some(interval_at(
                Instant::now() + checkpoint_interval,
                checkpoint_interval,
            ))
        };
        let mut timeout_poll = tokio::time::interval(Duration::from_millis(50));
        timeout_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;

                failure = self.failure_rx.recv() => {
                    let failure =
                        failure.ok_or_else(|| anyhow::anyhow!("failure channel closed"))?;
                    let _ = self
                        .state
                        .abort_in_flight_checkpoint(self.id, "attempt failed".to_string())
                        .await;
                    return Ok(self.await_failure_window_and_recover(failure).await);
                }
                _ = timeout_poll.tick() => {
                    if let Some(checkpoint_id) = self
                        .state
                        .in_flight_checkpoint_timed_out(checkpoint_timeout)
                        .await
                    {
                        let _ = self
                            .state
                            .abort_in_flight_checkpoint(
                                self.id,
                                format!("checkpoint {checkpoint_id} timed out"),
                            )
                            .await;
                        println!(
                            "[MASTER] Checkpoint timeout attempt={} checkpoint_id={}",
                            self.id, checkpoint_id
                        );
                        return Ok(AttemptOutcome::Recover(HashSet::new()));
                    }
                }
                _ = async {
                    if let Some(tick) = checkpoint_tick.as_mut() {
                        tick.tick().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    if let Err(error) = self.begin_and_fanout_checkpoint().await {
                        println!(
                            "[MASTER] Interval checkpoint skipped attempt={}: {}",
                            self.id, error
                        );
                    }
                }
                state_poll = async {
                    poll.tick().await;
                    poll_client_states(&self.clients, &self.state).await
                } => {
                    if !state_poll.failures.is_empty() {
                        for (worker_id, error) in &state_poll.failures {
                            self.record_failure(&FailureEvent {
                                worker_id: worker_id.clone(),
                                kind: FailureKind::StatePollFailure,
                                detail: error.to_string(),
                            })
                            .await;
                        }
                        let _ = self
                            .state
                            .abort_in_flight_checkpoint(self.id, "state poll failure".to_string())
                            .await;
                        return Ok(execution_poll_outcome(&state_poll.failures));
                    }
                    if all_have_status(
                        &state_poll.states,
                        &self.clients,
                        StreamTaskStatus::Finished,
                    ) {
                        let _ = self
                            .state
                            .abort_in_flight_checkpoint(
                                self.id,
                                "pipeline finished with in-flight checkpoint".to_string(),
                            )
                            .await;
                        return Ok(AttemptOutcome::Finished);
                    }
                }
            }
        }
    }

    async fn begin_and_fanout_checkpoint(&self) -> Result<u64, String> {
        if !self.state.has_checkpointable_tasks().await {
            return Err("no checkpointable tasks".to_string());
        }
        let checkpoint_id = self
            .state
            .begin_checkpoint(self.id)
            .await
            .map_err(|error| match error {
                CheckpointStartError::AlreadyInFlight { checkpoint_id } => {
                    format!("already in flight checkpoint_id={checkpoint_id}")
                }
                CheckpointStartError::NoCheckpointableTasks => {
                    "no checkpointable tasks".to_string()
                }
            })?;
        println!(
            "[MASTER] Triggering checkpoint {} attempt={}",
            checkpoint_id, self.id
        );
        let futures = self.clients.iter().map(|(worker_id, client)| {
            let worker_id = worker_id.clone();
            async move {
                (
                    worker_id,
                    client.trigger_checkpoint_barrier(checkpoint_id).await,
                )
            }
        });
        for (worker_id, result) in futures::future::join_all(futures).await {
            match result {
                Ok(true) => {}
                Ok(false) => {
                    let _ = self
                        .state
                        .abort_in_flight_checkpoint(
                            self.id,
                            format!("trigger rejected by {worker_id}"),
                        )
                        .await;
                    return Err(format!("trigger rejected by {worker_id}"));
                }
                Err(error) => {
                    let _ = self
                        .state
                        .abort_in_flight_checkpoint(
                            self.id,
                            format!("trigger failed on {worker_id}: {error}"),
                        )
                        .await;
                    return Err(format!("trigger failed on {worker_id}: {error}"));
                }
            }
        }
        Ok(checkpoint_id)
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
        sleep(STATUS_POLL).await;
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
