use std::collections::{HashMap, HashSet};

use kameo::actor::ActorRef;
use tokio::time::{
    interval_at, sleep, timeout, Duration, Instant, MissedTickBehavior,
};

use crate::common::failure::{workers_to_replace, FailureEvent, FailureKind};
use crate::orchestrator::orchestrator::WorkerHealthWatchHandle;
use crate::runtime::consts::{
    runtime_consts, MASTER_CHECKPOINT_INTERVAL, MASTER_CHECKPOINT_TIMEOUT,
    MASTER_FAILURE_AGGREGATION_WINDOW, MASTER_STATE_POLL_INTERVAL, MASTER_STATE_POLL_TIMEOUT,
};
use crate::runtime::observability::snapshot_types::{PipelineSnapshot, WorkerSnapshot};
use crate::runtime::observability::StreamTaskStatus;

use super::super::checkpoint::CheckpointStartError;
use super::super::events::LifecycleEvent;
use super::super::state::MasterState;
use super::super::worker_client::WorkerCallError;
use super::session::{map_session_error, GetWorkerState, TriggerBarrier, WorkerSession};
use super::{
    AggWindowEnd, AttemptOutcome, AttemptPhase, CheckpointBarriersDone, CheckpointTick,
    ExecutionAttempt, FailureMsg, PollResult, PollTick,
};

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
            &self.session_list(),
            &self.state,
            status,
            Some(STATUS_TIMEOUT),
        )
        .await
    }

    pub(super) fn run(
        &mut self,
        actor_ref: ActorRef<Self>,
    ) -> (tokio::task::JoinHandle<()>, Option<WorkerHealthWatchHandle>) {
        let failure_rx = self
            .failure_rx
            .take()
            .expect("failure_rx available once per attempt");
        let worker_ids: HashSet<_> = self.sessions.keys().cloned().collect();
        let health_poll = self
            .state
            .orchestrator
            .run_health_poll(worker_ids, self.failure_tx.clone());

        let poll_interval = runtime_consts().duration(MASTER_STATE_POLL_INTERVAL);
        let checkpoint_interval = runtime_consts().duration(MASTER_CHECKPOINT_INTERVAL);

        let run_loop = tokio::spawn(async move {
            let mut poll = interval_at(Instant::now() + poll_interval, poll_interval);
            let mut checkpoint_tick = optional_interval(checkpoint_interval);
            let mut failure_rx = failure_rx;
            loop {
                // Wait only. Checkpoint arm before poll so a slow get_worker_state
                // cannot starve CheckpointStarted under `biased`.
                tokio::select! {
                    biased;
                    failure = failure_rx.recv() => {
                        match failure {
                            Some(failure) => {
                                if actor_ref.tell(FailureMsg(failure)).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    _ = optional_tick(&mut checkpoint_tick) => {
                        if actor_ref.tell(CheckpointTick).await.is_err() {
                            break;
                        }
                    }
                    _ = poll.tick() => {
                        if actor_ref.tell(PollTick).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        (run_loop, health_poll)
    }

    pub(super) async fn on_poll_tick(&mut self, actor_ref: ActorRef<Self>) {
        {
            let Some(slot) = self.run.as_mut() else {
                return;
            };
            // Aggregation must not freeze observation; only one poll batch at a time.
            if slot.poll_in_flight {
                return;
            }
            slot.poll_in_flight = true;
        }
        let sessions = self.session_list();
        let state = self.state.clone();
        let poll_rpc_timeout = runtime_consts().duration(MASTER_STATE_POLL_TIMEOUT);
        tokio::spawn(async move {
            let poll = poll_session_states(&sessions, &state, poll_rpc_timeout).await;
            let _ = actor_ref.tell(PollResult(poll)).await;
        });
    }

    pub(super) async fn on_poll_result(&mut self, state_poll: StatePoll) {
        {
            let Some(slot) = self.run.as_mut() else {
                return;
            };
            slot.poll_in_flight = false;
        }

        // FailureAggregation owns Recover: fold poll failures into the window.
        if self.phase == AttemptPhase::FailureAggregation {
            for (worker_id, error) in state_poll.failures {
                let failure = FailureEvent {
                    worker_id,
                    kind: FailureKind::StatePollFailure,
                    detail: error.to_string(),
                };
                self.record_failure(&failure).await;
                if let Some(events) = self
                    .run
                    .as_mut()
                    .and_then(|slot| slot.failure_events.as_mut())
                {
                    events.push(failure);
                }
            }
            return;
        }

        let checkpoint_timeout = runtime_consts().duration(MASTER_CHECKPOINT_TIMEOUT);
        let draining = self.phase == AttemptPhase::Draining;
        if !state_poll.failures.is_empty() {
            for (worker_id, error) in &state_poll.failures {
                self.record_failure(&FailureEvent {
                    worker_id: worker_id.clone(),
                    kind: FailureKind::StatePollFailure,
                    detail: error.to_string(),
                })
                .await;
            }
            // Draining owns Finish: do not divert into Recover.
            if !draining {
                self.abort_in_flight("state poll failure").await;
                let replace = state_poll
                    .failures
                    .iter()
                    .map(|(worker_id, _)| worker_id.clone())
                    .collect();
                self.complete_run(Ok(AttemptOutcome::Recover(replace)));
                return;
            }
        }
        if all_workers(&state_poll.states, self.sessions.keys(), |s| {
            s.all_tasks_in(&[StreamTaskStatus::Finished, StreamTaskStatus::Closed])
        }) {
            self.abort_in_flight("pipeline finished with in-flight checkpoint")
                .await;
            self.complete_run(Ok(AttemptOutcome::Finished));
            return;
        }
        if let Some(checkpoint_id) = self
            .state
            .in_flight_checkpoint_timed_out(checkpoint_timeout)
            .await
        {
            self.abort_in_flight(format!("checkpoint {checkpoint_id} timed out"))
                .await;
            println!(
                "[MASTER] Checkpoint timeout attempt={} checkpoint_id={}",
                self.id, checkpoint_id
            );
            if draining {
                return;
            }
            self.complete_run(Ok(AttemptOutcome::Recover(HashSet::new())));
            return;
        }
        if self.phase == AttemptPhase::Checkpointing
            && self.state.in_flight_checkpoint_id().await.is_none()
        {
            self.phase.clear_checkpointing();
        }
    }

    pub(super) async fn on_checkpoint_tick(&mut self, actor_ref: ActorRef<Self>) {
        if self.run.is_none() || self.phase != AttemptPhase::Running {
            return;
        }
        let checkpoint_id = match self.state.begin_checkpoint(self.id).await {
            Ok(checkpoint_id) => checkpoint_id,
            Err(error) => {
                let detail = match error {
                    CheckpointStartError::AlreadyInFlight { checkpoint_id } => {
                        format!("already in flight checkpoint_id={checkpoint_id}")
                    }
                    CheckpointStartError::NoCheckpointableTasks => {
                        "no checkpointable tasks".to_string()
                    }
                };
                println!(
                    "[MASTER] Interval checkpoint skipped attempt={}: {}",
                    self.id, detail
                );
                if self.state.in_flight_checkpoint_id().await.is_some() {
                    self.phase = AttemptPhase::Checkpointing;
                }
                return;
            }
        };
        // Own phase before session RPCs so Drain/Failure can abort without waiting
        // on barrier fan-out.
        self.phase = AttemptPhase::Checkpointing;
        println!(
            "[MASTER] Triggering checkpoint {} attempt={}",
            checkpoint_id, self.id
        );
        let sessions = self.session_list();
        tokio::spawn(async move {
            let error = trigger_checkpoint_barriers(sessions, checkpoint_id).await;
            let _ = actor_ref
                .tell(CheckpointBarriersDone {
                    checkpoint_id,
                    error,
                })
                .await;
        });
    }

    pub(super) async fn on_checkpoint_barriers_done(&mut self, msg: CheckpointBarriersDone) {
        if self.run.is_none() {
            return;
        }
        let Some(error) = msg.error else {
            return;
        };
        if self.phase == AttemptPhase::Draining {
            println!(
                "[MASTER] Checkpoint {} barrier fan-out finished during drain attempt={}",
                msg.checkpoint_id, self.id
            );
            return;
        }
        self.abort_in_flight(error.clone()).await;
        println!(
            "[MASTER] Interval checkpoint skipped attempt={}: {}",
            self.id, error
        );
        self.phase.clear_checkpointing();
    }

    pub(super) async fn on_failure(
        &mut self,
        failure: FailureEvent,
        actor_ref: ActorRef<Self>,
    ) {
        if self.run.is_none() {
            return;
        }
        // Draining owns Finish; late fatals are recorded only.
        if self.phase == AttemptPhase::Draining {
            self.record_failure(&failure).await;
            return;
        }
        if self.phase == AttemptPhase::FailureAggregation {
            self.record_failure(&failure).await;
            if let Some(events) = self
                .run
                .as_mut()
                .and_then(|slot| slot.failure_events.as_mut())
            {
                events.push(failure);
            }
            return;
        }

        self.abort_in_flight("attempt failed").await;
        self.record_failure(&failure).await;
        self.phase = AttemptPhase::FailureAggregation;
        if let Some(slot) = self.run.as_mut() {
            slot.failure_events = Some(vec![failure]);
        }

        let window = runtime_consts().duration(MASTER_FAILURE_AGGREGATION_WINDOW);
        tokio::spawn(async move {
            sleep(window).await;
            let _ = actor_ref.tell(AggWindowEnd).await;
        });
    }

    pub(super) async fn on_agg_window_end(&mut self) {
        if self.phase != AttemptPhase::FailureAggregation {
            return;
        }
        let Some(slot) = self.run.as_mut() else {
            return;
        };
        let Some(events) = slot.failure_events.take() else {
            return;
        };

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
            if let Some(session) = self.sessions.remove(worker_id) {
                let _ = session.stop_gracefully().await;
            }
        }
        self.complete_run(Ok(AttemptOutcome::Recover(replace)));
    }

    async fn abort_in_flight(&self, reason: impl Into<String>) {
        let _ = self
            .state
            .abort_in_flight_checkpoint(self.id, reason.into())
            .await;
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
    sessions: &[(String, ActorRef<WorkerSession>)],
    state: &MasterState,
    status: StreamTaskStatus,
    timeout: Option<Duration>,
) -> Result<(), HashSet<String>> {
    let start = Instant::now();
    let poll_rpc_timeout = runtime_consts().duration(MASTER_STATE_POLL_TIMEOUT);
    loop {
        let poll = poll_session_states(sessions, state, poll_rpc_timeout).await;
        if !poll.failures.is_empty() {
            return Err(poll
                .failures
                .into_iter()
                .map(|(worker_id, _)| worker_id)
                .collect());
        }
        if all_workers(
            &poll.states,
            sessions.iter().map(|(worker_id, _)| worker_id),
            |s| s.all_tasks_in(&[status]),
        ) {
            return Ok(());
        }
        if let Some(timeout) = timeout {
            if start.elapsed() > timeout {
                return Err(sessions
                    .iter()
                    .map(|(worker_id, _)| worker_id)
                    .filter(|worker_id| {
                        !poll
                            .states
                            .get(*worker_id)
                            .map(|s| s.all_tasks_in(&[status]))
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect());
            }
        }
        sleep(STATUS_POLL).await;
    }
}

/// Fan out barrier triggers via sessions; returns the first worker error if any.
async fn trigger_checkpoint_barriers(
    sessions: Vec<(String, ActorRef<WorkerSession>)>,
    checkpoint_id: u64,
) -> Option<String> {
    let futures = sessions.into_iter().map(|(worker_id, session)| async move {
        let result = session.ask(TriggerBarrier(checkpoint_id)).await;
        (worker_id, result)
    });
    for (worker_id, result) in futures::future::join_all(futures).await {
        match result {
            Ok(true) => {}
            Ok(false) => return Some(format!("trigger rejected by {worker_id}")),
            Err(error) => return Some(format!("trigger failed on {worker_id}: {error:?}")),
        }
    }
    None
}

fn optional_interval(period: Duration) -> Option<tokio::time::Interval> {
    if period.is_zero() {
        None
    } else {
        let mut interval = interval_at(Instant::now() + period, period);
        // Skip backlog after a slow begin_checkpoint; Burst would wake the
        // checkpoint arm in a tight loop once the arm is re-enabled.
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Some(interval)
    }
}

async fn optional_tick(tick: &mut Option<tokio::time::Interval>) {
    match tick.as_mut() {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

async fn poll_session_states(
    sessions: &[(String, ActorRef<WorkerSession>)],
    state: &MasterState,
    rpc_timeout: Duration,
) -> StatePoll {
    let futures = sessions.iter().map(|(worker_id, session)| {
        let worker_id = worker_id.clone();
        let session = session.clone();
        async move {
            let result = match timeout(rpc_timeout, session.ask(GetWorkerState)).await {
                Ok(result) => result.map_err(map_session_error),
                Err(_) => Err(WorkerCallError::Unreachable(format!(
                    "get_worker_state timed out after {rpc_timeout:?}"
                ))),
            };
            (worker_id, result)
        }
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

fn all_workers<'a>(
    states: &HashMap<String, WorkerSnapshot>,
    worker_ids: impl IntoIterator<Item = &'a String>,
    pred: impl Fn(&WorkerSnapshot) -> bool,
) -> bool {
    let mut any = false;
    for worker_id in worker_ids {
        any = true;
        if !states.get(worker_id).map(&pred).unwrap_or(false) {
            return false;
        }
    }
    any
}
