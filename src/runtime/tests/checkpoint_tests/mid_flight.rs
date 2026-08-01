//! Kill while a checkpoint is in-flight (before it can complete).

use anyhow::{anyhow, Result};

use crate::runtime::master::LifecycleEvent;
use crate::runtime::tests::cluster_harness::{
    LifecycleOracle, PipelineLaunchSpec, RecoveryReport, RuntimeEnv, TestCluster, WorkerKillMode,
};
use crate::runtime::tests::recovery_tests::RecoveryTimeouts;

use super::sink_oracle::assert_sink_matches_offline_datagen;
use super::support::{
    harness_finish_pipeline, shutdown_after, wait_for_checkpoint_completed,
    wait_for_checkpoint_started, wait_until_attempt0_running,
};
use super::CheckpointWorkload;

fn report_has_checkpoint_failed(report: &RecoveryReport, checkpoint_id: u64) -> bool {
    let needle = format!("checkpoint_failed {checkpoint_id}");
    report
        .attempts
        .values()
        .any(|a| a.events.iter().any(|e| e.contains(&needle)))
}

fn report_has_checkpoint_completed(report: &RecoveryReport, checkpoint_id: u64) -> bool {
    let needle = format!("checkpoint_completed {checkpoint_id}");
    report
        .attempts
        .values()
        .any(|attempt| attempt.events.iter().any(|event| event.contains(&needle)))
}

/// Kill while the first checkpoint is in-flight (no prior complete). No sink EO check.
pub async fn run_checkpoint_mid_flight_kill_no_prior(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
    mode: WorkerKillMode,
) -> Result<RecoveryReport> {
    let timeouts = RecoveryTimeouts::for_env(env);
    let cluster = TestCluster::launch(env, launch).await?;
    let result = async {
        let target = cluster
            .worker_ids()
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("expected at least one worker"))?;
        let mut cursor = 0;

        cluster.start_execution().await?;
        wait_until_attempt0_running(&cluster.master(), &mut cursor, timeouts.attempt_running)
            .await?;

        // Kill on CheckpointStarted (in-flight). Waiting for BarrierInjected races completion:
        // with flush-on-barrier, CP1 often completes before kill → restore=Some(1).
        let in_flight_id = wait_for_checkpoint_started(
            &cluster.master(),
            &mut cursor,
            timeouts.attempt_running,
            1,
        )
        .await?;

        println!("[TEST] mid-flight kill (no prior) worker={target} in_flight={in_flight_id}");
        cluster
            .worker(&target)
            .ok_or_else(|| anyhow!("missing worker {target}"))?
            .kill_with(mode)
            .await?;

        let started = LifecycleOracle::wait_for(
            &cluster.master(),
            &mut cursor,
            timeouts.recovery_started + timeouts.replacement + timeouts.attempt1_running,
            |event| {
                matches!(
                    event,
                    LifecycleEvent::AttemptStarted { attempt_id: 1, .. }
                )
            },
        )
        .await?;
        match &started.event {
            LifecycleEvent::AttemptStarted {
                restore_checkpoint_id: Option::None,
                ..
            } => {}
            LifecycleEvent::AttemptStarted {
                restore_checkpoint_id: Some(id),
                ..
            } if env != RuntimeEnv::Kube || *id != in_flight_id => {
                return Err(anyhow!(
                    "expected restore=None after mid-flight kill with no prior complete CP, got Some({id}) \
                     (in-flight {in_flight_id} likely completed before kill)"
                ));
            }
            LifecycleEvent::AttemptStarted { .. } => {}
            other => {
                return Err(anyhow!("expected AttemptStarted, got {other:?}"));
            }
        }

        LifecycleOracle::wait_for(
            &cluster.master(),
            &mut cursor,
            timeouts.attempt1_running,
            |event| {
                matches!(
                    event,
                    LifecycleEvent::AttemptRunning { attempt_id: 1, .. }
                )
            },
        )
        .await?;

        harness_finish_pipeline(&cluster, &mut cursor, env, 1).await?;

        let events = cluster.master().lifecycle_events_since(0).await?;
        if env != RuntimeEnv::Kube && events.iter().any(|r| {
            matches!(
                &r.event,
                LifecycleEvent::CheckpointCompleted {
                    checkpoint_id
                } if *checkpoint_id == in_flight_id
            )
        }) {
            RecoveryReport::from_events(&events).print();
            return Err(anyhow!(
                "in-flight checkpoint {in_flight_id} must not CheckpointCompleted before mid-flight kill recovery"
            ));
        }

        let report = RecoveryReport::from_events(&events);
        report.print();
        if env == RuntimeEnv::Kube {
            assert_mid_flight_restore_none_or_in_flight(&report, in_flight_id)?;
        } else {
            assert_mid_flight_restore_none(&report, in_flight_id)?;
        }
        Ok(report)
    }
    .await;
    shutdown_after(&cluster, result).await
}

/// Complete CP1, kill during in-flight CP2, restore from CP1, sink/offline check.
pub async fn run_checkpoint_mid_flight_kill_after_safe(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
    mode: WorkerKillMode,
) -> Result<RecoveryReport> {
    let timeouts = RecoveryTimeouts::for_env(env);
    let cluster = TestCluster::launch(env, launch).await?;
    let result = async {
        let target = cluster
            .worker_ids()
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("expected at least one worker"))?;
        let mut cursor = 0;

        cluster.start_execution().await?;
        wait_until_attempt0_running(&cluster.master(), &mut cursor, timeouts.attempt_running)
            .await?;

        let safe_id = wait_for_checkpoint_completed(
            &cluster.master(),
            &mut cursor,
            timeouts.attempt_running,
        )
        .await?;

        // Kill on CheckpointStarted (still a race on kube: pod may finish the CP
        // before force-delete / health poll lands — fail fast below if restore advances).
        let in_flight_id = wait_for_checkpoint_started(
            &cluster.master(),
            &mut cursor,
            timeouts.attempt_running,
            safe_id + 1,
        )
        .await?;

        println!(
            "[TEST] mid-flight kill (after safe={safe_id}) worker={target} in_flight={in_flight_id}"
        );
        cluster
            .worker(&target)
            .ok_or_else(|| anyhow!("missing worker {target}"))?
            .kill_with(mode)
            .await?;

        // Wait for any attempt-1 start, then assert restore id (do not filter in the
        // predicate — a lost race with restore=Some(in_flight) would hang until timeout).
        let started = LifecycleOracle::wait_for(
            &cluster.master(),
            &mut cursor,
            timeouts.recovery_started + timeouts.replacement + timeouts.attempt1_running,
            |event| {
                matches!(
                    event,
                    LifecycleEvent::AttemptStarted { attempt_id: 1, .. }
                )
            },
        )
        .await?;
        match &started.event {
            LifecycleEvent::AttemptStarted {
                restore_checkpoint_id: Some(id),
                ..
            } if *id == safe_id => {}
            LifecycleEvent::AttemptStarted {
                restore_checkpoint_id: Some(id),
                ..
            } if env != RuntimeEnv::Kube || (*id != safe_id && *id != in_flight_id) => {
                return Err(anyhow!(
                    "expected restore=Some({safe_id}) after mid-flight kill of in-flight {in_flight_id}, \
                     got Some({id}) (in-flight likely completed before kube pod death / failure detection)"
                ));
            }
            LifecycleEvent::AttemptStarted {
                restore_checkpoint_id: Option::None,
                ..
            } => {
                return Err(anyhow!(
                    "expected restore=Some({safe_id}) after mid-flight kill, got None"
                ));
            }
            LifecycleEvent::AttemptStarted { .. } => {}
            other => {
                return Err(anyhow!("expected AttemptStarted, got {other:?}"));
            }
        }

        LifecycleOracle::wait_for(
            &cluster.master(),
            &mut cursor,
            timeouts.attempt1_running,
            |event| {
                matches!(
                    event,
                    LifecycleEvent::AttemptRunning { attempt_id: 1, .. }
                )
            },
        )
        .await?;

        harness_finish_pipeline(&cluster, &mut cursor, env, 1).await?;

        let snapshot = cluster.storage().snapshot().await?;
        assert_sink_matches_offline_datagen(
            &cluster.master(),
            &snapshot,
            env,
            CheckpointWorkload::PassThrough,
        )
        .await?;

        let events = cluster.master().lifecycle_events_since(0).await?;
        if env != RuntimeEnv::Kube && events.iter().any(|r| {
            matches!(
                &r.event,
                LifecycleEvent::CheckpointCompleted {
                    checkpoint_id
                } if *checkpoint_id == in_flight_id
            )
        }) {
            RecoveryReport::from_events(&events).print();
            return Err(anyhow!(
                "in-flight checkpoint {in_flight_id} must not CheckpointCompleted"
            ));
        }

        let report = RecoveryReport::from_events(&events);
        report.print();
        if env == RuntimeEnv::Kube {
            assert_mid_flight_restore_prior_or_in_flight(&report, safe_id, in_flight_id)?;
        } else {
            assert_mid_flight_restore_prior(&report, safe_id, in_flight_id)?;
        }
        Ok(report)
    }
    .await;
    shutdown_after(&cluster, result).await
}

/// Mid-flight kill with no completed CP: attempt 1 restores None; in-flight was failed.
pub fn assert_mid_flight_restore_none(
    report: &RecoveryReport,
    in_flight_id: u64,
) -> Result<()> {
    let attempt1 = report.attempt(1)?;
    if !attempt1.trigger.contains("restore=None") {
        return Err(anyhow!(
            "attempt 1 should restore=None after mid-flight kill with no prior CP: {}",
            attempt1.trigger
        ));
    }
    if !report_has_checkpoint_failed(report, in_flight_id) {
        return Err(anyhow!(
            "expected checkpoint_failed {in_flight_id} after mid-flight kill"
        ));
    }
    Ok(())
}

fn assert_mid_flight_restore_none_or_in_flight(
    report: &RecoveryReport,
    in_flight_id: u64,
) -> Result<()> {
    let attempt1 = report.attempt(1)?;
    let completed = report_has_checkpoint_completed(report, in_flight_id);
    if completed && attempt1.trigger.contains(&format!("restore=Some({in_flight_id})")) {
        return Ok(());
    }
    if !completed && attempt1.trigger.contains("restore=None") {
        if report_has_checkpoint_failed(report, in_flight_id) {
            return Ok(());
        }
        return Err(anyhow!(
            "in-flight checkpoint {in_flight_id} did not complete or fail"
        ));
    }
    Err(anyhow!(
        "in-flight checkpoint {in_flight_id} completed={completed}, but attempt 1 was {}",
        attempt1.trigger
    ))
}

/// Mid-flight kill after a safe CP: attempt 1 restores that CP; in-flight CP2+ was failed.
pub fn assert_mid_flight_restore_prior(
    report: &RecoveryReport,
    safe_checkpoint_id: u64,
    in_flight_id: u64,
) -> Result<()> {
    let attempt1 = report.attempt(1)?;
    if !attempt1
        .trigger
        .contains(&format!("restore=Some({safe_checkpoint_id})"))
    {
        return Err(anyhow!(
            "attempt 1 should restore=Some({safe_checkpoint_id}): {}",
            attempt1.trigger
        ));
    }
    if !report_has_checkpoint_failed(report, in_flight_id) {
        return Err(anyhow!(
            "expected checkpoint_failed {in_flight_id} after mid-flight kill"
        ));
    }
    Ok(())
}

fn assert_mid_flight_restore_prior_or_in_flight(
    report: &RecoveryReport,
    safe_checkpoint_id: u64,
    in_flight_id: u64,
) -> Result<()> {
    let attempt1 = report.attempt(1)?;
    let completed = report_has_checkpoint_completed(report, in_flight_id);
    if completed && attempt1.trigger.contains(&format!("restore=Some({in_flight_id})")) {
        return Ok(());
    }
    if !completed && attempt1
        .trigger
        .contains(&format!("restore=Some({safe_checkpoint_id})"))
    {
        if report_has_checkpoint_failed(report, in_flight_id) {
            return Ok(());
        }
        return Err(anyhow!(
            "in-flight checkpoint {in_flight_id} did not complete or fail"
        ));
    }
    Err(anyhow!(
        "in-flight checkpoint {in_flight_id} completed={completed}, but attempt 1 was {}",
        attempt1.trigger
    ))
}
