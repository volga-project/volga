use std::time::Duration;

use anyhow::{anyhow, Result};

use crate::api::CheckpointSpec;
use crate::runtime::master::LifecycleEvent;
use crate::runtime::master::LifecycleEventRecord;

use super::dump::{PromQueryRange, Sample};
use super::spec::{
    resolved_checkpoint_interval, resolved_checkpoint_timeout, OracleConfig,
};

#[derive(Clone, Debug)]
pub struct KillWindow {
    pub at_unix: f64,
    pub restore_at_unix: f64,
    pub after_checkpoint: u64,
    /// Lifecycle sequences from kill through restore (inclusive).
    pub seq_lo: u64,
    pub seq_hi: u64,
}

impl KillWindow {
    pub fn contains(&self, ts: f64, extra: Duration) -> bool {
        let lo = self.at_unix;
        let hi = self.restore_at_unix + extra.as_secs_f64();
        ts >= lo && ts <= hi
    }

    pub fn contains_seq(&self, sequence: u64) -> bool {
        sequence >= self.seq_lo && sequence <= self.seq_hi
    }
}

#[derive(Clone, Debug, Default)]
pub struct PromSeriesSet {
    pub checkpoint_completed: Vec<Sample>,
    pub checkpoint_failed: Vec<Sample>,
    pub records_sent_rate: Vec<Sample>,
    pub watermark_lag_p99: Vec<Sample>,
}

impl PromSeriesSet {
    pub fn from_dump(dump: &serde_json::Value) -> Result<Self> {
        let series = dump
            .get("series")
            .and_then(|v| v.as_object())
            .ok_or_else(|| anyhow!("dump missing series"))?;
        Ok(Self {
            checkpoint_completed: parse_named(series, "checkpoint_completed")?,
            checkpoint_failed: parse_named(series, "checkpoint_failed")?,
            records_sent_rate: parse_named(series, "records_sent_rate")?,
            watermark_lag_p99: parse_named(series, "watermark_lag_p99")?,
        })
    }
}

fn parse_named(
    series: &serde_json::Map<String, serde_json::Value>,
    name: &str,
) -> Result<Vec<Sample>> {
    let raw = series
        .get(name)
        .ok_or_else(|| anyhow!("dump missing series `{name}`"))?;
    let range: PromQueryRange = serde_json::from_value(raw.clone())?;
    Ok(range.samples_summed())
}

/// Fail the process if any oracle fails. Prom checks are skipped when `prom` is None.
pub fn evaluate(
    oracles: &OracleConfig,
    events: &[LifecycleEventRecord],
    kill: Option<&KillWindow>,
    prom: Option<&PromSeriesSet>,
    checkpoint: &CheckpointSpec,
) -> Result<()> {
    let mut failures = Vec::new();
    if let Err(err) = unexpected_fatal(oracles, events, kill) {
        failures.push(err.to_string());
    }
    if let Some(prom) = prom {
        if let Err(err) = watermark_advancing(oracles, prom) {
            failures.push(err.to_string());
        }
        if let Err(err) = lag_p99(oracles, prom) {
            failures.push(err.to_string());
        }
        if let Err(err) = checkpoint_health(oracles, prom, kill, checkpoint) {
            failures.push(err.to_string());
        }
        if let Some(kill) = kill {
            if let Err(err) = restore_catchup(oracles, prom, kill) {
                failures.push(err.to_string());
            }
            if let Err(err) = restore_checkpoint_id(events, kill) {
                failures.push(err.to_string());
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("bench oracles failed:\n  - {}", failures.join("\n  - ")))
    }
}

fn unexpected_fatal(
    oracles: &OracleConfig,
    events: &[LifecycleEventRecord],
    kill: Option<&KillWindow>,
) -> Result<()> {
    if oracles.allow_unexpected_fatal {
        return Ok(());
    }
    for record in events {
        match &record.event {
            LifecycleEvent::PipelineFailed { detail } => {
                return Err(anyhow!("PipelineFailed: {detail}"));
            }
            LifecycleEvent::WorkerFailure {
                kind,
                worker_id,
                detail,
                ..
            } => {
                let expected_kill = kill
                    .map(|k| k.contains_seq(record.sequence))
                    .unwrap_or(false)
                    && matches!(
                        kind.as_str(),
                        "HeartbeatUnavailable"
                            | "StatePollFailure"
                            | "PodUnhealthy"
                            | "TransportDisconnect",
                    );
                if !expected_kill {
                    return Err(anyhow!(
                        "unexpected WorkerFailure worker={worker_id} kind={kind}: {detail}"
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn watermark_advancing(oracles: &OracleConfig, prom: &PromSeriesSet) -> Result<()> {
    let lag = &prom.watermark_lag_p99;
    let sent = &prom.records_sent_rate;
    if lag.len() < 2 {
        return Err(anyhow!(
            "WM advancing: no watermark_lag_p99 series (Prom scrape missing?)"
        ));
    }
    let stale = oracles.watermark_stale_while_sending.as_secs_f64();
    let mut run_start = 0usize;
    for i in 1..lag.len() {
        let same = (lag[i].value - lag[run_start].value).abs() < 1.0;
        if same {
            let dt = lag[i].ts - lag[run_start].ts;
            if dt >= stale {
                let sending = rate_at(sent, lag[i].ts).unwrap_or(0.0) > 0.1;
                if sending {
                    return Err(anyhow!(
                        "WM advancing: lag p99 stuck at {:.1}ms for {dt:.0}s while records_sent still increasing",
                        lag[i].value
                    ));
                }
            }
        } else {
            run_start = i;
        }
    }
    Ok(())
}

fn lag_p99(oracles: &OracleConfig, prom: &PromSeriesSet) -> Result<()> {
    let lag = &prom.watermark_lag_p99;
    if lag.is_empty() {
        return Err(anyhow!("lag p99: no watermark_lag_p99 series"));
    }
    let bound = oracles.lag_p99.as_secs_f64() * 1000.0;
    let sustain = oracles.lag_p99_sustain.as_secs_f64();
    let mut over_start: Option<f64> = None;
    for sample in lag {
        if sample.value > bound {
            let start = *over_start.get_or_insert(sample.ts);
            if sample.ts - start >= sustain {
                return Err(anyhow!(
                    "lag p99: {:.1}ms > {:.0}ms for {:.0}s (bound sustain {:.0}s)",
                    sample.value,
                    bound,
                    sample.ts - start,
                    sustain
                ));
            }
        } else {
            over_start = None;
        }
    }
    Ok(())
}

fn checkpoint_health(
    oracles: &OracleConfig,
    prom: &PromSeriesSet,
    kill: Option<&KillWindow>,
    checkpoint: &CheckpointSpec,
) -> Result<()> {
    let interval = resolved_checkpoint_interval(checkpoint);
    let timeout = resolved_checkpoint_timeout(checkpoint);
    let silence = interval * oracles.checkpoint_silence_intervals.max(1);
    let completed = &prom.checkpoint_completed;
    if completed.len() < 2 {
        return Err(anyhow!(
            "checkpoint health: no volga_checkpoint_completed series"
        ));
    }
    // Dump uses `increase(...[5m])`: activity is a positive sample, not a rising counter.
    let mut last_activity = completed[0].ts;
    for sample in completed {
        if sample.value > 0.5 {
            last_activity = sample.ts;
        } else if sample.ts - last_activity >= silence.as_secs_f64() {
            let in_kill = kill
                .map(|k| k.contains(sample.ts, timeout))
                .unwrap_or(false);
            if !in_kill {
                return Err(anyhow!(
                    "checkpoint health: no completed checkpoint for {:.0}s (limit {}×{:?})",
                    sample.ts - last_activity,
                    oracles.checkpoint_silence_intervals,
                    interval
                ));
            }
        }
    }
    if !oracles.allow_checkpoint_fail_outside_kill {
        for sample in &prom.checkpoint_failed {
            if sample.value > 0.5 {
                let in_kill = kill
                    .map(|k| k.contains(sample.ts, timeout))
                    .unwrap_or(false);
                if !in_kill {
                    return Err(anyhow!(
                        "checkpoint health: failed counter increased outside kill window"
                    ));
                }
            }
        }
    }
    Ok(())
}

fn restore_catchup(
    oracles: &OracleConfig,
    prom: &PromSeriesSet,
    kill: &KillWindow,
) -> Result<()> {
    let bound = oracles.lag_p99.as_secs_f64() * 1000.0;
    let deadline = kill.restore_at_unix + oracles.restore_catchup.as_secs_f64();
    let under = prom.watermark_lag_p99.iter().any(|s| {
        s.ts >= kill.restore_at_unix && s.ts <= deadline && s.value <= bound
    });
    if !under {
        return Err(anyhow!(
            "restore catch-up: lag p99 not ≤ {:.0}ms within {:?} after restore",
            bound,
            oracles.restore_catchup
        ));
    }
    Ok(())
}

fn restore_checkpoint_id(events: &[LifecycleEventRecord], kill: &KillWindow) -> Result<()> {
    let ok = events.iter().any(|record| {
        matches!(
            &record.event,
            LifecycleEvent::AttemptStarted {
                restore_checkpoint_id: Some(id),
                ..
            } if *id >= kill.after_checkpoint
        )
    });
    if ok {
        Ok(())
    } else {
        Err(anyhow!(
            "restore catch-up: no AttemptStarted restore_checkpoint_id >= {}",
            kill.after_checkpoint
        ))
    }
}

fn rate_at(samples: &[Sample], ts: f64) -> Option<f64> {
    samples
        .iter()
        .rev()
        .find(|s| s.ts <= ts + 2.5)
        .map(|s| s.value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::master::LifecycleEventRecord;

    fn cfg() -> OracleConfig {
        OracleConfig::default()
    }

    fn sample_range(start: f64, step: f64, values: &[f64]) -> Vec<Sample> {
        values
            .iter()
            .enumerate()
            .map(|(i, v)| Sample {
                ts: start + i as f64 * step,
                value: *v,
            })
            .collect()
    }

    fn kill_window() -> KillWindow {
        KillWindow {
            at_unix: 100.0,
            restore_at_unix: 110.0,
            after_checkpoint: 3,
            seq_lo: 1,
            seq_hi: 2,
        }
    }

    #[test]
    fn heartbeat_after_restore_fails() {
        let events = vec![LifecycleEventRecord {
            sequence: 9,
            event: LifecycleEvent::WorkerFailure {
                attempt_id: 1,
                worker_id: "w0".into(),
                kind: "HeartbeatUnavailable".into(),
                detail: "late".into(),
            },
        }];
        let kill = KillWindow {
            at_unix: 100.0,
            restore_at_unix: 110.0,
            after_checkpoint: 1,
            seq_lo: 1,
            seq_hi: 2,
        };
        assert!(unexpected_fatal(&cfg(), &events, Some(&kill)).is_err());
    }

    #[test]
    fn heartbeat_during_kill_ok() {
        let events = vec![LifecycleEventRecord {
            sequence: 1,
            event: LifecycleEvent::WorkerFailure {
                attempt_id: 0,
                worker_id: "w0".into(),
                kind: "HeartbeatUnavailable".into(),
                detail: "gone".into(),
            },
        }];
        let kill = KillWindow {
            at_unix: 100.0,
            restore_at_unix: 110.0,
            after_checkpoint: 1,
            seq_lo: 1,
            seq_hi: 2,
        };
        assert!(unexpected_fatal(&cfg(), &events, Some(&kill)).is_ok());
    }

    #[test]
    fn transport_disconnect_during_kill_ok() {
        let events = vec![LifecycleEventRecord {
            sequence: 1,
            event: LifecycleEvent::WorkerFailure {
                attempt_id: 0,
                worker_id: "w1".into(),
                kind: "TransportDisconnect".into(),
                detail: "stream_messages failed".into(),
            },
        }];
        let kill = KillWindow {
            at_unix: 100.0,
            restore_at_unix: 110.0,
            after_checkpoint: 1,
            seq_lo: 1,
            seq_hi: 2,
        };
        assert!(unexpected_fatal(&cfg(), &events, Some(&kill)).is_ok());
    }

    #[test]
    fn stale_lag_while_sending_fails() {
        let prom = PromSeriesSet {
            watermark_lag_p99: sample_range(0.0, 5.0, &[100.0; 8]),
            records_sent_rate: sample_range(0.0, 5.0, &[10.0; 8]),
            ..Default::default()
        };
        assert!(watermark_advancing(&cfg(), &prom).is_err());
    }

    #[test]
    fn stale_lag_while_not_sending_passes() {
        let prom = PromSeriesSet {
            watermark_lag_p99: sample_range(0.0, 5.0, &[100.0; 8]),
            records_sent_rate: sample_range(0.0, 5.0, &[0.0; 8]),
            ..Default::default()
        };
        assert!(watermark_advancing(&cfg(), &prom).is_ok());
    }

    #[test]
    fn lag_p99_sustain_fails() {
        let mut values = vec![1000.0; 5];
        values.extend(std::iter::repeat(20_000.0).take(14));
        let prom = PromSeriesSet {
            watermark_lag_p99: sample_range(0.0, 5.0, &values),
            ..Default::default()
        };
        assert!(lag_p99(&cfg(), &prom).is_err());
    }

    #[test]
    fn checkpoint_silence_fails() {
        let prom = PromSeriesSet {
            checkpoint_completed: vec![
                Sample { ts: 0.0, value: 0.0 },
                Sample { ts: 100.0, value: 0.0 },
            ],
            ..Default::default()
        };
        assert!(checkpoint_health(&cfg(), &prom, None, &CheckpointSpec::default()).is_err());
    }

    #[test]
    fn checkpoint_failed_outside_kill_fails() {
        let prom = PromSeriesSet {
            checkpoint_completed: sample_range(0.0, 5.0, &[1.0; 4]),
            checkpoint_failed: vec![
                Sample { ts: 0.0, value: 0.0 },
                Sample { ts: 20.0, value: 1.0 },
            ],
            ..Default::default()
        };
        assert!(checkpoint_health(&cfg(), &prom, None, &CheckpointSpec::default()).is_err());
    }

    #[test]
    fn restore_checkpoint_id_too_low_fails() {
        let kill = kill_window();
        let events = vec![LifecycleEventRecord {
            sequence: 1,
            event: LifecycleEvent::AttemptStarted {
                attempt_id: 1,
                restore_checkpoint_id: Some(2),
            },
        }];
        assert!(restore_checkpoint_id(&events, &kill).is_err());
    }

    #[test]
    fn restore_lag_never_under_bound_fails() {
        let kill = kill_window();
        let prom = PromSeriesSet {
            watermark_lag_p99: vec![Sample {
                ts: 70.0,
                value: 50_000.0,
            }],
            ..Default::default()
        };
        assert!(restore_catchup(&cfg(), &prom, &kill).is_err());
    }

    #[test]
    fn evaluate_without_prom_fails_on_worker_failure() {
        let events = vec![LifecycleEventRecord {
            sequence: 1,
            event: LifecycleEvent::WorkerFailure {
                attempt_id: 0,
                worker_id: "w0".into(),
                kind: "WorkerPanic".into(),
                detail: "boom".into(),
            },
        }];
        assert!(evaluate(&cfg(), &events, None, None, &CheckpointSpec::default()).is_err());
    }
}
