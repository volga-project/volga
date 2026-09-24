//! Per-call statement/row/byte notes. The counter lives on the tokio task so a
//! commit and a maintain tick on the same client do not mix.

use std::cell::Cell;
use std::future::Future;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::task_local;

use crate::runtime::metrics::MetricsLabels;
use crate::runtime::operators::window::metrics;

pub(super) const MAINTAIN_INTERVAL: Duration = Duration::from_secs(10);

pub(super) fn maintain_interval_elapsed(last: Option<Instant>, now: Instant) -> bool {
    last.is_none_or(|t| now.saturating_duration_since(t) >= MAINTAIN_INTERVAL)
}

#[derive(Clone, Copy, Default)]
struct CallCounts {
    stmts: u64,
    rows: u64,
    bytes: u64,
}

task_local! {
    static CALL: Cell<CallCounts>;
}

fn bump(f: impl FnOnce(&mut CallCounts)) {
    let _ = CALL.try_with(|cell| {
        let mut counts = cell.get();
        f(&mut counts);
        cell.set(counts);
    });
}

pub(super) fn note_stmt() {
    bump(|counts| counts.stmts = counts.stmts.saturating_add(1));
}

pub(super) fn note_rows(n: u64) {
    if n == 0 {
        return;
    }
    bump(|counts| counts.rows = counts.rows.saturating_add(n));
}

pub(super) fn note_bytes(n: u64) {
    if n == 0 {
        return;
    }
    bump(|counts| counts.bytes = counts.bytes.saturating_add(n));
}

#[derive(Clone)]
pub(super) struct CallMeter {
    pub task_id: String,
    pub labels: Option<MetricsLabels>,
}

pub(super) async fn observe<T>(
    meter: &CallMeter,
    op: &'static str,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    let Some(labels) = meter.labels.clone() else {
        return fut.await;
    };
    let started = Instant::now();
    let (result, counts) = CALL
        .scope(Cell::new(CallCounts::default()), async {
            let result = fut.await;
            let counts = CALL.with(|cell| cell.get());
            (result, counts)
        })
        .await;
    metrics::record_scylla_call(
        &meter.task_id,
        &labels,
        op,
        result.is_ok(),
        counts.stmts,
        counts.rows,
        counts.bytes,
        started.elapsed().as_secs_f64() * 1000.0,
    );
    result
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::runtime::metrics::{collect_task_metric_values, MetricsLabels};
    use crate::runtime::operators::window::metrics::{
        METRIC_WO_SCYLLA_CALLS, METRIC_WO_SCYLLA_FAILURES, METRIC_WO_SCYLLA_ROWS,
        METRIC_WO_SCYLLA_STATEMENTS,
    };

    #[test]
    fn maintain_skips_a_second_tick_inside_ten_seconds() {
        let t0 = Instant::now();
        assert!(maintain_interval_elapsed(None, t0));
        assert!(!maintain_interval_elapsed(
            Some(t0),
            t0 + Duration::from_secs(9)
        ));
        assert!(maintain_interval_elapsed(
            Some(t0),
            t0 + Duration::from_secs(10)
        ));
    }

    #[tokio::test]
    async fn failed_call_increments_failures_not_calls() {
        let _ = std::panic::catch_unwind(crate::runtime::metrics::init_metrics);
        let labels = MetricsLabels {
            pipeline_id: "p".to_string(),
            worker_id: "w".to_string(),
        };
        let meter = CallMeter {
            task_id: "wo-fail".to_string(),
            labels: Some(labels.clone()),
        };
        let err = observe(&meter, "load_raw", async {
            note_stmt();
            note_rows(4);
            Err::<(), _>(anyhow::anyhow!("boom"))
        })
        .await;
        assert!(err.is_err());
        let values = collect_task_metric_values(
            "wo-fail",
            Some(&labels),
            &[
                METRIC_WO_SCYLLA_CALLS,
                METRIC_WO_SCYLLA_FAILURES,
                METRIC_WO_SCYLLA_STATEMENTS,
                METRIC_WO_SCYLLA_ROWS,
            ],
        );
        assert_eq!(
            values
                .counters
                .get(METRIC_WO_SCYLLA_CALLS)
                .copied()
                .unwrap_or(0),
            0
        );
        assert_eq!(
            values
                .counters
                .get(METRIC_WO_SCYLLA_FAILURES)
                .copied()
                .unwrap_or(0),
            1
        );
        assert_eq!(
            values
                .counters
                .get(METRIC_WO_SCYLLA_STATEMENTS)
                .copied()
                .unwrap_or(0),
            1
        );
        assert_eq!(
            values
                .counters
                .get(METRIC_WO_SCYLLA_ROWS)
                .copied()
                .unwrap_or(0),
            4
        );

        let ok_meter = CallMeter {
            task_id: "wo-ok".to_string(),
            labels: Some(labels.clone()),
        };
        observe(&ok_meter, "load_tiles", async {
            Ok::<_, anyhow::Error>(())
        })
        .await
        .unwrap();
        let ok = collect_task_metric_values(
            "wo-ok",
            Some(&labels),
            &[METRIC_WO_SCYLLA_CALLS, METRIC_WO_SCYLLA_FAILURES],
        );
        assert_eq!(
            ok.counters
                .get(METRIC_WO_SCYLLA_CALLS)
                .copied()
                .unwrap_or(0),
            1
        );
        assert_eq!(
            ok.counters
                .get(METRIC_WO_SCYLLA_FAILURES)
                .copied()
                .unwrap_or(0),
            0
        );
    }
}
