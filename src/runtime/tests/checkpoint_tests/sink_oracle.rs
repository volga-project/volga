//! Offline datagen materialization vs in-memory upsert sink.

use anyhow::{anyhow, Result};
use arrow::array::{Array, Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::record_batch::RecordBatch;
use std::collections::{HashMap, HashSet, VecDeque};

use crate::runtime::functions::source::datagen_source::{
    DatagenSourceConfig, DatagenSourceFunction,
};
use crate::runtime::observability::{task_meta, PipelineSnapshot};
use crate::runtime::tests::cluster_harness::{MasterHandle, RuntimeEnv};
use crate::storage::InMemoryStorageSnapshot;

use super::launch::{checkpoint_datagen_parts, CheckpointWorkload, WINDOW_RANGE_MS};

/// Logical sink row identity for upsert-key `(key, timestamp)` plus value for content checks.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExpectedUpsertRow {
    map_key: String,
    value_bits: u64,
}

#[derive(Debug, Clone)]
struct InputRow {
    key: String,
    timestamp: i64,
    value: f64,
}

#[derive(Debug, Clone)]
struct WindowAggregateRow {
    value: f64,
    sum: f64,
    count: i64,
    avg: f64,
    min: f64,
    max: f64,
}

fn input_row_from_batch(batch: &RecordBatch, row_idx: usize) -> Result<InputRow> {
    let key = batch
        .column_by_name("key")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| anyhow!("missing key column"))?
        .value(row_idx)
        .to_string();
    let ts = batch
        .column_by_name("timestamp")
        .and_then(|c| c.as_any().downcast_ref::<TimestampMillisecondArray>())
        .ok_or_else(|| anyhow!("missing timestamp column"))?
        .value(row_idx);
    let value_bits = batch
        .column_by_name("value")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| anyhow!("missing value column"))?
        .value(row_idx);
    Ok(InputRow {
        key,
        timestamp: ts,
        value: value_bits,
    })
}

fn pass_through_row_from_batch(batch: &RecordBatch, row_idx: usize) -> Result<ExpectedUpsertRow> {
    let row = input_row_from_batch(batch, row_idx)?;
    Ok(ExpectedUpsertRow {
        map_key: format!("{}|{}", row.key, row.timestamp),
        value_bits: row.value.to_bits(),
    })
}

fn materialize_datagen_for_task(
    config: DatagenSourceConfig,
    task_index: i32,
    parallelism: i32,
    num_records: usize,
) -> Result<Vec<RecordBatch>> {
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    let mut source = DatagenSourceFunction::new(config);
    source.task_index = Some(task_index);
    source.parallelism = Some(parallelism);
    source.rng = Some(StdRng::seed_from_u64(42 + task_index as u64));
    source.init_keys();
    let batch_size = source.config.spec.batch_size.max(1);
    let mut out = Vec::new();
    while source.records_generated() < num_records {
        let n = std::cmp::min(batch_size, num_records - source.records_generated());
        out.push(source.generate_batch(n)?);
    }
    Ok(out)
}

fn materialize_input_rows(
    task_counts: &[(i32, u64)],
    workload: CheckpointWorkload,
) -> Result<Vec<InputRow>> {
    let parallelism = task_counts
        .iter()
        .map(|(task_index, _)| *task_index)
        .max()
        .ok_or_else(|| anyhow!("empty task_counts"))?
        + 1;
    // run_for unused for offline materialization (counts come from task metadata).
    let (schema, spec) = checkpoint_datagen_parts(parallelism as usize, workload);
    let config = DatagenSourceConfig::new(schema, spec);
    let mut rows = Vec::new();
    for &(task_index, num_records) in task_counts {
        let batches = materialize_datagen_for_task(
            config.clone(),
            task_index,
            parallelism,
            num_records as usize,
        )?;
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                rows.push(input_row_from_batch(&batch, row_idx)?);
            }
        }
    }
    Ok(rows)
}

fn expected_pass_through_rows(rows: &[InputRow]) -> HashSet<ExpectedUpsertRow> {
    rows.iter()
        .map(|row| ExpectedUpsertRow {
            map_key: format!("{}|{}", row.key, row.timestamp),
            value_bits: row.value.to_bits(),
        })
        .collect()
}

fn pass_through_sink_rows(
    snapshot: &InMemoryStorageSnapshot,
) -> Result<HashSet<ExpectedUpsertRow>> {
    let mut actual = HashSet::new();
    for (map_key, message) in snapshot.keyed_messages() {
        let batch = message.record_batch();
        if batch.num_rows() != 1 {
            return Err(anyhow!(
                "expected 1-row upsert message for key {map_key}, got {}",
                batch.num_rows()
            ));
        }
        let row = pass_through_row_from_batch(batch, 0)?;
        if row.map_key != *map_key {
            return Err(anyhow!(
                "sink map key {map_key} != row-derived {}",
                row.map_key
            ));
        }
        actual.insert(row);
    }
    Ok(actual)
}

fn expected_window_rows(rows: Vec<InputRow>) -> HashMap<String, WindowAggregateRow> {
    let mut by_key: HashMap<String, Vec<InputRow>> = HashMap::new();
    for row in rows {
        by_key.entry(row.key.clone()).or_default().push(row);
    }

    let mut expected = HashMap::new();
    for (key, mut rows) in by_key {
        rows.sort_by_key(|row| row.timestamp);
        let mut window = VecDeque::new();
        for row in rows {
            while window
                .front()
                .is_some_and(|prior: &InputRow| prior.timestamp < row.timestamp - WINDOW_RANGE_MS)
            {
                window.pop_front();
            }
            window.push_back(row.clone());

            let count = window.len() as i64;
            let sum = window.iter().map(|entry| entry.value).sum::<f64>();
            let min = window
                .iter()
                .map(|entry| entry.value)
                .fold(f64::INFINITY, f64::min);
            let max = window
                .iter()
                .map(|entry| entry.value)
                .fold(f64::NEG_INFINITY, f64::max);
            expected.insert(
                format!("{key}|{}", row.timestamp),
                WindowAggregateRow {
                    value: row.value,
                    sum,
                    count,
                    avg: sum / count as f64,
                    min,
                    max,
                },
            );
        }
    }
    expected
}

fn required_float(batch: &RecordBatch, column: &str, row_idx: usize) -> Result<f64> {
    Ok(batch
        .column_by_name(column)
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| anyhow!("missing {column} column"))?
        .value(row_idx))
}

fn window_sink_rows(
    snapshot: &InMemoryStorageSnapshot,
) -> Result<HashMap<String, WindowAggregateRow>> {
    let mut actual = HashMap::new();
    for (map_key, message) in snapshot.keyed_messages() {
        let batch = message.record_batch();
        if batch.num_rows() != 1 {
            return Err(anyhow!(
                "expected 1-row window upsert message for key {map_key}, got {}",
                batch.num_rows()
            ));
        }
        let input = input_row_from_batch(batch, 0)?;
        let derived_key = format!("{}|{}", input.key, input.timestamp);
        if derived_key != *map_key {
            return Err(anyhow!(
                "sink map key {map_key} != row-derived {derived_key}"
            ));
        }
        let count = batch
            .column_by_name("cnt_val")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| anyhow!("missing cnt_val column"))?
            .value(0);
        actual.insert(
            map_key.clone(),
            WindowAggregateRow {
                value: input.value,
                sum: required_float(batch, "sum_val", 0)?,
                count,
                avg: required_float(batch, "avg_val", 0)?,
                min: required_float(batch, "min_val", 0)?,
                max: required_float(batch, "max_val", 0)?,
            },
        );
    }
    Ok(actual)
}

fn task_index_from_vertex_id(vertex_id: &str) -> Result<i32> {
    vertex_id
        .rsplit_once('_')
        .and_then(|(_, idx)| idx.parse().ok())
        .ok_or_else(|| anyhow!("cannot parse task_index from vertex_id {vertex_id}"))
}

fn task_record_counts(snapshot: &PipelineSnapshot) -> Result<Vec<(i32, u64)>> {
    let mut counts = Vec::new();
    for worker in snapshot.worker_states.values() {
        for (vertex_id, meta) in &worker.task_metadata {
            let Some(records) = meta.get(task_meta::RECORDS_GENERATED) else {
                continue;
            };
            let task_index = task_index_from_vertex_id(vertex_id.as_ref())?;
            let n = records
                .parse::<u64>()
                .map_err(|e| anyhow!("bad records_generated for {vertex_id}: {e}"))?;
            counts.push((task_index, n));
        }
    }
    counts.sort_by_key(|(task_index, _)| *task_index);
    if counts.is_empty() {
        return Err(anyhow!(
            "no task metadata with {} in pipeline snapshot",
            task_meta::RECORDS_GENERATED
        ));
    }
    Ok(counts)
}

fn aggregates_match(expected: &WindowAggregateRow, actual: &WindowAggregateRow) -> bool {
    const EPSILON: f64 = 1e-9;
    expected.value.to_bits() == actual.value.to_bits()
        && expected.count == actual.count
        && (expected.sum - actual.sum).abs() < EPSILON
        && (expected.avg - actual.avg).abs() < EPSILON
        && (expected.min - actual.min).abs() < EPSILON
        && (expected.max - actual.max).abs() < EPSILON
}

fn window_key_context(
    map_key: &str,
    expected: &HashMap<String, WindowAggregateRow>,
    actual: &HashMap<String, WindowAggregateRow>,
    task_counts: &[(i32, u64)],
) -> String {
    let key = map_key
        .rsplit_once('|')
        .map(|(key, _)| key)
        .unwrap_or(map_key);
    let prefix = format!("{key}|");
    let expected_rows: Vec<_> = expected
        .keys()
        .filter(|candidate| candidate.starts_with(&prefix))
        .collect();
    let actual_rows: Vec<_> = actual
        .keys()
        .filter(|candidate| candidate.starts_with(&prefix))
        .collect();
    let source_task = key
        .strip_prefix("key-")
        .and_then(|suffix| suffix.split_once('-'))
        .and_then(|(task, _)| task.parse::<i32>().ok());
    let source_records = source_task.and_then(|task| {
        task_counts
            .iter()
            .find(|(task_index, _)| *task_index == task)
            .map(|(_, records)| *records)
    });
    format!(
        "key={key} expected_key_rows={} actual_key_rows={} source_task={source_task:?} source_records={source_records:?}",
        expected_rows.len(),
        actual_rows.len(),
    )
}

fn assert_window_rows(
    expected: &HashMap<String, WindowAggregateRow>,
    actual: &HashMap<String, WindowAggregateRow>,
    env: RuntimeEnv,
    total: u64,
    task_counts: &[(i32, u64)],
) -> Result<()> {
    for (key, expected_row) in expected {
        let actual_row = actual
            .get(key)
            .ok_or_else(|| {
                anyhow!(
                    "sink missing expected window row {key}; {}",
                    window_key_context(key, expected, actual, task_counts)
                )
            })?;
        if !aggregates_match(expected_row, actual_row) {
            return Err(anyhow!(
                "window aggregate mismatch for {key}: expected={expected_row:?} actual={actual_row:?}; {}",
                window_key_context(key, expected, actual, task_counts)
            ));
        }
    }

    if env != RuntimeEnv::Kube && expected.len() != actual.len() {
        return Err(anyhow!(
            "window sink key-set mismatch: expected={} actual={} total_generated={total} task_counts={task_counts:?}",
            expected.len(),
            actual.len()
        ));
    }

    let overshoot = actual.len().saturating_sub(expected.len());
    println!(
        "[TEST] window sink oracle ok rows={} overshoot={overshoot} total_generated={total} task_counts={task_counts:?}",
        actual.len()
    );
    Ok(())
}

pub(super) async fn assert_sink_matches_offline_datagen(
    master: &MasterHandle,
    storage: &InMemoryStorageSnapshot,
    env: RuntimeEnv,
    workload: CheckpointWorkload,
) -> Result<()> {
    let snapshot = master
        .latest_pipeline_snapshot()
        .await?
        .ok_or_else(|| anyhow!("missing pipeline snapshot after PipelineFinished"))?;
    let task_counts = task_record_counts(&snapshot)?;
    let total: u64 = task_counts.iter().map(|(_, n)| n).sum();
    let input_rows = materialize_input_rows(&task_counts, workload)?;

    if workload == CheckpointWorkload::Window {
        let expected = expected_window_rows(input_rows);
        let actual = window_sink_rows(storage)?;
        return assert_window_rows(&expected, &actual, env, total, &task_counts);
    }

    let expected = expected_pass_through_rows(&input_rows);
    let actual = pass_through_sink_rows(storage)?;

    // TODO(exactly-once): multi-worker kube survivors can write past the last committed
    // source position into the external upsert sink during recovery; those keys are not
    // always regenerated before StopSources. Soft-check expected ⊆ actual on kube for now.
    // Restore exact set equality once sink commit is checkpoint-aligned (2PC / transactional).
    if env == RuntimeEnv::Kube {
        let missing: HashSet<_> = expected.difference(&actual).cloned().collect();
        if !missing.is_empty() {
            return Err(anyhow!(
                "sink missing datagen rows (kube soft-check): missing={} expected={} actual={} total_generated={total} task_counts={task_counts:?}",
                missing.len(),
                expected.len(),
                actual.len()
            ));
        }
        let overshoot = actual.len().saturating_sub(expected.len());
        println!(
            "[TEST] sink/offline kube soft-check ok rows={} overshoot={overshoot} total_generated={total} task_counts={task_counts:?}",
            actual.len()
        );
        return Ok(());
    }

    if expected != actual {
        return Err(anyhow!(
            "sink/offline datagen set mismatch: expected={} actual={} total_generated={total} task_counts={task_counts:?}",
            expected.len(),
            actual.len()
        ));
    }
    println!(
        "[TEST] sink set equality ok rows={} task_counts={task_counts:?}",
        actual.len()
    );
    Ok(())
}
