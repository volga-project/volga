//! Offline datagen materialization vs in-memory upsert sink.

use anyhow::{anyhow, Result};
use arrow::array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;

use crate::runtime::functions::source::datagen_source::{DatagenSourceConfig, DatagenSourceFunction};
use crate::runtime::observability::{task_meta, PipelineSnapshot};
use crate::runtime::tests::cluster_harness::MasterHandle;
use crate::storage::InMemoryStorageSnapshot;

use super::launch::checkpoint_datagen_parts;

/// Logical sink row identity for upsert-key `(key, timestamp)` plus value for content checks.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExpectedUpsertRow {
    map_key: String,
    value_bits: u64,
}

fn row_from_batch(batch: &RecordBatch, row_idx: usize) -> Result<ExpectedUpsertRow> {
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
        .value(row_idx)
        .to_bits();
    Ok(ExpectedUpsertRow {
        map_key: format!("{key}|{ts}"),
        value_bits,
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

fn expected_rows_offline(task_counts: &[(i32, u64)]) -> Result<HashSet<ExpectedUpsertRow>> {
    let parallelism = task_counts
        .iter()
        .map(|(task_index, _)| *task_index)
        .max()
        .ok_or_else(|| anyhow!("empty task_counts"))?
        + 1;
    // run_for unused for offline materialization (counts come from task metadata).
    let (schema, spec) = checkpoint_datagen_parts(parallelism as usize);
    let config = DatagenSourceConfig::new(schema, spec);
    let mut expected = HashSet::new();
    for &(task_index, num_records) in task_counts {
        let batches = materialize_datagen_for_task(
            config.clone(),
            task_index,
            parallelism,
            num_records as usize,
        )?;
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                expected.insert(row_from_batch(&batch, row_idx)?);
            }
        }
    }
    Ok(expected)
}

fn sink_rows(snapshot: &InMemoryStorageSnapshot) -> Result<HashSet<ExpectedUpsertRow>> {
    let mut actual = HashSet::new();
    for (map_key, message) in snapshot.keyed_messages() {
        let batch = message.record_batch();
        if batch.num_rows() != 1 {
            return Err(anyhow!(
                "expected 1-row upsert message for key {map_key}, got {}",
                batch.num_rows()
            ));
        }
        let row = row_from_batch(batch, 0)?;
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

pub(super) async fn assert_sink_matches_offline_datagen(
    master: &MasterHandle,
    storage: &InMemoryStorageSnapshot,
) -> Result<()> {
    let snapshot = master
        .latest_pipeline_snapshot()
        .await?
        .ok_or_else(|| anyhow!("missing pipeline snapshot after PipelineFinished"))?;
    let task_counts = task_record_counts(&snapshot)?;
    let total: u64 = task_counts.iter().map(|(_, n)| n).sum();
    let expected = expected_rows_offline(&task_counts)?;
    let actual = sink_rows(storage)?;
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
