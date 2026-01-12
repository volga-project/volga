use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

use anyhow::Result;
use datafusion::common::ScalarValue;

use crate::api::pipeline_context::{ExecutionMode, PipelineContextBuilder};
use crate::common::test_utils::gen_unique_grpc_port;
use crate::runtime::functions::source::datagen_source::{DatagenSourceConfig, FieldGenerator};
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::stream_task::StreamTaskStatus;
use crate::runtime::worker::{Worker, WorkerConfig};
use crate::storage::{InMemoryStorageClient, InMemoryStorageServer};
use crate::transport::transport_backend_actor::TransportBackendType;
use crate::runtime::tests::test_utils::{
    create_window_input_schema, wait_for_status, window_rows_from_messages, WindowOutputRow,
};

fn parse_task_index_from_key(key: &str) -> Option<i32> {
    // Keys come from datagen as "key-{task_index}-{key_id}" when using FieldGenerator::Key.
    let mut parts = key.split('-');
    let prefix = parts.next()?;
    if prefix != "key" {
        return None;
    }
    let task = parts.next()?.parse::<i32>().ok()?;
    let _key_id = parts.next()?;
    Some(task)
}

async fn run_watermark_window_pipeline(
    parallelism: usize,
    num_unique_keys: usize,
    total_records: usize,
    out_of_orderness_ms: u64,
    lateness_ms: i64,
    step_ms: i64,
) -> Result<Vec<WindowOutputRow>> {
    crate::runtime::stream_task::MESSAGE_TRACE_ENABLED.store(
        true,
        std::sync::atomic::Ordering::Relaxed,
    );

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_server_addr).await?;

    let schema = create_window_input_schema();
    let sql = "SELECT timestamp, value, partition_key, \
               SUM(value) OVER w as sum_val, \
               COUNT(value) OVER w as cnt_val, \
               AVG(value) OVER w as avg_val \
               FROM datagen_source \
               WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp \
               RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW)";

    let start_ms: i64 = 1000;
    // let step_ms: i64 = 1;

    let mut fields = HashMap::new();
    fields.insert(
        "timestamp".to_string(),
        FieldGenerator::IncrementalTimestamp {
            start_ms,
            step_ms,
        },
    );
    fields.insert(
        "partition_key".to_string(),
        FieldGenerator::Key {
            num_unique: num_unique_keys,
        },
    );
    fields.insert(
        "value".to_string(),
        FieldGenerator::Values {
            values: vec![ScalarValue::Float64(Some(1.0)), ScalarValue::Float64(Some(2.0))],
        },
    );

    let mut datagen_cfg = DatagenSourceConfig::new(
        schema.clone(),
        None,
        Some(total_records),
        None,
        256,
        fields,
    );
    datagen_cfg.set_replayable(true);

    let context = PipelineContextBuilder::new()
        .with_parallelism(parallelism)
        .with_window_watermark_out_of_orderness_ms(out_of_orderness_ms)
        .with_source(
            "datagen_source".to_string(),
            crate::runtime::operators::source::source_operator::SourceConfig::DatagenSourceConfig(
                datagen_cfg,
            ),
            schema.clone(),
        )
        .with_sink(
            crate::runtime::operators::sink::sink_operator::SinkConfig::InMemoryStorageGrpcSinkConfig(
                format!("http://{}", storage_server_addr),
            ),
        )
        .with_execution_mode(ExecutionMode::Streaming)
        .sql(sql)
        .build();

    let logical_graph = context.get_logical_graph().unwrap().clone();
    let mut exec_graph = logical_graph.to_execution_graph();

    if lateness_ms < 0 {
        panic!("lateness_ms must be >= 0, got {}", lateness_ms);
    }
    // Set lateness directly on the execution graph for this test (PipelineContext/LogicalGraph stay unaware).
    let vertex_ids_snapshot: Vec<_> = exec_graph.get_vertices().keys().cloned().collect();
    for vid in vertex_ids_snapshot {
        if let Some(v) = exec_graph.get_vertex_mut(vid.as_ref()) {
            if let OperatorConfig::WindowConfig(ref mut cfg) = v.operator_config {
                cfg.lateness = Some(lateness_ms);
            }
        }
    }

    exec_graph.update_channels_with_node_mapping(None);
    let vertex_ids = exec_graph.get_vertices().keys().cloned().collect();

    // Sanity: ensure we actually built a window operator (planner-side watermark placement should attach to it).
    assert!(
        exec_graph
            .get_vertices()
            .values()
            .any(|v| matches!(v.operator_config, OperatorConfig::WindowConfig(_))),
        "expected execution graph to contain a Window operator"
    );

    let mut worker = Worker::new(WorkerConfig::new(
        "wm-e2e-worker".to_string(),
        exec_graph,
        vertex_ids,
        2,
        TransportBackendType::InMemory,
    ));

    worker.start().await;
    wait_for_status(&worker, StreamTaskStatus::Opened, Duration::from_secs(10)).await;
    worker.signal_tasks_run().await;
    wait_for_status(&worker, StreamTaskStatus::Finished, Duration::from_secs(60)).await;
    worker.signal_tasks_close().await;
    wait_for_status(&worker, StreamTaskStatus::Closed, Duration::from_secs(10)).await;
    worker.close().await;

    let mut storage_client =
        InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await?;
    let messages = storage_client.drain_vector().await?;
    let rows = window_rows_from_messages(messages);

    storage_server.stop().await;
    Ok(rows)
}

#[tokio::test]
async fn test_watermark_streaming_window_e2e_serial_exact_correctness() -> Result<()> {
    let parallelism: usize = 1;
    let total_records: usize = 3000;
    let start_ms: i64 = 1000;
    let window_len_ms: i64 = 2000;
    let window_max_rows: usize = (window_len_ms as usize) + 1; // inclusive
    let step_ms = 1;

    let mut rows = run_watermark_window_pipeline(
        parallelism,
        1,    // one key total
        total_records,
        0,    // no disorder
        250,  // lateness delay (ms) - still expect exact final results after terminal watermark
        step_ms,
    )
    .await?;

    assert_eq!(
        rows.len(),
        total_records,
        "expected exactly one output row per generated input row"
    );

    // 1) No duplicates by (timestamp, key)
    let mut counts: BTreeMap<(i64, String), usize> = BTreeMap::new();
    for r in &rows {
        let k = (r.timestamp_ms, r.partition_key.clone());
        *counts.entry(k).or_insert(0) += 1;
    }
    let dup = counts.iter().find(|(_, c)| **c != 1);
    assert!(
        dup.is_none(),
        "expected no duplicates; found {:?}",
        dup
    );
    assert_eq!(
        counts.len(),
        total_records,
        "expected exactly one unique (timestamp, key) per record"
    );

    // 2) Per-key timestamp semantics: strictly increasing and within expected range for that task.
    rows.sort_by(|a, b| (a.partition_key.as_str(), a.timestamp_ms).cmp(&(b.partition_key.as_str(), b.timestamp_ms)));
    let mut per_key: HashMap<String, Vec<i64>> = HashMap::new();
    for r in &rows {
        per_key.entry(r.partition_key.clone()).or_default().push(r.timestamp_ms);
    }
    assert_eq!(per_key.len(), 1, "expected a single key");
    for (k, ts_list) in &per_key {
        let _task_idx = parse_task_index_from_key(k).expect("task index");
        let expected = total_records;
        assert_eq!(
            ts_list.len(),
            expected,
            "unexpected row count for key={}",
            k
        );
        for w in ts_list.windows(2) {
            assert!(w[0] < w[1], "timestamps not strictly increasing for key={}", k);
        }
        assert_eq!(ts_list[0], start_ms, "unexpected start timestamp for key={}", k);
        assert_eq!(
            *ts_list.last().unwrap(),
            start_ms + (expected as i64) - 1,
            "unexpected end timestamp for key={}",
            k
        );
    }

    // 3) Window aggregate semantics per key (sliding range window over last 2000ms).
    // Since timestamps increment by 1ms, this is equivalent to "last 2001 rows including current".
    let mut expected_state: HashMap<String, (VecDeque<f64>, f64)> = HashMap::new(); // (window values, sum)
    for r in &rows {
        let entry = expected_state
            .entry(r.partition_key.clone())
            .or_insert_with(|| (VecDeque::new(), 0.0));
        entry.0.push_back(r.value);
        entry.1 += r.value;
        if entry.0.len() > window_max_rows {
            if let Some(v) = entry.0.pop_front() {
                entry.1 -= v;
            }
        }
        let exp_cnt = entry.0.len() as i64;
        let exp_sum = entry.1;
        let exp_avg = exp_sum / (exp_cnt as f64);

        assert_eq!(r.cnt, exp_cnt, "cnt mismatch for key={} ts={}", r.partition_key, r.timestamp_ms);
        assert!(
            (r.sum - exp_sum).abs() < 1e-9,
            "sum mismatch for key={} ts={} got={} expected={}",
            r.partition_key,
            r.timestamp_ms,
            r.sum,
            exp_sum
        );
        assert!(
            (r.avg - exp_avg).abs() < 1e-9,
            "avg mismatch for key={} ts={} got={} expected={}",
            r.partition_key,
            r.timestamp_ms,
            r.avg,
            exp_avg
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_watermark_streaming_window_e2e_parallel_disorder_bounded_loss() -> Result<()> {
    let parallelism: usize = 4;
    let total_records: usize = 12_000;

    // The goal here is not to demand a global ordering guarantee; it’s to ensure:
    // - no duplicates
    // - outputs are internally consistent
    // - loss (late drops) is bounded and visible via diagnostics
    // KeyBy can reorder across upstream partitions; with our deterministic datagen (step=1ms),
    // the worst-case skew is on the order of "records per source task".
    // let records_per_task = total_records / parallelism;
    // let out_of_orderness_ms: u64 = (records_per_task as u64) + 200; // small headroom
    // let lateness_ms: i64 = 1_000;

    let out_of_orderness_ms = 100;
    let lateness_ms = 100;
    let step_ms = 1;

    let rows = run_watermark_window_pipeline(
        parallelism,
        parallelism, // keys per task -> stable distribution but still parallel
        total_records,
        out_of_orderness_ms,
        lateness_ms,
        step_ms,
    )
    .await?;

    // 1) No duplicates by (timestamp, key)
    let mut counts: BTreeMap<(i64, String), usize> = BTreeMap::new();
    for r in &rows {
        let k = (r.timestamp_ms, r.partition_key.clone());
        *counts.entry(k).or_insert(0) += 1;
    }
    let dup = counts.iter().find(|(_, c)| **c != 1);
    assert!(
        dup.is_none(),
        "expected no duplicates; found {:?}",
        dup
    );

    // 2) Internal consistency
    for r in &rows {
        assert!(r.cnt >= 1, "cnt must be >= 1: {:?}", r);
        let exp_avg = r.sum / (r.cnt as f64);
        assert!(
            (r.avg - exp_avg).abs() < 1e-9,
            "avg mismatch row={:?}",
            r
        );
    }

    // 3) Diagnostics + bounded loss
    let missed = total_records.saturating_sub(rows.len());
    println!(
        "watermark_parallel: produced={} expected={} missed={} (ooo_ms={} lateness_ms={})",
        rows.len(),
        total_records,
        missed,
        out_of_orderness_ms,
        lateness_ms
    );
    let allowed_missed = total_records / 20; // 5%
    assert!(
        missed <= allowed_missed,
        "missed too many rows: missed={} allowed={} (produced={} expected={})",
        missed,
        allowed_missed,
        rows.len(),
        total_records
    );

    Ok(())
}

